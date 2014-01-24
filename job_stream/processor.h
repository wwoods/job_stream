#ifndef JOB_STREAM_PROCESSOR_H_
#define JOB_STREAM_PROCESSOR_H_

#include "job.h"
#include "message.h"
#include "module.h"
#include "yaml.h"

#include <boost/mpi.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <deque>
#include <functional>
#include <memory>
#include <queue>

namespace job_stream {
namespace processor {

/* Debug flag when testing library; 
  1 = basic messages (min output),
  2 = very, very verbose */
extern const int JOB_STREAM_DEBUG;


/** Generic thread-safe queue class. */
template<typename T>
class ThreadSafeQueue {
public:
    void push(const T& value) {
        boost::mutex::scoped_lock lock(this->mutex);
        this->queue.push_back(value);
    }

    bool pop(T& value) {
        boost::mutex::scoped_lock lock(this->mutex);
        if (this->queue.empty()) {
            return false;
        }
        value = this->queue.front();
        this->queue.pop_front();
        return true;
    }

    size_t size() {
        boost::mutex::scoped_lock lock(this->mutex);
        return this->queue.size();
    }

    /** Walk through queue calling canSteal() on each element; if canSteal
        returns true for any element, put that element in value, and remove
        it from our queue.  Returns true if something was stolen, false 
        otherwise.

        minCount - At least this many messages must be found in queue to
                remove first.
        */
    bool steal(bool (*canSteal)(const T& value), int minCount, T& value) {
        boost::mutex::scoped_lock lock(this->mutex);

        int firstEl = -1;
        int count = 0;
        for (int i = 0, m = this->queue.size(); i < m; i++) {
            if (canSteal(this->queue[i])) {
                count += 1;
                if (count == 1) {
                    firstEl = i;
                }
                if (count >= minCount) {
                    value = this->queue[firstEl];
                    this->queue.erase(this->queue.begin() + firstEl);
                    return true;
                }
            }
        }

        return false;
    }

private:
    std::deque<T> queue;
    boost::mutex mutex;
};



/** Holds information about a received MPI message, so that we process them in
    order. */
struct MpiMessage {
    /** The MPI message tag that this message belongs to */
    int tag;

    /** An owned version of the data in this message - deleted manually in
        destructor.  We would use unique_ptr, but we don't know the type of our 
        data! */
    void* data;

    MpiMessage(int tag, void* data);

    /** Kind of a factory method - depending on tag, deserialize message into
        the desired message class, and put it in data. */
    MpiMessage(int tag, const std::string& message);
    ~MpiMessage();

    template<typename T>
    T* getTypedData() const {
        return (T*)this->data;
    }
};



struct ProcessorReduceInfo {
    /** The number of child reduceTags this one is waiting for */
    int childTagCount;

    /** The workCount from our reduction; used to tell when a ring is done
        processing and can be settled (marked done). */
    uint64_t workCount;

    /** The parent's reduceTag */
    uint64_t parentTag;

    /* The ReduceBase that contains our tag.  We track this so we can call 
       done on it.  NULL for global ring, of course.*/
    job::ReducerBase* reducer;

    ProcessorReduceInfo() : childTagCount(0), parentTag(0), reducer(0), 
            workCount(0) {}
};



/** Handles communication and job dispatch, as well as input streaming */
class Processor {
    friend class job::SharedBase;
    friend class module::Module;

public:
    /* MPI message tags */
    enum ProcessorSendTag {
        TAG_WORK,
        TAG_DEAD_RING_TEST,
        TAG_DEAD_RING_IS_DEAD,
        TAG_STEAL,
        //Placeholder for number of tags
        TAG_COUNT,
    };

    /*  Profiler categories.  User time is the most important distinction; 
        others are internal. */
    enum ProcessorTimeType {
        TIME_USER,
        TIME_SYSTEM,
        TIME_COMM,
        //Placeholder for number of types
        TIME_COUNT,
    };

    /** Used to keep track of time spent not working (vs working). */
    class WorkTimer {
    public:
        WorkTimer(Processor* p, ProcessorTimeType timeType) : processor(p) {
            this->processor->_pushWorkTimer(timeType);
        }
        ~WorkTimer() {
            this->processor->_popWorkTimer();
        }

    private:
        Processor* processor;
    };

    static void addJob(const std::string& typeName, 
            std::function<job::JobBase* ()> allocator);
    static void addReducer(const std::string& typeName, 
            std::function<job::ReducerBase* ()> allocator);

    Processor(boost::mpi::communicator world, const YAML::Node& config);
    ~Processor();


    /** Add work to our workInQueue.  We now own wr. */
    void addWork(message::WorkRecord* wr);
    /** Allocate and return a new tag for reduction operations. */
    uint64_t getNextReduceTag();
    /** Return this Processor's rank */
    int getRank() const { return this->world.rank(); }
    /** Run all modules defined in config; inputLine (already trimmed) 
        determines whether we are using one row of input (the inputLine) or 
        stdin (if empty) */
    void run(const std::string& inputLine);
    /** Start a dead ring test for the given reduceTag */
    void startRingTest(uint64_t reduceTag, uint64_t parentTag, 
            job::ReducerBase* reducer);

protected:
    job::JobBase* allocateJob(module::Module* parent, const std::string& id, 
            const YAML::Node& config);
    job::ReducerBase* allocateReducer(module::Module* parent, 
            const YAML::Node& config);
    /* If we have an input thread, join it */
    void joinThreads();
    /* We got a steal message; decode it and maybe give them work. */
    void maybeAllowSteal(const std::string& messageBuffer);
    /* Listen for input events and put them on workOutQueue.  When this thread 
       is finished, it emits a TAG_DEAD_RING_TEST message for 0. */
    void processInputThread_main(const std::string& inputLine);
    /* Process a previously received mpi message */
    void process(const MpiMessage& message);
    /* Try to receive the current request, or make a new one */
    bool tryReceive();

private:
    struct _WorkTimerRecord {
        uint64_t tsStart;
        uint64_t timeChild;
        uint64_t clkStart;
        uint64_t clksChild;
        int timeType;

        _WorkTimerRecord(uint64_t start, uint64_t clock, int type) 
                : tsStart(start), clkStart(clock), timeChild(0), clksChild(0),
                    timeType(type) {}
    };

    /* Prevent steal message spam */
    bool canSteal;
    /** Array containing how many cpu clocks were spent in each type of 
        operation.  Indexed by ProcessorTimeType */
    std::unique_ptr<uint64_t[]> clksByType;
    /** Running count of messages by tag */
    std::unique_ptr<uint64_t[]> msgsByTag;
    /* The stdin management thread; only runs on node 0 */
    boost::thread* processInputThread;
    /* The current message receiving buffer */
    std::string recvBuffer;
    /* THe current message receiving request */
    boost::optional<boost::mpi::request> recvRequest;
    /* The current number of assigned tags for reductions */
    uint64_t reduceTagCount;
    std::map<uint64_t, ProcessorReduceInfo> reduceInfoMap;
    /* We have to keep track of how many 
    /* The root module defined by the main config file */
    std::unique_ptr<job::JobBase> root;
    /* Set when eof is reached on stdin (or input line), or if our index is not
       zero. */
    bool sawEof;
    /* Set when we send ring test for 0 */
    bool sentEndRing0;
    /* True until quit message is received (ring 0 is completely closed). */
    bool shouldRun;
    /** Array containing how much time was spent in each type of operation.
        Indexed by ProcessorTimeType */
    std::unique_ptr<uint64_t[]> timesByType;
    //Any work waiting to be done on this Processor.
    std::queue<MpiMessage> workInQueue;
    /* workOutQueue gets redistributed to all workers; MPI is not implicitly
       thread-safe, that is why this queue exists.  Used for input only at
       the moment. */
    ThreadSafeQueue<message::WorkRecord*> workOutQueue;
    int workTarget;
    std::vector<_WorkTimerRecord> workTimers;
    boost::mpi::communicator world;

    /* Enqueue a line of input (stdin or argv) to system */
    void _enqueueInputWork(const std::string& line);
    /* Return rank + 1, modulo world size */
    int _getNextRank();
    /* Increment and wrap workTarget, return new value */
    int _getNextWorker();
    /** Send in a non-blocking manner (asynchronously, receiving all the while)
        */
    void _nonBlockingSend(int dest, int tag, const std::string& message);
    /** Push down a new timer section */
    void _pushWorkTimer(ProcessorTimeType userWork);
    /** Pop the last timer section */
    void _popWorkTimer();
};

}//processor
}//job_stream

#endif//JOB_STREAM_PROCESSOR_H_
