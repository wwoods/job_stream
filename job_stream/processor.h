#ifndef JOB_STREAM_PROCESSOR_H_
#define JOB_STREAM_PROCESSOR_H_

#include "job.h"
#include "message.h"
#include "module.h"
#include "types.h"
#include "workerThread.h"
#include "yaml.h"

#include <boost/mpi.hpp>
#include <deque>
#include <fstream>
#include <functional>
#include <memory>
#include <list>
#include <thread>

namespace job_stream {
namespace processor {
    class AnyUniquePtr;
}
}

namespace std {
    void swap(job_stream::processor::AnyUniquePtr& p1,
            job_stream::processor::AnyUniquePtr& p2);
}

namespace job_stream {
namespace processor {

/* Debug flag when testing library;
  1 = basic messages (min output),
  2 = very, very verbose
  3 = overly verbose */
extern const int JOB_STREAM_DEBUG;

/** The number of CPUs in the cluster, or -1 if uninitialized.
 * */
extern int cpuCount;

/** A queue of serialized initial work.  If populated (non-zero size), then this
    is used in lieu of either an initial input line or stdin. */
extern std::vector<std::string> initialWork;

/** Generic thread-safe queue class with unique_ptrs. */
template<typename T>
class ThreadSafeQueue {
public:
    void push(std::unique_ptr<T> value) {
        Lock lock(this->mutex);
        this->queue.push_back(std::move(value));
    }

    bool pop(std::unique_ptr<T>& value) {
        Lock lock(this->mutex);
        if (this->queue.empty()) {
            return false;
        }
        value = std::move(this->queue.front());
        this->queue.pop_front();
        return true;
    }

    size_t size() {
        Lock lock(this->mutex);
        return this->queue.size();
    }

private:
    std::deque<std::unique_ptr<T>> queue;
    Mutex mutex;
};



/** A cheat for a unique_ptr to any class which can be retrieved later and is
    properly deleted.  Application is responsible for keeping track of the type
    though.  This just prevents you from needing to write a tricky destructor,
    so you can focus on logic. */
class AnyUniquePtr {
public:
    AnyUniquePtr() {}

    template<class T>
    AnyUniquePtr(std::unique_ptr<T> ptr) :
            _impl(new AnyPtrImpl<T>(std::move(ptr))) {}

    AnyUniquePtr(AnyUniquePtr&& other) : _impl(std::move(other._impl)) {}

    virtual ~AnyUniquePtr() {}

    operator bool() const {
        return (bool)this->_impl;
    }

    template<class T>
    AnyUniquePtr& operator=(std::unique_ptr<T> ptr) {
        this->_impl.reset(new AnyPtrImpl<T>(std::move(ptr)));
        return *this;
    }

    template<class T>
    T* get() {
        if (!this->_impl) {
            return 0;
        }
        return (T*)this->_impl->get();
    }

    void reset() {
        this->_impl.reset();
    }

    template<class T>
    void reset(T* ptr) {
        this->_impl.reset(new AnyPtrImpl<T>(std::unique_ptr<T>(ptr)));
    }

    void swapWith(AnyUniquePtr& other) {
        std::swap(this->_impl, other._impl);
    }

private:
    struct AnyPtrImplBase {
        AnyPtrImplBase(void* ptr) : void_ptr(ptr) {}
        virtual ~AnyPtrImplBase() {}

        void* get() { return void_ptr; }

    protected:
        void* void_ptr;
    };

    template<class T>
    struct AnyPtrImpl : public AnyPtrImplBase {
        AnyPtrImpl(std::unique_ptr<T> ptr) : AnyPtrImplBase((void*)ptr.get()),
                t_ptr(std::move(ptr)) {}

        virtual ~AnyPtrImpl() {}
    private:
        std::unique_ptr<T> t_ptr;
    };

    std::unique_ptr<AnyPtrImplBase> _impl;
};



/** Holds information about a received MPI message, so that we process them in
    order. */
struct MpiMessage {
    /** The MPI message tag that this message belongs to */
    int tag;

    MpiMessage(int tag, AnyUniquePtr data);

    /** Kind of a factory method - depending on tag, deserialize message into
        the desired message class, and put it in data. */
    MpiMessage(int tag, std::string&& message);

    MpiMessage(MpiMessage&& other) : tag(other.tag) {
        std::swap(this->data, other.data);
        std::swap(this->encodedMessage, other.encodedMessage);
    }

    ~MpiMessage() {}

    /** Returns the serialized version of the data, NOT the whole message! */
    std::string getSerializedData() const;

    template<typename T>
    T* getTypedData() const {
        this->_ensureDecoded();
        return this->data.get<T>();
    }

private:
    /** An owned version of the data in this message - deleted manually in
        destructor.  We would use unique_ptr, but we don't know the type of our
        data! */
    mutable AnyUniquePtr data;

    /** The string representing this message, serialized. */
    mutable std::string encodedMessage;

    /** If data == 0, populate it by deserializing encodedMessage. */
    void _ensureDecoded() const;

    /** For serialization only */
    MpiMessage() {}

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->tag;
        if (Archive::is_saving::value) {
            //Ensure we're serialized
            std::string msg = this->getSerializedData();
            ar & msg;
        }
        else {
            ar & this->encodedMessage;
            this->data.reset();
        }
    }
};

typedef std::unique_ptr<MpiMessage> MpiMessagePtr;
typedef std::list<MpiMessagePtr> MpiMessageList;


struct ProcessorReduceInfo {
    /** The number of child reduceTags this one is waiting for */
    int childTagCount;

    /** Any messages waiting on this tag to resume (childTagCount != 0, waiting
        for 0). */
    std::vector<MpiMessagePtr> messagesWaiting;

    /** The count of messages generated within this reduction. */
    uint64_t countCreated;

    /** The count of messages processed within this reduction. */
    uint64_t countProcessed;

    /** The parent's reduceTag */
    uint64_t parentTag;

    /* The ReduceBase that contains our tag.  We track this so we can call
       done on it.  NULL for global ring, of course.*/
    job::ReducerBase* reducer;

    ProcessorReduceInfo() : childTagCount(0), countCreated(0),
            countProcessed(0), parentTag(0), reducer(0) {}

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->childTagCount & this->messagesWaiting;
        ar & this->countCreated & this->countProcessed;
        ar & this->parentTag;
        //Note - boost pointer serialization takes care of matching up our
        //reducer in this run with our reducer in a continuation.
        ar & this->reducer;
    }
};



/** Information tied to an in-progress send */
struct ProcessorSendInfo {
    boost::mpi::request request;

    ProcessorSendInfo(boost::mpi::request request) : request(request) {}
};



/** Size and other information about a processor from a checkpoint file. */
struct CheckpointInfo {
    uint64_t jobTreeSize;
    uint64_t messagesWaiting;
    uint64_t reduceMapSize;
    uint64_t totalBytes;
    uint64_t totalUserBytes;
    std::map<int, uint64_t> countByTag;
    std::map<int, uint64_t> bytesByTag;
    std::unique_ptr<message::StealRing> stealRing;

    /** Initialize with all zeroes */
    CheckpointInfo() : jobTreeSize(0), messagesWaiting(0), reduceMapSize(0),
            totalBytes(0), totalUserBytes(0) {}
};



/** Handles communication and job dispatch, as well as input streaming */
class Processor {
    friend class job::ReducerBase;
    friend class job::SharedBase;
    friend class module::Module;
    friend class WorkerThread;

public:
    /* MPI message tags */
    enum ProcessorSendTag {
        TAG_WORK,
        /** Work that is en-route to the only host capable of handling it. */
        TAG_REDUCE_WORK,
        TAG_DEAD_RING_TEST,
        TAG_DEAD_RING_IS_DEAD,
        TAG_STEAL,
        /** A group of messages - tag, body, tag, body, etc. */
        TAG_GROUP,
        /** Advance checkpoint to next state (either start or send) */
        TAG_CHECKPOINT_NEXT,
        /** Processor ready to submit checkpoint data */
        TAG_CHECKPOINT_READY,
        /** Processor's data required to resume operations. */
        TAG_CHECKPOINT_DATA,
        /** Force a checkpoint */
        TAG_CHECKPOINT_FORCE,
        /** Placeholder for number of tags */
        TAG_COUNT,
    };

    /*  Profiler categories.  User time is the most important distinction;
        others are internal. */
    enum ProcessorTimeType {
        TIME_IDLE,
        TIME_USER,
        TIME_SYSTEM,
        TIME_COMM,
        //Placeholder for number of types
        TIME_COUNT,
    };

    /** Checkpoint states.
        Checkpoints go something like this:

        CHECKPOINT_NONE: No checkpoint operations in progress
        CHECKPOINT_SYNC: A checkpoint has started, waiting for all processors
            to wait CHECKPOINT_SYNC_WAIT_MS after having zero-sized
            messageQueue and sendRequests lists.
        CHECKPOINT_GATHER: For rank 0 only, wait for everyone else to send their
            data and then package it up into the checkpoint file.  For rank 1,
            send our state once, and then wait for TAG_CHECKPOINT_NEXT, at which
            point we're done.
        */
    enum CheckpointState {
        CHECKPOINT_NONE,
        CHECKPOINT_SYNC,
        /** Rank 0 only; waiting for everyone else to send */
        CHECKPOINT_GATHER
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

    static int CHECKPOINT_SYNC_WAIT_MS;

    static void addJob(const std::string& typeName,
            std::function<job::JobBase* ()> allocator);
    static void addReducer(const std::string& typeName,
            std::function<job::ReducerBase* ()> allocator);

    Processor(std::shared_ptr<boost::mpi::environment> env,
            boost::mpi::communicator world,
            const std::string& config, const std::string& checkpointName);
    ~Processor();

    /** Add work to our workInQueue when the current job finishes.  We now own
        wr. */
    void addWork(std::unique_ptr<message::WorkRecord> wr);
    /** Ability to use registered method to allocate a JobBase derivative. */
    static job::JobBase* allocateJobForDeserialize(const std::string& typeId);
    /** Ability to use registered method to allocate a ReducerBase derivative. */
    static job::ReducerBase* allocateReducerForDeserialize(const std::string& typeId);
    /** Useful for systems such as python; this function is called once per loop
        in the main thread.  If it returns true, operation stops (shouldRun is
        set to false).
        */
    std::function<bool ()> checkExternalSignals;
    /** Allocate and return a new tag for reduction operations. */
    uint64_t getNextReduceTag();
    /** Return this Processor's rank */
    int getRank() const { return this->world.rank(); }
    /** If set, called instead of printing any output work.  Note that when this is called,
        the work is NO LONGER UNDER CHECKPOINT!  That is, if the work needs to be
        checkpoint-safe, then the application defining this callback must write it to disk
        or make it safe in its own way. */
    std::function<void (std::unique_ptr<serialization::AnyType> output)>
            handleOutputCallback;
    /** Returns statistics about some processor from a serialized checkpoint
        buffer. */
    void populateCheckpointInfo(CheckpointInfo& info,
            const std::string& buffer);
    /** Registers the given ReducerBase and reduce tag as having a
        reduceMapPatch that needs to be cleared on successful completion of
        the current work. */
    void registerReduceReset(job::ReducerBase* rb, uint64_t reduceTag);
    /** Run all modules defined in config; inputLine (already trimmed)
        determines whether we are using one row of input (the inputLine) or
        stdin (if empty) */
    void run(const std::string& inputLine);
    /** Sets the checkpointInterval on this processor; if called after
        instantiation and before run(), affects the first checkpoint as well.
        Otherwise, only takes effect after the next checkpoint. */
    void setCheckpointInterval(int intervalMs) {
            this->checkpointInterval = intervalMs; }
    /** Can be used to turn stealing off; not recommended. */
    void setStealEnabled(bool enable) {
            this->_stealEnabled = enable; }
    /** Start a dead ring test for the given reduceTag.  The ring won't actually
        be started until the work that started it completes.  Otherwise
        checkpoints would be broken.  See _startRingTest() for implementation.
        */
    void startRingTest(uint64_t reduceTag, uint64_t parentTag,
            job::ReducerBase* reducer);

protected:
    job::JobBase* allocateJob(module::Module* parent, const std::string& id,
            const YAML::Node& config);
    job::ReducerBase* allocateReducer(module::Module* parent,
            const YAML::Node& config);
    /** Force a checkpoint after current work completes.  Optionally, force
        a quit (exit non-zero) after the checkpoint completes.
        */
    void forceCheckpoint(bool forceQuit = false);
    /** Called to handle a ring test is dead message, within either tryReceive
        or a work thread. */
    void handleDeadRing(MpiMessagePtr message);
    /** Called to handle a ring test message, possibly within tryReceive, or
        possibly a work thread.  If we are the sentry for the message's
        ring, we are called in work loop.  Otherwise in tryReceive.  Unless
        checkpointing interfered. */
    void handleRingTest(MpiMessagePtr message);
    /** If we have an input thread, join it */
    void joinThreads();
    /** Initialize local timing info */
    void localTimersInit();
    /** Merge local timing info into globals */
    void localTimersMerge();
    /** We got a steal message; decode it and maybe give someone work. */
    void maybeAllowSteal(const std::string& messageBuffer);
    /** Called in a worker thread; using the current thread, pull down some
        work and process it, while respecting checkpoint criteria.

        workerId - ID of worker thread; used to limit number of active workers
                according to capacity.

        Returns true if work happened, or false if there was nothing to do
        (which inspires the thread to sleep) */
    bool processInThread(int workerId);
    /** Listen for input events and put them on workOutQueue.  When this thread
        is finished, it emits a TAG_DEAD_RING_TEST message for 0. */
    void processInputThread_main(const std::string& inputLine);
    /** Try to receive the current request, or make a new one */
    bool tryReceive();

private:
    struct _WorkTimerRecord {
        uint64_t tsStart;
        uint64_t timeChild;
        uint64_t clkStart;
        uint64_t clksChild;
        int timeType;

        _WorkTimerRecord(uint64_t start, uint64_t clock, int type)
                : tsStart(start), timeChild(0), clkStart(clock), clksChild(0),
                    timeType(type) {}
    };

    /** Information about a WorkerThread - specifically, outbound messages
        waiting on the current work's completion (and the completion of
        checkpoint operations) before being sent. */
    struct _WorkerInfo {
        _WorkerInfo() : p(0), isLocked(false), hasReduceTag(false),
                reduceTag(0) {}

        struct RingTestInfo {
            uint64_t reduceTag, parentTag;
            job::ReducerBase* reducer;

            RingTestInfo(uint64_t reduceTag, uint64_t parentTag,
                    job::ReducerBase* reducer) : reduceTag(reduceTag),
                    parentTag(parentTag), reducer(reducer) {}
        };

        struct ReduceMapResetInfo {
            job::ReducerBase* reducer;
            uint64_t reduceTag;

            ReduceMapResetInfo(job::ReducerBase* reducer, uint64_t reduceTag)
                    : reducer(reducer), reduceTag(reduceTag) {}
        };

        processor::Processor* p;
        /** This is the only part that is serialized; the MpiMessagePtr that we
            started with, before any reduction ring modifications.

            Note that it is VITAL no state changes happen outside of the control
            of this struct, including network communication queuing.  Otherwise,
            a checkpoint will re-enqueue the work, when it is partially
            complete, and work will be duplicated. */
        std::string work;
        std::list<ReduceMapResetInfo> reduceMapResets;
        std::list<RingTestInfo> outboundRings;
        std::list<std::unique_ptr<message::WorkRecord>> outboundWork;
        std::list<std::unique_ptr<message::_Message>> outboundMessages;
        bool isLocked;
        bool hasReduceTag;
        /** If hasReduceTag is set, then this is the reduceTag of the work
            occurring in this thread.  Ergo, it needs to be decremented on
            checkpoint load, since its childTagCount gets incremented when
            work begins. */
        uint64_t reduceTag;

        void lockForWork();

        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->work;
            ar & this->hasReduceTag;
            ar & this->reduceTag;
        }
    };
    typedef std::list<_WorkerInfo> _WorkerInfoList;

    /** The file being used to write the current checkpoint. */
    std::unique_ptr<std::ofstream> checkpointFile;
    /** The archive writing to checkpointFile */
    std::unique_ptr<serialization::OArchive> checkpointAr;
    /** The name of the checkpoint file */
    std::string checkpointFileName;
    /** Time between checkpoints (ms).  Time from completion to start.  */
    int checkpointInterval;
    /** Time to next checkpoint (ms).  Negative means disabled. */
    int checkpointNext;
    /** Quit on a successful checkpoint?  Used for debugging */
    bool checkpointQuit;
    /** Start time for checkpoint, just for stats tracking */
    uint64_t tsCheckpointStart;
    CheckpointState checkpointState;
    /** Count of processors that we're waiting on information from.  Only used
        on rank 0. */
    int checkpointWaiting;
    /** Our config as a string.  Used for checkpointing to ensure that an
        existing checkpoint file matches what the user wanted. */
    std::string _configStr;
    /** If true (default), then work is processed depth-first rather than
        breadth-first.  This has the benefit of e.g. allowing progress meters
        on frame close to be more accurate.  Additionally, if a bug occurs
        later in the pipeline, it will be discovered sooner with a depth-first
        approach.

        Note that if one stage of the pipeline depends on a prior stage
        (conversed over database records, for instance), the different stages
        may still be separated by using Frames (one Frame for stage 1, and
        a second Frame for stage 2).  The input to this system would then be
        garbage, and the first Frame would need to initialize the input.
        */
    bool depthFirst;
    /** Our environment */
    std::shared_ptr<boost::mpi::environment> env;
    /** Global array; see localClksByType */
    std::unique_ptr<uint64_t[]> globalClksByType;
    /** The root global config node, which is globally guarded. */
    YAML::GuardedNode globalConfig;
    std::unique_ptr<uint64_t[]> globalTimesByType;
    /** The queue of serialized, initial work.  Stolen into our possession on
        construction. */
    std::vector<std::string> initialWork;
    /** Array containing how many cpu clocks were spent in each type of
        operation.  Indexed by ProcessorTimeType */
    static thread_local std::unique_ptr<uint64_t[]> localClksByType;
    /** Array containing how much time was spent in each type of operation.
        Indexed by ProcessorTimeType */
    static thread_local std::unique_ptr<uint64_t[]> localTimesByType;
    /** The main thread id - useful for verifying there aren't errors in where
        certain functions are called (must be main) */
    std::thread::id mainThreadId;
    /** Running count of messages by tag */
    std::unique_ptr<uint64_t[]> msgsByTag;
    /* The stdin management thread; only runs on node 0 */
    std::unique_ptr<std::thread> processInputThread;
    /* Buffers corresponding to requests */
    message::_Message recvBuffer;
    /* The current message receiving requests (null when dead) */
    boost::optional<boost::mpi::request> recvRequest;
    /* The current number of assigned tags for reductions */
    uint64_t reduceTagCount;
    std::map<uint64_t, ProcessorReduceInfo> reduceInfoMap;
    /* The root module defined by the main config file */
    std::unique_ptr<job::JobBase> root;
    /* Set when eof is reached on stdin (or input line), or if our index is not
       zero. */
    bool sawEof;
    /** Currently pending outbound nonBlocking requests */
    std::list<ProcessorSendInfo> sendRequests;
    /* Set when we send ring test for 0 */
    bool sentEndRing0;
    /* True until quit message is received (ring 0 is completely closed). */
    bool shouldRun;
    /* True if and only if this processor was restored from a checkpoint. */
    bool wasRestored;
    /** Exceptions from worker threads. */
    std::vector<std::exception_ptr> workerErrors;
    /** Allocated threads doing work. */
    std::vector<std::unique_ptr<WorkerThread>> workers;
    //Any work waiting to be done on this Processor.
    MpiMessageList workInQueue;
    /** workOutQueue gets redistributed to all workers; MPI is not implicitly
       thread-safe, that is why this queue exists.  Used for input only at
       the moment. */
    ThreadSafeQueue<message::WorkRecord> workOutQueue;
    /** Outbound messages, which are sent from the main thread each loop. */
    ThreadSafeQueue<message::_Message> messageQueue;
    /** Number of worker threads that are allowed to work.  Adjusted by steal
        ring. */
    int workersActive;
    /** Work happening right now, including enough static information to resume
        after a checkpoint. */
    _WorkerInfoList workerWork;
    static thread_local _WorkerInfoList::iterator workerWorkThisThread;
    int workTarget;
    static thread_local std::vector<_WorkTimerRecord> workTimers;
    boost::mpi::communicator world;

    bool _stealEnabled;
    //Pending steal messages get stored specially for checkpoints, so that they
    //always get processed first.
    MpiMessagePtr _stealMessage;

    /** Locks shared resources - workInQueue, reduceInfoMap.  ANY WORK requiring
        a mutex lock should go through
        workerWorkThisThread->lockForWork().  That way, the work is locked in
        until it finishes and all transient activity that would interfere with
        checkpoints completes. */
    Mutex _mutex;

    /** Locks reduceTagCount.  See getNextReduceTag() for why this is separate.
        */
    Mutex _mutexForReduceTags;

    /** Adds work to our outbound work queue immediately. */
    void _addWork(std::unique_ptr<message::WorkRecord> wr);
    /** Raises a runtime_error if this is not the main thread */
    void _assertMain();
    /** Update all asynchronous MPI operations to ensure our buffers are full */
    void _checkMpi();
    /** Called to reduce a childTagCount on a ProcessorReduceInfo for a given
        reduceTag.  Optionally dispatch messages pending. */
    void _decrReduceChildTag(uint64_t reduceTag, bool wasProcessed = false);
    /** Send work from workOutQueue to either our local workInQueue or send to
        a remote source. */
    void _distributeWork(std::unique_ptr<message::WorkRecord> wr);
    /** Enqueue a line of input (stdin or argv) to system */
    void _enqueueInputWork(const std::string& line);
public:
    /** Return cpu time for this thread in ms */
    static uint64_t _getThreadCpuTimeMs();
private:
    /** Return rank + 1, modulo world size */
    int _getNextRank();
    /** Increment and wrap workTarget, return new value */
    int _getNextWorker();
    /** Get work for a Worker, or return a null unique_ptr if there is no
        work (or we're waiting on a checkpoint).

        workerId - the ID of the worker thread asking for work.  Used to
                constrain standard work based on workersActive. */
    MpiMessagePtr _getWork(int workerId);
    /** Locks this Processor's mutex and adds work to workInQueue.
        */
    void _lockAndAddWork(MpiMessagePtr msg);
    /** Send in a non-blocking manner (asynchronously, receiving all the while).
        reduceTags are any tags that should be kept alive by the fact that this
        is in the process of being sent.
        */
    void _nonBlockingSend(message::Header header, std::string payload);
    void _nonBlockingSend(message::_Message&& msg);
    /** Push down a new timer section */
    void _pushWorkTimer(ProcessorTimeType userWork);
    /** Pop the last timer section */
    void _popWorkTimer();
public:
    /** Modify the current work timer, allotting the given measurements to the
     * given category rather than the current timer.
     * */
    void _modifyWorkTimer(ProcessorTimeType timeType, uint64_t wallTimeMs,
            uint64_t cpuTimeMs);
private:
    /** Start a dead ring test for the given reduceTag IMMEDIATELY.  Shouldn't
        be used during work, or it would break checkpoints. */
    void _startRingTest(uint64_t reduceTag, uint64_t parentTag,
            job::ReducerBase* reducer);

    /** Update current checkpoint state, given # of ms difference from last
        loop.

        Essentially, CHECKPOINT_NONE -> CHECKPOINT_START -> worker stopped
        and nothing left to send -> CHECKPOINT_SYNC -> all sync'd, plus a delay
        -> CHECKPOINT_SEND -> send data to 0, resume operations.

        */
    void _updateCheckpoints(int msDiff);


    friend class boost::serialization::access;
    /** Either write a checkpoint or restore our state from one. */
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->_configStr;
        ar & this->root;
        ar & this->reduceTagCount & this->reduceInfoMap;
        ar & this->workInQueue;
        ar & this->sentEndRing0;
        ar & this->workerWork;
        ar & this->_stealMessage;
        if (Archive::is_saving::value && JOB_STREAM_DEBUG) {
            for (auto& m : this->workerWork) {
                if (!m.hasReduceTag) {
                    continue;
                }
                fprintf(stderr, "Known checkpoint work: %lu\n", m.reduceTag);
            }
        }
    }
};

}//processor
}//job_stream

#endif//JOB_STREAM_PROCESSOR_H_
