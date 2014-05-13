
#include "job.h"
#include "message.h"
#include "module.h"
#include "processor.h"
#include "workerThread.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <ctime>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <sys/resource.h>

namespace mpi = boost::mpi;

namespace std {
    void swap(job_stream::processor::AnyUniquePtr& p1,
            job_stream::processor::AnyUniquePtr& p2) {
        p1.swapWith(p2);
    }
}

namespace job_stream {
namespace processor {

typedef std::map<std::string, std::function<job::JobBase* ()>> JobTypeMapType;
JobTypeMapType& jobTypeMap() {
    //Ensure this gets instantiated when it needs to; if it were outside a
    //function, it isn't ready for auto registration.
    static JobTypeMapType result;
    return result;
}

typedef std::map<std::string, std::function<job::ReducerBase* ()>> ReducerTypeMapType;
ReducerTypeMapType& reducerTypeMap() {
    //Ensure this gets instantiated when it needs to; if it were outside a
    //function, it isn't ready for auto registration.
    static ReducerTypeMapType result;
    return result;
}

thread_local std::vector<Processor::_WorkTimerRecord> Processor::workTimers;
thread_local std::unique_ptr<uint64_t[]> Processor::localClksByType;
thread_local std::unique_ptr<uint64_t[]> Processor::localTimesByType;

extern const int JOB_STREAM_DEBUG = 0;

class _Z_impl {
public:
    _Z_impl(std::string s) { 
        if (s.size() > 40) {
            s = s.substr(s.size() - 40);
        }
        this->s = std::move(s);
        printf("%i %s BEGIN\n", getpid(), this->s.c_str()); 
    }
    _Z_impl(int line) { printf("%i   %i CONTINUE\n", getpid(), line); }
    ~_Z_impl() { if (!this->s.empty()) printf("%i %s END\n", getpid(), 
            this->s.c_str()); }

    std::string s;

private:
    static std::vector<_Z_impl> list;
};
#if 0
#define ZZ _Z_impl(std::string(__FILE__) + std::string(":") \
        + boost::lexical_cast<std::string>(__LINE__) + std::string(":") \
        + std::string(__FUNCTION__));
#define ZZZ _Z_impl(__LINE__);
#else
#define ZZ
#define ZZZ
#endif

/** STATICS **/
void Processor::addJob(const std::string& typeName, 
        std::function<job::JobBase* ()> allocator) {
    JobTypeMapType& jtm = jobTypeMap();
    if (jtm.count("module") == 0) {
        //initialize map with values
        jtm["module"] = module::Module::make;
    }

    if (jtm.count(typeName) != 0) {
        std::ostringstream ss;
        ss << "Job type already defined: " << typeName;
        throw std::runtime_error(ss.str());
    }

    jtm[typeName] = allocator;
}


void Processor::addReducer(const std::string& typeName, 
        std::function<job::ReducerBase* ()> allocator) {
    ReducerTypeMapType& rtm = reducerTypeMap();
    if (rtm.count(typeName) != 0) {
        std::ostringstream ss;
        ss << "Reducer type already defined: " << typeName;
        throw std::runtime_error(ss.str());
    }
    
    rtm[typeName] = allocator;
}
/** END STATICS **/



MpiMessage::MpiMessage(int tag, AnyUniquePtr data) : tag(tag), 
        data(std::move(data)) {}


MpiMessage::MpiMessage(int tag, std::string&& message) : tag(tag),
        encodedMessage(message) {
}


std::string MpiMessage::serialized() const {
    if (!this->data) {
        //Already serialized!
        return this->encodedMessage;
    }

    if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        return this->data.get<message::WorkRecord>()->serialized();
    }
    else if (tag == Processor::TAG_DEAD_RING_TEST
            || tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        return this->data.get<message::DeadRingTestMessage>()->serialized();
    }
    else {
        throw std::runtime_error("MpiMessage::serialized() didn't find tag");
    }
}


void MpiMessage::_ensureDecoded() const {
    if (this->data) {
        return;
    }

    std::string&& message = std::move(this->encodedMessage);
    if (this->tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        this->data.reset(new message::WorkRecord(message));
    }
    else if (this->tag == Processor::TAG_DEAD_RING_TEST
            || this->tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        this->data.reset(new message::DeadRingTestMessage(message));
    }
    else {
        std::ostringstream ss;
        ss << "Unrecognized tag for MpiMessage(): " << tag;
        throw std::runtime_error(ss.str());
    }
}



Processor::Processor(std::unique_ptr<mpi::environment> env, 
        mpi::communicator world, 
        const YAML::Node& config, const std::string& checkpointFile)
            : checkpointFileName(checkpointFile), reduceTagCount(0),
                env(std::move(env)), wasRestored(0), world(world),
                workingCount(0) {
    if (world.size() >= (1 << message::WorkRecord::TAG_ADDRESS_BITS)) {
        throw std::runtime_error("MPI world too large.  See TAG_ADDRESS_BITS "
                "in message.h");
    }

    //Register our base types so that we can create checkpoints
    serialization::registerType<job::SharedBase, job::SharedBase>();
    serialization::registerType<job::JobBase, job::SharedBase>();
    serialization::registerType<job::ReducerBase, job::SharedBase>();
    serialization::registerType<module::Module, job::JobBase, job::SharedBase>();

    if (!this->checkpointFileName.empty()) {
        std::ifstream cf(this->checkpointFileName);
        if (cf) {
            //File exists, we should resume from last checkpoint!
            std::ostringstream err;
            serialization::IArchive ar(cf);
            int wsize, bufferRank, i;
            serialization::decode(ar, wsize);
            if (wsize != this->world.size()) {
                err << "MPI world wrong size for continuation; should be "
                        << wsize;
                throw std::runtime_error(err.str());
            }

            //File is (rank, encoded string) pairs
            std::string buffer;
            for (i = 0; i < wsize; i++) {
                serialization::decode(ar, bufferRank);
                serialization::decode(ar, buffer);
                if (bufferRank == this->getRank()) {
                    serialization::decode(buffer, *this);
                    break;
                }
            }
            if (i == wsize) {
                err << "Could not find rank " << this->getRank() << " in "
                        << "checkpoint file";
                throw std::runtime_error(err.str());
            }

            //Populate config for all of our jobs
            this->root->processor = this;
            this->root->populateAfterRestore(config, config);

            this->wasRestored = true;
            fprintf(stderr, "%i resumed from checkpoint "
                    "(%i pending messages)\n",
                    this->getRank(), this->workInQueue.size());
        }
    }

    if (!this->root) {
        //Generate this->root from config
        this->allocateJob(NULL, "root", config);
    }

    //Turn off checkpointing for all processors by default (re-enabled for
    //processor 1 in run()).
    this->checkpointNext = -1;
    this->checkpointInterval = 600 * 1000;
    if (this->checkpointFileName.empty()) {
        this->checkpointInterval = -1;
    }
    this->checkpointState = Processor::CHECKPOINT_NONE;
}


Processor::~Processor() {
}


MpiMessagePtr Processor::getWork() {
    MpiMessagePtr ptr;
    Lock lock(this->mutex);

    if (this->checkpointState != Processor::CHECKPOINT_NONE) {
        //No work can be done during a checkpoint operation.
        return ptr;
    }

    if (!this->workInQueue.empty()) {
        ptr = std::move(this->workInQueue.front());
        this->workInQueue.pop_front();
        //Before letting others get work, we want to ensure that a ring test
        //knows we're in progress.  This is just an optimization.
        if (ptr->tag == Processor::TAG_WORK 
                || ptr->tag == Processor::TAG_REDUCE_WORK) {
            uint64_t reduceTag = ptr->getTypedData<message::WorkRecord>()
                    ->getReduceTag();
            auto& rim = this->reduceInfoMap[reduceTag];
            //Stop ring tests from progressing
            rim.childTagCount += 1;
        }
        if (JOB_STREAM_DEBUG >= 1) {
            fprintf(stderr, "%i %lu processing %i ", this->getRank(), 
                    message::Location::getCurrentTimeMs(), ptr->tag);
            if (ptr->tag == Processor::TAG_WORK 
                    || ptr->tag == Processor::TAG_REDUCE_WORK) {
                for (const std::string& s 
                        : ptr->getTypedData<message::WorkRecord>()->getTarget()
                        ) {
                    fprintf(stderr, "::%s", s.c_str());
                }
                fprintf(stderr, " ON %lu\n",
                        ptr->getTypedData<message::WorkRecord>()->getReduceTag());
            }
            fprintf(stderr, "\n");
        }

        //We had work, meaning that a thread is working on this
        this->workingCount += 1;
    }

    return ptr;
}


/** Synchronize information about each processor's effectiveness at the end
    of execution. */
struct ProcessorInfo {
    int pctUserTime;
    int pctUserCpu;
    uint64_t userCpuTotal;
    int pctMpiTime;
    uint64_t msgsTotal;
    int pctUserMsgs;

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->pctUserTime;
        ar & this->pctUserCpu;
        ar & this->userCpuTotal;
        ar & this->pctMpiTime;
        ar & this->msgsTotal;
        ar & this->pctUserMsgs;
    }
};


void Processor::run(const std::string& inputLine) {
    this->mainThreadId = std::this_thread::get_id();
    this->sawEof = false;
    this->sentEndRing0 = false;
    this->shouldRun = true;
    this->localTimersInit();
    this->globalClksByType.reset(new uint64_t[Processor::TIME_COUNT]);
    this->globalTimesByType.reset(new uint64_t[Processor::TIME_COUNT]);
    for (int i = 0; i < Processor::TIME_COUNT; i++) {
        this->globalClksByType[i] = 0;
        this->globalTimesByType[i] = 0;
    }
    this->msgsByTag.reset(new uint64_t[Processor::TAG_COUNT]);
    for (int i = 0; i < Processor::TAG_COUNT; i++) {
        this->msgsByTag[i] = 0;
    }

    //Figure out our siblings
    std::vector<std::string> hosts;
    mpi::all_gather(this->world, this->env->processor_name(), hosts);

    this->workTarget = 1;
    if (this->world.rank() == 0) {
        if (!this->wasRestored) {
            //The first work message MUST go to rank 0, so that ring 1 ends up
            //there
            this->workTarget = -1;
            this->processInputThread.reset(new std::thread(std::bind(
                    &Processor::processInputThread_main, this, inputLine)));

            //The first processor also starts the steal ring
            this->_nonBlockingSend(
                    message::Header(Processor::TAG_STEAL, this->_getNextRank()),
                    message::StealRing(this->world.size()).serialized());
        }
        else {
            this->sawEof = true;
            //sentEndRing0 was serialized.  That way, if input was still open,
            //we'll start the ring test for 0 properly on resume here.
        }

        //First processor is responsible for checkpointing
        this->checkpointNext =  this->checkpointInterval;
    }
    else {
        this->sawEof = true;
        this->sentEndRing0 = true;
    }
    
    //Start up our workers
    unsigned int compute = std::max(1u, std::thread::hardware_concurrency());
    for (unsigned int i = 0; i < compute; i++) {
        this->workers.emplace_back(new WorkerThread(this));
    }

    //Begin tallying time spent in system vs user functionality.
    std::unique_ptr<WorkTimer> outerTimer(new WorkTimer(this, 
            Processor::TIME_IDLE));

    int dest, tag;
    std::unique_ptr<message::WorkRecord> work;
    uint64_t tsLastLoop = message::Location::getCurrentTimeMs();
    while (this->shouldRun) {
        uint64_t tsThisLoop = message::Location::getCurrentTimeMs();

        //Update communications
        this->_checkMpi();

        if (!this->sentEndRing0) {
            //Eof found and we haven't sent ring!  Send it
            if (this->sawEof) {
                this->startRingTest(0, 0, 0);
                this->sentEndRing0 = true;
            }
        }

        //Distribute any outbound work
        while (this->workOutQueue.pop(work)) {
            this->_distributeWork(std::move(work));
        }

        //Check on checkpointing
        this->_updateCheckpoints(tsThisLoop - tsLastLoop);

        tsLastLoop = tsThisLoop;

        //Wait a bit
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    //Stop timer, report on user vs not
    outerTimer.reset();
    this->localTimersMerge();

    //Stop all threads
    this->joinThreads();

    uint64_t timesTotal = 0;
    //Aggregate main thread localTimes to get total sim time
    for (int i = 0; i < Processor::TIME_COUNT; i++) {
        timesTotal += this->localTimesByType[i];
    }
    uint64_t msgsTotal = 0;
    uint64_t msgsUser = 0;
    for (int i = 0; i < Processor::TAG_COUNT; i++) {
        msgsTotal += this->msgsByTag[i];
        if (i == Processor::TAG_WORK || i == Processor::TAG_REDUCE_WORK
                || i == Processor::TAG_GROUP) {
            msgsUser += this->msgsByTag[i];
        }
    }

    //Finish all outstanding activity and send a message to everyone letting
    //them know we're done.  Otherwise, messages won't line up and MPI complains
    //about messages being truncated.
    while (true) {
        this->_checkMpi();
        if (this->sendRequests.empty() && !this->tryReceive()) {
            break;
        }
    }
    this->world.send(this->getRank(), 0, message::_Message(
            message::Header(0, this->getRank()), ""));
    this->recvRequest->wait();
    this->world.barrier();

    //Stop divide by zero
    timesTotal = (timesTotal > 0) ? timesTotal : 1;
    msgsTotal = (msgsTotal > 0) ? msgsTotal : 1;

    ProcessorInfo myInfo;
    myInfo.pctUserTime = (int)(1000 * this->globalTimesByType[
            Processor::TIME_USER] / timesTotal);
    myInfo.userCpuTotal = this->globalClksByType[Processor::TIME_USER];
    myInfo.pctMpiTime = (int)(1000 * this->globalTimesByType[
            Processor::TIME_COMM] / timesTotal);
    uint64_t userTimeNonZero = this->globalTimesByType[Processor::TIME_USER];
    userTimeNonZero = (userTimeNonZero > 0) ? userTimeNonZero : 1;
    myInfo.pctUserCpu = (int)(1000 * this->globalClksByType[
            Processor::TIME_USER] / userTimeNonZero);
    myInfo.msgsTotal = msgsTotal;
    myInfo.pctUserMsgs = (int)(1000 * msgsUser / msgsTotal);

    //Send info to rank 0, which prints out everything in order
    if (this->world.rank() != 0) {
        mpi::gather(this->world, myInfo, 0);
    }
    else {
        std::vector<ProcessorInfo> infos;
        mpi::gather(this->world, myInfo, infos, 0);

        int totalTime = 0;
        uint64_t totalCpu = 0;
        int totalCpuTime = 0;
        for (int i = 0, m = infos.size(); i < m; i++) {
            ProcessorInfo& pi = infos[i];
            totalTime += pi.pctUserTime;
            totalCpu += pi.userCpuTotal;
            totalCpuTime += pi.pctUserCpu * pi.pctUserTime;
            fprintf(stderr,
                    "%i %i%% user time (%i%% mpi), %i%% user cpu, "
                        "%lu messages (%i%% user)\n", 
                    i, pi.pctUserTime / 10, pi.pctMpiTime / 10, 
                    pi.pctUserCpu / 10, pi.msgsTotal, pi.pctUserMsgs / 10);
        }
        fprintf(stderr, "C %i%% user time, %i%% user cpu, "
                "quality %.2f cpus, ran %.3fs\n", 
                totalTime / 10, totalCpuTime / 10000,
                (double)totalCpu / timesTotal, timesTotal * 0.001);
    }
}


void Processor::addWork(std::unique_ptr<message::WorkRecord> wr) {
    Lock lock(this->mutex);
    this->reduceInfoMap[wr->getReduceTag()].countCreated += 1;
    this->workOutQueue.push(std::move(wr));
}


void Processor::_distributeWork(std::unique_ptr<message::WorkRecord> wr) {
    int dest, rank = this->getRank();
    int tag = Processor::TAG_WORK;
    const std::vector<std::string>& target = wr->getTarget();
    if (target.size() > 1 && target[target.size() - 2] == "output") {
        //Reduced output for top-level reducer
        if (target[target.size() - 1] != "reduced") {
            std::ostringstream ss;
            ss << "Unexpected tag after output: " << target[target.size() - 1];
            throw std::runtime_error(ss.str());
        }
        dest = 0;
        tag = Processor::TAG_REDUCE_WORK;
    }
    else if (target.size() > 0 && target[target.size() - 1] == "output") {
        dest = wr->getReduceHomeRank();
        tag = Processor::TAG_REDUCE_WORK;
    }
    else {
        if (this->root->wouldReduce(*wr)) {
            dest = this->_getNextWorker();
        }
        else {
            //Let stealing take care of it.
            dest = rank;
        }
    }

    uint64_t reduceTagToFree = wr->getReduceTag();
    if (dest != rank) {
        this->_nonBlockingSend(
                message::Header(tag, dest), wr->serialized());
    }
    else {
        Lock lock(this->mutex);
        this->workInQueue.emplace_back(new MpiMessage(tag, std::move(wr)));
    }
}


void Processor::startRingTest(uint64_t reduceTag, uint64_t parentTag, 
        job::ReducerBase* reducer) {
    Lock lock(this->mutex);

    if (reduceTag != 0 && this->reduceInfoMap.count(reduceTag) != 0) {
        std::ostringstream ss;
        ss << "Ring already existed? " << reduceTag;
        throw std::runtime_error(ss.str());
    }

    ProcessorReduceInfo& pri = this->reduceInfoMap[reduceTag];
    pri.reducer = reducer;
    pri.parentTag = parentTag;

    if (parentTag != reduceTag) {
        //Stop the parent from being closed until the child is closed.
        this->reduceInfoMap[parentTag].childTagCount += 1;
        //We're being processed, thus our ring is held up as well
        pri.childTagCount += 1;
        //startRingTest() is only called immediately before actually processing
        //the first packet (for non-global rings)
        pri.countCreated = 1;
    }

    if (JOB_STREAM_DEBUG) {
        fprintf(stderr, "Starting ring test on %lu (from %lu) - %lu / %lu\n",
                reduceTag, parentTag, pri.countProcessed, pri.countCreated);
    }

    auto m = message::DeadRingTestMessage(this->world.rank(),
            reduceTag);
    this->messageQueue.push(std::unique_ptr<message::_Message>(
            new message::_Message(
                message::Header(Processor::TAG_DEAD_RING_TEST, 
                    this->_getNextRank()), m.serialized())));
}


uint64_t Processor::getNextReduceTag() {
    uint64_t result = 0x0;
    int countBits = (64 - message::WorkRecord::TAG_ADDRESS_BITS);
    uint64_t countMask = (1 << countBits) - 1;

    Lock lock(this->mutex);

    //Don't allow 0x0, since that is reserved for application reduction.
    //0x1 is reserved for top module reduce.
    if (this->reduceTagCount < 2) {
        this->reduceTagCount = 2;
    }

    //Overflow for rank part checked in Processor constructor
    result |= ((uint64_t)this->world.rank() << countBits);
    result |= (this->reduceTagCount & countMask);
    this->reduceTagCount += 1;
    return result;
}


void Processor::joinThreads() {
    if (this->processInputThread) {
        this->processInputThread->join();
    }
    for (std::unique_ptr<WorkerThread>& wt : this->workers) {
        wt->join();
    }
}


void Processor::localTimersInit() {
    this->localClksByType.reset(new uint64_t[Processor::TIME_COUNT]);
    this->localTimesByType.reset(new uint64_t[Processor::TIME_COUNT]);
    for (int i = 0; i < Processor::TIME_COUNT; i++) {
        this->localClksByType[i] = 0;
        this->localTimesByType[i] = 0;
    }
}


void Processor::localTimersMerge() {
    Lock lock(this->mutex);
    for (int i = 0; i < Processor::TIME_COUNT; i++) {
        this->globalClksByType[i] += this->localClksByType[i];
        this->globalTimesByType[i] += this->localTimesByType[i];
    }
}


bool sortWorkBySize(std::list<std::shared_ptr<MpiMessage>>::iterator a,
        std::list<std::shared_ptr<MpiMessage>>::iterator b) {
    return (*a)->getTypedData<message::WorkRecord>()->serialized().size() >
            (*b)->getTypedData<message::WorkRecord>()->serialized().size();
}


void Processor::maybeAllowSteal(const std::string& messageBuffer) {
    WorkTimer stealTimer(this, Processor::TIME_SYSTEM);
    message::StealRing sr(messageBuffer);
    if (JOB_STREAM_DEBUG >= 3) {
        fprintf(stderr, "%i checking steal ring...\n", this->getRank());
    }

    //This method requires heavy manipulation of workInQueue, so keep it locked
    Lock lock(this->mutex);

    //Go through our queue and see if we need work or can donate work.
    std::deque<MpiMessageList::iterator> stealable;
    int unstealableWork = 0;
    auto iter = this->workInQueue.begin();
    for (; iter != this->workInQueue.end();
            iter++) {
        if ((*iter)->tag == Processor::TAG_WORK) {
            stealable.push_back(iter);
        }
        else if ((*iter)->tag == Processor::TAG_REDUCE_WORK) {
            unstealableWork += 1;
        }
    }

    //Can we donate work?
    bool iNeedWork = false;
    if (stealable.size() == 0) {
        if (unstealableWork == 0) {
            iNeedWork = true;
        }
    }
    else if (!this->sendRequests.empty()) {
        //We don't want to donate any work if we're already sending some.
    }
    else {
        //Does anyone else need work?
        int wsize = this->world.size();
        int rank = this->getRank();
        int needWork = 0;
        for (int i = rank - 1, m = rank - wsize; i > m; --i) {
            int r = i;
            if (r < 0) {
                r += wsize;
            }
            if (sr.needsWork[r]) {
                needWork += 1;
            }
        }

        if (needWork > 0) {
            //Sort stealable work from least desirable to most desirable
            //TODO - broken by unique_ptr instead of shared_ptr, bleh.
            //std::sort(stealable.begin(), stealable.end(), sortWorkBySize);

            int stealTop = stealable.size();
            int stealSlice = stealTop / needWork;
            if (unstealableWork < stealSlice) {
                stealSlice = (stealTop + unstealableWork) / (needWork + 1);
            }
            int stealBottom = std::max(0, stealTop - stealSlice * needWork);
            for (int i = rank - 1, m = rank - wsize; i > m; --i) {
                int r = i;
                if (r < 0) {
                    r += wsize;
                }
                if (!sr.needsWork[r]) continue;

                //Give work to this rank!
                sr.needsWork[r] = false;

                message::GroupMessage msgOut;
                int stealNext = std::min(stealTop, stealBottom + stealSlice);
                for (int j = stealBottom; j < stealNext; j++) {
                    const MpiMessage& msg = **stealable[j];
                    msgOut.add(msg.tag, msg.serialized());
                    this->workInQueue.erase(stealable[j]);
                }
                stealBottom = stealNext;

                this->_nonBlockingSend(
                        message::Header(Processor::TAG_GROUP, r),
                        msgOut.serialized());

                if (stealBottom == stealTop) {
                    break;
                }
            }
        }
    }

    //Forward the steal ring!
    sr.needsWork[this->getRank()] = iNeedWork;
    this->_nonBlockingSend(
            message::Header(Processor::TAG_STEAL, this->_getNextRank()),
            sr.serialized());

    if (JOB_STREAM_DEBUG >= 3) {
        fprintf(stderr, "%i done with steal ring\n", this->getRank());
    }
}


void Processor::_enqueueInputWork(const std::string& line) {
    //All input goes straight to the root module by default...
    std::string data;

    try {
        data = this->root->parseAndSerialize(line);
    }
    catch (const std::exception& e) {
        fprintf(stderr, "While processing %s\n", line.c_str());
        throw e;
    }
    std::vector<std::string> inputDest;
    this->addWork(std::unique_ptr<message::WorkRecord>(
            new message::WorkRecord(inputDest, std::move(data))));
}


void Processor::processInputThread_main(const std::string& inputLine) {
    try {
        if (inputLine.empty()) {
            //Read trimmed lines from stdin, use those as input.
            std::string line;
            while (!std::cin.eof()) {
                std::getline(std::cin, line);
                boost::algorithm::trim(line);
                if (!line.empty()) {
                    this->_enqueueInputWork(line);
                }
            }
        }
        else {
            this->_enqueueInputWork(inputLine);
        }

        //All input handled if we reach here, start a quit request
        this->sawEof = true;
    }
    catch (const std::exception& e) {
        fprintf(stderr, "Caught exception in inputThread: %s\n", e.what());
        throw;
    }
}


job::JobBase* Processor::allocateJob(module::Module* parent, 
        const std::string& id, const YAML::Node& config) {
    std::string type;
    if (!config["type"]) {
        //It's a module
        type = "module";
    }
    else {
        type = config["type"].as<std::string>();
    }

    auto allocatorIter = jobTypeMap().find(type);
    if (allocatorIter == jobTypeMap().end()) {
        std::ostringstream msg;
        msg << "Unknown job type: " << type;
        throw std::runtime_error(msg.str());
    }

    auto job = allocatorIter->second();
    const YAML::Node* globalConfig;
    if (this->root == 0) {
        //we're allocating the root
        globalConfig = &config;
    }
    else {
        globalConfig = &this->root->getConfig();
    }
    job->setup(this, parent, id, config, *globalConfig);
    if (!this->root) {
        this->root = std::unique_ptr<job::JobBase>(job);
    }
    job->postSetup();
    return job;
}


job::ReducerBase* Processor::allocateReducer(module::Module* parent,
        const YAML::Node& config) {
    std::string type;
    const YAML::Node* realConfig = &config;
    YAML::Node blank;
    if (config.IsScalar()) {
        type = config.as<std::string>();
        realConfig = &blank;
    }
    else {
        type = config["type"].as<std::string>();
    }

    auto allocatorIter = reducerTypeMap().find(type);
    if (allocatorIter == reducerTypeMap().end()) {
        std::ostringstream msg;
        msg << "Unknown reducer type: " << type;
        throw std::runtime_error(msg.str());
    }

    auto reducer = allocatorIter->second();
    reducer->setup(this, parent, "output", *realConfig, 
            this->root->getConfig());
    reducer->postSetup();
    return reducer;
}


void Processor::_checkMpi() {
    //Send any pending messages (outside of TIME_COMM to allow for serialization
    //to count against job_stream...
    std::unique_ptr<message::_Message> msg;
    while (this->messageQueue.pop(msg)) {
        this->_nonBlockingSend(std::move(*msg));
    }

    WorkTimer sendTimer(this, Processor::TIME_COMM);

    //Fill any outbound buffers
    auto it = this->sendRequests.begin();
    while (it != this->sendRequests.end()) {
        if (it->request.test()) {
            Lock lock(this->mutex);
            this->sendRequests.erase(it++);
        }
        else {
            it++;
        }
    }

    //See if we have any messages; while we do, load them.
    this->tryReceive();
}


void Processor::decrReduceChildTag(uint64_t reduceTag, bool wasProcessed) {
    Lock lock(this->mutex);

    auto& ri = this->reduceInfoMap[reduceTag];
    if (wasProcessed) {
        ri.countProcessed += 1;
    }
    ri.childTagCount -= 1;
    if (ri.childTagCount == 0) {
        //This ring is released; go ahead and prioritize any work that was
        //waiting on this ring.
        auto it = this->workInQueue.begin();
        for (auto& m : ri.messagesWaiting) {
            this->workInQueue.insert(it, std::move(m));
        }
        ri.messagesWaiting.clear();
    }
}


void Processor::forceCheckpoint() {
    if (this->checkpointFileName.empty()) {
        throw std::runtime_error("Cannot forceCheckpoint() with no checkpoint "
                "file specified!  Use -c");
    }
    this->messageQueue.push(std::unique_ptr<message::_Message>(
            new message::_Message(message::Header(
                Processor::TAG_CHECKPOINT_FORCE, 0), "")));
}


void Processor::handleRingTest(MpiMessagePtr message) {
    //Handle a ring test check in either tryReceive or the main work loop.
    message::DeadRingTestMessage& dm = *message->getTypedData<
            message::DeadRingTestMessage>();

    //Most ring test handling functionality (dispatchDone aside) requires access
    //to our reduceInfoMap
    Lock lock(this->mutex);

    //Set to true to add our work to the message and forward it to next in loop
    bool forward = false;
    ProcessorReduceInfo& reduceInfo = this->reduceInfoMap[dm.reduceTag];

    //Step one - is something holding this queue back?
    if (reduceInfo.childTagCount) {
        //Hold onto this ring test until we no longer have activity holding this
        //ring back.
        reduceInfo.messagesWaiting.push_back(std::move(message));
    }
    else {
        //Add our counts into this message.  We will either forward or process
        //this ring.
        dm.processed += reduceInfo.countProcessed;
        dm.created += reduceInfo.countCreated;

        //If we're not the sentry, we just forward it and we're done.
        forward = true;
        if (dm.sentryRank == this->getRank()) {
            //The pass is done and we're in a worker thread.
            dm.pass += 1;
            if (JOB_STREAM_DEBUG) {
                fprintf(stderr, "Dead ring %lu pass %i - %lu / %lu / %lu\n",
                        dm.reduceTag, dm.pass, dm.processed, dm.created,
                        dm.createdLast);
            }

            if (dm.createdLast == dm.created && dm.created == dm.processed) {
                //This ring is officially done (unless recurrence happens)
                if (dm.reduceTag == 0) {
                    //Global
                    this->shouldRun = false;
                    forward = false;
                }
                else {
                    //Unlock so that done() handling can happen in parallel
                    this->mutex.unlock();
                    bool noRecur = reduceInfo.reducer->dispatchDone(
                            dm.reduceTag);
                    this->mutex.lock();

                    if (noRecur) {
                        uint64_t parentTag = reduceInfo.parentTag;
                        this->decrReduceChildTag(parentTag);

                        auto it = this->reduceInfoMap.find(dm.reduceTag);
                        this->reduceInfoMap.erase(it);

                        forward = false;
                    }

                    //If there was recurrence, then the fact that we're
                    //forwarding the ring test will cause it to be reset.
                    //The created messages will stop the test from dying before
                    //they have been processed.
                }

                if (!forward) {
                    //This ring is dead, tell everyone to remove it.
                    this->messageQueue.push(
                            std::unique_ptr<message::_Message>(
                                new message::_Message(
                                    message::Header(
                                        Processor::TAG_DEAD_RING_IS_DEAD,
                                        this->_getNextRank()),
                                    dm.serialized())));

                    if (JOB_STREAM_DEBUG) {
                        fprintf(stderr, "Dead ring %lu took %lu ms\n",
                                dm.reduceTag,
                                message::Location::getCurrentTimeMs()
                                    - dm.tsTestStarted);
                    }
                }
            }

            if (forward) {
                //Ring still alive; reset ring test
                dm.createdLast = dm.created;
                dm.created = 0;
                dm.processed = 0;
            }
        }
    }

    if (forward) {
        //Add our work count and happily forward it along
        this->messageQueue.push(std::unique_ptr<message::_Message>(
                new message::_Message(
                    message::Header(Processor::TAG_DEAD_RING_TEST,
                        this->_getNextRank()),
                    dm.serialized())));
    }
}


void Processor::process(MpiMessagePtr message) {
    int tag = message->tag;
    if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        auto& wr = *message->getTypedData<message::WorkRecord>();
        wr.markStarted();

        //A WorkRecord might get rerouted to a different reduce tag than it
        //starts, so we'll need to remember the original tag to decrement
        uint64_t oldReduceTag = wr.getReduceTag();

        try {
            this->root->dispatchWork(wr);
            this->decrReduceChildTag(oldReduceTag, true);
        }
        catch (const std::exception& e) {
            this->decrReduceChildTag(oldReduceTag);
            std::ostringstream ss;
            ss << "While processing work with target: ";
            const std::vector<std::string>& target = wr.getTarget();
            for (int i = 0, m = target.size(); i < m; i++) {
                if (i != 0) {
                    ss << "::";
                }
                ss << target[i];
            }
            fprintf(stderr, "%s\n", ss.str().c_str());
            throw;
        }

        if (JOB_STREAM_DEBUG) {
            Lock lock(this->mutex);
            auto& it = this->reduceInfoMap[wr.getReduceTag()];
            fprintf(stderr, "REDUCE %lu DOWN TO %lu\n", wr.getReduceTag(),
                    it.childTagCount);
        }
    }
    else if (tag == Processor::TAG_DEAD_RING_TEST) {
        this->handleRingTest(std::move(message));
    }
    else if (tag == Processor::TAG_STEAL) {
        //This must be handled in the main thread; send it to ourselves.  It
        //was probably laid over to a work thread so that a checkpoint could
        //go through.
        this->messageQueue.push(std::unique_ptr<message::_Message>(
            new message::_Message(
                message::Header(tag, this->getRank()), message->serialized())));
    }
    else {
        std::ostringstream ss;
        ss << "Unrecognized tag: " << tag;
        throw std::runtime_error(ss.str());
    }

    //This thread is no longer working
    {
        Lock lock(this->mutex);
        this->workingCount -= 1;
    }
}


bool Processor::tryReceive() {
    WorkTimer recvTimer(this, Processor::TIME_COMM);

    bool wasSteal = false;
    std::string stealBuffer;

    if (!this->recvRequest) {
        this->recvRequest = this->world.irecv(mpi::any_source, mpi::any_tag,
                this->recvBuffer);
    }

    boost::optional<mpi::status> recvMaybe = this->recvRequest->test();
    if (!recvMaybe) {
        //Nothing received yet
        return false;
    }

    //We got a message!
    mpi::status recv = *recvMaybe;
    { //Scope for processing the received message
        WorkTimer systemTimer(this, Processor::TIME_SYSTEM);

        message::_Message& msg = this->recvBuffer;
        int tag = msg.header.tag;

        //NOTE - The way boost::mpi receives into strings corrupts the 
        //string.  That's why we copy it here with a substr() call.
        if (JOB_STREAM_DEBUG >= 3) {
            fprintf(stderr, "%i got message len %lu\n", this->getRank(),
                    (unsigned long)msg.buffer.size());
        }

        this->msgsByTag[tag] += 1;

        if (this->getRank() != msg.header.dest) {
            //Forward it to the proper destination.  This code isn't used, it
            //is layover from some code for a different distribution method that
            //was being trialed.  It's left in because dest is still a part of
            //header.
            this->_nonBlockingSend(std::move(msg));
        }
        else if (tag == Processor::TAG_STEAL) {
            //Since maybeAllowSteal() can result in a tryReceive call when
            //it uses _nonBlockingSend, we have to start the next request
            //before processing.
            wasSteal = true;
            std::swap(stealBuffer, msg.buffer);
        }
        else if (tag == Processor::TAG_GROUP) {
            message::GroupMessage group;
            serialization::decode(msg.buffer, group);
            Lock lock(this->mutex);
            for (int i = 0, m = group.messageTags.size(); i < m; i++) {
                this->workInQueue.emplace_back(new MpiMessage(
                        group.messageTags[i],
                        std::move(group.messages[i])));
            }
        }
        else if (tag == Processor::TAG_DEAD_RING_TEST) {
            MpiMessagePtr mpiMessage(new MpiMessage(tag,
                    std::move(msg.buffer)));
            //We'll handle it now if we're not the sentry, or put it in our
            //work queue if we are.
            if (mpiMessage->getTypedData<message::DeadRingTestMessage>()
                    ->sentryRank != this->getRank()
                    && this->checkpointState == Processor::CHECKPOINT_NONE) {
                this->handleRingTest(std::move(mpiMessage));
            }
            else {
                this->workInQueue.emplace_back(std::move(mpiMessage));
            }
        }
        else if (tag == Processor::TAG_DEAD_RING_IS_DEAD) {
            message::DeadRingTestMessage dm(msg.buffer);
            if (dm.sentryRank != this->getRank()) {
                Lock lock(this->mutex);
                if (this->reduceInfoMap[dm.reduceTag].messagesWaiting.size()) {
                    throw std::runtime_error("Shouldn't have any messages "
                            "still waiting on a dead ring!");
                }
                this->reduceInfoMap.erase(dm.reduceTag);

                if (dm.reduceTag == 0) {
                    this->shouldRun = false;
                }

                //Forward so it dies on all ranks
                this->_nonBlockingSend(
                        message::Header(Processor::TAG_DEAD_RING_IS_DEAD,
                            _getNextRank()), 
                        std::move(msg.buffer));
            }
        }
        else if (tag == Processor::TAG_CHECKPOINT_NEXT) {
            //We must be in either CHECKPOINT_NONE or CHECKPOINT_SYNC
            if (JOB_STREAM_DEBUG >= 1) {
                fprintf(stderr, "%i Got TAG_CHECKPOINT_NEXT\n",
                        this->getRank());
            }
            if (this->checkpointState == Processor::CHECKPOINT_NONE) {
                this->checkpointState = Processor::CHECKPOINT_START;
            }
            else if (this->checkpointState == Processor::CHECKPOINT_SYNC) {
                this->checkpointState = Processor::CHECKPOINT_SEND;
            }
            else {
                std::ostringstream buf;
                buf << "Got TAG_CHECKPOINT_NEXT out of step: "
                        << this->checkpointState;
                throw std::runtime_error(buf.str());
            }
            if (JOB_STREAM_DEBUG >= 1) {
                fprintf(stderr, "%i checkpointState -> %i\n", this->getRank(),
                        this->checkpointState);
            }
        }
        else if (tag == Processor::TAG_CHECKPOINT_READY) {
            if (JOB_STREAM_DEBUG >= 1) {
                fprintf(stderr, "%i Got TAG_CHECKPOINT_READY\n",
                        this->getRank());
            }
            if (this->getRank() != 0) {
                throw std::runtime_error(
                        "I'm not rank 0, got TAG_CHECKPOINT_READY?");
            }
            this->_updateCheckpointSync(recv.source());
        }
        else if (tag == Processor::TAG_CHECKPOINT_DATA) {
            if (JOB_STREAM_DEBUG >= 1) {
                fprintf(stderr, "%i Got TAG_CHECKPOINT_DATA\n",
                        this->getRank());
            }
            if (this->getRank() != 0) {
                throw std::runtime_error(
                        "I'm not rank 0, got TAG_CHECKPOINT_DATA?");
            }
            if (this->checkpointState != Processor::CHECKPOINT_GATHER) {
                throw std::runtime_error(
                        "Got TAG_CHECKPOINT_DATA info without being in state "
                            "CHECKPOINT_GATHER");
            }

            fprintf(stderr, "%i knows all about %i!\n", this->getRank(),
                    recv.source());
            serialization::encode(*this->checkpointAr, (int)recv.source());
            serialization::encode(*this->checkpointAr, msg.buffer);
            this->checkpointWaiting -= 1;
        }
        else if (tag == Processor::TAG_CHECKPOINT_FORCE) {
            if (this->getRank() != 0) {
                throw std::runtime_error(
                        "I'm not rank 0, got TAG_CHECKPOINT_FORCE?");
            }

            Lock lock(this->mutex);

            //If we're not already checkpointing...
            if (this->checkpointState == Processor::CHECKPOINT_NONE) {
                this->checkpointNext = 0;
                //Simulate a 1ms step to start the checkpoint
                this->_updateCheckpoints(1);
            }

        }
        else {
            Lock lock(this->mutex);
            this->workInQueue.emplace_back(new MpiMessage(tag, 
                    std::move(msg.buffer)));
        }
    }

    //std::move() with a buffer doesn't always seem to swap them, so make
    //sure that the next payload has a new home
    std::string nextBuffer;
    std::swap(nextBuffer, this->recvBuffer.buffer);
    this->recvRequest = this->world.irecv(mpi::any_source, mpi::any_tag, 
            this->recvBuffer);

    if (wasSteal && this->shouldRun) {
        if (this->checkpointState == Processor::CHECKPOINT_NONE) {
            this->maybeAllowSteal(stealBuffer);
        }
        else {
            this->workInQueue.emplace_front(new MpiMessage(Processor::TAG_STEAL,
                    std::move(stealBuffer)));
        }
    }

    return true;
}


void Processor::_assertMain() {
    if (this->mainThreadId != std::this_thread::get_id()) {
        throw std::runtime_error("Not called in main thread!");
    }
}


int Processor::_getNextRank() {
    return (this->world.rank() + 1) % this->world.size();
}


int Processor::_getNextWorker() {
    if (this->workTarget < 0) {
        //First input work special case, ensures ring 1 triggers on rank 0
        this->workTarget = 0;
        return 0;
    }
    //Tends to be slower
    //return rand() % this->world.size();
    int size = (int)this->world.size();
    this->workTarget = (int)((this->workTarget + 1) % size);
    return this->workTarget;
}


void Processor::_nonBlockingSend(message::Header header,
        std::string payload) {
    this->_nonBlockingSend(message::_Message(std::move(header),
            std::move(payload)));
}


void Processor::_nonBlockingSend(message::_Message&& msg){
    //Send non-blocking (though we don't leave this function until our message
    //is sent) so that we don't freeze the application waiting for the send
    //buffer.
    this->_assertMain();
    int dest = msg.header.dest;

    //Don't count isend as TIME_COMM, since it mostly includes the serialization
    //time and shouldn't count towards MPI time.
    //WorkTimer timer(this, Processor::TIME_COMM);
    mpi::request request = this->world.isend(dest, msg.header.tag, msg);
    this->sendRequests.emplace_back(request);
}


void Processor::_pushWorkTimer(ProcessorTimeType timeType) {
    this->workTimers.emplace_back(message::Location::getCurrentTimeMs(),
            this->_getThreadCpuTimeMs(), timeType);
}


void Processor::_popWorkTimer() {
    uint64_t clksSpent = this->_getThreadCpuTimeMs();
    uint64_t timeSpent = message::Location::getCurrentTimeMs();
    Processor::_WorkTimerRecord& record = this->workTimers.back();
    clksSpent -= record.clkStart;
    timeSpent -= record.tsStart;

    this->localClksByType[record.timeType] += clksSpent - record.clksChild;
    this->localTimesByType[record.timeType] += timeSpent - record.timeChild;
    this->workTimers.pop_back();

    int m = this->workTimers.size() - 1;
    if (m >= 0) {
        Processor::_WorkTimerRecord& parent = this->workTimers[m];
        parent.timeChild += timeSpent;
        parent.clksChild += clksSpent;
    }
}


uint64_t Processor::_getThreadCpuTimeMs() {
    rusage clksTime;
    getrusage(RUSAGE_THREAD, &clksTime);
    uint64_t val = clksTime.ru_utime.tv_sec * 1000 + clksTime.ru_utime.tv_usec / 1000;
    return val;
}


void Processor::_updateCheckpoints(int msDiff) {
    Lock lock(this->mutex);

    if (this->checkpointState == Processor::CHECKPOINT_NONE) {
        if (this->checkpointNext >= 0) {
            this->checkpointNext -= msDiff;
            if (this->checkpointNext < 0) {
                this->checkpointNext = -1;
                fprintf(stderr, "%i Checkpoint starting\n", this->getRank());
                for (int i = 1, m = this->world.size(); i < m; i++) {
                    this->_nonBlockingSend(
                            message::Header(Processor::TAG_CHECKPOINT_NEXT, i),
                            "");
                }
                this->tsCheckpointStart = message::Location::getCurrentTimeMs();
                //Waiting on all processors to sync up
                this->checkpointWaiting = this->world.size();
                this->checkpointState = Processor::CHECKPOINT_START;
            }
        }
        //otherwise disabled
    }
    else if (this->checkpointState == Processor::CHECKPOINT_START) {
        //We are a processor who has been asked to stop work and report our
        //state to processor 0 in order to generate a checkpoint.
        Lock lock(this->mutex);
        if (this->workingCount == 0 && this->sendRequests.size() == 0) {
            this->_nonBlockingSend(message::Header(
                    Processor::TAG_CHECKPOINT_READY, 0), "");
            //Now we just wait for everyone to enter this state
            this->checkpointState = Processor::CHECKPOINT_SYNC;
            if (JOB_STREAM_DEBUG >= 1) {
                fprintf(stderr, "%i checkpointState -> %i\n", this->getRank(),
                        this->checkpointState);
            }
        }
    }
    else if (this->checkpointState == Processor::CHECKPOINT_SYNC) {
        //Waiting for everyone to enter and stay in this state for 1 second;
        //root node will inform us with a TAG_CHECKPOINT_NEXT message.
        if (this->getRank() == 0 && this->checkpointWaiting == 0) {
            if (this->checkpointNext < -Processor::CHECKPOINT_SYNC_WAIT_MS) {
                //OK, everything's synced.  Get the checkpoint data
                for (int i = 1, m = this->world.size(); i < m; i++) {
                    this->_nonBlockingSend(message::Header(
                            Processor::TAG_CHECKPOINT_NEXT, i), "");
                }
                this->checkpointWaiting = this->world.size() - 1;
                this->checkpointFile.reset(new std::ofstream(
                        this->checkpointFileName.c_str()));
                this->checkpointAr.reset(new serialization::OArchive(
                        *this->checkpointFile));
                serialization::encode(*this->checkpointAr,
                        (int)this->world.size());
                serialization::encode(*this->checkpointAr,
                        (int)this->getRank());
                //Encode ourselves into a buffer, then put that in the file;
                //this is analogous to how we receive messages from other
                //processors.
                serialization::encode(*this->checkpointAr,
                        serialization::encode(*this));
                this->checkpointState = Processor::CHECKPOINT_GATHER;
                if (JOB_STREAM_DEBUG >= 1) {
                    fprintf(stderr, "%i checkpointState -> %i\n",
                            this->getRank(), this->checkpointState);
                }
            }
            else {
                this->checkpointNext -= msDiff;
            }
        }
    }
    else if (this->checkpointState == Processor::CHECKPOINT_SEND) {
        //Encode our state and transmit it to the root node.
        if (JOB_STREAM_DEBUG >= 1) {
            fprintf(stderr, "%i checkpoint sending state, resuming "
                    "computation\n", this->getRank());
        }
        this->_nonBlockingSend(message::Header(Processor::TAG_CHECKPOINT_DATA,
                0), "");
        //Begin working again.
        this->checkpointState = Processor::CHECKPOINT_NONE;
    }
    else if (this->checkpointState == Processor::CHECKPOINT_GATHER) {
        if (this->checkpointWaiting == 0) {
            //Done
            this->checkpointAr.reset();
            this->checkpointFile.reset();
            fprintf(stderr, "%i Checkpoint took %i ms, resuming computation\n",
                    this->getRank(), message::Location::getCurrentTimeMs()
                        - this->tsCheckpointStart);
            //Begin the process again
            this->checkpointState = Processor::CHECKPOINT_NONE;
            this->checkpointNext = this->checkpointInterval;
            //WHAT!?!??!
            exit(12);
        }
    }
}


void Processor::_updateCheckpointSync(int rank) {
    this->checkpointWaiting -= 1;
    this->checkpointNext = -1;
}

} //processor
} //job_stream
