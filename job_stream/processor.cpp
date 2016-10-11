
#include "job.h"
#include "job_stream.h"
#include "message.h"
#include "module.h"
#include "processor.h"
#include "workerThread.h"

#include "debug_internals.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <ctime>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <sys/resource.h>

#if defined(__APPLE__)
    #include <mach/mach_init.h>
    #include <mach/thread_act.h>
    #include <mach/mach_port.h>
#endif

namespace mpi = boost::mpi;

namespace std {
    void swap(job_stream::processor::AnyUniquePtr& p1,
            job_stream::processor::AnyUniquePtr& p2) {
        p1.swapWith(p2);
    }
}

namespace YAML {
    job_stream::Mutex GuardedNode::mutex;
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

thread_local Processor::_WorkerInfoList::iterator Processor::workerWorkThisThread;
thread_local std::vector<Processor::_WorkTimerRecord> Processor::workTimers;
thread_local std::unique_ptr<uint64_t[]> Processor::localClksByType;
thread_local std::unique_ptr<uint64_t[]> Processor::localTimesByType;

extern const int JOB_STREAM_DEBUG = 0;
const int NO_STEALING = 0;


/** STATICS **/
const int DEFAULT_CHECKPOINT_SYNC_WAIT_MS = 10000;
int Processor::CHECKPOINT_SYNC_WAIT_MS
        = DEFAULT_CHECKPOINT_SYNC_WAIT_MS;
int cpuCount = -1;
std::vector<std::string> initialWork;


/** Returns the current (1 min avg) system load */
double getCurrentLoad() {
    double load;
    int r = getloadavg(&load, 1);
    ASSERT(r >= 1, "Failed to get load average?");
    return load;
}


void Processor::addJob(const std::string& typeName,
        std::function<job::JobBase* ()> allocator) {
    JobTypeMapType& jtm = jobTypeMap();
    if (jtm.count("module") == 0) {
        //initialize map with values
        jtm["module"] = module::Module::make;
    }

    if (jtm.count(typeName) != 0) {
        fprintf(stderr, "WARNING: Job type already defined: %s\n",
                typeName.c_str());
    }

    jtm[typeName] = allocator;
}


void Processor::addReducer(const std::string& typeName,
        std::function<job::ReducerBase* ()> allocator) {
    ReducerTypeMapType& rtm = reducerTypeMap();
    if (rtm.count(typeName) != 0) {
        fprintf(stderr, "WARNING: Reducer type already defined: %s\n",
                typeName.c_str());
    }

    rtm[typeName] = allocator;
}
/** END STATICS **/



MpiMessage::MpiMessage(int tag, AnyUniquePtr data) : tag(tag),
        data(std::move(data)) {}


MpiMessage::MpiMessage(int tag, std::string&& message) : tag(tag),
        encodedMessage(message) {
}


std::string MpiMessage::getSerializedData() const {
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
        throw std::runtime_error("MpiMessage::getSerializedData() didn't find tag");
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


void Processor::_WorkerInfo::lockForWork() {
    if (!this->isLocked) {
        this->isLocked = 1;
        this->p->_mutex.lock();
    }
}



Processor::Processor(std::shared_ptr<mpi::environment> env,
        mpi::communicator world,
        const std::string& configStr, const std::string& checkpointFile)
            : checkpointFileName(checkpointFile), checkpointQuit(false),
              depthFirst(true), reduceTagCount(0), env(env),
              sentEndRing0(false), wasRestored(0), world(world),
              _configStr(configStr), _stealEnabled(true) {
    //Step 1 - steal queued initialWork, so that if we throw an error, it
    //does not get repeated without being requeued.
    this->initialWork = job_stream::processor::initialWork;
    job_stream::processor::initialWork.clear();

    if (world.size() >= (1lu << message::WorkRecord::TAG_ADDRESS_BITS)) {
        throw std::runtime_error("MPI world too large.  See TAG_ADDRESS_BITS "
                "in message.h");
    }

    //Register our base types so that we can create checkpoints
    serialization::registerType<job::SharedBase, job::SharedBase>();
    serialization::registerType<job::JobBase, job::SharedBase>();
    serialization::registerType<job::ReducerBase, job::SharedBase>();
    serialization::registerType<module::Module, job::JobBase, job::SharedBase>();

    { //Configure JobLog header
        std::ostringstream info;
        info << this->getRank() << ":" << boost::asio::ip::host_name()
                << ":" << getpid();
        JobLog::header = info.str();
    }

    YAML::Node config = YAML::Load(configStr);
    //Set up our globalConfig GuardedNode.
    this->globalConfig.set(config);

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

            ASSERT(this->_configStr == configStr,
                    "Checkpoint file '" << this->checkpointFileName << "' is "
                    "from a different config file");

            //Populate config for all of our jobs
            this->root->parent = 0;
            this->root->processor = this;
            job::ReducerReallocMap reducerRemap;
            this->root->populateAfterRestore(&this->globalConfig, config,
                    reducerRemap);

            //Remap our reduction info based on the remapping
            for (auto it = this->reduceInfoMap.begin();
                    it != this->reduceInfoMap.end(); it++) {
                if (it->second.reducer) {
                    auto it2 = reducerRemap.find(it->second.reducer);
                    ASSERT(it2 != reducerRemap.end(),
                            "Reducer not remapped?  " << it->second.reducer);
                    it->second.reducer = it2->second;
                }
            }

            //Put work that was in progress back at the top of the queue
            for (auto& m : this->workerWork) {
                MpiMessagePtr ptr;
                serialization::decode(m.work, ptr);
                //Gets incremented on work start, was incremented at checkpoint
                //time
                if (m.hasReduceTag) {
                    ASSERT(this->reduceInfoMap[m.reduceTag].childTagCount > 0,
                            "Bad tag count: "
                            << this->reduceInfoMap[m.reduceTag].childTagCount);
                    this->_decrReduceChildTag(m.reduceTag);
                }
                this->workInQueue.emplace_front(std::move(ptr));
            }
            this->workerWork.clear();

            //Reset steal ring to re-sense environment
            if (this->_stealMessage) {
                this->_stealMessage.reset(new MpiMessage(Processor::TAG_STEAL,
                        message::StealRing(this->world.size()).serialized()));
            }

            this->wasRestored = true;
            JobLog() << "resumed from checkpoint (" << this->workInQueue.size()
                    << " pending messages)";
            if (JOB_STREAM_DEBUG) {
                for (auto m = this->workInQueue.begin(),
                        e = this->workInQueue.end(); m != e; m++) {
                    JobLog() << "  message " << (*m)->tag;
                }
            }
        }
    }

    if (!this->root && !config["__isCheckpointProcessorOnly"]) {
        //Generate this->root from config
        this->allocateJob(NULL, "jobs", config);
    }

    //Turn off checkpointing for all processors by default (re-enabled for
    //processor 0 in run()).
    this->checkpointNext = -1;
    this->checkpointInterval = 600 * 1000;
    this->checkpointState = Processor::CHECKPOINT_NONE;
}


Processor::~Processor() {
}


struct _BsFixForBoostSerialization {
    _BsFixForBoostSerialization(job::JobBase* root,
            std::map<uint64_t, ProcessorReduceInfo>& reduceInfoMap)
            : r(*root), map(reduceInfoMap) {}

private:
    job::JobBase& r;
    std::map<uint64_t, ProcessorReduceInfo>& map;

    friend class boost::serialization::access;
    void serialize(serialization::OArchive& ar, const unsigned int version) {
        serialization::encodeAsPtr(ar, this->r);
        serialization::encode(ar, this->map);
    }
};


void Processor::populateCheckpointInfo(CheckpointInfo& info,
        const std::string& buffer) {
    //Load them into us
    info.totalBytes += buffer.size();
    serialization::decode(buffer, *this);

    //Sample the rootSize, which includes reducers, jobs, etc.
    uint64_t rootSize = serialization::encodeAsPtr(*this->root).size();
    info.jobTreeSize += rootSize;
    //We have to package reduceInfoMap with job tree for measuring, so that
    //we can subtract the size of the job::ReducerBase's in the tree (which
    //are also pointed to by reduceInfoMap, but we don't want to double count
    //them).
    info.reduceMapSize += serialization::encode(
            _BsFixForBoostSerialization(this->root.get(),
                this->reduceInfoMap))
            .size() - rootSize;

    info.messagesWaiting += this->workInQueue.size();
    for (MpiMessagePtr& m : this->workInQueue) {
        info.countByTag[m->tag] += 1;
        info.bytesByTag[m->tag] += m->getSerializedData().size();
        if (m->tag == Processor::TAG_WORK
                || m->tag == Processor::TAG_REDUCE_WORK) {
            auto* ptr = m->getTypedData<message::WorkRecord>();
            info.totalUserBytes += ptr->getWorkSize();
        }
        else if (m->tag == Processor::TAG_GROUP) {
            auto* group = m->getTypedData<message::GroupMessage>();
            for (int i = 0, im = group->messageTags.size(); i < im; i++) {
                if (group->messageTags[i] == Processor::TAG_WORK) {
                    MpiMessage wr(group->messageTags[i],
                            std::move(group->messages[i]));
                    auto* ptr = wr.getTypedData<message::WorkRecord>();
                    info.totalUserBytes += ptr->getWorkSize();
                }
            }
        }
    }

    if (this->_stealMessage) {
        info.stealRing.reset(new message::StealRing(
                this->_stealMessage->getSerializedData()));
    }
}


void Processor::registerReduceReset(job::ReducerBase* rb, uint64_t reduceTag) {
    this->workerWorkThisThread->reduceMapResets.emplace_back(rb, reduceTag);
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
    std::string hostName;

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->pctUserTime;
        ar & this->pctUserCpu;
        ar & this->userCpuTotal;
        ar & this->pctMpiTime;
        ar & this->msgsTotal;
        ar & this->pctUserMsgs;
        ar & this->hostName;
    }
};


void Processor::run(const std::string& inputLine) {
    this->mainThreadId = std::this_thread::get_id();
    this->sawEof = false;
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
        bool usingStdin = false;
        if (!this->wasRestored) {
            if (inputLine.empty() && this->initialWork.size() == 0) {
                usingStdin = true;
            }

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
        if (this->checkpointFileName.empty()) {
            this->checkpointInterval = -1;
        }
        else if (usingStdin && isatty(fileno(stdin))) {
            //Checkpoints disabled for interactive mode
            JobLog() << "Checkpoints disabled; launched in interactive mode ("
                    "input is stdin and not a pipe)";
            this->checkpointFileName = "";
            this->checkpointInterval = -1;
        }
        else {
            //Using checkpoints
            JobLog() << "Using " << this->checkpointFileName << " as "
                    "checkpoint file";
        }
        this->checkpointNext =  this->checkpointInterval;
    }
    else {
        this->sawEof = true;
        this->sentEndRing0 = true;
    }

    //Start up our workers
    unsigned int compute = std::max(1u, std::thread::hardware_concurrency());
    //All processors start with 0 workers active, which is changed when the
    //steal ring passes by
    this->workersActive = 0;
    for (unsigned int i = 0; i < compute; i++) {
        this->workers.emplace_back(new WorkerThread(this, i));
    }
    if (!this->_stealEnabled || NO_STEALING) {
        this->workersActive = this->workers.size();

        //No stealing; therefore, we have as many CPUs as we have.  Use
        //compute.
        cpuCount = compute;
    }

    //Begin tallying time spent in system vs user functionality.
    std::unique_ptr<WorkTimer> outerTimer(new WorkTimer(this,
            Processor::TIME_IDLE));

    try {
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
                    this->_startRingTest(0, 0, 0);
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

            //Check for errors; we don't re-raise until the end of this function
            if (this->workerErrors.size() != 0) {
                this->shouldRun = false;
            }

            //Check external control signals
            if (this->checkExternalSignals && this->checkExternalSignals()) {
                this->shouldRun = false;
            }
        }
    }
    catch (...) {
        //If an exception happens in the main thread, then we will catch it and
        //join our threads before re-raising.  Otherwise, for whatever reason,
        //e.g. the python interpreter does not properly exit.
        this->joinThreads();
        throw;
    }

    //Stop all threads
    this->joinThreads();

    //Stop timer, report on user vs not
    outerTimer.reset();
    this->localTimersMerge();

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
    myInfo.hostName = boost::asio::ip::host_name();

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
            //Don't make JobLog, we're reporting on someone else
            fprintf(stderr,
                    "%i_%s %i%% user time (%i%% mpi), %i%% user cpu, "
                        "%lu messages (%i%% user)\n",
                    i, pi.hostName.c_str(),
                    pi.pctUserTime / 10, pi.pctMpiTime / 10, pi.pctUserCpu / 10,
                    pi.msgsTotal, pi.pctUserMsgs / 10);
        }
        //Don't make JobLog, reporting on whole cluster
        fprintf(stderr, "C %i%% user time, %i%% user cpu, "
                "quality %.2f cpus, ran %.3fs\n",
                totalTime / 10, totalCpuTime / 10000,
                (double)totalCpu / timesTotal, timesTotal * 0.001);
    }

    //Did we stop because of an error?  Rethrow it!
    if (this->workerErrors.size() != 0) {
        std::rethrow_exception(this->workerErrors[0]);
    }
}


void Processor::startRingTest(uint64_t reduceTag, uint64_t parentTag,
        job::ReducerBase* reducer) {
    this->workerWorkThisThread->outboundRings.emplace_back(reduceTag, parentTag,
            reducer);
}


void Processor::addWork(std::unique_ptr<message::WorkRecord> wr) {
    this->workerWorkThisThread->outboundWork.emplace_back(std::move(wr));
}


void Processor::_addWork(std::unique_ptr<message::WorkRecord> wr) {
    Lock lock(this->_mutex);
    this->reduceInfoMap[wr->getReduceTag()].countCreated += 1;
    this->workOutQueue.push(std::move(wr));
}


void Processor::_distributeWork(std::unique_ptr<message::WorkRecord> wr) {
    //We don't need workerWorkThisThread->lockForWork() since this always
    //happens in main.
    this->_assertMain();

    int dest, rank = this->getRank();
    int tag = Processor::TAG_WORK;
    const std::vector<std::string>& target = wr->getTarget();
    if (target.size() > 1 && target[target.size() - 2] == "output") {
        //Reduced output for top-level reducer
        ASSERT(target.size() == 2, "Top-level reducer output not top level? "
                << target.size());
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
    else if (this->_stealEnabled && !NO_STEALING) {
        //Stealing does a good job of distributing work
        dest = rank;
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

    if (dest != rank) {
        this->_nonBlockingSend(
                message::Header(tag, dest), wr->serialized());
    }
    else {
        this->_lockAndAddWork(MpiMessagePtr(new MpiMessage(tag,
                std::move(wr))));
    }
}


uint64_t Processor::getNextReduceTag() {
    uint64_t result = 0x0;
    uint64_t countBits = (64 - message::WorkRecord::TAG_ADDRESS_BITS);
    uint64_t countMask = (1lu << countBits) - 1;

    //Even though this changes state that is saved by a checkpoint, it's a
    //volatile value, so we don't take the main processor lock.
    //If the checkpoint value for this saved out is used in a generated ring,
    //then that ring will still not go into the checkpoint because the processor
    //lock will have been maintained the entire time.  Ergo, were the checkpoint
    //to be restored, there would be no inconsistency anymore.
    Lock lock(this->_mutexForReduceTags);

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
    Lock lock(this->_mutex);
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
    //Always happens in the main thread, so that we can avoid the most costly
    //operations with workerWorkThisThread
    this->_assertMain();

    WorkTimer stealTimer(this, Processor::TIME_SYSTEM);
    message::StealRing sr(messageBuffer);
    if (JOB_STREAM_DEBUG >= 3) {
        JobLog() << "checking steal ring...";
    }

    //This method requires heavy manipulation of workInQueue, so keep it locked
    Lock lock(this->_mutex);

    int rank = this->getRank();
    sr.capacity[rank] = this->workers.size();
    sr.slots[rank] = this->workersActive;
    sr.work[rank] = this->workerWork.size();
    sr.load[rank] = getCurrentLoad();

    if (!this->_stealEnabled || NO_STEALING) {
        this->workersActive = this->workers.size();
    }
    else {

    //Go through our queue and see if we need work or can donate work.
    std::deque<MpiMessageList::iterator> stealable;
    int unstealableWork = 0;
    auto iter = this->workInQueue.begin();
    for (; iter != this->workInQueue.end();
            iter++) {
        if ((*iter)->tag == Processor::TAG_WORK) {
            stealable.push_back(iter);
            sr.work[rank]++;
        }
        else if ((*iter)->tag == Processor::TAG_REDUCE_WORK) {
            unstealableWork += 1;
            sr.work[rank]++;
        }
    }

    //Calculate our number of slots based on work in the system
    int wsize = this->world.size();
    bool ringActive = true;
    for (int i = 0; i < wsize; i++) {
        if (sr.capacity[i] == 0) {
            ringActive = false;
            break;
        }
    }

    bool ringSlotOnly = false;
    int totalSlots = 0;
    int totalWork = 0;
    std::vector<int> assignedSlots(wsize);
    //Tracks # of messages possessed - messages expected (based on slots)
    std::vector<int> workOverflow(wsize);
    if (ringActive) {
        //Figure out our ideal number of slots
        std::vector<double> projectedOverload(wsize);
        for (int i = 0; i < wsize; i++) {
            totalWork += sr.work[i];
            assignedSlots[i] = 0;
            //Take load attributable to us out of the equation
            int slotWork = std::min(sr.capacity[i], sr.work[i]);
            projectedOverload[i] = (sr.load[i] - slotWork) / sr.capacity[i];
            //Always round up - that is, if we have one processor with a high
            //capacity and one with low capacity, we want the one with high
            //capacity to be chosen.  So, bias the projected overload as though
            //each machine already has more work
            projectedOverload[i] += 1.0 / sr.capacity[i];
        }

        //Work always goes to least overloaded
        //Bias is there to make sure that if the steal ring is taking awhile
        //(lots of network traffic), stuff still gets processed.  Downside is
        //that it's not the ideal balance of work.
        //TODO - bias is OK... but ideally we'd just keep high-size work on
        //its home reducer node.  That's part of the issue with stealing.  AND
        //we'd steal away "wouldReduce" work.
        int bias = 10;
        for (int i = 0; i < totalWork+bias; i++) {
            double least = 1e300;
            int leastRank = -1;
            for (int k = 0; k < wsize; k++) {
                if (projectedOverload[k] < least) {
                    least = projectedOverload[k];
                    leastRank = k;
                }
            }

            projectedOverload[leastRank] += 1.0 / sr.capacity[leastRank];
            assignedSlots[leastRank]++;
            totalSlots++;
        }

        //Calculate needs; totalSlots == totalWork!
        for (int i = 0; i < wsize; i++) {
            workOverflow[i] = sr.work[i] - assignedSlots[i];
        }

        //TODO - should this / above calculation be throttled by capacity?  That
        //may automatically be handled by the division by capacity above, this
        //may keep small machines from taking on too much work if we don't
        //limit machines.
        if (assignedSlots[rank] != this->workersActive) {
            if (JOB_STREAM_DEBUG >= 3) {
                JobLog() << "Upgrading workers from " << this->workersActive
                        << " to " << assignedSlots[rank] << " (load "
                        << sr.load[rank] << " / " << sr.capacity[rank] << ")";
            }
            this->workersActive = assignedSlots[rank];
            sr.slots[rank] = this->workersActive;
        }
    }

    //Do we know how many CPUs are in cluster?
    if (cpuCount < 0 && ringActive) {
        int newCount = 0;
        for (int i = 0; i < wsize; i++) {
            newCount += sr.capacity[i];
        }
        cpuCount = newCount;
    }

    //Can we donate work?
    if (!ringActive) {
        //We don't have all initialization information yet, don't start work
    }
    else if (stealable.size() == 0) {
        //We can't donate any work, and our slots are already calculated.
        //Do nothing more!
    }
    else if (!this->sendRequests.empty()) {
        //We don't want to donate any work if we're already sending some.
    }
    else if (workOverflow[rank] > 0) {
        if (JOB_STREAM_DEBUG >= 3) {
            JobLog() << "Allowing steal, my work " << sr.work[rank] << " > max "
                    << assignedSlots[rank] << " (total slots " << totalSlots
                    << ")";
        }

        //TODO - Sort stealable so that smaller messages (less transfer
        //overhead) get sent first

        int stealBottom = 0;
        while (workOverflow[rank] > 0 && stealBottom < stealable.size()) {
            int mostNeedy = -1;
            int mostNeed = 0;
            for (int i = 0; i < wsize; i++) {
                if (workOverflow[i] < mostNeed) {
                    mostNeed = workOverflow[i];
                    mostNeedy = i;
                }
            }

            if (mostNeedy < 0) {
                break;
            }

            //Give work to mostNeedy!
            message::GroupMessage msgOut;
            while (workOverflow[rank] > 0 && workOverflow[mostNeedy] < 0
                    && stealBottom < stealable.size()) {
                const MpiMessage& msg = **stealable[stealBottom];
                msgOut.add(msg.tag, msg.getSerializedData());
                this->workInQueue.erase(stealable[stealBottom++]);
                workOverflow[rank]--;
                workOverflow[mostNeedy]++;
                sr.work[rank]--;
                sr.work[mostNeedy]++;
            }

            this->_nonBlockingSend(
                    message::Header(Processor::TAG_GROUP, mostNeedy),
                    msgOut.serialized());
        }
    }

    } //DISABLE_STEAL

    //Forward the steal ring!
    this->_nonBlockingSend(
            message::Header(Processor::TAG_STEAL, this->_getNextRank()),
            sr.serialized());

    if (JOB_STREAM_DEBUG >= 3) {
        JobLog() << "done with steal ring";
    }
}


void Processor::_enqueueInputWork(const std::string& line) {
    //All input goes straight to the root module by default...
    std::string data;

    try {
        data = this->root->parseAndSerialize(line);
    }
    catch (const std::exception& e) {
        fprintf(stderr, "While processing input line: %s\n", line.c_str());
        throw;
    }
    std::vector<std::string> inputDest;
    this->_addWork(std::unique_ptr<message::WorkRecord>(
            new message::WorkRecord(inputDest, std::move(data))));
}


void Processor::processInputThread_main(const std::string& inputLine) {
    try {
        //If input is not interactive, block checkpoints until ALL input is
        //in the system.  Otherwise checkpoints aren't exactly valid (might
        //resume and "complete" even though only half of input was imported
        //into system).
        if (!this->checkpointFileName.empty()) {
            //TODO - This should not lock persay, but rather enqueue any
            //checkpoint request.  That is, work should continue, but no
            //checkpoints until all input is spooled.
            this->_mutex.lock();
        }

        if (!inputLine.empty()) {
            //Use string of inputLine as initial work
            this->_enqueueInputWork(inputLine);
        }
        else if (this->initialWork.size() != 0) {
            //Initial work was set; send that
            for (int i = 0, m = this->initialWork.size(); i < m; i++) {
                std::vector<std::string> inputDest;
                this->_addWork(std::unique_ptr<message::WorkRecord>(
                        new message::WorkRecord(inputDest,
                            std::move(this->initialWork[i]))));
            }
        }
        else {
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

        //All input handled if we reach here, start a quit request
        this->sawEof = true;
        if (!this->checkpointFileName.empty()) {
            this->_mutex.unlock();
        }
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
        msg << "Unknown job type (did you instantiate an instance?): " << type;
        throw std::runtime_error(msg.str());
    }

    auto job = allocatorIter->second();
    job->allocationName = type;
    job->setup(this, parent, id, config, &this->globalConfig);
    if (!this->root) {
        this->root = std::unique_ptr<job::JobBase>(job);
    }
    job->postSetup();
    return job;
}


job::JobBase* Processor::allocateJobForDeserialize(const std::string& typeId) {
    auto allocatorIter = jobTypeMap().find(typeId);
    ASSERT(allocatorIter != jobTypeMap().end(), "Unknown job type: "
            << typeId);

    job::JobBase* r = allocatorIter->second();
    return r;
}


job::ReducerBase* Processor::allocateReducer(module::Module* parent,
        const YAML::Node& config) {
    std::string type;
    const YAML::Node* realConfig = &config;
    YAML::Node blank;
    if (config.IsScalar()) {
        type = config.as<std::string>();
        blank["type"] = YAML::Clone(config);
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
    reducer->allocationName = type;
    reducer->setup(this, parent, "output", *realConfig,
            &this->globalConfig);
    reducer->postSetup();
    return reducer;
}


job::ReducerBase* Processor::allocateReducerForDeserialize(
        const std::string& typeId) {
    auto allocatorIter = reducerTypeMap().find(typeId);
    ASSERT(allocatorIter != reducerTypeMap().end(), "Unknown reducer type: "
            << typeId);

    job::ReducerBase* r = allocatorIter->second();
    return r;
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
            Lock lock(this->_mutex);
            this->sendRequests.erase(it++);
        }
        else {
            it++;
        }
    }

    //See if we have any messages; while we do, load them.
    this->tryReceive();
}


void Processor::_decrReduceChildTag(uint64_t reduceTag, bool wasProcessed) {
    //NOTE - _MUTEX MUST ALREADY BE LOCKED WHEN CALLED!!!
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

    if (JOB_STREAM_DEBUG) {
        JobLog() << "REDUCE " << reduceTag << " DOWN TO "
                << ri.childTagCount;
    }
}


void Processor::forceCheckpoint(bool forceQuit) {
    if (this->checkpointFileName.empty()) {
        throw std::runtime_error("Cannot forceCheckpoint() with no checkpoint "
                "file specified!  Use -c");
    }
    std::string payload = "";
    if (forceQuit) {
        payload = "q";
    }
    this->workerWorkThisThread->outboundMessages.emplace_back(
            new message::_Message(message::Header(
                Processor::TAG_CHECKPOINT_FORCE, 0), payload));
}


void Processor::handleDeadRing(MpiMessagePtr msg) {
    message::DeadRingTestMessage& dm = *msg->getTypedData<
            message::DeadRingTestMessage>();
    if (dm.sentryRank != this->getRank()) {
        this->workerWorkThisThread->lockForWork();
        if (this->reduceInfoMap[dm.reduceTag].messagesWaiting.size()) {
            throw std::runtime_error("Shouldn't have any messages "
                    "still waiting on a dead ring!");
        }
        this->reduceInfoMap.erase(dm.reduceTag);

        if (dm.reduceTag == 0) {
            this->shouldRun = false;
        }

        //Forward so it dies on all ranks (technically, since we're in a lock,
        //we don't have to use outboundMessages, but for consistency we will
        this->workerWorkThisThread->outboundMessages.emplace_back(
                new message::_Message(
                    message::Header(Processor::TAG_DEAD_RING_IS_DEAD,
                        _getNextRank()),
                    //Minor inefficiency since we didn't actually change dm, but
                    //oh well.
                    std::move(dm.serialized())));
    }
}


void Processor::handleRingTest(MpiMessagePtr message) {
    //Handle a ring test check in either tryReceive or the main work loop.
    message::DeadRingTestMessage& dm = *message->getTypedData<
            message::DeadRingTestMessage>();

    //Most ring test handling functionality (dispatchDone aside) requires access
    //to our reduceInfoMap
    this->workerWorkThisThread->lockForWork();

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
                JobLog() << "Dead ring " << dm.reduceTag << " pass "
                        << dm.pass << " - " << dm.processed << " / "
                        << dm.created << " / " << dm.createdLast;
            }

            if (dm.createdLast == dm.created && dm.created == dm.processed) {
                //This ring is officially done (unless recurrence happens)
                uint64_t dmTestStarted = dm.tsTestStarted;
                uint64_t dmTag = dm.reduceTag;
                std::string dmSerialized = dm.serialized();
                if (dm.reduceTag == 0) {
                    //Global
                    this->shouldRun = false;
                    forward = false;
                }
                else {
                    //Unlock so that done() handling can happen in parallel
                    job::ReducerBase* reduceInfoReducer = reduceInfo.reducer;
                    uint64_t parentTag = reduceInfo.parentTag;
                    //For better parallelism, we unlock our mutex during
                    //dispatchDone (which calls user code).  In exchange, this
                    //code path invalidates reduceInfo, and must not change ANY
                    //state saved in a checkpoint until we take the lock back.
                    this->_mutex.unlock();
                    bool noRecur = reduceInfoReducer->dispatchDone(dmTag);
                    this->_mutex.lock();

                    if (noRecur) {
                        //Purge the entry from the reducer's records
                        reduceInfoReducer->purgeDeadRing(dmTag);

                        this->_decrReduceChildTag(parentTag);

                        auto it = this->reduceInfoMap.find(dmTag);
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
                    this->workerWorkThisThread->outboundMessages.emplace_back(
                                new message::_Message(
                                    message::Header(
                                        Processor::TAG_DEAD_RING_IS_DEAD,
                                        this->_getNextRank()),
                                    dmSerialized));

                    if (JOB_STREAM_DEBUG) {
                        JobLog() << "Dead ring " << dmTag
                                << " took "
                                << message::Location::getCurrentTimeMs()
                                    - dmTestStarted
                                << " ms";
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
        this->workerWorkThisThread->outboundMessages.emplace_back(
                new message::_Message(
                    message::Header(Processor::TAG_DEAD_RING_TEST,
                        this->_getNextRank()),
                    dm.serialized()));
    }
}


bool Processor::processInThread(int workerId) {
    //Get work and initialize this->workerWorkThisThread
    MpiMessagePtr message = this->_getWork(workerId);
    if (!message) {
        return false;
    }

    //NOTE - IT IS MANDATORY that no network communication, or any type of
    //state-changing work, happens only within the context of
    //this->workerWorkThisThread.
    bool wasWork = false;
    uint64_t workReduceTag = 0;

    int tag = message->tag;
    std::exception_ptr error;
    //Catch-all to make sure cleanup happens even if we have an error
    try {
        if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
            auto& wr = *message->getTypedData<message::WorkRecord>();
            wr.markStarted();

            wasWork = true;
            workReduceTag = wr.getReduceTag();

            try {
                this->root->dispatchWork(wr);
            }
            catch (const std::exception& e) {
                JobLog log;
                log << "While processing work with target: ";
                const std::vector<std::string>& target = wr.getTarget();
                for (int i = 0, m = target.size(); i < m; i++) {
                    if (i != 0) {
                        log << "::";
                    }
                    log << target[i];
                }
                throw;
            }
        }
        else if (tag == Processor::TAG_DEAD_RING_IS_DEAD) {
            this->handleDeadRing(std::move(message));
        }
        else if (tag == Processor::TAG_DEAD_RING_TEST) {
            this->handleRingTest(std::move(message));
        }
        else if (tag == Processor::TAG_STEAL) {
            //This must be handled in the main thread; send it to ourselves.  It
            //was probably laid over to a work thread so that a checkpoint could
            //go through.
            this->workerWorkThisThread->outboundMessages.emplace_back(
                    new message::_Message(
                        message::Header(tag, this->getRank()),
                            message->getSerializedData()));
        }
        else {
            ERROR("Unrecognized tag: " << tag);
        }
    }
    catch (...) {
        error = std::current_exception();
        //We set shouldRun to false here to accomplish the following:
        //1. Abort other threads as soon as possible
        //2. Since our cleanup takes our lock, we guarantee that no
        //   checkpoints will occur between cleanup and the rethrowing
        //   of an error.
        this->shouldRun = false;
    }

    {
        //Cleanup work, start rings, commit to send.  By acquiring the lock
        //we ensure that no checkpoint is happening (and as a bonus, wait for
        //any pending checkpoint to finish).
        //Note - We _MUST_ do this step even if an error occurred!  Otherwise,
        //reduction locks that were acquired will not be released.
        Lock lock(this->_mutex);
        for (auto& m : this->workerWorkThisThread->outboundRings) {
            this->_startRingTest(m.reduceTag, m.parentTag, m.reducer);
        }
        for (auto& m : this->workerWorkThisThread->outboundWork) {
            this->_addWork(std::move(m));
        }
        for (auto& m : this->workerWorkThisThread->outboundMessages) {
            this->messageQueue.push(std::move(m));
        }
        for (auto& m : this->workerWorkThisThread->reduceMapResets) {
            m.reducer->purgeCheckpointReset(m.reduceTag);
        }
        if (this->workerWorkThisThread->hasReduceTag) {
            this->_decrReduceChildTag(this->workerWorkThisThread->reduceTag,
                    true);
        }

        if (JOB_STREAM_DEBUG >= 1) {
            JobLog() << "Done with work in this thread, " << tag;
        }

        //All done, was it locked for work?
        if (this->workerWorkThisThread->isLocked) {
            this->_mutex.unlock();
        }

        this->workerWork.erase(this->workerWorkThisThread);
        this->workerWorkThisThread = Processor::_WorkerInfoList::iterator();
    }

    if (error) {
        std::rethrow_exception(error);
    }

    return true;
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
            JobLog() << "got message len " << msg.buffer.size();
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
            //Direct mutex lock is OK here, since the message is DONE once
            //we emplace these into workInQueue
            Lock lock(this->_mutex);
            for (int i = 0, m = group.messageTags.size(); i < m; i++) {
                this->workInQueue.emplace_back(new MpiMessage(
                        group.messageTags[i],
                        std::move(group.messages[i])));
            }
        }
        else if (tag == Processor::TAG_DEAD_RING_TEST) {
            MpiMessagePtr mpiMessage(new MpiMessage(tag,
                    std::move(msg.buffer)));
            //Just drop it in the work queue, which is the completion of the
            //receive operation.
            Lock lock(this->_mutex);
            this->workInQueue.emplace_front(std::move(mpiMessage));
        }
        else if (tag == Processor::TAG_DEAD_RING_IS_DEAD) {
            MpiMessagePtr mpiMessage(new MpiMessage(tag,
                    std::move(msg.buffer)));
            //Just drop it in the work queue, which is the completion of the
            //receive operation.
            Lock lock(this->_mutex);
            this->workInQueue.emplace_front(std::move(mpiMessage));
        }
        else if (tag == Processor::TAG_CHECKPOINT_NEXT) {
            //We must be in either CHECKPOINT_NONE or CHECKPOINT_SYNC
            if (JOB_STREAM_DEBUG >= 1) {
                JobLog() << "got TAG_CHECKPOINT_NEXT";
            }
            if (this->checkpointState == Processor::CHECKPOINT_NONE) {
                this->_mutex.lock();
                this->checkpointState = Processor::CHECKPOINT_SYNC;
                this->checkpointNext = 0;
            }
            else if (this->checkpointState == Processor::CHECKPOINT_SYNC) {
                this->checkpointState = Processor::CHECKPOINT_GATHER;
                this->checkpointWaiting = 1;
            }
            else if (this->checkpointState == Processor::CHECKPOINT_GATHER) {
                //All checkpoints received, resume computation.
                this->checkpointState = Processor::CHECKPOINT_NONE;
                this->_mutex.unlock();
            }
            else {
                std::ostringstream buf;
                buf << "Got TAG_CHECKPOINT_NEXT out of step: "
                        << this->checkpointState;
                throw std::runtime_error(buf.str());
            }
            if (JOB_STREAM_DEBUG >= 1) {
                JobLog() << "checkpointState -> " << this->checkpointState;
            }
        }
        else if (tag == Processor::TAG_CHECKPOINT_READY) {
            if (JOB_STREAM_DEBUG >= 1) {
                JobLog() << "got TAG_CHECKPOINT_READY from "
                        << (int)recv.source();
            }
            if (this->getRank() != 0) {
                throw std::runtime_error(
                        "I'm not rank 0, got TAG_CHECKPOINT_READY?");
            }
            this->checkpointWaiting -= 1;
        }
        else if (tag == Processor::TAG_CHECKPOINT_DATA) {
            if (JOB_STREAM_DEBUG >= 1) {
                JobLog() << "got TAG_CHECKPOINT_DATA";
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

            if (JOB_STREAM_DEBUG >= 1) {
                JobLog() << "got checkpoint data from " << recv.source();
            }
            serialization::encode(*this->checkpointAr, (int)recv.source());
            serialization::encode(*this->checkpointAr, msg.buffer);
            this->checkpointWaiting -= 1;
        }
        else if (tag == Processor::TAG_CHECKPOINT_FORCE) {
            if (this->getRank() != 0) {
                throw std::runtime_error(
                        "I'm not rank 0, got TAG_CHECKPOINT_FORCE?");
            }

            Lock lock(this->_mutex);

            //If we're not already checkpointing...
            if (this->checkpointState == Processor::CHECKPOINT_NONE) {
                this->checkpointNext = 0;
            }

            //Force quit?
            if (msg.buffer.size() != 0 && msg.buffer[0] == 'q') {
                this->checkpointQuit = true;
            }

        }
        else {
            this->_lockAndAddWork(MpiMessagePtr(new MpiMessage(tag,
                    std::move(msg.buffer))));
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
            Lock lock(this->_mutex);
            this->_stealMessage.reset(new MpiMessage(Processor::TAG_STEAL,
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


MpiMessagePtr Processor::_getWork(int workerId) {
    MpiMessagePtr ptr;
    //Note, acquiring our mutex ensures that no checkpoint operation is
    //occurring
    Lock lock(this->_mutex);

    ASSERT(this->checkpointState == Processor::CHECKPOINT_NONE, "Checkpoints "
            "should be disallowed within mutex?");

    if (this->_stealMessage) {
        //Steal message ALWAYS has priority
        std::swap(ptr, this->_stealMessage);
    }
    else if (this->workInQueue.empty()) {
        //No work to do!  Return the empty pointer.
        return ptr;
    }
    else if (this->workersActive <= workerId) {
        auto mtag = this->workInQueue.front()->tag;
        if (mtag == Processor::TAG_WORK || cpuCount < 0) {
            //If we're disabled, we can't take on work that could be stolen
            //by someone else or requires an active slot.
            //
            //Also applies to cpuCount < 0, indicating that the ring is not yet
            //initialized.
            return ptr;
        }
    }

    //This function is called from the thread that will be doing the work, so
    //initialize our worker stuff
    ASSERT(this->workerWorkThisThread == Processor::_WorkerInfoList::iterator(),
            "Thread already has current work?");
    this->workerWork.emplace_front();
    this->workerWorkThisThread = this->workerWork.begin();
    this->workerWorkThisThread->p = this;

    //If ptr is null at this point, we need to take first work from workInQueue.
    //Otherwise, it is steal ring.
    if (!ptr) {
        ptr = std::move(this->workInQueue.front());
        this->workInQueue.pop_front();
    }

    this->workerWorkThisThread->work = serialization::encode(ptr);
    //Before letting others get work, we want to ensure that a ring test
    //knows we're in progress.  This is just an optimization (it blocks the
    //ring test from being sent around since we _KNOW_ it's not done).
    if (ptr->tag == Processor::TAG_WORK
            || ptr->tag == Processor::TAG_REDUCE_WORK) {
        uint64_t reduceTag = ptr->getTypedData<message::WorkRecord>()
                ->getReduceTag();
        auto& rim = this->reduceInfoMap[reduceTag];
        //Stop ring tests from progressing
        rim.childTagCount += 1;
        this->workerWorkThisThread->hasReduceTag = true;
        this->workerWorkThisThread->reduceTag = reduceTag;
    }
    if (JOB_STREAM_DEBUG >= 1) {
        JobLog jl;
        jl << message::Location::getCurrentTimeMs() << " processing tag "
                << ptr->tag;
        if (ptr->tag == Processor::TAG_WORK
                || ptr->tag == Processor::TAG_REDUCE_WORK) {
            jl << ", path: jobs";
            for (const std::string& s
                    : ptr->getTypedData<message::WorkRecord>()->getTarget()
                    ) {
                jl << "::" << s;
            }
            jl << " ON " << ptr->getTypedData<message::WorkRecord>()
                    ->getReduceTag();
        }
    }

    return ptr;
}


void Processor::_lockAndAddWork(MpiMessagePtr msg) {
    Lock lock(this->_mutex);
    if (this->depthFirst) {
        this->workInQueue.emplace_front(std::move(msg));
    }
    else {
        this->workInQueue.emplace_back(std::move(msg));
    }
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
    uint64_t wt = message::Location::getCurrentTimeMs();
    uint64_t ct = this->_getThreadCpuTimeMs();
    this->workTimers.emplace_back(wt, ct, timeType);
}


void Processor::_popWorkTimer() {
    uint64_t clksSpent = this->_getThreadCpuTimeMs();
    uint64_t timeSpent = message::Location::getCurrentTimeMs();
    Processor::_WorkTimerRecord& record = this->workTimers.back();
    clksSpent -= record.clkStart;
    timeSpent -= record.tsStart;

    //_getThreadCpuTimeMs() more accurate that getCurrentTimeMs, use that when
    //needed.
    timeSpent = std::max(timeSpent, clksSpent);

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


void Processor::_modifyWorkTimer(ProcessorTimeType timeType,
        uint64_t wallTimeMs, uint64_t cpuTimeMs) {
    //Want:
    // * Parent timers to ignore the provided times
    // * the immediate parent to skip over provided time
    ASSERT(wallTimeMs >= cpuTimeMs, "Input was not normalized to account "
            "for resolution differences? "
            << wallTimeMs << " < " << cpuTimeMs);

    uint64_t wallNow = message::Location::getCurrentTimeMs();
    uint64_t cpuNow = this->_getThreadCpuTimeMs();
    Processor::_WorkTimerRecord& record = this->workTimers.back();

    //Change record to be of type TIME_SYSTEM; essentially, this function is
    //only used to splice in the actual user time from another thread.
    ASSERT(record.timeType == Processor::TIME_USER,
            "Was not a USER timer that is being modified? "
            << record.timeType);
    record.timeType = Processor::TIME_SYSTEM;

    //Adjust clksChild and timeChild to max of current minus
    uint64_t maxWt, maxCt;
    maxWt = wallNow - (record.tsStart + record.timeChild);
    maxCt = cpuNow - (record.clkStart + record.clksChild);
    record.timeChild += std::min(maxWt, wallTimeMs);
    record.clksChild += std::min(maxCt, cpuTimeMs);

    //Integrate time
    this->localClksByType[timeType] += cpuTimeMs;
    this->localTimesByType[timeType] += wallTimeMs;
}


void Processor::_startRingTest(uint64_t reduceTag, uint64_t parentTag,
        job::ReducerBase* reducer) {
    Lock lock(this->_mutex);

    if (reduceTag != 0 && reduceTag != 1
            && this->reduceInfoMap.count(reduceTag) != 0) {
        ERROR("Ring already existed? " << reduceTag);
    }

    ProcessorReduceInfo& pri = this->reduceInfoMap[reduceTag];
    pri.reducer = reducer;
    pri.parentTag = parentTag;

    if (parentTag != reduceTag) {
        //Stop the parent from being closed until the child is closed.
        this->reduceInfoMap[parentTag].childTagCount += 1;
        //childTagCount and countCreated both start at zero, since the ring test
        //is started after the initial work.
    }

    if (JOB_STREAM_DEBUG) {
        JobLog() << "Starting ring test on " << reduceTag << " (from "
                << parentTag << ") - " << pri.countProcessed << " / "
                << pri.countCreated;
    }

    auto m = message::DeadRingTestMessage(this->world.rank(),
            reduceTag);
    this->messageQueue.push(std::unique_ptr<message::_Message>(
            new message::_Message(
                message::Header(Processor::TAG_DEAD_RING_TEST,
                    this->_getNextRank()), m.serialized())));
}


uint64_t Processor::_getThreadCpuTimeMs() {
    uint64_t val = 0;
    #if defined(__linux__) || defined(unix) || defined(__unix__)
        rusage clksTime;
        getrusage(RUSAGE_THREAD, &clksTime);
        val = (clksTime.ru_utime.tv_sec * 1000
                + clksTime.ru_utime.tv_usec / 1000
                + clksTime.ru_stime.tv_sec * 1000
                + clksTime.ru_stime.tv_usec / 1000);
    #elif defined(__APPLE__)
        //mac os x
        mach_port_t thread;
        kern_return_t kr;
        mach_msg_type_number_t count;
        thread_basic_info_data_t info;

        thread = mach_thread_self();
        count = THREAD_BASIC_INFO_COUNT;
        kr = thread_info(thread, THREAD_BASIC_INFO, (thread_info_t)&info,
                &count);
        if (kr == KERN_SUCCESS) {
            val = (info.user_time.seconds * 1000
                    + info.user_time.microseconds / 1000
                    + info.system_time.seconds * 1000
                    + info.system_time.microseconds / 1000);
        }

        mach_port_deallocate(mach_task_self(), thread);
    #endif //OS CHECKS
    return val;
}


void Processor::_updateCheckpoints(int msDiff) {
    //NOTE - While a checkpoint is in progress, the processor's mutex STAYS
    //LOCKED IN THE MAIN THREAD!  This makes other operations that would change
    //our state (such as finishing of jobs) wait until the checkpoint is over.
    if (this->checkpointState == Processor::CHECKPOINT_NONE) {
        if (this->checkpointNext >= 0) {
            this->checkpointNext -= msDiff;
            if (this->checkpointNext < 0) {
                this->checkpointNext = 0;
                //Lock that mutex up.
                this->_mutex.lock();

                JobLog() << "Checkpoint starting";
                for (int i = 1, m = this->world.size(); i < m; i++) {
                    this->_nonBlockingSend(
                            message::Header(Processor::TAG_CHECKPOINT_NEXT, i),
                            "");
                }
                this->tsCheckpointStart = message::Location::getCurrentTimeMs();
                //Waiting on all processors to sync up
                this->checkpointWaiting = this->world.size();
                this->checkpointState = Processor::CHECKPOINT_SYNC;
            }
        }
        //otherwise disabled
    }
    else if (this->checkpointState == Processor::CHECKPOINT_SYNC) {
        //Waiting for everyone to enter and stay in this state for 1 second;
        //root node will inform us with a TAG_CHECKPOINT_NEXT message.
        if (this->checkpointNext >= -Processor::CHECKPOINT_SYNC_WAIT_MS) {
            if (this->messageQueue.size() != 0
                    || this->sendRequests.size() != 0) {
                this->checkpointNext = 0;
            }
            else {
                this->checkpointNext -= msDiff;
            }

            if (this->checkpointNext < -Processor::CHECKPOINT_SYNC_WAIT_MS) {
                //Our sync is complete.  Alert node 0, which will tell us when
                //to send our state.

                this->_nonBlockingSend(message::Header(
                        Processor::TAG_CHECKPOINT_READY, 0), "");
            }
        }
        else {
            //Probably invalid... since we're sending above.  Should work
            //with that though.
            ASSERT(this->messageQueue.size() == 0
                    && this->sendRequests.size() == 0, "While syncing got a "
                    << "message: " << this->messageQueue.size() << ", "
                    << this->sendRequests.size());

            if (this->getRank() == 0 && this->checkpointWaiting == 0) {
                JobLog() << "Checkpoint activity synced after "
                        << message::Location::getCurrentTimeMs()
                            - this->tsCheckpointStart
                        << "ms, including mandatory "
                        << Processor::CHECKPOINT_SYNC_WAIT_MS
                        << "ms quiet period";

                //OK, everything's synced.  Get the checkpoint data
                for (int i = 1, m = this->world.size(); i < m; i++) {
                    this->_nonBlockingSend(message::Header(
                            Processor::TAG_CHECKPOINT_NEXT, i), "");
                }
                this->checkpointWaiting = this->world.size() - 1;
                this->checkpointFile.reset(new std::ofstream(
                        (this->checkpointFileName + ".new").c_str()));
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
                    JobLog() << "checkpointState -> " << this->checkpointState;
                }
            }
        }
    }
    else if (this->checkpointState == Processor::CHECKPOINT_GATHER) {
        if (this->getRank() != 0) {
            if (this->checkpointWaiting == 1) {
                this->checkpointWaiting = 0;
                if (JOB_STREAM_DEBUG >= 1) {
                    JobLog() << "checkpoint sending state, resuming computation";
                }
                this->_nonBlockingSend(message::Header(
                        Processor::TAG_CHECKPOINT_DATA, 0),
                        serialization::encode(*this));
            }
        }
        else if (this->checkpointWaiting == 0) {
            //Done
            this->checkpointAr.reset();
            this->checkpointFile.reset();

            //File rename now that we have a completed checkpoint
            if (std::rename((this->checkpointFileName + ".new").c_str(),
                    this->checkpointFileName.c_str())) {
                ERROR("Failed to rename " << this->checkpointFileName
                        << ".new");
            }

            JobLog() << "Checkpoint took "
                    << message::Location::getCurrentTimeMs()
                        - this->tsCheckpointStart
                    << "ms, resuming computation";
            //Start processing again on this node
            this->checkpointState = Processor::CHECKPOINT_NONE;
            this->checkpointNext = this->checkpointInterval;
            if (this->checkpointQuit) {
                exit(RETVAL_CHECKPOINT_FORCED);
            }

            for (int i = 1, im = this->world.size(); i < im; i++) {
                this->_nonBlockingSend(message::Header(
                        Processor::TAG_CHECKPOINT_NEXT, i), "");
            }

            this->_mutex.unlock();
        }
    }
}


} //processor
} //job_stream
