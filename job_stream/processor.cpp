
#include "job.h"
#include "message.h"
#include "module.h"
#include "processor.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <ctime>
#include <exception>
#include <iostream>
#include <map>
#include <string>

namespace mpi = boost::mpi;

namespace job_stream {
namespace processor {

static std::map<std::string, std::function<job::JobBase* ()> > jobTypeMap;
static std::map<std::string, std::function<job::ReducerBase* ()> > 
        reducerTypeMap;

extern const int JOB_STREAM_DEBUG = 0;

/** STATICS **/
void Processor::addJob(const std::string& typeName, 
        std::function<job::JobBase* ()> allocator) {
    if (jobTypeMap.count("module") == 0) {
        //initialize map with values
        jobTypeMap["module"] = module::Module::make;
    }

    if (jobTypeMap.count(typeName) != 0) {
        std::ostringstream ss;
        ss << "Job type already defined: " << typeName;
        throw std::runtime_error(ss.str());
    }

    jobTypeMap[typeName] = allocator;
}


void Processor::addReducer(const std::string& typeName, 
        std::function<job::ReducerBase* ()> allocator) {
    if (reducerTypeMap.count(typeName) != 0) {
        std::ostringstream ss;
        ss << "Reducer type already defined: " << typeName;
        throw std::runtime_error(ss.str());
    }
    
    reducerTypeMap[typeName] = allocator;
}
/** END STATICS **/



MpiMessage::MpiMessage(int tag, void* data) : tag(tag), data(data) {}


MpiMessage::MpiMessage(int tag, std::string&& message) : tag(tag),
        data(0), encodedMessage(message) {
}


MpiMessage::~MpiMessage() {
    if (!this->data) {
        //ok, never allocated
    }
    else if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        delete (message::WorkRecord*)this->data;
    }
    else if (tag == Processor::TAG_DEAD_RING_TEST
            || tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        delete (message::DeadRingTestMessage*)this->data;
    }
    else {
        throw std::runtime_error("~MpiMessage() didn't find tag?");
    }
}


std::string MpiMessage::serialized() const {
    if (this->data == 0) {
        //Already serialized!
        return this->encodedMessage;
    }

    if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        return ((message::WorkRecord*)this->data)->serialized();
    }
    else if (tag == Processor::TAG_DEAD_RING_TEST
            || tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        return ((message::DeadRingTestMessage*)this->data)->serialized();
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
        this->data = new message::WorkRecord(message);
    }
    else if (this->tag == Processor::TAG_DEAD_RING_TEST
            || this->tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        this->data = new message::DeadRingTestMessage(message);
    }
    else {
        std::ostringstream ss;
        ss << "Unrecognized tag for MpiMessage(): " << tag;
        throw std::runtime_error(ss.str());
    }
}



Processor::Processor(mpi::communicator world, const YAML::Node& config)
        : processInputThread(0), reduceTagCount(0), world(world) {
    if (world.size() >= (1 << message::WorkRecord::TAG_ADDRESS_BITS)) {
        throw std::runtime_error("MPI world too large.  See TAG_ADDRESS_BITS "
                "in message.h");
    }

    //Assigns this->root
    this->allocateJob(NULL, "root", config);
}


Processor::~Processor() {
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
    this->sawEof = false;
    this->sentEndRing0 = false;
    this->shouldRun = true;
    this->clksByType.reset(new uint64_t[Processor::TIME_COUNT]);
    this->timesByType.reset(new uint64_t[Processor::TIME_COUNT]);
    for (int i = 0; i < Processor::TIME_COUNT; i++) {
        this->clksByType[i] = 0;
        this->timesByType[i] = 0;
    }
    this->msgsByTag.reset(new uint64_t[Processor::TAG_COUNT]);
    for (int i = 0; i < Processor::TAG_COUNT; i++) {
        this->msgsByTag[i] = 0;
    }
    this->workTarget = this->world.rank();
    if (this->world.rank() == 0) {
        //The first work message MUST go to rank 0, so that ring 1 ends up there
        this->workTarget = -1;
        this->processInputThread = new boost::thread(boost::bind(
                &Processor::processInputThread_main, this, inputLine));

        //The first processor also starts the steal ring
        this->world.send(this->_getNextRank(), Processor::TAG_STEAL,
                message::StealRing(this->world.size()).serialized());
    }
    else {
        this->sawEof = true;
        this->sentEndRing0 = true;
    }

    int dest, tag;
    message::WorkRecord* work;
    MpiMessage* msg;
    //Begin tallying time spent in system vs user functionality.
    std::unique_ptr<WorkTimer> outerTimer(new WorkTimer(this, 
            Processor::TIME_SYSTEM));
    while (this->shouldRun) {
        //See if we have any messages; while we do, load them.
        this->tryReceive();

        if (!this->sentEndRing0) {
            //Eof found and we haven't sent ring!  Send it
            if (this->sawEof) {
                this->startRingTest(0, 0, 0);
                this->sentEndRing0 = true;
            }

            //Distribute any outbound work
            while (this->workOutQueue.pop(work)) {
                this->addWork(work);
            }
        }

        //Process as much work as we can (trying to send work will result in
        //a tryReceive()...  except when it goes locally)
        if (!this->workInQueue.empty()) {
            msg = &this->workInQueue.front();
            this->process(*msg);
            this->workInQueue.pop_front();
        }
    }
    //Stop timer, report on user vs not
    outerTimer.reset(0);
    uint64_t clksTotal = 0;
    uint64_t timesTotal = 0;
    for (int i = 0; i < Processor::TIME_COUNT; i++) {
        clksTotal += this->clksByType[i];
        timesTotal += this->timesByType[i];
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

    //Stop all threads
    this->joinThreads();

    //Cancel any outstanding request
    if (this->recvRequest) {
        this->world.send(this->world.rank(), Processor::TAG_COUNT,
                std::string());
        this->recvRequest->wait();
    }

    //Let everyone catch up so that partial request has no effect...
    this->world.barrier();

    ProcessorInfo myInfo;
    myInfo.pctUserTime = (int)(100 * this->timesByType[Processor::TIME_USER]
            / timesTotal);
    myInfo.userCpuTotal = this->clksByType[Processor::TIME_USER];
    myInfo.pctMpiTime = (int)(100 * this->timesByType[Processor::TIME_COMM]
            / timesTotal);
    myInfo.pctUserCpu = (int)(100 * this->clksByType[Processor::TIME_USER]
            / clksTotal);
    myInfo.msgsTotal = msgsTotal;
    myInfo.pctUserMsgs = (int)(100 * msgsUser / msgsTotal);

    //Send info to rank 0, which prints out everything in order
    if (this->world.rank() != 0) {
        mpi::gather(this->world, myInfo, 0);
    }
    else {
        std::vector<ProcessorInfo> infos;
        mpi::gather(this->world, myInfo, infos, 0);

        int totalTime = 0;
        int totalCpu = 0;
        for (int i = 0, m = infos.size(); i < m; i++) {
            ProcessorInfo& pi = infos[i];
            totalTime += pi.pctUserTime;
            totalCpu += pi.userCpuTotal;
            fprintf(stderr, 
                    "%i %i%% user time (%i%% mpi), %i%% user cpu, "
                        "%lu messages (%i%% user)\n", 
                    i, pi.pctUserTime, pi.pctMpiTime, pi.pctUserCpu, 
                    pi.msgsTotal, pi.pctUserMsgs);
        }
        fprintf(stderr, "C %i%% user time, quality %.2f ticks/ms\n", totalTime, 
                (double)totalCpu / timesTotal);
    }
}


void Processor::addWork(message::WorkRecord* wr) {
    int dest, rank = this->getRank();
    int tag = Processor::TAG_WORK;
    const std::vector<std::string>& target = wr->getTarget();
    if (target.size() > 0 && target[target.size() - 1] == "output") {
        dest = wr->getReduceHomeRank();
        tag = Processor::TAG_REDUCE_WORK;
    }
    else {
        dest = this->_getNextWorker();
    }

    if (dest != rank) {
        this->_nonBlockingSend(dest, tag, wr->serialized());
        delete wr;
    }
    else {
        this->workInQueue.emplace_back(tag, wr);
    }
}


void Processor::startRingTest(uint64_t reduceTag, uint64_t parentTag, 
        job::ReducerBase* reducer) {
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
    }

    if (JOB_STREAM_DEBUG) {
        fprintf(stderr, "Starting ring test on %lu (from %lu) - %lu\n", 
                reduceTag, parentTag, pri.workCount);
    }

    auto m = message::DeadRingTestMessage(this->world.rank(),
            reduceTag, pri.workCount);
    WorkTimer sendTimer(this, Processor::TIME_COMM);
    this->world.send(this->_getNextRank(), Processor::TAG_DEAD_RING_TEST,
            m.serialized());
}


uint64_t Processor::getNextReduceTag() {
    uint64_t result = 0x0;
    int countBits = (64 - message::WorkRecord::TAG_ADDRESS_BITS);
    uint64_t countMask = (1 << countBits) - 1;

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
        delete this->processInputThread;
        this->processInputThread = 0;
    }
}


void Processor::maybeAllowSteal(const std::string& messageBuffer) {
    message::StealRing sr(messageBuffer);
    if (JOB_STREAM_DEBUG >= 2) {
        printf("%i checking steal ring...\n", this->getRank());
    }

    //Go through our queue and see if we need work or can donate work.
    std::vector<std::list<MpiMessage>::iterator> stealable;
    int unstealableWork = 0;
    for (auto iter = this->workInQueue.begin(); iter != this->workInQueue.end();
            iter++) {
        if (iter->tag == Processor::TAG_WORK) {
            stealable.push_back(iter);
        }
        else if (iter->tag == Processor::TAG_REDUCE_WORK) {
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
    else {
        //Does anyone else need work?
        int wsize = this->world.size();
        int rank = this->getRank();
        int needWork = 0;
        for (int i = 0; i < wsize; i++) {
            if (i == rank) continue;
            if (sr.needsWork[i]) {
                needWork += 1;
            }
        }

        if (needWork > 0) {
            int stealTop = stealable.size();
            int stealSlice = stealTop / needWork;
            if (unstealableWork < stealSlice) {
                stealSlice = (stealTop + unstealableWork) / (needWork + 1);
            }
            int stealBottom = std::max(0, stealTop - stealSlice * needWork);
            for (int i = 0; i < wsize; i++) {
                if (i == rank) continue;
                if (!sr.needsWork[i]) continue;

                //Give work to this rank!
                sr.needsWork[i] = false;

                message::GroupMessage msgOut;
                int stealNext = std::min(stealTop, stealBottom + stealSlice);
                for (int j = stealBottom; j < stealNext; j++) {
                    const MpiMessage& msg = *stealable[j];
                    msgOut.add(msg.tag, msg.serialized());
                    this->workInQueue.erase(stealable[j]);
                }
                stealBottom = stealNext;

                //IMPORTANT - Once we start sending work, tryReceive() may get 
                //called within _nonblockingSend, which might call 
                //maybeAllowSteal again.  So we can only use our local 
                //variables.
                this->_nonBlockingSend(i, Processor::TAG_GROUP, 
                        msgOut.serialized());

                if (stealBottom == stealTop) {
                    break;
                }
            }
        }
    }

    //Forward the steal ring!
    sr.needsWork[this->getRank()] = iNeedWork;
    this->_nonBlockingSend(this->_getNextRank(), Processor::TAG_STEAL,
            sr.serialized());

    if (JOB_STREAM_DEBUG >= 2) {
        printf("%i done with steal ring\n", this->getRank());
    }
}


void Processor::_enqueueInputWork(const std::string& line) {
    //All input goes straight to the root module by default...
    std::vector<std::string> inputDest;
    this->workOutQueue.push(new message::WorkRecord(inputDest,
            this->root->parseAndSerialize(line)));
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

    auto allocatorIter = jobTypeMap.find(type);
    if (allocatorIter == jobTypeMap.end()) {
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

    auto allocatorIter = reducerTypeMap.find(type);
    if (allocatorIter == reducerTypeMap.end()) {
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


void Processor::process(MpiMessage& message) {
    int tag = message.tag;
    if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        auto& wr = *message.getTypedData<message::WorkRecord>();
        wr.markStarted();
        //workCount stops the system from settling while there are still pending
        //messages.
        this->reduceInfoMap[wr.getReduceTag()].workCount += 1;
        if (JOB_STREAM_DEBUG) {
            fprintf(stderr, "REDUCE %lu UP TO %lu\n", wr.getReduceTag(), 
                    this->reduceInfoMap[wr.getReduceTag()].workCount);
        }

        try {
            this->root->dispatchWork(wr);
        }
        catch (const std::exception& e) {
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
    }
    else if (tag == Processor::TAG_DEAD_RING_TEST) {
        auto& dm = *message.getTypedData<message::DeadRingTestMessage>();
        //First pass?
        bool passItOn = true;
        //Needs to be requeued at the end of our message stack, to be checked
        //again when we've done work?  If true, makes passItOn implicitly false
        bool requeue = false;

        if (JOB_STREAM_DEBUG >= 2) {
            fprintf(stderr, "Dead ring check %lu\n", dm.reduceTag);
        }

        if (this->reduceInfoMap[dm.reduceTag].childTagCount) {
            //We're holding onto this one.  Pay it forward... would be more 
            //efficient to hold onto it, but let's not do that for now.  It's 
            //late.
            //Note that we use -1 instead of zero because, if it's NOT held up
            //by a child the next time this ring test comes around, we still
            //don't want to use that "passWork" value, since that pass was
            //compromised.
            dm.pass = -1;
            requeue = true;
        }
        else if (dm.sentryRank == this->world.rank()) {
            dm.pass += 1;
            if (JOB_STREAM_DEBUG) {
                fprintf(stderr, "Dead ring %lu pass %i - %lu / %lu\n", 
                        dm.reduceTag, dm.pass, dm.passWork, dm.allWork);
            }

            if (dm.pass >= 2 && dm.allWork == dm.passWork) {
                //Now we might remove it, if there is no recurrence
                bool tellEveryone = false;
                if (dm.reduceTag == 0) {
                    //Global
                    this->shouldRun = false;
                    tellEveryone = true;
                }
                else {
                    //We're the sentry, we MUST have a reducer.
                    auto it = this->reduceInfoMap.find(dm.reduceTag);
                    if (it->second.reducer->dispatchDone(dm.reduceTag)) {
                        uint64_t parentTag = it->second.parentTag;
                        this->reduceInfoMap[parentTag].childTagCount -= 1;

                        this->reduceInfoMap.erase(it);
                        tellEveryone = true;
                    }
                    else {
                        //There was recurrence; setting the pass to 0 is needed
                        //so that we won't re-sample the workCount down below
                        //(in this method), and see that they're the same the
                        //next time we get the ring test and exit the ring
                        //early.
                        dm.pass = 0;
                    }
                }

                if (tellEveryone) {
                    //This ring is dead, tell everyone to remove it.
                    { //comm timer scope
                        WorkTimer commTimer(this, Processor::TIME_COMM);
                        for (int i = 0, m = this->world.size(); i < m; i++) {
                            if (i == dm.sentryRank) {
                                continue;
                            }
                            this->world.send(i, 
                                    Processor::TAG_DEAD_RING_IS_DEAD, 
                                    dm.serialized());
                        }
                    }

                    if (JOB_STREAM_DEBUG) {
                        fprintf(stderr, "Dead ring %lu took %lu ms\n", 
                                dm.reduceTag,
                                message::Location::getCurrentTimeMs() 
                                    - dm.tsTestStarted);
                    }

                    //Ring is dead and we sent the messages, don't keep passing
                    //the test around.
                    passItOn = false;
                }
            }
            
            if (passItOn) {
                dm.allWork = dm.passWork;
                dm.passWork = this->reduceInfoMap[dm.reduceTag].workCount;
            }
        }
        else {
            uint64_t workCount = this->reduceInfoMap[dm.reduceTag].workCount;
            dm.passWork += workCount;
            if (JOB_STREAM_DEBUG >= 2) {
                fprintf(stderr, "Dead ring %lu added %lu from %i\n",
                        dm.reduceTag, workCount, this->getRank());
            }
        }

        if (requeue) {
            this->workInQueue.emplace_back(std::move(message));
        }
        else if (passItOn) {
            WorkTimer sendTimer(this, Processor::TIME_COMM);
            this->world.send(this->_getNextRank(),
                    Processor::TAG_DEAD_RING_TEST,
                    dm.serialized());
        }

        if (JOB_STREAM_DEBUG >= 2) {
            fprintf(stderr, "Dead ring check %lu done\n", dm.reduceTag);
        }
    }
    else if (tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        auto& dm = *message.getTypedData<message::DeadRingTestMessage>();
        this->reduceInfoMap.erase(dm.reduceTag);

        if (dm.reduceTag == 0) {
            this->shouldRun = false;
        }
    }
    else {
        std::ostringstream ss;
        ss << "Unrecognized tag: " << tag;
        throw std::runtime_error(ss.str());
    }
}


bool Processor::tryReceive() {
    //First run filter
    WorkTimer timer(this, Processor::TIME_COMM);

    bool wasSteal = false;
    std::string stealBuffer;

    if (this->recvRequest) {
        boost::optional<mpi::status> recv = this->recvRequest.get().test();
        if (recv) {
            WorkTimer systemTimer(this, Processor::TIME_SYSTEM);

            //NOTE - The way boost::mpi receives into strings corrupts the 
            //string.  That's why we copy it here with a substr() call.
            if (JOB_STREAM_DEBUG >= 2) {
                fprintf(stderr, "%i got message len %lu\n", this->getRank(),
                        (unsigned long)this->recvBuffer.size());
            }

            this->msgsByTag[recv->tag()] += 1;

            //Steal requests happen out of sync with everything else; they can
            //even be serviced in the middle of work!
            if (recv->tag() == Processor::TAG_STEAL) {
                //Since maybeAllowSteal() can result in a tryReceive call when
                //it uses _nonBlockingSend, we have to start the next request
                //before processing.
                wasSteal = true;
                std::swap(stealBuffer, this->recvBuffer);
            }
            else if (recv->tag() == Processor::TAG_GROUP) {
                message::GroupMessage group;
                serialization::decode(this->recvBuffer, group);
                for (int i = 0, m = group.messageTags.size(); i < m; i++) {
                    this->workInQueue.emplace_back(group.messageTags[i],
                            std::move(group.messages[i]));
                }
            }
            else {
                this->workInQueue.emplace_back(recv->tag(), 
                        std::move(this->recvBuffer));
            }
        }
        else {
            //Nothing to receive yet
            return false;
        }
    }

    //Start next request.  clear() is necessary since 
    //mpi::boost::communicator::irecv has weird behavior with std::move'd string
    //buffers, and might end up overwriting the old data anyway somehow.
    this->recvBuffer.clear();
    this->recvRequest = this->world.irecv(mpi::any_source, mpi::any_tag, 
            this->recvBuffer);

    if (wasSteal) {
        WorkTimer stealTimer(this, Processor::TIME_SYSTEM);
        this->maybeAllowSteal(stealBuffer);
    }

    return true;
}


int Processor::_getNextRank() {
    return (this->world.rank() + 1) % this->world.size();
}


int Processor::_getNextWorker() {
    int size = (int)this->world.size();
    this->workTarget = (int)((this->workTarget + 1) % size);
    return this->workTarget;
}


void Processor::_nonBlockingSend(int dest, int tag, 
        const std::string& message) {
    //Send non-blocking (though we don't leave this function until our message
    //is sent) so that we don't freeze the application waiting for the send
    //buffer.
    WorkTimer timer(this, Processor::TIME_COMM);
    mpi::request sendReq = this->world.isend(dest, tag, message);
    while (!sendReq.test()) {
        //Send isn't done, see if we can receive our next message...
        this->tryReceive();
    }
}


void Processor::_pushWorkTimer(ProcessorTimeType timeType) {
    this->workTimers.emplace_back(message::Location::getCurrentTimeMs(),
            std::clock(), timeType);
}


void Processor::_popWorkTimer() {
    uint64_t clksSpent = std::clock();
    uint64_t timeSpent = message::Location::getCurrentTimeMs();
    Processor::_WorkTimerRecord& record = this->workTimers.back();
    clksSpent -= record.clkStart;
    timeSpent -= record.tsStart;

    this->clksByType[record.timeType] += clksSpent - record.clksChild;
    this->timesByType[record.timeType] += timeSpent - record.timeChild;
    this->workTimers.pop_back();

    int m = this->workTimers.size() - 1;
    if (m >= 0) {
        Processor::_WorkTimerRecord& parent = this->workTimers[m];
        parent.timeChild += timeSpent;
        parent.clksChild += clksSpent;
    }
}

} //processor
} //job_stream
