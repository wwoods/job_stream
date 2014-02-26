
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

extern const int JOB_STREAM_DEBUG = 1;

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
#define ZZZ _Z_impl(std::string(__FILE__) + std::string(":") \
        + boost::lexical_cast<std::string>(__LINE__) + std::string(":") \
        + std::string(__FUNCTION__));
#define ZZ _Z_impl(__LINE__);
#else
#define ZZZ
#define ZZ
#endif

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



Processor::Processor(std::unique_ptr<mpi::environment> env, 
        mpi::communicator world, 
        const YAML::Node& config) : processInputThread(0), reduceTagCount(0), 
            env(std::move(env)), world(world) {
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

    //Figure out our siblings
    std::vector<std::string> hosts;
    mpi::all_gather(this->world, this->env->processor_name(), hosts);

    this->workTarget = 1;
    if (this->world.rank() == 0) {
        //The first work message MUST go to rank 0, so that ring 1 ends up there
        this->workTarget = -1;
        this->processInputThread = new boost::thread(boost::bind(
                &Processor::processInputThread_main, this, inputLine));

        //The first processor also starts the steal ring
        this->_nonBlockingSend(Processor::TAG_STEAL, 
                message::Header(this->_getNextRank()),
                message::StealRing(this->world.size()).serialized());
    }
    else {
        this->sawEof = true;
        this->sentEndRing0 = true;
    }

    int dest, tag;
    message::WorkRecord* work;
    std::shared_ptr<MpiMessage> msg;
    //Begin tallying time spent in system vs user functionality.
    std::unique_ptr<WorkTimer> outerTimer(new WorkTimer(this, 
            Processor::TIME_SYSTEM));
    while (this->shouldRun) {
        //Update communications
        this->checkMpi();

        if (!this->sentEndRing0) {
            //Eof found and we haven't sent ring!  Send it
            if (this->sawEof) {
                this->startRingTest(0, 0, 0);
                this->sentEndRing0 = true;
            }

            //Distribute any outbound work
            while (this->workOutQueue.pop(work)) {
                //deletes work if it is not added locally.
                this->addWork(work);
            }
        }

        //Process as much work as we can (trying to send work will result in
        //a tryReceive()...  except when it goes locally)
        if (!this->workInQueue.empty()) {
            msg = this->workInQueue.front();
            if (JOB_STREAM_DEBUG >= 1) {
                fprintf(stderr, "%i %lu processing %i ", this->getRank(), 
                        message::Location::getCurrentTimeMs(), msg->tag);
                if (msg->tag == Processor::TAG_WORK 
                        || msg->tag == Processor::TAG_REDUCE_WORK) {
                    for (const std::string& s : msg->getTypedData<message::WorkRecord>()->getTarget()) {
                        fprintf(stderr, "::%s", s.c_str());
                    }
                }
                fprintf(stderr, "\n");
            }
            this->process(msg);
            //Pop after process so that e.g. dead ring tests can look at the
            //reduceTag of what we're currently working on
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

    //Finish all outstanding activity and send a message to everyone letting
    //them know we're done.  Otherwise, messages won't line up and MPI complains
    //about messages being truncated.
    while (!this->sendRequests.empty() || this->tryReceive()) {
        this->checkMpi();
    }
    this->world.send(this->getRank(), 0, message::_Message(
            message::Header(this->getRank()), ""));
    this->recvRequest->wait();
    this->world.barrier();

    //Stop divide by zero
    timesTotal = (timesTotal > 0) ? timesTotal : 1;
    clksTotal = (clksTotal > 0) ? clksTotal : 1;
    msgsTotal = (msgsTotal > 0) ? msgsTotal : 1;

    ProcessorInfo myInfo;
    myInfo.pctUserTime = (int)(1000 * this->timesByType[Processor::TIME_USER]
            / timesTotal);
    myInfo.userCpuTotal = this->clksByType[Processor::TIME_USER];
    myInfo.pctMpiTime = (int)(1000 * this->timesByType[Processor::TIME_COMM]
            / timesTotal);
    uint64_t userTimeNonZero = this->timesByType[Processor::TIME_USER];
    userTimeNonZero = (userTimeNonZero > 0) ? userTimeNonZero : 1;
    myInfo.pctUserCpu = (int)(1000000 * this->clksByType[Processor::TIME_USER]
            / (CLOCKS_PER_SEC * userTimeNonZero));
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
                "quality %.2f ticks/ms, ran %.3fs\n", 
                totalTime / 10, totalCpuTime / 10000,
                (double)totalCpu / timesTotal, timesTotal * 0.001);
    }
}


void Processor::addWork(message::WorkRecord* wr) {
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

    if (dest != rank) {
        std::vector<uint64_t> reduceTags;
        reduceTags.push_back(wr->getReduceTag());
        this->_nonBlockingSend(tag,
                message::Header(dest, std::move(reduceTags)), 
                wr->serialized());
        delete wr;
    }
    else {
        this->workInQueue.emplace_back(new MpiMessage(tag, wr));
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
    this->_nonBlockingSend(Processor::TAG_DEAD_RING_TEST,
            message::Header(this->_getNextRank()),
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


bool sortWorkBySize(std::list<std::shared_ptr<MpiMessage>>::iterator a,
        std::list<std::shared_ptr<MpiMessage>>::iterator b) {
    return (*a)->getTypedData<message::WorkRecord>()->serialized().size() >
            (*b)->getTypedData<message::WorkRecord>()->serialized().size();
}


void Processor::maybeAllowSteal(const std::string& messageBuffer) {
    message::StealRing sr(messageBuffer);
    if (JOB_STREAM_DEBUG >= 2) {
        fprintf(stderr, "%i checking steal ring...\n", this->getRank());
    }

    //Go through our queue and see if we need work or can donate work.
    std::deque<std::list<std::shared_ptr<MpiMessage>>::iterator> stealable;
    int unstealableWork = 0;
    //REMEMBER - First work is in progress and should not be stolen!  Even if
    //the first work isn't the first in the queue, don't let someone steal it,
    //since that means we would have nothing else to process
    bool seenFirst = false;
    auto iter = this->workInQueue.begin();
    for (; iter != this->workInQueue.end();
            iter++) {
        if ((*iter)->tag == Processor::TAG_WORK) {
            if (seenFirst) {
                stealable.push_back(iter);
            }
            seenFirst = true;
        }
        else if ((*iter)->tag == Processor::TAG_REDUCE_WORK) {
            unstealableWork += 1;
            seenFirst = true;
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
            std::sort(stealable.begin(), stealable.end(), sortWorkBySize);

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
                std::vector<uint64_t> reduceTags;
                int stealNext = std::min(stealTop, stealBottom + stealSlice);
                for (int j = stealBottom; j < stealNext; j++) {
                    const MpiMessage& msg = **stealable[j];
                    reduceTags.push_back(msg.getTypedData<message::WorkRecord>()
                            ->getReduceTag());
                    msgOut.add(msg.tag, msg.serialized());
                    this->workInQueue.erase(stealable[j]);
                }
                stealBottom = stealNext;

                this->_nonBlockingSend(Processor::TAG_GROUP, 
                        message::Header(r, std::move(reduceTags)),
                        msgOut.serialized());

                if (stealBottom == stealTop) {
                    break;
                }
            }
        }
    }

    //Forward the steal ring!
    sr.needsWork[this->getRank()] = iNeedWork;
    this->_nonBlockingSend(Processor::TAG_STEAL, 
            message::Header(this->_getNextRank()),
            sr.serialized());

    if (JOB_STREAM_DEBUG >= 2) {
        fprintf(stderr, "%i done with steal ring\n", this->getRank());
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


void Processor::checkMpi() {
    WorkTimer sendTimer(this, Processor::TIME_COMM);

    //Fill any outbound buffers
    bool sentOrReceived = true;

    uint64_t start = message::Location::getCurrentTimeMs();
    while (sentOrReceived) {
        sentOrReceived = false;

        auto it = this->sendRequests.begin();
        while (it != this->sendRequests.end()) {
            sentOrReceived = true;
            if (it->request.test()) {
                for (uint64_t reduceTag : it->reduceTags) {
                    this->decrReduceChildTag(reduceTag);
                }
                this->sendRequests.erase(it++);
            }
            else {
                it++;
            }
        }

        //See if we have any messages; while we do, load them.
        sentOrReceived |= this->tryReceive();

        if (message::Location::getCurrentTimeMs() - start >= 10) {
            break;
        }
    }
}


void Processor::decrReduceChildTag(uint64_t reduceTag) {
    auto& ri = this->reduceInfoMap[reduceTag];
    ri.childTagCount -= 1;
    if (ri.childTagCount == 0) {
        auto it = this->workInQueue.begin();
        if (it != this->workInQueue.end()) {
            it++;
        }
        for (auto& m : ri.messagesWaiting) {
            this->workInQueue.insert(it, std::move(m));
        }
        ri.messagesWaiting.clear();
    }
}


void Processor::handleRingTest(std::shared_ptr<MpiMessage> message,
        bool isWork) {ZZZ
    //Handle a ring test check in either tryReceive or the main work loop.
    message::DeadRingTestMessage& dm = *message->getTypedData<
            message::DeadRingTestMessage>();

    //Set to true to add our work to the message and forward it to next in loop
    bool forward = false;
ZZ
    //Step one - is something holding this queue back?
    if (this->reduceInfoMap[dm.reduceTag].childTagCount) {ZZZ
        //Hold onto this ring test until we no longer have activity holding this
        //ring back.
        dm.pass = 0;
        this->reduceInfoMap[dm.reduceTag].messagesWaiting.push_back(
                std::move(message));
    }
    else {ZZZ
        //Go backwards through our work queue; if we hit work with
        //this ring's reduce tag, put this message after that.
        //Otherwise, handle it if we're work, or put this up top if we're in
        //the receive loop.  Reduces have priority.
        bool inserted = false;
        auto it = this->workInQueue.end();
        while (it != this->workInQueue.begin()) {
            --it;
            MpiMessage& mm = **it;
            if (mm.tag == Processor::TAG_WORK
                    || mm.tag == Processor::TAG_REDUCE_WORK) {
                if (mm.getTypedData<message::WorkRecord>()
                        ->getReduceTag() == dm.reduceTag) {
                    it++;
                    this->workInQueue.insert(it, std::move(message));
                    inserted = true;
                    break;
                }
            }
        }

        if (!inserted) {ZZZ
            //No work or anything holding us back.  If this ring 
            //does not originate from us, go ahead and forward
            //it.  Otherwise, put it at the top of our queue so it
            //gets processed ASAP.  We can't process it here in
            //case dispatchDone() would be called.
            if (dm.sentryRank != this->getRank()) {
                forward = true;
            }
            else if (!isWork) {
                //We're not part of the main work loop, we're in a receive.
                //So put this message in the main work loop, where we'll try
                //again.
                //Remember that the top of the workInQueue is our current
                //work.
                if (this->workInQueue.empty()) {
                    this->workInQueue.push_front(std::move(message));
                }
                else {
                    auto it = this->workInQueue.begin();
                    it++;
                    this->workInQueue.insert(it, std::move(message));
                }
            }
            else {ZZZ
                //The pass is done and we're in the work loop.
                dm.pass += 1;
                if (JOB_STREAM_DEBUG) {
                    fprintf(stderr, "Dead ring %lu pass %i - %lu / %lu\n", 
                            dm.reduceTag, dm.pass, dm.passWork, dm.allWork);
                }

                forward = true;
                if (dm.pass >= 2 && dm.allWork == dm.passWork) {ZZZ
                    //Now we might remove it, if there is no recurrence
                    if (dm.reduceTag == 0) {ZZZ
                        //Global
                        this->shouldRun = false;
                        forward = false;
                    }
                    else {ZZZ
                        //We're the sentry, we MUST have a reducer.
                        ZZ
                        auto it = this->reduceInfoMap.find(dm.reduceTag);
                        ZZ
                        if (it->second.reducer->dispatchDone(dm.reduceTag)) {
                            ZZ
                            uint64_t parentTag = it->second.parentTag;
                        ZZ
                            this->decrReduceChildTag(parentTag);
                        ZZ

                            this->reduceInfoMap.erase(it);
                        ZZ
                            forward = false;
                        }
                        else {
                            ZZ
                            //There was recurrence; setting the pass to 0 is 
                            //needed so that we won't re-sample the workCount 
                            //down below (in this method), and see that they're 
                            //the same the next time we get the ring test and 
                            //exit the ring early.
                            dm.pass = 0;
                        }
                        ZZ
                    }

                    if (!forward) {ZZZ
                        //This ring is dead, tell everyone to remove it.
                        this->_nonBlockingSend(
                                Processor::TAG_DEAD_RING_IS_DEAD,
                                message::Header(_getNextRank()), 
                                dm.serialized());

                        if (JOB_STREAM_DEBUG) {
                            fprintf(stderr, "Dead ring %lu took %lu ms\n", 
                                    dm.reduceTag,
                                    message::Location::getCurrentTimeMs() 
                                        - dm.tsTestStarted);
                        }
                    }
                }
                else {ZZZ
                    //Move passWork into allWork since the pass is done.
                    dm.allWork = dm.passWork;
                    dm.passWork = 0;
                }
            }
        }
    }
ZZ
    if (forward) {ZZZ
        //Add our work count and happily forward it along
        dm.passWork += this->reduceInfoMap[dm.reduceTag].workCount;
        { //Scope for TIME_COMM
            WorkTimer sendTimer(this, Processor::TIME_COMM);
            this->_nonBlockingSend(Processor::TAG_DEAD_RING_TEST,
                    message::Header(this->_getNextRank()),
                    dm.serialized());
        }
    }
}


void Processor::process(std::shared_ptr<MpiMessage> message) {
    int tag = message->tag;
    if (tag == Processor::TAG_WORK || tag == Processor::TAG_REDUCE_WORK) {
        auto& wr = *message->getTypedData<message::WorkRecord>();
        wr.markStarted();
        //workCount stops the system from settling while there are still pending
        //messages.
        this->reduceInfoMap[wr.getReduceTag()].workCount += 1;
        //Note that we're working on this to delay the ring.  Note that ONLY
        //ONE WORK() message will be worked on at a time.
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
        this->handleRingTest(message, true);
    }
    else if (tag == Processor::TAG_DEAD_RING_IS_DEAD) {
        auto& dm = *message->getTypedData<message::DeadRingTestMessage>();
        if (dm.sentryRank != this->getRank()) {
            if (this->reduceInfoMap[dm.reduceTag].messagesWaiting.size()) {
                printf("%i OMG OMG OMG OMG %lu %i %i %i\n", getpid(), dm.reduceTag,
                        dm.sentryRank, getRank(), tag);
                throw std::runtime_error("YPU");
            }
            this->reduceInfoMap.erase(dm.reduceTag);

            if (dm.reduceTag == 0) {
                this->shouldRun = false;
            }

            //Forward so it dies on all ranks
            this->_nonBlockingSend(
                    Processor::TAG_DEAD_RING_IS_DEAD,
                    message::Header(_getNextRank()), 
                    message->serialized());
        }
    }
    else {
        std::ostringstream ss;
        ss << "Unrecognized tag: " << tag;
        throw std::runtime_error(ss.str());
    }
}


bool Processor::tryReceive() {ZZZ
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
ZZ
    //We got a message!
    mpi::status recv = *recvMaybe;
    { //Scope for processing the received message
        WorkTimer systemTimer(this, Processor::TIME_SYSTEM);ZZZ

        int tag = recv.tag();
        message::_Message& msg = this->recvBuffer;

        //NOTE - The way boost::mpi receives into strings corrupts the 
        //string.  That's why we copy it here with a substr() call.
        if (JOB_STREAM_DEBUG >= 2) {
            fprintf(stderr, "%i got message len %lu\n", this->getRank(),
                    (unsigned long)msg.buffer.size());
        }

        this->msgsByTag[tag] += 1;

        if (this->getRank() != msg.header.dest) {ZZZ
            //Forward it to the proper destination.
            this->_nonBlockingSend(tag, std::move(msg.header), 
                    std::move(msg.buffer));
        }
        else if (tag == Processor::TAG_STEAL) {ZZZ
            //Since maybeAllowSteal() can result in a tryReceive call when
            //it uses _nonBlockingSend, we have to start the next request
            //before processing.
            wasSteal = true;
            std::swap(stealBuffer, msg.buffer);
        }
        else if (tag == Processor::TAG_GROUP) {ZZZ
            message::GroupMessage group;
            serialization::decode(msg.buffer, group);
            for (int i = 0, m = group.messageTags.size(); i < m; i++) {
                this->workInQueue.emplace_back(new MpiMessage(
                        group.messageTags[i],
                        std::move(group.messages[i])));
            }
        }
        else if (tag == Processor::TAG_DEAD_RING_TEST) {ZZZ
            std::shared_ptr<MpiMessage> mpiMessage(new MpiMessage(tag, 
                    std::move(msg.buffer)));
            this->handleRingTest(mpiMessage, false);
        }
        else {ZZZ
            this->workInQueue.emplace_back(new MpiMessage(tag, 
                    std::move(msg.buffer)));
        }
    }
ZZ
    //std::move() with a buffer doesn't always seem to swap them, so make
    //sure that the next payload has a new home
    std::string nextBuffer;
    std::swap(nextBuffer, this->recvBuffer.buffer);
    this->recvRequest = this->world.irecv(mpi::any_source, mpi::any_tag, 
            this->recvBuffer);
ZZ
    if (wasSteal) {
        WorkTimer stealTimer(this, Processor::TIME_SYSTEM);
        this->maybeAllowSteal(stealBuffer);
    }
ZZ
    return true;
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


void Processor::_nonBlockingSend(int tag, message::Header header,
        std::string payload) {
    //Send non-blocking (though we don't leave this function until our message
    //is sent) so that we don't freeze the application waiting for the send
    //buffer.
    int dest = header.dest;

    //Mark that these tags are in process and these rings are definitely not
    //done
    for (uint64_t reduceTag : header.reduceTags) {
        this->reduceInfoMap[reduceTag].childTagCount += 1;
    }

    message::_Message msg(std::move(header), std::move(payload));
    WorkTimer timer(this, Processor::TIME_COMM);
    mpi::request request = this->world.isend(dest, tag, msg);
    this->sendRequests.emplace_back(request, std::move(msg.header.reduceTags));
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
