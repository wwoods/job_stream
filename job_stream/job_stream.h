#ifndef JOB_STREAM_H_
#define JOB_STREAM_H_

#include "invoke.h"
#include "message.h"
#include "job.h"
#include "processor.h"
#include "serialization.h"
#include "types.h"
#include "yaml.h"

#include <boost/mpi.hpp>
#include <functional>
#include <string>

namespace job_stream {
    /** When you have a type where you either aren't sure or don't care what
        it is, use this.  Used primarily for special types of jobs or reducers
        (see Frame) */
    typedef job_stream::serialization::AnyType AnyType;


    /** Application exit codes for forced exits */
    enum ApplicationRetval {
        RETVAL_OK = 0,
        RETVAL_CHECKPOINT_FORCED = 0x10,
        RETVAL_CHECKPOINT_WAS_DONE = 0x11,
    };


    /** Specialized Job base class. */
    template<typename T_derived, typename T_input>
    class Job : public job::JobBase {
    public:
        /** Note - don't use a constructor.  Use postSetup() instead. */
        Job() {
            //Template static member instantiation.  Gotta reference it.
            _autoRegister.doNothing();
        }

        /** Function to override that actually processes work */
        virtual void handleWork(std::unique_ptr<T_input> work) = 0;


        /** Pass work into handleWork and possibly emit new tuples.   */
        virtual void dispatchWork(message::WorkRecord& input) {
            this->currentRecord = &input;
            input.putWorkInto(this->currentWork);

            { //timer scope
                processor::Processor::WorkTimer timer(this->processor,
                        processor::Processor::TIME_USER);
                this->handleWork(std::move(this->currentWork));
            }

            this->currentRecord = 0;
        }


        virtual bool wouldReduce(message::WorkRecord& work) {
            return false;
        }


        template<class T_output> void emit(T_output* o,
                const std::string& target = "") {
            this->emit(*o, target);
        }


        template<class T_output> void emit(std::unique_ptr<T_output>& o,
                const std::string& target = "") {
            this->emit(*o, target);
        }


        template<class T_output> void emit(std::unique_ptr<T_output>&& o,
                const std::string& target = "") {
            //r-value constructor - we can take o.  We don't have to, but we
            //certainly can.
            this->emit(*o, target);
            o.reset(0);
        }


        template<class T_output> void emit(T_output&& output,
                const std::string& target = "") {
            processor::Processor::WorkTimer timer(this->processor,
                    processor::Processor::TIME_SYSTEM);

            std::string nextTarget;
            if (target.empty()) {
                nextTarget = this->config["to"].template as<std::string>();
            }
            else {
                nextTarget = this->config["to"][target]
                        .template as<std::string>();
            }

            std::unique_ptr<message::WorkRecord> wr(new message::WorkRecord(
                    this->getTargetForJob(nextTarget),
                    serialization::encodeAsPtr(output)));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(std::move(wr));
        }

    private:
        static thread_local std::unique_ptr<T_input> currentWork;

        virtual std::string getInputTypeName() {
            return typeid(T_input).name();
        }

        friend class boost::serialization::access;
        /*  Serialization for checkpoints; only used bottom up.  You do not need
            to register your own derived classes! */
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & boost::serialization::base_object<job::JobBase>(*this);
        }

        /* Auto class registration. */
        struct _AutoRegister {
            //http://stackoverflow.com/questions/1819131/c-static-member-initalization-template-fun-inside
            _AutoRegister() {
                job_stream::job::addJob(T_derived::NAME(), _AutoRegister::make);
                serialization::registerType<T_derived, Job<T_derived, T_input>,
                        job::JobBase, job::SharedBase>();
            }

            /** Called by constructor to force static member instantiation. */
            void doNothing() {}

            static T_derived* make() { return new T_derived(); }
        };
        static _AutoRegister _autoRegister;
    };

    template<typename T_derived, typename T_input>
            thread_local std::unique_ptr<T_input>
            Job<T_derived, T_input>::currentWork;
    template<typename T_derived, typename T_input>
            typename Job<T_derived, T_input>::_AutoRegister
            Job<T_derived, T_input>::_autoRegister;


    /** Specialized reducer class.  Default implementation relies on operator+,
        so have it defined (into T_accum from both T_accum and T_input)  */
    template<typename T_derived, typename T_accum, typename T_input = T_accum>
    class Reducer : public job::ReducerBase {
    public:
        /** Note - don't use a constructor.  Use postSetup() instead. */
        Reducer() {
            //Template static member instantiation.  Gotta reference it.
            _autoRegister.doNothing();
        }

        /** Called to initialize the accumulator for this reduce.  May be called
            several times on different hosts, whose results will later be merged
            in handleJoin(). */
        virtual void handleInit(T_accum& current) {}

        /** Used to add a new output to this accumulator */
        virtual void handleAdd(T_accum& current,
                std::unique_ptr<T_input> work) = 0;

        /** Called to join this Reducer with the accumulator from another */
        virtual void handleJoin(T_accum& current,
                std::unique_ptr<T_accum> other) = 0;

        /** Called when the reduction is complete, or nearly - recur() may be
            used to keep the reduction alive (inject new work into this
            reduction). */
        virtual void handleDone(T_accum& current) = 0;


        /** Called by system to call handleDone() with proper setup */
        virtual bool dispatchDone(uint64_t reduceTag) {
            this->setCurrentReduce(reduceTag);
            this->currentRecord = this->currentReduce->originalWork.get();
            this->hadRecurrence = false;
            this->targetIsModule = true;

            { //timer scope
                processor::Processor::WorkTimer timer(this->processor,
                        processor::Processor::TIME_USER);
                this->handleDone(*this->currentReduce->accumulator);
            }

            this->targetIsModule = false;
            this->currentRecord = 0;
            this->unsetCurrentReduce();

            if (!this->hadRecurrence) {
                //The processor that calls this function ends up calling
                //this->reduceMap.erase(reduceTag) for us with a lock on our
                //mutex.  This is done in the processor so that no
                //checkpoint-state-changing operations happen outside of
                //the processor's lock.
                return true;
            }
            return false;
        }


        /** Called by system to call handleInit() with proper setup.  Also gets
            the reduce ring started and sets up calculation done checks.
            Returns true if a new ring was started, false if one was continued.
            */
        virtual bool dispatchInit(message::WorkRecord& work) {
            uint64_t tag = 1;
            int homeRank = 0;
            bool reallyInit = true;

            Lock lock(this->reduceMapMutex);

            //So this is a tiny hack (for elegance though!), but essentially
            //if our work's target is the root module, that means root has
            //a reducer.  But since init work can go anywhere, and is not
            //part of a closed system in that it originates from a single
            //WorkRecord, we have to use reserved tagId 1 to refer to a top
            //level reducer (whose parent is 0, which stops the program from
            //prematurely exiting).
            if (work.getTarget().size() != 0) {
                homeRank = this->processor->getRank();
                //Takes the non-primary processor lock, so this is OK.
                tag = this->processor->getNextReduceTag();
                if (this->reduceMap.count(tag) != 0) {
                    std::ostringstream ss;
                    ss << "Bad tag?  Duplicate " << tag;
                    throw std::runtime_error(ss.str());
                }
            }
            else {
                //This path is only for tag 1
                if (homeRank != this->processor->getRank()
                        || this->reduceMap.count(tag) != 0) {
                    reallyInit = false;
                }
            }

            if (reallyInit) {
                //Make a patch for the reduceMap which will erase this record on
                //checkpoint restore.
                this->reduceMapPatches[tag] = "";
                this->processor->registerReduceReset(this, tag);

                //Fill out the reduceMap with our new information.
                job::ReduceAccumulator<T_accum>& record = this->reduceMap[tag];
                record.originalWork.reset(new message::WorkRecord(
                        work.serialized()));
                record.accumulator.reset(new T_accum());
                record.gotFirstWork = false;
                record.shouldBePurged = false;
                record.mutex.lock();

                { //timer scope
                    processor::Processor::WorkTimer timer(this->processor,
                            processor::Processor::TIME_USER);
                    this->handleInit(*record.accumulator);
                }

                //Track when this reduction is finished
                this->processor->startRingTest(tag, work.getReduceTag(), this);
            }

            //Now that we've backed up the original work, overwrite the reduce
            //params.
            work.setReduce(homeRank, tag);
            return reallyInit;
        }


        /** Called by system to call handleAdd() with proper setup */
        virtual void dispatchAdd(message::WorkRecord& work) {
            this->currentRecord = &work;
            this->setCurrentReduce(work.getReduceTag());
            work.putWorkInto(this->currentWork);

            { //timer scope
                processor::Processor::WorkTimer timer(this->processor,
                        processor::Processor::TIME_USER);
                this->handleAdd(*this->currentReduce->accumulator,
                        std::move(this->currentWork));
            }

            this->unsetCurrentReduce();
            this->currentRecord = 0;
        }


        /** Called by system to call handleJoin() with proper setup */
        virtual void dispatchJoin(message::WorkRecord& work) {
            this->currentRecord = &work;
            this->setCurrentReduce(work.getReduceTag());
            work.putWorkInto(this->currentJoin);

            { //timer scope
                processor::Processor::WorkTimer timer(this->processor,
                        processor::Processor::TIME_USER);
                this->handleJoin(*this->currentReduce->accumulator,
                        std::move(this->currentJoin));
            }

            this->unsetCurrentReduce();
            this->currentRecord = 0;
        }


        template<class T_output> void emit(std::unique_ptr<T_output>& o) {
            this->emit(*o);
        }


        template<class T_output> void emit(std::unique_ptr<T_output>&& o) {
            this->emit(*o);
        }


        template<class T_output> void emit(T_output* o) {
            this->emit(*o);
        }


        /** Output some work from this module */
        template<class T_output> void emit(T_output&& output) {
            processor::Processor::WorkTimer timer(this->processor,
                    processor::Processor::TIME_SYSTEM);

            std::unique_ptr<message::WorkRecord> wr(new message::WorkRecord(
                    this->getTargetForReducer(),
                    serialization::encodeAsPtr(output)));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(std::move(wr));
        }


        template<class T_output> void recur(std::unique_ptr<T_output>& output,
                const std::string& target = "") {
            this->recur(*output, target);
        }


        template<class T_output> void recur(std::unique_ptr<T_output>&& output,
                const std::string& target = "") {
            this->recur(*output, target);
        }


        template<class T_output> void recur(T_output* output,
                const std::string& target = "") {
            this->recur(*output, target);
        }


        /** Output some work back into this module at job named under config
            recurTo.target.  If target is empty (default), send back to
            module input. */
        template<class T_output> void recur(T_output&& output,
                const std::string& target = "") {
            processor::Processor::WorkTimer timer(this->processor,
                    processor::Processor::TIME_SYSTEM);

            //Make sure the chain takes... bit of a hack, but we need the
            //currentRecord (tuple that caused our reduce) to maintain its
            //original reduce tag and information in case it itself is part of
            //a reduce.
            int oldRank = this->currentRecord->getReduceHomeRank();
            uint64_t oldTag = this->currentRecord->getReduceTag();
            this->currentRecord->setReduce(this->processor->getRank(),
                    this->currentReduceTag);

            std::vector<std::string> ntarget;
            if (target.empty()) {
                if (!this->config["recurTo"]) {
                    ntarget = this->getTargetForJob(
                            this->parent->config["input"]
                                .template as<std::string>());
                }
                else {
                    ntarget = this->getTargetForJob(
                            this->config["recurTo"].template as<
                                std::string>());
                }
            }
            else {
                ntarget = this->getTargetForJob(
                        this->config["recurTo"][target].template as<
                            std::string>());
            }

            std::unique_ptr<message::WorkRecord> wr(new message::WorkRecord(
                    ntarget, serialization::encodeAsPtr(output)));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(std::move(wr));

            //Restore old reduce information and set hadRecurrence so that our
            //reduction ring isn't marked dead
            this->currentRecord->setReduce(oldRank, oldTag);
            this->hadRecurrence = true;
        }


    private:
        static thread_local uint64_t currentReduceTag;
        static thread_local job::ReduceAccumulator<T_accum>* currentReduce;
        static thread_local std::unique_ptr<T_input> currentWork;
        static thread_local std::unique_ptr<T_accum> currentJoin;
        /** Used in dispatchDone() to see if we had recurrence.  If we did not,
            the reduction is finished. */
        static thread_local bool hadRecurrence;
        std::map<uint64_t, job::ReduceAccumulator<T_accum>> reduceMap;
        /** Mutex for altering reduceMap's structure.  Note that when this
            mutex is taken, the processor's mutex MUST NOT be taken inside of
            its lock (if already locked, that's OK).  This affects e.g.
            dispatchInit when we get our reduce tag for a new ring.  That is
            why the processor uses a different lock for that functionality.
            */
        Mutex reduceMapMutex;
        /** User data checkpointed before it is modified. */
        std::map<uint64_t, std::string> reduceMapPatches;

        virtual std::string getInputTypeName() {
            return typeid(T_input).name();
        }

        /** Purges checkpoint reset information.  Called after
            processor->registerReduceReset() */
        void purgeCheckpointReset(uint64_t reduceTag) override {
            Lock lock(this->reduceMapMutex);
            this->reduceMapPatches.erase(reduceTag);
            auto& mapTag = this->reduceMap[reduceTag];
            mapTag.mutex.unlock();
            if (mapTag.shouldBePurged) {
              this->reduceMap.erase(reduceTag);
            }
        }

        /** Purges a dead ring from our reduceMap. */
        void purgeDeadRing(uint64_t reduceTag) override {
            Lock lock(this->reduceMapMutex);
            this->reduceMap[reduceTag].shouldBePurged = true;
        }

        /** Set currentReduce to point to the right ReduceAccumulator */
        void setCurrentReduce(uint64_t reduceTag) {
            { //Lock and release reduceMap before locking the reduction
                Lock lock(this->reduceMapMutex);
                auto iter = this->reduceMap.find(reduceTag);
                if (iter == this->reduceMap.end()) {
                    std::ostringstream ss;
                    ss << "ReduceTag " << reduceTag << " not found on "
                            << this->processor->getRank() << "!";
                    throw std::runtime_error(ss.str());
                }

                this->currentReduceTag = reduceTag;
                this->currentReduce = &iter->second;
            }

            this->currentReduce->mutex.lock();
            this->processor->registerReduceReset(this, reduceTag);
            {
                Lock lock(this->reduceMapMutex);
                if (this->reduceMapPatches.count(reduceTag) != 0) {
                    //A dispatchInit leads to either a dispatchWork or
                    //dispatchAdd, so thany prior reset supercedes the current
                    //one.
                }
                else {
                    this->reduceMapPatches[reduceTag] = serialization::encode(
                            *this->currentReduce);
                }
            }
        }


        /** Unlock working with this particular reduceTag */
        void unsetCurrentReduce() {
            //Most cleanup actually happens in purgeCheckpointReset
            this->currentReduce = 0;
        }

        //I *think* partial specialization of friend class with our T_derived
        //isn't allowed.  A pity.
        template<typename Td, typename T1, typename T2, typename T3>
        friend class Frame;

        friend class boost::serialization::access;
        /*  Serialization for checkpoints; only used bottom up.  You do not need
            to register your own derived classes! */
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            Lock lock(this->reduceMapMutex);

            ar & boost::serialization::base_object<job::ReducerBase>(*this);
            ar & this->reduceMap;
            //We don't protect user data during checkpoint operations.  So,
            //we checkpoint it before any work occurs (see
            //setCurrentReduce).  Save those checkpoints.
            ar & this->reduceMapPatches;
            if (Archive::is_loading::value) {
                for (auto& m : this->reduceMapPatches) {
                    if (m.second.empty()) {
                        this->reduceMap.erase(m.first);
                    }
                    else {
                        serialization::decode(m.second,
                                this->reduceMap[m.first]);
                    }
                }
                this->reduceMapPatches.clear();
            }
        }

        /* Auto class registration. */
        struct _AutoRegister {
            //http://stackoverflow.com/questions/1819131/c-static-member-initalization-template-fun-inside
            _AutoRegister() {
                job_stream::job::addReducer(T_derived::NAME(),
                        _AutoRegister::make);
                serialization::registerType<T_derived,
                        Reducer<T_derived, T_accum, T_input>,
                        job::ReducerBase, job::SharedBase>();
            }

            /** Called by constructor to force static member instantiation. */
            void doNothing() {}

            static T_derived* make() { return new T_derived(); }
        };
        static _AutoRegister _autoRegister;
    };

    template<typename T_derived, typename T_accum, typename T_input>
    thread_local uint64_t Reducer<T_derived, T_accum, T_input>::currentReduceTag
            = (uint64_t)-1;
    template<typename T_derived, typename T_accum, typename T_input>
    thread_local job::ReduceAccumulator<T_accum>*
            Reducer<T_derived, T_accum, T_input>::currentReduce = 0;
    template<typename T_derived, typename T_accum, typename T_input>
    thread_local std::unique_ptr<T_input>
            Reducer<T_derived, T_accum, T_input>::currentWork;
    template<typename T_derived, typename T_accum, typename T_input>
    thread_local std::unique_ptr<T_accum>
            Reducer<T_derived, T_accum, T_input>::currentJoin;
    template<typename T_derived, typename T_accum, typename T_input>
    thread_local bool Reducer<T_derived, T_accum, T_input>::hadRecurrence
            = false;
    template<typename T_derived, typename T_accum, typename T_input>
    typename Reducer<T_derived, T_accum, T_input>::_AutoRegister
    Reducer<T_derived, T_accum, T_input>::_autoRegister;


    /** A Frame is a special type of Reducer that has special logic based on
        the first type of data it receives.  If T_first == T_recur, you
        probably don't need a Frame, you probably want a Reducer.  But, for
        instance, an algorithm that runs a simulation over and over until
        a dynamic number of trials have been completed (based on the results
        of past trials) should be implemented as a Frame. */
    template<typename T_derived, typename T_accum, typename T_first,
            typename T_work = T_accum>
    class Frame : public Reducer<T_derived, T_accum, AnyType> {
    public:
        /** Note - don't use a constructor.  Use postSetup() instead. */
        Frame() {
            //Template static member instantiation.  Gotta reference it.
            _autoRegister.doNothing();
        }

        /** Handles the first work that initiates the Reduce loop */
        virtual void handleFirst(T_accum& current,
                std::unique_ptr<T_first> first) = 0;
        /** Handles any subsequent work in this Reduce loop (from recur) */
        virtual void handleNext(T_accum& current,
                std::unique_ptr<T_work> work) = 0;

        /** The AnyType resolution of handleAdd */
        void handleAdd(T_accum& current, std::unique_ptr<AnyType> work) {
            if (!this->currentReduce->gotFirstWork) {
                this->currentReduce->gotFirstWork = true;
                this->handleFirst(current, std::move(work->as<T_first>()));
            }
            else {
                this->handleNext(current, std::move(work->as<T_work>()));
            }
        }

        /** Frames don't have a handleJoin, since there is one authority, which
            is the original processor to get the message. */
        void handleJoin(T_accum& current, std::unique_ptr<T_accum> other) {
            throw std::runtime_error("Unimplemented, should never be "
                    "triggered (Frame::handleJoin).");
        }

    private:
        virtual std::string getInputTypeName() {
            return typeid(T_first).name();
        }

        friend class boost::serialization::access;
        /*  Serialization for checkpoints; only used bottom up.  You do not need
            to register your own derived classes! */
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & boost::serialization::base_object<
                    Reducer<T_derived, T_accum, AnyType>>(*this);
        }

        /* Auto class registration. */
        struct _AutoRegister {
            //http://stackoverflow.com/questions/1819131/c-static-member-initalization-template-fun-inside
            _AutoRegister() {
                //Frames inherit from Reducer, no need to addReducer.  We just
                //need to set up the serialization inheritance chain
                serialization::registerType<T_derived,
                        Frame<T_derived, T_accum, T_first, T_work>,
                        Reducer<T_derived, T_accum, AnyType>,
                        job::ReducerBase, job::SharedBase>();
            }

            /** Called by constructor to force static member instantiation. */
            void doNothing() {}
        };
        static _AutoRegister _autoRegister;
    };

    template<typename T_derived, typename T_accum, typename T_first,
            typename T_work>
    typename Frame<T_derived, T_accum, T_first, T_work>::_AutoRegister
    Frame<T_derived, T_accum, T_first, T_work>::_autoRegister;

    /** A structure used to launch the system with non-argc, argv arguments.
        */
    struct SystemArguments {
        /** The YAML text that is used as the config file */
        std::string config;
        /** The callback function called every loop.  If it returns true, then
            processing is aborted. */
        std::function<bool ()> checkExternalSignals;
        /** The file to use for checkpointing, if any */
        std::string checkpointFile;
        /** The interval between the completion of one checkpoint and the
            beginning of another, in milliseconds. */
        int checkpointIntervalMs;
        /** The time between all nodes being ready for a checkpoint and the
            checkpoint actually starting.  Used to account for e.g. different
            times on different machines for send and receive (send finishes, so
            gets cleared, but then receive starts afterward). */
        int checkpointSyncIntervalMs;
        /** Useful mostly for debugging, disables the steal ring */
        bool disableSteal;
        /** Callback for output tuples, rather than printing them out.  Note that this
            function becomes responsible for checkpointing the output work! */
        std::function<void (std::unique_ptr<AnyType>)> handleOutputCallback;
        /** The initial input string, if any.  Leave empty for stdin or work
            enqueued via queueInitialWork() */
        std::string inputLine;

        SystemArguments() : checkpointIntervalMs(600 * 1000),
                checkpointSyncIntervalMs(-1), disableSteal(false) {}
    };

    /** Returns a human-readable string denoting details about the checkpoint
        file specified. */
    std::string checkpointInfo(const std::string& path);


    /** Returns the number of CPUs in this cluster; throws an error if the
     * steal ring hasn't gone around yet and CPU detection hasn't happened.
     * */
    int getCpuCount();


    /** Returns this machine's MPI rank. */
    int getRank();


    /** Add work to the initialWork queue, which overrides stdin or the
        argc, argv combination. */
    template<typename T>
    void queueInitialWork(T&& work) {
        processor::initialWork.emplace_back(serialization::encodeAsPtr(work));
    }


    /** Run the processor, processing either the input line specified in
        (argc, argv) excepting flags, the job_stream::processor::initialWork
        queue (populated through job_stream::queueInitialWork), or stdin. */
    void runProcessor(int argc, char** argv);
    void runProcessor(const SystemArguments& args);
}

#endif//JOB_STREAM_H_
