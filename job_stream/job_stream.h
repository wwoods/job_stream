#ifndef JOB_STREAM_H_
#define JOB_STREAM_H_

#include "message.h"
#include "job.h"
#include "processor.h"
#include "serialization.h"
#include "yaml.h"

#include <boost/mpi.hpp>
#include <functional>
#include <string>

namespace job_stream {
    /** When you have a type where you either aren't sure or don't care what
        it is, use this.  Used primarily for special types of jobs or reducers
        (see Frame) */
    typedef job_stream::serialization::AnyType AnyType;


    /** Specialized Job base class. */
    template<typename T_input>
    class Job : public job::JobBase {
    public:
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
                    serialization::encode(&output)));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(std::move(wr));
        }

    private:
        std::unique_ptr<T_input> currentWork;

        virtual std::string getInputTypeName() {
            return typeid(T_input).name();
        }
    };


    /** Specialized reducer class.  Default implementation relies on operator+,
        so have it defined (into T_accum from both T_accum and T_input)  */
    template<typename T_accum, typename T_input = T_accum>
    class Reducer : public job::ReducerBase {
    public:
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
            this->currentReduce = 0;

            if (!this->hadRecurrence) {
                this->reduceMap.erase(reduceTag);
                return true;
            }
            return false;
        }


        /** Called by system to call handleInit() with proper setup.  Also gets
            the reduce ring started and sets up calculation done checks. */
        virtual void dispatchInit(message::WorkRecord& work) {
            uint64_t tag = 1;
            int homeRank = 0;
            bool reallyInit = true;
            //So this is a tiny hack (for elegance though!), but essentially
            //if our work's target is the root module, that means root has
            //a reducer.  But since init work can go anywhere, and is not
            //part of a closed system in that it originates from a single 
            //WorkRecord, we have to use reserved tagId 1 to refer to a top
            //level reducer (whose parent is 0, which stops the program from
            //prematurely exiting).
            if (work.getTarget().size() != 0) {
                homeRank = this->processor->getRank();
                tag = this->processor->getNextReduceTag();
                if (this->reduceMap.count(tag) != 0) {
                    throw std::runtime_error("Bad tag?  Duplicate");
                }
            }
            else {
                if (homeRank != this->processor->getRank()
                        || this->reduceMap.count(tag) != 0) {
                    reallyInit = false;
                }
            }

            if (reallyInit) {
                job::ReduceAccumulator<T_accum>& record = this->reduceMap[tag];
                record.originalWork.reset(new message::WorkRecord(
                        work.serialized()));
                record.accumulator.reset(new T_accum());
                record.gotFirstWork = false;

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

            this->currentReduce = 0;
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

            this->currentReduce = 0;
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
                    serialization::encode(&output)));
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
                            this->parent->getConfig()["input"]
                                .template as<std::string>());
                }
                else {
                    ntarget = this->getTargetForJob(
                            this->config["recurTo"].template as<std::string>());
                }
            }
            else {
                ntarget = this->getTargetForJob(this->config["recurTo"][target]
                        .template as<std::string>());
            }

            std::unique_ptr<message::WorkRecord> wr(new message::WorkRecord(
                    ntarget, serialization::encode(&output)));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(std::move(wr));

            //Restore old reduce information and set hadRecurrence so that our
            //reduction ring isn't marked dead
            this->currentRecord->setReduce(oldRank, oldTag);
            this->hadRecurrence = true;
        }


    private:
        uint64_t currentReduceTag;
        job::ReduceAccumulator<T_accum>* currentReduce;
        std::unique_ptr<T_input> currentWork;
        std::unique_ptr<T_accum> currentJoin;
        /** Used in dispatchDone() to see if we had recurrence.  If we did not,
            the reduction is finished. */
        bool hadRecurrence;
        std::map<uint64_t, job::ReduceAccumulator<T_accum>> reduceMap;

        virtual std::string getInputTypeName() {
            return typeid(T_input).name();
        }


        /** Set currentReduce to point to the right ReduceAccumulator */
        void setCurrentReduce(uint64_t reduceTag) {
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

        template<typename T1, typename T2, typename T3>
        friend class Frame;
    };


    /** A Frame is a special type of Reducer that has special logic based on
        the first type of data it receives.  If T_first == T_recur, you 
        probably don't need a Frame, you probably want a Reducer.  But, for
        instance, an algorithm that runs a simulation over and over until
        a dynamic number of trials have been completed (based on the results
        of past trials) should be implemented as a Frame. */
    template<typename T_accum, typename T_first, typename T_work = T_accum>
    class Frame : public Reducer<T_accum, AnyType> {
    public:
        /** Handles the first work that initiates the Reduce loop */
        virtual void handleFirst(T_accum& current, 
                std::unique_ptr<T_first> first) = 0;
        /** Handles any subsequent work in this Reduce loop (from recur) */
        virtual void handleWork(T_accum& current,
                std::unique_ptr<T_work> work) = 0;

        /** The AnyType resolution of handleAdd */
        void handleAdd(T_accum& current, std::unique_ptr<AnyType> work) {
            if (!this->currentReduce->gotFirstWork) {
                this->currentReduce->gotFirstWork = true;
                this->handleFirst(current, std::move(work->as<T_first>()));
            }
            else {
                this->handleWork(current, std::move(work->as<T_work>()));
            }
        }

    private:
        virtual std::string getInputTypeName() {
            return typeid(T_first).name();
        }
    };



    void addJob(const std::string& typeName, 
            std::function<job::JobBase* ()> allocator);
    void addReducer(const std::string& typeName, 
            std::function<job::ReducerBase* ()> allocator);
    void runProcessor(int argc, char** argv);
}

#endif//JOB_STREAM_H_
