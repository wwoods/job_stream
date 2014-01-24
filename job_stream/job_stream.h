#ifndef JOB_STREAM_H_
#define JOB_STREAM_H_

#include "message.h"
#include "job.h"
#include "processor.h"
#include "serialization.h"
#include "yaml.h"

#include <boost/lexical_cast.hpp>
#include <boost/mpi.hpp>
#include <functional>
#include <string>

namespace job_stream {
    /** Specialized Job base class. */
    template<typename T_input>
    class Job : public job::JobBase {
    public:
        /** Pass work into handleWork and possibly emit new tuples.   */
        virtual void dispatchWork(message::WorkRecord& input) {
            this->currentRecord = &input;
            input.putWorkInto(this->currentWork);

            { //timer scope
                processor::Processor::WorkTimer timer(this->processor, 
                        processor::Processor::TIME_USER);
                this->handleWork(*this->currentWork);
            }

            this->currentWork.reset(0);
            this->currentRecord = 0;
        }


        virtual void handleWork(T_input& work) = 0;


        template<class T_output> void emit(const T_output& output) {
            this->emit(output, "");
        }


        template<class T_output> void emit(const T_output& output, 
                const std::string& target) {
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

            message::WorkRecord* wr = new message::TypedWorkRecord<T_output>(
                    this->getTargetForJob(nextTarget), new T_output(output));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(wr);
        }

    protected:
        std::unique_ptr<T_input> currentWork;

        virtual std::string parseAndSerialize(const std::string& line) {
            return serialization::encode(boost::lexical_cast<T_input>(line));
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
        virtual void handleAdd(T_accum& current, T_input& work) {
            current += work;
        }

        /** Called to join this Reducer with the accumulator from another */
        virtual void handleJoin(T_accum& current, T_accum& other) {
            current += other;
        }

        /** Called when the reduction is complete, or nearly - recur() may be 
            used to keep the reduction alive (inject new work into this 
            reduction). */
        virtual void handleDone(T_accum& current) {
            this->emit(current);
        }


        /** Called by system to call handleDone() with proper setup */
        virtual bool dispatchDone(uint64_t reduceTag) {
            this->setCurrentReduce(reduceTag);
            this->currentRecord = this->currentReduce->originalWork.get();
            this->hadRecurrence = false;

            { //timer scope
                processor::Processor::WorkTimer timer(this->processor, 
                        processor::Processor::TIME_USER);
                this->handleDone(*this->currentReduce->accumulator);
            }

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
                        *this->currentWork);
            }

            this->currentWork.reset();
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
                        *this->currentJoin);
            }

            this->currentJoin.reset();
            this->currentReduce = 0;
            this->currentRecord = 0;
        }


        /** Output some work from this module */
        template<class T_output> void emit(const T_output& output) {
            processor::Processor::WorkTimer timer(this->processor, 
                    processor::Processor::TIME_SYSTEM);

            message::WorkRecord* wr = new message::TypedWorkRecord<T_output>(
                    this->getTargetForReducer(), new T_output(output));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(wr);
        }


        /** Output some work back into this module (from input location) */
        template<class T_output> void recur(const T_output& output) {
            this->recur(output, "");
        }


        /** Output some work back into this module at job named under config
            recurTo.target */
        template<class T_output> void recur(const T_output& output, 
                const std::string& target) {
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

            message::WorkRecord* wr = new message::TypedWorkRecord<T_output>(
                    ntarget, new T_output(output));
            wr->chainFrom(*this->currentRecord);
            this->processor->addWork(wr);

            //Restore old reduce information and set hadRecurrence so that our
            //reduction ring isn't marked dead
            this->currentRecord->setReduce(oldRank, oldTag);
            this->hadRecurrence = true;
        }


    protected:
        uint64_t currentReduceTag;
        job::ReduceAccumulator<T_accum>* currentReduce;
        std::unique_ptr<T_input> currentWork;
        std::unique_ptr<T_accum> currentJoin;
        /** Used in dispatchDone() to see if we had recurrence.  If we did not,
            the reduction is finished. */
        bool hadRecurrence;
        std::map<uint64_t, job::ReduceAccumulator<T_accum> > reduceMap;

        virtual std::string parseAndSerialize(const std::string& line) {
            return serialization::encode(boost::lexical_cast<T_input>(line));
        }

    private:
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
    };



    void addJob(const std::string& typeName, 
            std::function<job::JobBase* ()> allocator);
    void addReducer(const std::string& typeName, 
            std::function<job::ReducerBase* ()> allocator);
    void runProcessor(int argc, char** argv);
}

#endif//JOB_STREAM_H_
