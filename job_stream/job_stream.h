#ifndef JOB_STREAM_H_
#define JOB_STREAM_H_

#include "message.h"
#include "job.h"
#include "processor.h"
#include "serialization.h"
#include "yaml.h"

#include <boost/function.hpp>
#include <boost/mpi.hpp>
#include <string>

namespace job_stream {
    /** Specialized Job base class. */
    template<class T_input>
    class Job : public job::JobBase {
    public:
        /* Pass work into handleWork and possibly emit new tuples.   */
        virtual void dispatchWork(message::WorkRecord& input) {
            this->currentRecord = &input;
            input.putWorkInto(this->currentWork);
            try {
                this->handleWork(this->currentWork);
                this->currentRecord = 0;
            }
            catch (const std::exception& e) {
                this->currentRecord = 0;
                throw;
            }
        }


        virtual void handleWork(T_input& work) = 0;


        template<class T_output> void emit(const T_output& output) {
            this->emit(output, "");
        }


        template<class T_output> void emit(const T_output& output, 
                const std::string& target) {
            std::string payload = serialization::encode(output);
            if (!target.empty()) {
                this->sendTo(this->config["to"][target], payload);
            }
            else {
                this->sendTo(this->config["to"], payload);
            }
        }

    protected:
        T_input currentWork;
    };



    /** Specialized reducer class */
    template<class T_input, class T_accum = T_input>
    class Reducer : public job::ReducerBase {
    public:
        /** Handle initialization of accumulator.  Must be callable several 
            times, and the results must be mergable via handleMore. */
        virtual void handleInit(T_accum& current) = 0;
        virtual void handleMore(T_accum& current, T_input& more) = 0;
        virtual void handleDone(T_accum& current) {
            this->emit(current);
        }


        virtual bool dispatchDone(uint64_t reduceTag) {
            this->setCurrentReduce(reduceTag);
            this->currentRecord = this->currentReduce->originalWork;
            this->hadRecurrence = false;
            this->handleDone(this->currentReduce->accumulator);

            this->currentRecord = 0;
            this->currentReduce = 0;

            if (!this->hadRecurrence) {
                this->reduceMap.erase(reduceTag);
                return true;
            }
            return false;
        }


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
                record.originalWork = new message::WorkRecord(
                        work.serialized());
                this->handleInit(record.accumulator);

                //Track when this reduction is finished
                this->processor->startRingTest(tag, work.getReduceTag(), this);
            }

            //Now that we've backed up the original work, overwrite the reduce
            //params.
            work.setReduce(homeRank, tag);
        }


        virtual void dispatchWork(message::WorkRecord& work) {
            this->currentRecord = &work;
            work.putWorkInto(this->currentWork);
            this->setCurrentReduce(work.getReduceTag());
            this->handleMore(this->currentReduce->accumulator, 
                    this->currentWork);
            this->currentReduce = 0;
            this->currentRecord = 0;
        }


        /** Output some work from this module */
        template<class T_output> void emit(const T_output& output) {
            this->sendModuleOutput(serialization::encode(output));
        }


        /** Output some work back into this module (from input location) */
        template<class T_output> void recur(const T_output& output) {
            this->recur(output, "");
        }


        /** Output some work back into this module at job named under config
            recurTo.target */
        template<class T_output> void recur(const T_output& output, 
                const std::string& target) {
            std::string payload = serialization::encode(output);
            if (target.empty()) {
                this->sendTo(this->config["recurTo"], payload);
            }
            else {
                this->sendTo(this->config["recurTo"][target], payload);
            }

            this->hadRecurrence = true;
        }


    protected:
        job::ReduceAccumulator<T_accum>* currentReduce;
        T_input currentWork;
        /** Used in dispatchDone() to see if we had recurrence.  If we did not,
            the reduction is finished. */
        bool hadRecurrence;
        std::map<uint64_t, job::ReduceAccumulator<T_accum> > reduceMap;

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

            this->currentReduce = &iter->second;
        }
    };



    void addJob(const std::string& typeName, 
            boost::function<job::JobBase* ()> allocator);
    void addReducer(const std::string& typeName, 
            boost::function<job::ReducerBase* ()> allocator);
    void runProcessor(int argc, char** argv);
}

#endif//JOB_STREAM_H_
