#ifndef JOB_STREAM_JOB_H_
#define JOB_STREAM_JOB_H_

#include "message.h"
#include "yaml.h"

#include <memory>
#include <string>

namespace job_stream {
namespace module {
    class Module;
}
namespace processor {
    class Processor;
}

namespace job {
    /** Unspecialized, internal job / reducer base class.  All jobs should 
        actually derive from job_stream::Job<WorkType>, and all reducers from
        job_stream::Reducer<AccumulatorType[, WorkType]>. */
    class SharedBase {
        friend class module::Module;
        friend class processor::Processor;

    public:
        SharedBase();
        virtual ~SharedBase();

        /* Can override to check config / do something at setup() time. */
        virtual void postSetup() {}

        const YAML::Node& getConfig() const { return this->config; }

        /* For debug; return double-colon delimited job id. */
        std::string getFullName() const;

        /* To make dispatching class types easier, the constructor does nothing.
           This function goes ahead and sets up the job so it can receive work.
           */
        void setup(processor::Processor* processor,
                module::Module* parent,
                const std::string& id,
                const YAML::Node& config, 
                const YAML::Node& globalConfig);

    protected:
        /* Our job's specific config, including "to", "id", and "type". */
        YAML::Node config;

        /* The current WorkRecord being processed; NULL out of processing */
        message::WorkRecord* currentRecord;

        /* Our module's config, from global module_config or submodule
         * instantiation */
        YAML::Node globalConfig;

        /* This job's name (local to its module) */
        std::string id;

        /* This job / module's parent module */
        module::Module* parent;
    
        /* The processor that we are running under */
        processor::Processor* processor;

        /** Given a string (input from stdin or console), convert it to our
            input type and serialize it back to a string that can be sent
            as work. */
        virtual std::string parseAndSerialize(const std::string& line) = 0;

        /* Put the given payload into module's output.  Called by reducers. */
        void sendModuleOutput(const std::string& payload);

        /* Put the given payload, which is a boost::serialized archive, into a
           WorkRecord and dispatch it to a given target from config. */
        void sendTo(const YAML::Node& targetList, const std::string& payload);
    };


    /** Base class for Jobs */
    class JobBase : public SharedBase {
    public:
        /* Pass work to handleWork() function in templated override. */
        virtual void dispatchWork(message::WorkRecord& work) = 0;
    };


    /** Base class for Reducers, which take multiple outputs and combine them. 
        */
    class ReducerBase : public SharedBase {
    public:
        /** Dispatch to templated init */
        virtual void dispatchInit(message::WorkRecord& work) = 0;

        /** Dispatch to templated add T_init to T_accum */
        virtual void dispatchAdd(message::WorkRecord& work) = 0;

        /** Dispatch to templated join T_accum with T_accum */
        virtual void dispatchJoin(message::WorkRecord& work) = 0;

        /** Dispatch to templated done.  Returns true if no recurrence occurred,
            and the ring is fully dead. */
        virtual bool dispatchDone(uint64_t reduceTag) = 0;
    };


    template<class T_accum>
    struct ReduceAccumulator {
        /* Copy of the original WorkRecord that spawned this reduction */
        std::unique_ptr<message::WorkRecord> originalWork;

        /* The accumulator for this record */
        T_accum accumulator;
    };
}
}

#endif//JOB_STREAM_JOB_H_
