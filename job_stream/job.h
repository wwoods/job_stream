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

        /** If you notice you parallelism isn't as good as you might expect,
            call this function periodically in your job / reducer.  It updates
            all of the asynchronous MPI operations, meaning that buffers that
            were once full may be filled again. */
        void checkMpi();

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

        /** Since reducers can have handleWork() vs handleDone(), one of which
            has ::output as the target, and the other is the target of the 
            module itself.  */
        bool targetIsModule;

        /** Given a target relative to our module, return the whole path to
            that target. */
        std::vector<std::string> getTargetForJob(std::string target);

        /** Get a target based on our config, if we are a reducer. */
        std::vector<std::string> getTargetForReducer();

        /** Look at our template arguments and return typeid(T).name() so that
            we can cast basic input types into the system appropriately. */
        virtual std::string getInputTypeName() = 0;

        /* Put the given payload into module's output.  Called by reducers. */
        //void sendModuleOutput(const std::string& payload);

        /* Put the given payload, which is a boost::serialized archive, into a
           WorkRecord and dispatch it to a given target from config. */
        //void sendTo(const YAML::Node& targetList, const std::string& payload);

        /** Take a line of stdin or argv input and convert it to the 
            appropriate type for this job (or module's first job). */
        std::string parseAndSerialize(const std::string& line);
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
        std::unique_ptr<T_accum> accumulator;

        /* Used for Frames, allows first work to be distinguished.... */
        bool gotFirstWork;
    };
}
}

#endif//JOB_STREAM_JOB_H_
