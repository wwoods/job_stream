#ifndef JOB_STREAM_JOB_H_
#define JOB_STREAM_JOB_H_

#include "message.h"
#include "types.h"
#include "yaml.h"

#include <memory>
#include <mutex>
#include <string>

namespace job_stream {
namespace module {
    class Module;
}
namespace job {
    class JobBase;
    class ReducerBase;
}
namespace processor {
    class Processor;
}

namespace job {
    void addJob(const std::string& typeName, 
            std::function<job::JobBase* ()> allocator);

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

        /** Call this at any point to force a checkpoint after the current
            work completes. */
        void forceCheckpoint();

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

        /** Used when restoring from a checkpoint, populate our and all
            allocated child jobs / reducers based on config.  Also calls
            postSetup, since that was not triggered during checkpoint restore.

            Default behavior is just to call setup() with our extant parameters,
            and then call postSetup.
            */
        virtual void populateAfterRestore(const YAML::Node& globalConfig,
                const YAML::Node& config);

    protected:
        /* Our job's specific config, including "to", "id", and "type". */
        YAML::Node config;

        /* The current WorkRecord being processed; NULL out of processing */
        static thread_local message::WorkRecord* currentRecord;

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
        static thread_local bool targetIsModule;

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

    private:
        friend class boost::serialization::access;
        /*  Serialization for checkpoints; only used bottom up.  You do not need
            to register your own derived classes!  We just need to detect if
            something is a Reducer, and if so, save its reduceMap. */
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            //NOTE - A LOT of links are NOT serialized.  The reason we serialize
            //SharedBase (and jobs and reducers at all) is because ReduceInfo
            //fingerprints need to link back to the correct objects.  So, we
            //archive the tree's shape in general, but then fill it in from the
            //config file.
            ar & this->parent;
            ar & this->id;
        }
    };


    /** Base class for Jobs */
    class JobBase : public SharedBase {
    public:
        /** Pass work to handleWork() function in templated override. */
        virtual void dispatchWork(message::WorkRecord& work) = 0;

        /** Pass work to see if it would start a new reduction. */
        virtual bool wouldReduce(message::WorkRecord& work) = 0;

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & boost::serialization::base_object<SharedBase>(*this);
        }
    };


    /** Base class for Reducers, which take multiple outputs and combine them. 
        */
    class ReducerBase : public SharedBase {
    public:
        /** Dispatch to templated init */
        virtual bool dispatchInit(message::WorkRecord& work) = 0;

        /** Dispatch to templated add T_init to T_accum */
        virtual void dispatchAdd(message::WorkRecord& work) = 0;

        /** Dispatch to templated join T_accum with T_accum */
        virtual void dispatchJoin(message::WorkRecord& work) = 0;

        /** Dispatch to templated done.  Returns true if no recurrence occurred,
            and the ring is fully dead. */
        virtual bool dispatchDone(uint64_t reduceTag) = 0;

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & boost::serialization::base_object<SharedBase>(*this);
        }
    };


    template<class T_accum>
    struct ReduceAccumulator {
        /* Copy of the original WorkRecord that spawned this reduction */
        std::unique_ptr<message::WorkRecord> originalWork;

        /* The accumulator for this record */
        std::unique_ptr<T_accum> accumulator;

        /** To prevent client code from needing its own locks, we ensure that one
            Reducer's methods aren't called simulateously for the same current
            reduction. */
        Mutex mutex;

        /* Used for Frames, allows first work to be distinguished.... */
        bool gotFirstWork;

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->originalWork & this->accumulator & this->gotFirstWork;
        }
    };
}
}

#endif//JOB_STREAM_JOB_H_
