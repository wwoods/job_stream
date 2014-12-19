
#include "job.h"
#include "message.h"
#include "module.h"
#include "processor.h"
#include "pythonType.h"
#include "yaml.h"

#include <boost/lexical_cast.hpp>
#include <exception>

namespace job_stream {
namespace job {

thread_local message::WorkRecord* SharedBase::currentRecord = 0;
thread_local bool SharedBase::targetIsModule = false;


void addJob(const std::string& typeName,
        std::function<job::JobBase* ()> allocator) {
    processor::Processor::addJob(typeName, allocator);
}


void addReducer(const std::string& typeName,
        std::function<job::ReducerBase* ()> allocator) {
    processor::Processor::addReducer(typeName, allocator);
}


SharedBase::SharedBase() {
}


SharedBase::~SharedBase() {
}


void SharedBase::forceCheckpoint(bool forceQuit) {
    this->processor->forceCheckpoint(forceQuit);
}


std::string SharedBase::getFullName() const {
    if (this->parent) {
        std::ostringstream fname;
        fname << this->parent->getFullName();
        if (this->id[0] == '[') {
            fname << this->id;
        }
        else {
            fname << "." << this->id;
        }
        return fname.str();
    }
    return this->id;
}


void SharedBase::lockOutCheckpointsUntilCompletion() {
    this->processor->workerWorkThisThread->lockForWork();
}


void SharedBase::setup(processor::Processor* processor, module::Module* parent,
        const std::string& id, const YAML::Node& config,
        YAML::GuardedNode* globalConfig) {
    this->processor = processor;
    this->parent = parent;
    this->id = id;

    //Setup private GuardedNodes for config
    this->__config.set(config);
    this->__globalConfig = globalConfig;

    //Now set public members
    this->config.set(this->__config);
    this->globalConfig.set(*this->__globalConfig);
}


void SharedBase::populateAfterRestore(YAML::GuardedNode* globalConfig,
        const YAML::Node& config, ReducerReallocMap& reducerMap) {
    //Parent and id were populated by serialization, processor by parent.
    this->setup(this->processor, this->parent, this->id, config,
            globalConfig);
    this->postSetup();
}


std::vector<std::string> SharedBase::_getTargetSiblingOrReducer(
        bool isReducerEmit) {
    std::vector<std::string> targetNew = this->currentRecord->getTarget();

    //Make sure our new target points to our parent module (if we are a reducer,
    //this will still point to the right place since our "parent" is our module)
    module::Module* p = this->parent;
    if (!this->targetIsModule) {
        //We want it to point to a module, always
        targetNew.pop_back();
    }

    //At this point, targetNew refers to p.  We let one reducer slide if this
    //was a reducer emit, since that means that the top reducer has been
    //processed.
    bool firstEmit = isReducerEmit;
    while (!p->hasReducer() || firstEmit) {
        firstEmit = false;
        //We're sending to at least a sibling of this node, so pop off our last
        //target element
        if (p->parent) {
            //Root module doesn't show up as a "target"
            targetNew.pop_back();
        }
        //See if it has a "to" field; note that the root module does not!
        const YAML::LockedNode& pTo = p->config["to"];
        if (!pTo) {
            if (!p->parent) {
                //Root level, has no reducer.  By convention, send
                //output::reduced.
                break;
            }
            std::ostringstream ss;
            ss << "Module " << p->getFullName() << " needs 'to'";
            throw std::runtime_error(ss.str());
        }
        //Find the next target in this chain; if it's not output, we're done
        //(found a suitable sibling)
        std::string next = pTo.as<std::string>();
        if (next != "output") {
            targetNew.push_back(next);
            return targetNew;
        }
        p = p->parent;
    }

    targetNew.push_back("output");
    if (isReducerEmit && !p->parent) {
        if (targetNew.size() != 1) {
            throw std::runtime_error("Bad target..?  "
                    "Shouldn't have anything in targetNew.");
        }
        targetNew.push_back("reduced");
    }
    return targetNew;
}


std::vector<std::string> SharedBase::getTargetForJob(std::string target) {
    if (target == "output") {
        return this->_getTargetSiblingOrReducer(false);
    }

    std::vector<std::string> targetNew = this->currentRecord->getTarget();
    //On our first send, target includes the job (it already did).  We
    //want to redirect to targetList based on the module level.  So we
    //always pop the last part of target.
    //...unless the work is already pointed at the current module (recur or emit
    //from within the "done" method of a reducer)
    if (!this->targetIsModule) {
        targetNew.pop_back();
    }
    targetNew.push_back(target);
    return targetNew;
}


std::vector<std::string> SharedBase::getTargetForReducer() {
    //Called in the context of a Reducer, meaning currentRecord's target was
    //the record that started the reduce - that is, it points to our module.
    //Note that this function is used for emit(), not recur().
    return this->_getTargetSiblingOrReducer(true);

    std::vector<std::string> targetNew = this->currentRecord->getTarget();
    if (!this->targetIsModule) {
        //We want it to point to a module
        targetNew.pop_back();
    }


    //Now it's the module containing our reducer; we want to go to a sibling
    //job.  So..
    if (this->parent->getLevel() != 0) {
        const YAML::LockedNode& parentTo = this->parent->config["to"];
        if (!parentTo) {
            std::ostringstream ss;
            ss << "Module " << this->parent->getFullName() << " needs 'to'";
            throw std::runtime_error(ss.str());
        }
        targetNew.pop_back();
        targetNew.push_back(parentTo.as<std::string>());
    }
    else {
        //Final output (that is, this is the top module, and we are final
        //output).
        targetNew.push_back("output");
        targetNew.push_back("reduced");
    }
    return targetNew;
}


std::string SharedBase::parseAndSerialize(const std::string& line) {
    //NOTE - The reason we try a pre-defined list of types rather than having
    //the templated job override a virtual parseAndSerialize interface is so
    //that standard users don't have to define istream methods for their custom
    //types.  Only the very first job (that gets input work) would need it
    //defined.  Easy workaround if anyone needs that ability: have a
    //string->UserType job before the real first job.
    std::string typeName = this->getInputTypeName();

    #define TRY_TYPE(T) \
            if (typeName == typeid(T).name()) { \
                T val = boost::lexical_cast<T>(line); \
                return serialization::encodeAsPtr(val); \
            }

    TRY_TYPE(std::string);
    TRY_TYPE(job_stream::python::SerializedPython);
    TRY_TYPE(float);
    TRY_TYPE(double);
    TRY_TYPE(uint64_t);
    TRY_TYPE(int64_t);
    TRY_TYPE(unsigned int);
    TRY_TYPE(int);
    TRY_TYPE(unsigned short);
    TRY_TYPE(short);
    TRY_TYPE(unsigned char);
    TRY_TYPE(char);

    std::ostringstream ss;
    ss << "Unrecognized work type for input: " << typeName;
    throw std::runtime_error(ss.str());
    #undef TRY_TYPE
}

} //job
} //job_stream
