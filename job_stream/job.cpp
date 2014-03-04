
#include "job.h"
#include "message.h"
#include "module.h"
#include "processor.h"
#include "yaml.h"

#include <boost/lexical_cast.hpp>
#include <exception>

namespace job_stream {
namespace job {

thread_local message::WorkRecord* SharedBase::currentRecord = 0;
thread_local bool SharedBase::targetIsModule = false;


SharedBase::SharedBase() {
}


SharedBase::~SharedBase() {
}


std::string SharedBase::getFullName() const {
    if (this->parent) {
        return this->parent->getFullName() + "::" + this->id;
    }
    return this->id;
}


void SharedBase::setup(processor::Processor* processor, module::Module* parent,
        const std::string& id, const YAML::Node& config, 
        const YAML::Node& globalConfig) {
    this->processor = processor;
    this->parent = parent;
    this->id = id;
    this->config = config;
    this->globalConfig = globalConfig;
}


std::vector<std::string> SharedBase::getTargetForJob(std::string target) {
    std::vector<std::string> targetNew = this->currentRecord->getTarget();
    //On our first send, target includes the job (it already did).  We
    //want to redirect to targetList based on the module level.  So we 
    //always pop the last part of target.
    //...unless it's the root module (recur on top-level reducer)
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
    std::vector<std::string> targetNew = this->currentRecord->getTarget();
    if (!this->targetIsModule) {
        //We want it to point to a module
        targetNew.pop_back();
    }


    //Now it's the module containing our reducer; we want to go to a sibling
    //job.  So..
    if (this->parent->getLevel() != 0) {
        const YAML::Node& parentTo = this->parent->getConfig()["to"];
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
    std::string typeName = this->getInputTypeName();

    #define TRY_TYPE(T) \
            if (typeName == typeid(T).name()) { \
                T val = boost::lexical_cast<T>(line); \
                return serialization::encode(&val); \
            }

    TRY_TYPE(std::string);
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
