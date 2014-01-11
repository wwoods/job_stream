
#include "module.h"
#include "processor.h"

namespace job_stream {
namespace module {

Module* Module::make() {
    return new Module();
}


Module::Module() : level(0) {
}


Module::~Module() {
    //Delete all of our jobs
    for (auto iter = this->jobMap.begin(); iter != this->jobMap.end(); iter++) {
        delete iter->second;
    }
    this->jobMap.clear();
}


void Module::postSetup() {
    if (!this->config["input"]) {
        std::ostringstream ss;
        ss << "Module " << this->getFullName() << " has no input defined";
        throw std::runtime_error(ss.str());
    }

    //Assign our level
    if (this->parent) {
        this->level = this->parent->level + 1;
        if (!this->config["to"]) {
            std::ostringstream ss;
            ss << "Module " << this->getFullName() << " needs a 'to'";
            throw std::runtime_error(ss.str());
        }
    }

    //Set up reducer
    if (this->config["reducer"]) {
        this->reducer = this->processor->allocateReducer(this, 
                this->config["reducer"]);
    }
    else {
        this->reducer = 0;
    }
}


void Module::dispatchWork(message::WorkRecord& work) {
    this->currentRecord = &work;

    if (processor::JOB_STREAM_DEBUG >= 2) {
        std::ostringstream ss;
        ss << "Dispatching: " << this->getFullName();
        fprintf(stderr, "%s\n", ss.str().c_str());
    }

    const std::vector<std::string>& target = work.getTarget();
    if (this->level == target.size()) {
        //We're the end goal (this work just started living in our module).  
        //If we have a reducer, we have to tag this work and create a new 
        //reduction context
        if (this->reducer) {
            this->reducer->dispatchInit(work);
        }

        //Then, pass to our input job.
        std::string firstJob = this->config["input"].as<std::string>();
        work.redirectTo(firstJob);
    }

    //Now we're looking at the input job.
    std::string curTarget = target[this->level];
    if (curTarget == "output") {
        //Reduce, or do 1 job step on output...
        bool isReduced = false;
        if (target.size() == this->level + 2) {
            if (target[this->level + 1] != "reduced") {
                throw std::runtime_error("Extended output not reduced?");
            }
            isReduced = true;
        }

        if (!this->reducer || isReduced) {
            //This is finalized output.
            if (!this->config["to"]) {
                if (this->level != 0) {
                    std::ostringstream ss;
                    ss << "Module '" << this->id << "' needs 'to' configured";
                    throw std::runtime_error(ss.str());
                }
                else {
                    //Print as str to stdout for root module by default
                    std::string workAsString;
                    try {
                        workAsString = work.getWorkAsString();
                    }
                    catch (const boost::archive::archive_exception& e) {
                        fprintf(stderr, "Work reached output and wasn't "
                                "string\n");
                        throw;
                    }
                    printf("%s\n", workAsString.c_str());
                }
            }
            else {
                throw std::runtime_error("shouldn't be reached; implemented in "
                        "sendModuleOutput.");
            }
        }
        else {
            //When a reducer is active, output is just a JobBase that is the 
            //reducer.
            this->reducer->dispatchWork(work);
        }
    }
    else {
        //Process the work under the appropriate job (or forward to next module)
        this->getJob(curTarget)->dispatchWork(work);
    }

    this->currentRecord = 0;

    if (processor::JOB_STREAM_DEBUG >= 2) {
        std::ostringstream ss;
        ss << "Completed dispatch " << this->getFullName();
        fprintf(stderr, "%s\n", ss.str().c_str());
    }
}


job::JobBase* Module::getJob(const std::string& id) {
    auto jobIter = this->jobMap.find(id);
    if (jobIter != this->jobMap.end()) {
        return jobIter->second;
    }

    //Make the job
    if (id == "output") {
        if (!this->reducer) {
            std::ostringstream ss;
            ss << "Module does not have a reducer: " << this->getFullName();
            throw std::runtime_error(ss.str());
        }
        return this->reducer;
    }
    else if (!this->config["jobs"][id]) {
        std::ostringstream msg;
        msg << "Unrecognized job id: '" << this->getFullName() << "::" << id 
                << "'";
        throw std::runtime_error(msg.str());
    }

    const YAML::Node& config = this->config["jobs"][id];
    job::JobBase* job = this->processor->allocateJob(this, id, config);
    this->jobMap[id] = job;
    return job;
}

} //module
} //job_stream
