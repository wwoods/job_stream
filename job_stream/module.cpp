
#include "module.h"
#include "processor.h"

#include <boost/lexical_cast.hpp>

namespace job_stream {
namespace module {

Module* Module::make() {
    return new Module();
}


Module::Module() : level(0) {
}


void Module::postSetup() {
    if (!this->config["input"]) {
        if (this->config["jobs"].IsSequence()) {
            //Pipeline!  Since all of the code relies on named jobs, as they
            //are more flexible, we have to replace the jobs node with a named
            //version.
            YAML::Node newJobs;
            this->config["input"] = "1";
            int jobId = 1;
            for (int i = 0, m = this->config["jobs"].size(); i < m; i++) {
                YAML::Node n = this->config["jobs"][i];
                if (n["to"]) {
                    std::ostringstream ss;
                    ss << "Job " << jobId << " under " << this->getFullName();
                    ss << " cannot have a 'to' configured.  If you need to";
                    ss << " use 'to', you'll need to use named jobs instead";
                    ss << " of a list.";
                    throw std::runtime_error(ss.str());
                }

                if (i < m - 1) {
                    n["to"] = boost::lexical_cast<std::string>(jobId + 1);
                }
                else {
                    n["to"] = "output";
                }
                newJobs[boost::lexical_cast<std::string>(jobId)] = n;
                jobId += 1;
            }

            this->config["jobs"] = newJobs;
        }
        else {
            std::ostringstream ss;
            ss << "Module " << this->getFullName() << " has no input defined";
            throw std::runtime_error(ss.str());
        }
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
        this->reducer.reset(this->processor->allocateReducer(this, 
                this->config["reducer"]));
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
            if (processor::JOB_STREAM_DEBUG >= 2) {
                std::ostringstream ss;
                ss << "Passing to " << this->reducer->getFullName();
                fprintf(stderr, "%s\n", ss.str().c_str());
            }
            this->reducer->dispatchAdd(work);
        }
    }
    else {
        //Process the work under the appropriate job (or forward to next module)
        if (processor::JOB_STREAM_DEBUG >= 2) {
            std::ostringstream ss;
            ss << "Passing to " << this->getJob(curTarget)->getFullName();
            fprintf(stderr, "%s\n", ss.str().c_str());
        }
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
        return jobIter->second.get();
    }

    //Make the job
    if (id == "output") {
        std::ostringstream ss;
        ss << "getJob() called for output? " << this->getFullName();
        throw std::runtime_error(ss.str());
    }
    else if (!this->config["jobs"][id]) {
        std::ostringstream msg;
        msg << "Unrecognized job id: '" << this->getFullName() << "::" << id 
                << "'";
        throw std::runtime_error(msg.str());
    }

    const YAML::Node& config = this->config["jobs"][id];
    job::JobBase* job = this->processor->allocateJob(this, id, 
            config);
    this->jobMap[id].reset(job);
    return job;
}


std::string Module::parseAndSerialize(const std::string& line) {
    return this->getJob(this->config["input"].as<std::string>())
            ->parseAndSerialize(line);
}

} //module
} //job_stream
