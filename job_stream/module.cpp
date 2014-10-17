
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


void Module::populateAfterRestore(YAML::GuardedNode* globalConfig,
        const YAML::Node& config) {
    job::JobBase::populateAfterRestore(globalConfig, config);

    for (auto it = this->jobMap.begin(); it != this->jobMap.end(); it++) {
        it->second->processor = this->processor;
        it->second->populateAfterRestore(globalConfig,
                config["jobs"][it->second->id]);
    }

    if (this->reducer) {
        this->reducer->processor = this->processor;
        this->reducer->populateAfterRestore(globalConfig, config["reducer"]);
    }
}


void Module::postSetup() {
    //Sanity checks - jobs cannot be a sequence if input is defined.
    //Lock our config and get it.
    auto conf = this->config;
    if (conf["jobs"].IsSequence()) {
        if (conf["input"]) {
            std::ostringstream ss;
            ss << "Module " << this->getFullName() << " has input defined but "
                    "jobs is a list";
            throw std::runtime_error(ss.str());
        }
    }
    else if (!conf["input"]) {
        if (!conf["jobs"].IsSequence()) {
            std::ostringstream ss;
            ss << "Module " << this->getFullName() << " has no input defined";
            throw std::runtime_error(ss.str());
        }
    }

    //Is this module framed?
    if (conf["frame"]) {
        if (conf["reducer"]) {
            std::ostringstream ss;
            ss << "Module " << this->getFullName() << " cannot define a "
                    "reducer as it has a frame defined";
            throw std::runtime_error(ss.str());
        }

        //Clone our frame into reducer in case frame is a reference (since we
        //change its recurTo parameter).
        conf["reducer"] = YAML::Clone(conf["frame"]);
        if (conf["input"]) {
            conf["reducer"]["recurTo"] = conf["input"].as<
                    std::string>();
        }
        else {
            conf["reducer"]["recurTo"] = "0";
        }

        conf["input"] = "output";
        conf.remove("frame");
    }

    if (conf["jobs"].IsSequence()) {
        //Pipeline!  Since all of the code relies on named jobs, as they
        //are more flexible, we have to replace the jobs node with a named
        //version.
        YAML::Node newJobs;

        //If there is no input, then we need to set it to the first job in the
        //sequence.
        if (!conf["input"]) {
            conf["input"] = "0";
        }

        int jobId = 0;
        for (int i = 0, m = conf["jobs"].size(); i < m; i++) {
            YAML::LockedNode n = conf["jobs"][i];
            if (n["to"]) {
                std::ostringstream ss;
                ss << "Job " << jobId << " under " << this->getFullName();
                ss << " cannot have a 'to' configured.  If you need to";
                ss << " use 'to', you'll need to use named jobs instead";
                ss << " of a list.";
                throw std::runtime_error(ss.str());
            }

            //Clone the node in case it is a repeated reference (*submodule)
            YAML::Node nc = YAML::Clone(n);
            if (i < m - 1) {
                nc["to"] = boost::lexical_cast<std::string>(jobId + 1);
            }
            else {
                nc["to"] = "output";
            }
            newJobs[boost::lexical_cast<std::string>(jobId)] = nc;
            jobId += 1;
        }

        conf["jobs"] = newJobs;
    }

    //Assign our level
    if (this->parent) {
        this->level = this->parent->level + 1;
        if (!conf["to"]) {
            std::ostringstream ss;
            ss << "Module " << this->getFullName() << " needs a 'to'";
            throw std::runtime_error(ss.str());
        }
    }

    //Set up reducer, unless we've started from a checkpoint
    if (conf["reducer"] && !this->reducer) {
        this->reducer.reset(this->processor->allocateReducer(this,
                conf["reducer"]._getNode()));
    }
}


void Module::dispatchWork(message::WorkRecord& work) {
    this->currentRecord = &work;
    //If we end up assigning this work to a new reduction, we need to decrement
    //the placeholder childTagCount put on it in dispatchInit (after this work
    //is handled, of course)
    //Also make sure it's us that changed the ring, not some other reducer
    uint64_t startedReduceTag = 0;
    bool startedNewRing = false;

    if (processor::JOB_STREAM_DEBUG >= 2) {
        JobLog() << "Dispatching: " << this->getFullName();
    }

    const std::vector<std::string>& target = work.getTarget();
    if (this->level == target.size()) {
        //We're the end goal (this work just started living in our module).
        //If we have a reducer, we have to tag this work and create a new
        //reduction context
        if (this->reducer && this->reducer->dispatchInit(work)) {
            startedNewRing = true;
            startedReduceTag = work.getReduceTag();
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
                    printf("%s\n", work.getWorkAsString().c_str());
                }
            }
            else {
                throw std::runtime_error("shouldn't be reached; implemented in "
                        "SharedBase::getTarget...");
            }
        }
        else {
            //When a reducer is active, output is just a JobBase that is the
            //reducer.
            if (processor::JOB_STREAM_DEBUG >= 2) {
                JobLog() << "Passing to " << this->reducer->getFullName()
                        << ", tag " << work.getReduceTag();
            }
            this->reducer->dispatchAdd(work);
        }
    }
    else {
        //Process the work under the appropriate job (or forward to next module)
        if (processor::JOB_STREAM_DEBUG >= 2) {
            JobLog() << "Passing to " << this->getJob(curTarget)->getFullName();
        }
        this->getJob(curTarget)->dispatchWork(work);
    }

    if (startedNewRing) {
        this->processor->decrReduceChildTag(startedReduceTag, true);
    }

    this->currentRecord = 0;

    if (processor::JOB_STREAM_DEBUG >= 2) {
        JobLog() << "Completed dispatch " << this->getFullName();
    }
}


bool Module::wouldReduce(message::WorkRecord& work) {
    const std::vector<std::string>& target = work.getTarget();
    std::string curTarget;
    if (this->level >= target.size()) {
        if (this->reducer) {
            return true;
        }
        curTarget = this->config["input"].as<std::string>();
    }
    else {
        curTarget = target[this->level];
    }

    return this->getJob(curTarget)->wouldReduce(work);
}


job::JobBase* Module::getJob(const std::string& id) {
    Lock lock(this->mutex);

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

    const YAML::Node& config = this->config["jobs"][id]._getNode();
    job::JobBase* job = this->processor->allocateJob(this, id,
            config);
    this->jobMap[id].reset(job);
    return job;
}


std::string Module::getInputTypeName() {
    YAML::LockedNode input = this->config["input"];
    if (input.as<std::string>() == "output") {
        return this->reducer->getInputTypeName();
    }
    return this->getJob(input.as<std::string>())
            ->getInputTypeName();
}

} //module
} //job_stream
