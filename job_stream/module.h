#ifndef JOB_STREAM_MODULE_H_
#define JOB_STREAM_MODULE_H_

#include "job.h"
#include "message.h"
#include "types.h"
#include "yaml.h"

#include <map>
#include <memory>
#include <mutex>

namespace job_stream {
namespace module {

/** A collection of jobs with an optional reducing component. */
class Module : public job::JobBase {
public:
    static Module* make();

    Module();

    /* Find the Job responsible for processing work and run it. */
    virtual void dispatchWork(message::WorkRecord& work);

    /** Would the given work make a new reduction? */
    virtual bool wouldReduce(message::WorkRecord& work);

    /* Get our module level */
    int getLevel() const { return this->level; }

    /* Ensure that our config keys are set */
    virtual void postSetup();

protected:
    /** Return a JobBase for the given job id */
    job::JobBase* getJob(const std::string& id);

    /** Returns the input type of the first job (or reducer input) */
    virtual std::string getInputTypeName();

private:
    /* Map of job ids to JobBase */
    std::map<std::string, std::unique_ptr<job::JobBase> > jobMap;

    /* This module's level (base-0; for indexing arrays).  Points to the child
       job of this module, more specifically. */
    int level;

    Mutex mutex;

    /* Our reducer, if any */
    std::unique_ptr<job::ReducerBase> reducer;
};

}
} //job_stream

#endif//JOB_STREAM_MODULE_H_
