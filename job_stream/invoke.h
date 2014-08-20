#ifndef JOB_STREAM_INVOKE_H_
#define JOB_STREAM_INVOKE_H_

#include <boost/optional.hpp>
#include "libexecstream/exec-stream.h"

namespace job_stream {
namespace invoke {

/** Runs an application to completion, returning up to two strings for stdout
    and stderr. */
std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs);

/** Called by main job_stream right before processor (and thus MPI routines and
    job_stream threads) starts. */
void _init();

} //invoke
} //job_stream

#endif//JOB_STREAM_INVOKE_H_
