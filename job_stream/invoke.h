#ifndef JOB_STREAM_INVOKE_H_
#define JOB_STREAM_INVOKE_H_

#include <string>
#include <tuple>
#include <vector>

namespace job_stream {
namespace invoke {

/** Runs an application to completion, returning up to two strings for stdout
    and stderr.

    maxSeconds - If non-zero, terminate this program if it runs longer than
    maxSeconds.  */
std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs, int maxSeconds = 0);

/** Called by main job_stream right before processor (and thus MPI routines and
    job_stream threads) starts. */
void _init();

} //invoke
} //job_stream

#endif//JOB_STREAM_INVOKE_H_
