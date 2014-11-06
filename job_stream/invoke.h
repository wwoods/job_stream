#ifndef JOB_STREAM_INVOKE_H_
#define JOB_STREAM_INVOKE_H_

#include <string>
#include <tuple>
#include <vector>

namespace job_stream {
namespace invoke {

/** Runs an application to completion, returning up to two strings for stdout
    and stderr.

    Retries up to 20 times for "No child processes" issue.  Raises an exception
    on any other error. */
std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs);

/** Runs an application to completion, returning up to two strings for stdout
    and stderr.

    transientErrors - If specified, is a list of expected substrings to look
            for in stderr.  If any of the strings in the vector exist in stderr,
            the program will be re-launched, up to maxRetries times. */
std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs,
        const std::vector<std::string>& transientErrors,
        int maxRetries = 20);

/** Called by main job_stream right before processor (and thus MPI routines and
    job_stream threads) starts. */
void _init();

/** Test object; construct it for when you want the invoke to NOT delay before
    running the application again on a transient error. */
struct FakeInvokeWait {
    FakeInvokeWait();
    ~FakeInvokeWait();
};

} //invoke
} //job_stream

#endif//JOB_STREAM_INVOKE_H_
