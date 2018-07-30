
#include "debug_internals.h"
#include "invoke.h"

#include "job_stream.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/process.hpp>

#include <functional>
#include <memory>
#include <sstream>

#if defined(OS_LINUX) || defined(OS_APPLE)
    #include <sys/stat.h>
#endif

namespace ba = boost::asio;
namespace bp = boost::process;

namespace job_stream {
namespace invoke {

int isTestCondition = 0;

void _init() {
}


FakeInvokeWait::FakeInvokeWait() {
    isTestCondition++;
}
FakeInvokeWait::~FakeInvokeWait() {
    --isTestCondition;
}


std::tuple<std::string, std::string> _statusToResultOrException(
        const std::vector<std::string>& progAndArgs,
        const std::tuple<int, std::string, std::string>& r) {
    if (std::get<0>(r) != 0) {
        std::ostringstream ss;
        ss << "Bad exit on from";
        for (const std::string& arg : progAndArgs) {
            ss << " " << arg;
        }
        ss << ": ****\n*STDERR*\n" << std::get<2>(r);
        ss << "\n\n*STDOUT*\n" << std::get<1>(r);
        ss << "\n**** Bad Exit";
        throw std::runtime_error(ss.str());
    }

    return std::make_tuple(std::get<1>(r), std::get<2>(r));
}


std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs) {
    const auto r = runWithStdin(progAndArgs, "");
    return _statusToResultOrException(progAndArgs, r);
}


std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs,
        const std::vector<std::string>& transientErrors,
        int maxRetries) {
    int timeToNext = 1;
    for (int trial = 0, trialm = maxRetries; trial < trialm; trial++) {
        if (trial > 0) {
            JobLog() << "TRYING AGAIN (" << trial+1 << ") IN "
                    << timeToNext << " SEC";
            if (isTestCondition == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(
                        timeToNext * 1000));
            }
            timeToNext += 1;
        }

        const auto r = runWithStdin(progAndArgs, "");
        if (std::get<0>(r) != 0) {
            //Can we retry?
            if (trial == trialm - 1) {
                JobLog() << "RAISING FAILURE CONDITIONALLY ON FINAL TRIAL "
                        << trial+1;
                return _statusToResultOrException(progAndArgs, r);
            }

            //Check if it's transient.
            //We only retry if "No child processes" is the error.  This
            //is an OS thing that is transient.

            std::string emsg(std::get<2>(r));
            auto nf = std::string::npos;
            bool isTransient = false;
            if (emsg.find("No child processes") != nf) {
                isTransient = true;
            }
            else {
                for (int tranI = 0, tranIm = transientErrors.size();
                        tranI < tranIm; tranI++) {
                    if (emsg.find(transientErrors[tranI]) != nf) {
                        isTransient = true;
                        break;
                    }
                }
            }

            if (isTransient) continue;

            //If we reach here, we know it will fail, and is non-transient.
            JobLog() << "RAISING NON-TRANSIENT FAILURE AFTER TRIAL "
                    << trial+1;
        }
        return _statusToResultOrException(progAndArgs, r);
    }

    throw std::runtime_error("Shouldn't happen - must exit before this.");
}


std::tuple<int, std::string, std::string> runWithStdin(
        const std::vector<std::string>& progAndArgs,
        const std::string& stdin) {
    //Forking is REALLY slow, even though it avoids the "No child processes"
    //issue we see when forking under mpi.  We'll try a NO_FORK version, which
    //simply re-launches the process if we get a "No child processes"
    //message, which seems to happen regularly here for whatever reason.
    auto env_self = boost::this_process::environment();
    bp::environment env = env_self;
    for (bp::environment::iterator it = env.begin(); it != env.end(); ++it) {
        const std::string name = (*it).get_name();
        if (name.find("OMPI_") != std::string::npos
                || name.find("OPAL_") != std::string::npos) {
            env.erase(name);
        }
    }

    //Now that the environment is configured properly, ensure that the
    //executable exists.
    {
        std::ifstream fileExists(progAndArgs[0]);
        if (!fileExists) {
            ERROR("Program not found (replace with absolute path?): '"
                    << progAndArgs[0] << "'");
        }

        #if defined(OS_LINUX) || defined(OS_APPLE)
        //Also need to check for executable permissions
        struct stat st;
        if (stat(progAndArgs[0].c_str(), &st) < 0) {
            ERROR("POSIX stat() failed on: '" << progAndArgs[0] << "'");
        }

        if ((st.st_mode & S_IEXEC) == 0) {
            ERROR("Program specified not executable: '" << progAndArgs[0]
                    << "'");
        }
        #endif
    }

    //Buffer stdout and stderr, so that we won't lock up if the program
    //pushes a lot of bytes
    ba::io_service io_service;
    std::future<std::string> out, err;

    //Format stdin in a way boost::process understands
    bp::opstream input;

    std::vector<std::string> progArgs(progAndArgs);
    progArgs.erase(progArgs.begin());
    bp::child es(progAndArgs[0], progArgs,
            bp::std_out > out, bp::std_err > err,
            bp::std_in < input, io_service, env);

    input << stdin;
    input.flush();
    input.pipe().close();

    io_service.run();
    es.wait();
    int status = es.exit_code();
    return std::make_tuple(status, out.get(), err.get());
}

} //invoke
} //job_stream
