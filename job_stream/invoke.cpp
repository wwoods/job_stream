
#include "invoke.h"

#include "job_stream.h"
#include "josuttis/fdstream.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/process.hpp>
#include <memory>
#include <sstream>

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


std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs) {
    return run(progAndArgs, std::vector<std::string>());
}


std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs,
        const std::vector<std::string>& transientErrors,
        int maxRetries) {
    //Forking is REALLY slow, even though it avoids the "No child processes"
    //issue we see when forking under mpi.  We'll try a NO_FORK version, which
    //simply re-launches the process if we get a "No child processes"
    //message, which seems to happen regularly here for whatever reason.
    bp::context ctx;
    ctx.stdout_behavior = bp::capture_stream();
    ctx.stderr_behavior = bp::capture_stream();
    ctx.stdin_behavior = bp::close_stream();
    ctx.environment = bp::self::get_environment();
    for (auto& k : ctx.environment) {
        if (k.first.find("OMPI_") != std::string::npos
                || k.first.find("OPAL_") != std::string::npos) {
            ctx.environment.erase(k.first);
        }
    }
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
        std::ostringstream out, err;
        try {
            bp::child es = bp::launch(progAndArgs[0], progAndArgs, ctx);
            auto status = es.wait();
            out << es.get_stdout().rdbuf();
            err << es.get_stderr().rdbuf();
            if (!status.exited() || status.exit_status() != 0) {
                std::ostringstream ss;
                ss << "Bad exit from";
                for (const std::string& arg : progAndArgs) {
                    ss << " " << arg;
                }
                ss << ": ****\n*STDERR*\n" << err.str();
                ss << "\n\n*STDOUT*\n" << out.str();
                ss << "\n**** Bad Exit";
                throw std::runtime_error(ss.str());
            }
            return std::make_tuple(out.str(), err.str());
        }
        catch (const std::exception& e) {
            std::string emsg(e.what());
            auto nf = std::string::npos;
            //We only retry if "No child processes" is the error.  This
            //is an OS thing that is transient.
            if (trial == trialm - 1) {
                throw;
            }

            bool isTransient = false;
            if (emsg.find("No child processes") != nf) {
                isTransient = true;
            }
            else {
                for (int tranI = 0, tranIm = transientErrors.size();
                        tranI < tranIm; tranI++) {
                    if (err.str().find(transientErrors[tranI]) != nf) {
                        isTransient = true;
                        break;
                    }
                }
            }

            if (!isTransient) {
                throw;
            }
        }
    }
}

} //invoke
} //job_stream
