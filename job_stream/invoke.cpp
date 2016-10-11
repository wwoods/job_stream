
#include "debug_internals.h"
#include "invoke.h"

#include "job_stream.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#define BOOST_PROCESS_WINDOWS_USE_NAMED_PIPE
#include <boost/process.hpp>

#include <functional>
#include <memory>
#include <sstream>

#if defined(OS_LINUX) || defined(OS_APPLE)
    #include <sys/stat.h>
#endif

namespace ba = boost::asio;
namespace bp = boost::process;

#if defined(BOOST_POSIX_API)
typedef ba::posix::stream_descriptor bpAsio;
#elif defined(BOOST_WINDOWS_API)
typedef ba::windows::stream_handle bpAsio;
#else
#  error "Unsupported platform."
#endif

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

            //Buffer stdout and stderr, so that we won't lock up if the program
            //pushes a lot of bytes
            boost::array<char, 4096> outBuffer, errBuffer;
            ba::io_service io_service;
            bpAsio outReader(io_service);
            bpAsio errReader(io_service);

            outReader.assign(es.get_stdout().handle().release());
            errReader.assign(es.get_stderr().handle().release());

            std::function<void(const boost::system::error_code&, std::size_t)>
                    outEnd, errEnd;
            auto outBegin = [&] {
                outReader.async_read_some(boost::asio::buffer(outBuffer),
                        boost::bind(outEnd, ba::placeholders::error,
                            ba::placeholders::bytes_transferred));
            };
            outEnd = [&](const boost::system::error_code& ec,
                    std::size_t bytesTransferred) {
                if (!ec) {
                    out << std::string(outBuffer.data(), bytesTransferred);
                    outBegin();
                }
            };
            auto errBegin = [&]() {
                errReader.async_read_some(boost::asio::buffer(errBuffer),
                        boost::bind(errEnd, ba::placeholders::error,
                            ba::placeholders::bytes_transferred));
            };
            errEnd = [&](const boost::system::error_code& ec,
                    std::size_t bytesTransferred) {
                if (!ec) {
                    err << std::string(errBuffer.data(), bytesTransferred);
                    errBegin();
                }
            };

            outBegin();
            errBegin();

            io_service.run();
            auto status = es.wait();
            if (!status.exited() || status.exit_status() != 0) {
                std::ostringstream ss;
                ss << "Bad exit on attempt " << trial+1 << " from";
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
                JobLog() << "RAISING TRANSIENT AFTER FAILURE NUMBER " << trial+1;
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
