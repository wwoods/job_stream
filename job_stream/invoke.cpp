
#include "invoke.h"

#include "job_stream.h"
#include "josuttis/fdstream.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/process.hpp>
#include <memory>
#include <sstream>

#define NO_FORK 1

namespace bp = boost::process;

namespace job_stream {
namespace invoke {

job_stream::Mutex forkLock;

uint64_t runningTotal = 0;
struct RunData {
    std::unique_ptr<bp::child> es;
    /** We buffer the program's output and error streams into our own
        stringstreams that we can control more precisely (that is, don't
        register something as a line unless there is actually a newline or the
        application is dead). */
    std::unique_ptr<std::stringstream> stdout;
    std::unique_ptr<std::stringstream> stderr;

    RunData() {
        throw std::runtime_error("RunData() with no params should not be "
                "called");
    }
    RunData(std::unique_ptr<bp::child> es)
            : stdout(new std::stringstream()), stderr(new std::stringstream()) {
        this->es = std::move(es);
    }
    RunData(RunData&& other) : stdout(std::move(other.stdout)),
            stderr(std::move(other.stderr)) {
        std::swap(this->es, other.es);
    }
};
std::map<std::string, RunData> running;

//[0] is read end, [1] is write end
int forker_input[2], forker_output[2], forker_error[2];
int forker_read, forker_write;
std::unique_ptr<boost::fdistream> forker_in;
std::unique_ptr<boost::fdostream> forker_out;

void _checkedPipe(int arr[]);
void _checkExists(const std::string& pid);
void _forkerHandleLine(const std::string& line);

void _init() {
    #if NO_FORK
    return;
    #endif
    runningTotal = 0;
    _checkedPipe(forker_input);
    _checkedPipe(forker_output);
    _checkedPipe(forker_error);

    pid_t child = fork();
    if (child == 0) {
        //child
        //We do NOT need to overwrite the stdin, stdout, stderr of this process,
        //since we use the pipes for communication.  Any error messages we
        //want to go to the error terminals of the parent program.
        //dup2(forker_input[0], 0);
        //dup2(forker_output[1], 1);
        //dup2(forker_error[1], 2);
        close(forker_input[1]);
        close(forker_output[0]);
        close(forker_error[0]);

        forker_read = forker_input[0];
        forker_write = forker_output[1];
        forker_in.reset(new boost::fdistream(forker_read));
        forker_out.reset(new boost::fdostream(forker_write));

        //Read command lines over and over and over...
        std::string line;
        while (std::getline(*forker_in, line)) {
            _forkerHandleLine(line);
        }
        //All done when input closes
        exit(0);
    }
    else {
        //parent
        close(forker_input[0]);
        close(forker_output[1]);
        close(forker_error[1]);

        forker_read = forker_output[0];
        forker_write = forker_input[1];
        forker_in.reset(new boost::fdistream(forker_read));
        forker_out.reset(new boost::fdostream(forker_write));
    }
}

void _checkedPipe(int arr[]) {
    auto status = pipe(arr);
    if (status == -1) {
        std::ostringstream ss;
        ss << "Failed to create pipe, status: " << status;
        throw std::runtime_error(ss.str());
    }
}

void _checkExists(const std::string& pid) {
    if (running.count(pid) != 1) {
        std::ostringstream ss;
        ss << "Pid not running: " << pid;
        throw std::runtime_error(pid);
    }
}

void _forkerHandleLine(const std::string& line) {
    std::string req = line.substr(1);
    std::string response;
    if (line[0] == 'R') {
        //Run, args delimited from program by --!!!!--
        size_t split = req.find("--!!!!--");
        if (split < 0) {
            throw std::runtime_error(
                    "Did not have prog--!!!!---args split!");
        }

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
        std::string progPart = req.substr(0, split);
        std::string argPart = req.substr(split + 8);
        std::vector<std::string> args;
        boost::algorithm::split(args, argPart, boost::is_any_of(" "));
        args.emplace(args.begin(), progPart);
        std::unique_ptr<bp::child> es(new bp::child(bp::launch(
                progPart, args, ctx)));
        runningTotal++;
        //printf("Launched %lu (%lu)\n", runningTotal,
        //       (unsigned long)es->get_id());
        std::ostringstream fakePid;
        fakePid << es->get_id();
        std::string fakePidStr = fakePid.str();
        running.emplace(fakePidStr, std::move(es));
        *forker_out << "K" << fakePidStr;
    }
    else if (line[0] == 'O') {
        //Get a line of stdout if available for line[1] pid
        _checkExists(req);
        auto& rec = running[req];
        auto& es = *rec.es;

        *rec.stdout << es.get_stdout().rdbuf();
        //Gotta clear status flags, reading from rdbuf can cause issues
        rec.stdout->clear();

        //Some wizardry to not read a line if we hit eof without newline
        std::string sline;
        auto oldPos = rec.stdout->tellg();
        if (std::getline(*rec.stdout, sline).eof()) {
            //No newline, reset pointer and check if process is alive
            rec.stdout->seekg(oldPos);
            if (es.poll().exited()) {
                *forker_out << "D";
            }
            else {
                *forker_out << "N";
            }
        }
        else {
            if (rec.stdout->bad()) {
                printf("Well what the heck happened here... stdout->bad?\n");
            }
            *forker_out << "K" << sline;
        }
    }
    else if (line[0] == 'E') {
        //Get a line of stderr if available
        _checkExists(req);
        auto& rec = running[req];
        auto& es = *rec.es;

        *rec.stderr << es.get_stderr().rdbuf();
        //Gotta clear status flags, reading from rdbuf can cause issues
        rec.stderr->clear();

        //Some wizardry to not read a line if we hit eof without newline
        std::string sline;
        auto oldPos = rec.stderr->tellg();
        if (std::getline(*rec.stderr, sline).eof()) {
            //No newline, reset pointer and check if process is alive
            rec.stderr->seekg(oldPos);
            if (es.poll().exited()) {
                *forker_out << "D";
            }
            else {
                *forker_out << "N";
            }
        }
        else {
            if (rec.stderr->bad()) {
                printf("Well what the heck happened here... stderr->bad?\n");
            }
            *forker_out << "K" << sline;
        }
    }
    else if (line[0] == 'K') {
        //Brutally murder the process
        _checkExists(req);
        auto& es = *running[req].es;
        if (!es.poll().exited()) {
            //Don't use SIGKILL; especially if it's an MPI program, we want to
            //give it a chance ot die peacefully.
            es.terminate(false);
        }
        int code = es.poll().exit_status();
        running.erase(req);

        *forker_out << "K" << code;
    }
    else if (line[0] == 'C') {
        //Cleanup requested, closure acknowledged
        _checkExists(req);
        auto& es = *running[req].es;
        if (!es.poll().exited()) {
            es.terminate(false);
        }
        int code = es.poll().exit_status();
        running.erase(req);

        *forker_out << "K" << code;
    }
    else {
        std::ostringstream err;
        err << "Unrecognized cmd: " << line;
        throw std::runtime_error(err.str());
    }

    *forker_out << "\n";
}

std::string forkerCmd(std::string command) {
    std::string result;
    Lock lockdown(forkLock);

    *forker_out << command << "\n";
    if (!std::getline(*forker_in, result)) {
        throw std::runtime_error("Failed to get line from invoker fork");
    }
    return result;
}

std::tuple<std::string, std::string> run(
        const std::vector<std::string>& progAndArgs, int maxSeconds) {
    #if NO_FORK
        //Forking is REALLY slow.  So, we'll try a NO_FORK version, which
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
        for (int trial = 0, trialm = 20; trial < trialm; trial++) {
            if (trial > 0) {
                fprintf(stderr, "TRYING AGAIN (%i) IN %i SEC\n", trial+1,
                        timeToNext);
                std::this_thread::sleep_for(std::chrono::milliseconds(
                        timeToNext * 1000));
                timeToNext += 1;
            }
            try {
                bp::child es = bp::launch(progAndArgs[0], progAndArgs, ctx);
                auto status = es.wait();
                std::ostringstream out, err;
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
                if (trial == trialm - 1
                        || emsg.find("No child processes") == nf) {
                    throw;
                }
            }
        }
    #endif
    std::ostringstream rcmd;
    rcmd << "R";
    for (int i = 0, im = progAndArgs.size() - 1; i <= im; i++) {
        rcmd << progAndArgs[i];
        if (i == 0) {
            rcmd << "--!!!!--";
        }
        else if (i != im) {
            rcmd << " ";
        }
    }
    std::string pid = forkerCmd(rcmd.str());
    if (pid[0] != 'K') {
        std::ostringstream err;
        err << "Forker failed to spawn: " << pid;
        throw std::runtime_error(err.str());
    }
    pid = pid.substr(1);

    std::string pidOut = "O" + pid, pidErr = "E" + pid;
    uint64_t terminateTime = message::Location::getCurrentTimeMs()
            + maxSeconds * 1000;

    std::ostringstream stdout, stderr;
    bool badOut = false, badErr = false;
    while (!badOut || !badErr) {
        //Ensure both buffers are exhausted  before quitting
        std::string line;
        line = forkerCmd(pidOut);
        if (line[0] == 'D') {
            badOut = true;
        }
        else if (line[0] == 'N') {
            //Nothing to report
        }
        else {
            stdout << line.substr(1) << "\n";
        }
        line = forkerCmd(pidErr);
        if (line[0] == 'D') {
            badErr = true;
        }
        else if (line[0] == 'N') {
            //Nothing to report
        }
        else {
            stderr << line.substr(1) << "\n";
        }

        //See if we should terminate
        if (maxSeconds != 0
                && message::Location::getCurrentTimeMs() > terminateTime) {
            line = forkerCmd("K" + pid);
            if (line[0] != 'K') {
                std::ostringstream err;
                err << "Failed to kill process " << pid << " (on node "
                        << boost::asio::ip::host_name() << ")";
                throw std::runtime_error(err.str());
            }
            else {
                std::ostringstream err;
                err << "Process " << pid << " (on node "
                        << boost::asio::ip::host_name() << ") killed, took "
                        << "longer than " << maxSeconds << " seconds";
                throw std::runtime_error(err.str());
            }
        }
    }
    std::string exitCode = forkerCmd("C" + pid);
    int code = boost::lexical_cast<int>(exitCode.substr(1));
    if (code != 0) {
        std::ostringstream ss;
        ss << "Program " << rcmd.str() << " exited with code " << code;
        ss << " (on node " << boost::asio::ip::host_name() << ")";
        ss << ", stderr: " << stderr.str();
        throw std::runtime_error(ss.str());
    }

    return std::make_tuple(stdout.str(), stderr.str());
}

} //invoke
} //job_stream
