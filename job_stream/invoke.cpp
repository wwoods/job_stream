
#include "invoke.h"

#include "job_stream.h"
#include "josuttis/fdstream.hpp"
#include "libexecstream/exec-stream.h"

#include <memory>
#include <sstream>

namespace job_stream {
namespace invoke {

job_stream::Mutex forkLock;

uint64_t runningTotal = 0;
struct RunData {
    std::unique_ptr<exec_stream_t> es;
    std::unique_ptr<std::stringstream> stdout;
    std::unique_ptr<std::stringstream> stderr;

    RunData() {}
    RunData(std::unique_ptr<exec_stream_t> es) : es(std::move(es)),
            stdout(new std::stringstream()), stderr(new std::stringstream()) {}
    RunData(RunData&& other) {
        std::swap(this->es, other.es);
        std::swap(this->stdout, other.stdout);
        std::swap(this->stderr, other.stderr);
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

        std::unique_ptr<exec_stream_t> es(new exec_stream_t(
                req.substr(0, split), req.substr(split + 8)));
        es->close_in();
        runningTotal++;
        std::ostringstream fakePid;
        fakePid << runningTotal;
        std::string fakePidStr = fakePid.str();
        running.emplace(fakePidStr, std::move(es));
        *forker_out << "K" << fakePidStr;
    }
    else if (line[0] == 'O') {
        //Get a line of stdout if available for line[1] pid
        _checkExists(req);
        auto& rec = running[req];
        auto& es = *rec.es;
        if (!es.is_alive()) {
            *forker_out << "D";
        }
        else {
            *rec.stdout << es.out().rdbuf();
            //Inefficient, copying buffer.  We don't want getline to
            //block though
            if (rec.stdout->str().find('\n') < 0) {
                *forker_out << "N";
            }
            else {
                std::string ll;
                std::getline(*rec.stdout, ll);
                *forker_out << "K" << ll;
            }
        }
    }
    else if (line[0] == 'E') {
        //Get a line of stderr if available
        _checkExists(req);
        auto& rec = running[req];
        auto& es = *rec.es;
        if (!es.is_alive()) {
            *forker_out << "D";
        }
        else {
            *rec.stderr << es.err().rdbuf();
            //Inefficient, copying buffer.  We don't want getline to
            //block though
            if (rec.stderr->str().find('\n') < 0) {
                *forker_out << "N";
            }
            else {
                std::string ll;
                std::getline(*rec.stderr, ll);
                *forker_out << "K" << ll;
            }
        }
    }
    else if (line[0] == 'C') {
        //Cleanup requested, closure acknowledged
        _checkExists(req);
        auto& es = *running[req].es;
        if (!es.close()) {
            es.kill(SIGTERM);
        }
        int code = es.exit_code();
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
        const std::vector<std::string>& progAndArgs) {
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
    }
    forkerCmd("C" + pid);
    return std::make_tuple(stdout.str(), stderr.str());
}

} //invoke
} //job_stream
