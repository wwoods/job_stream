
#include "catch.hpp"
#include "common.h"

#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#define BOOST_PROCESS_WINDOWS_USE_NAMED_PIPE
#include <boost/process.hpp>
#include <job_stream/message.h>

#include <functional>

namespace ba = boost::asio;
namespace bp = boost::process;

using string = std::string;
using Location = job_stream::message::Location;

#if defined(BOOST_POSIX_API)
typedef ba::posix::stream_descriptor bpAsio;
#elif defined(BOOST_WINDOWS_API)
typedef ba::windows::stream_handle bpAsio;
#else
#  error "Unsupported platform."
#endif


std::tuple<string, string> run(string prog, string args, string input) {
    auto r = runRetval(prog, args, input);
    INFO("Stderr: " << std::get<2>(r));
    INFO("Stdout: " << std::get<1>(r));
    REQUIRE(0 == std::get<0>(r));
    return std::tuple<string, string>(std::get<1>(r), std::get<2>(r));
}


std::tuple<int, string, string> runRetval(string prog, string args,
        string input) {
    bp::context ctx;
    ctx.stdin_behavior = bp::capture_stream();
    ctx.stdout_behavior = bp::capture_stream();
    ctx.stderr_behavior = bp::capture_stream();
    ctx.environment = bp::self::get_environment();
    std::vector<std::string> splitArgs;
    boost::algorithm::split(splitArgs, args, boost::is_any_of(" "));
    splitArgs.emplace(splitArgs.begin(), prog);
    bp::child es = bp::launch(prog, splitArgs, ctx);
    es.get_stdin() << input;
    es.get_stdin().close();

    std::ostringstream obuf, ebuf;
    boost::array<char, 4096> outBuffer, errBuffer;

    ba::io_service io_service;
    bpAsio outReader(io_service);
    bpAsio errReader(io_service);

    outReader.assign(es.get_stdout().handle().release());
    errReader.assign(es.get_stderr().handle().release());

    std::function<void(const boost::system::error_code&, std::size_t)> outEnd,
            errEnd;
    auto outBegin = [&]() {
        outReader.async_read_some(boost::asio::buffer(outBuffer),
                boost::bind(outEnd, ba::placeholders::error,
                    ba::placeholders::bytes_transferred));
    };
    outEnd = [&](const boost::system::error_code& ec,
            std::size_t bytesTransferred) {
        if (!ec) {
            obuf << std::string(outBuffer.data(), bytesTransferred);
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
            ebuf << std::string(errBuffer.data(), bytesTransferred);
            errBegin();
        }
    };

    outBegin();
    errBegin();

    io_service.run();
    auto stat = es.wait();

    INFO("Stdout: " << obuf.str());
    INFO("Stderr: " << ebuf.str());

    return std::tuple<int, string, string>(stat.exit_status(), obuf.str(),
            ebuf.str());
}


string getLastLine(string text) {
    int ptr = text.size() - 1;
    while (ptr != string::npos && boost::algorithm::trim_right_copy(
            text.substr(ptr + 1)).empty()) {
        ptr = text.rfind("\n", ptr - 1);
    }
    text = text.substr(ptr + 1);
    boost::algorithm::trim_right(text);
    return text;
}


std::vector<std::string> sortedLines(std::string input) {
    std::istringstream ss(input);
    std::string line;
    std::vector<std::string> result;
    while (std::getline(ss, line, '\n')) {
        result.push_back(line);
    }
    std::sort(result.begin(), result.end());
    return result;
}


std::vector<std::string> sortedLinesLimited(std::string input,
        const std::vector<std::string>& onlyIncludeIfMatching) {
    std::vector<std::string> result;
    std::istringstream ss(input);
    std::string line;
    while (std::getline(ss, line, '\n')) {
        for (int i = 0, m = onlyIncludeIfMatching.size(); i < m; i++) {
            if (onlyIncludeIfMatching[i] == line) {
                result.push_back(line);
                break;
            }
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}


void runWithExpectedOut(string prog, string args, string input, string output,
        bool lastOnly, bool ordered) {
    if (lastOnly && !ordered) {
        throw std::runtime_error("lastOnly && !ordered - bad param combination."
                "  lastOnly is incompatible with unordered");
    }

    string out, err;
    std::tie(out, err) = run(std::move(prog), std::move(args),
            std::move(input));

    INFO("Full stdout: " << out);
    INFO("Full stderr: " << err);
    if (ordered) {
        if (lastOnly) {
            out = getLastLine(std::move(out));
        }
        REQUIRE(output == out);
    }
    else {
        //Require all lines in output, but possibly out of order.
        REQUIRE_UNORDERED_LINES(output, out);
    }
}
