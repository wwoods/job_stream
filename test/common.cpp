
#include "catch.hpp"
#include "common.h"

#include <boost/algorithm/string.hpp>
#include <exec-stream.h>
#include <job_stream/message.h>

using string = std::string;
using Location = job_stream::message::Location;

void readall(std::istream& stream, std::ostringstream& ss) {
    ss << stream.rdbuf();
}


std::tuple<string, string> run(string prog, string args,
        string input) {
    exec_stream_t es(prog, args);
    es.in() << input;
    es.close_in();
    std::ostringstream obuf, ebuf;
    uint64_t start = Location::getCurrentTimeMs();
    while (es.is_alive() && Location::getCurrentTimeMs() <= start + 1000) {
        readall(es.out(), obuf);
        readall(es.err(), ebuf);
    }

    readall(es.out(), obuf);
    readall(es.err(), ebuf);

    bool didExit = true;
    if (!es.close()) {
        //Since these are MPI programs, best to use SIGTERM so it can clean up
        es.kill(SIGTERM);
        didExit = false;
    }

    INFO("Stdout: " << obuf.str());
    INFO("Stderr: " << ebuf.str());

    REQUIRE(didExit);
    REQUIRE(es.exit_code() == 0);
    return std::tuple<string, string>(obuf.str(), ebuf.str());
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
        auto sorted1 = sortedLines(output);
        auto sorted2 = sortedLines(out);

        for (int i = 0, m = sorted1.size(); i < m; i++) {
            REQUIRE(sorted2.size() > i);
            REQUIRE(sorted1[i] == sorted2[i]);
        }
        REQUIRE(sorted1.size() == sorted2.size());
    }
}
