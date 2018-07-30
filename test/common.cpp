
#include "catch.hpp"
#include "common.h"

#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <job_stream/invoke.h>
#include <job_stream/message.h>

#include <functional>

using string = std::string;
using Location = job_stream::message::Location;

std::tuple<string, string> run(string prog, string args, string input) {
    auto r = runRetval(prog, args, input);
    INFO("Stderr: " << std::get<2>(r));
    INFO("Stdout: " << std::get<1>(r));
    REQUIRE(0 == std::get<0>(r));
    return std::tuple<string, string>(std::get<1>(r), std::get<2>(r));
}


std::tuple<int, string, string> runRetval(string prog, string args,
        string input) {
    std::vector<std::string> splitArgs;
    boost::algorithm::split(splitArgs, args, boost::is_any_of(" "));
    splitArgs.emplace(splitArgs.begin(), prog);
    return job_stream::invoke::runWithStdin(splitArgs, input);
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
        bool lastOnly, bool ordered, std::function<string(string)> transform) {
    if (lastOnly && !ordered) {
        throw std::runtime_error("lastOnly && !ordered - bad param combination."
                "  lastOnly is incompatible with unordered");
    }

    string out, err;
    std::tie(out, err) = run(std::move(prog), std::move(args),
            std::move(input));

    INFO("Full stdout: " << out);
    INFO("Full stderr: " << err);
    out = transform(out);
    INFO("Transformed stdout: " << out);
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
