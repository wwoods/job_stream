
#pragma once

#include <string>

std::tuple<std::string, std::string> run(std::string prog, std::string args,
        std::string input);
std::tuple<int, std::string, std::string> runRetval(std::string prog,
        std::string args, std::string input);
void runWithExpectedOut(std::string prog, std::string args, std::string input,
        std::string output, bool lastOnly, bool ordered);
std::vector<std::string> sortedLines(std::string input);

#define REQUIRE_UNORDERED_LINES(a, b) { \
        auto sorted1 = sortedLines(a); \
        auto sorted2 = sortedLines(b); \
        \
        for (int i = 0, m = sorted1.size(); i < m; i++) { \
            REQUIRE(sorted2.size() > i); \
            REQUIRE(sorted1[i] == sorted2[i]); \
        } \
        REQUIRE(sorted1.size() == sorted2.size()); \
    }
