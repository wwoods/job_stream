
#pragma once

#include <string>

std::tuple<std::string, std::string> run(std::string prog, std::string args,
        std::string input);
std::tuple<std::string, std::string> runRetval(int expected, std::string prog,
        std::string args, std::string input);
void runWithExpectedOut(std::string prog, std::string args, std::string input,
        std::string output, bool lastOnly, bool ordered);
