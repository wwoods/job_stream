/** Tests the 
    */

#include "catch.hpp"

#include <boost/algorithm/string.hpp>
#include <exec-stream.h>

#include <tuple>

using string = std::string;

string readall(std::istream& stream) {
    std::ostringstream ss;
    ss << stream.rdbuf();
    return ss.str();
}


std::tuple<string, string> run(string prog, string args,
        string input) {
    exec_stream_t es(prog, args);
    es.in() << input;
    es.close_in();
    string obuf = readall(es.out());
    string ebuf = readall(es.err());
    if (!es.close()) {
        //Since these are MPI programs, best to use SIGTERM so it can clean up
        es.kill(SIGTERM);
    }

    INFO("Stdout: " << obuf);
    INFO("Stderr: " << ebuf);

    REQUIRE(es.exit_code() == 0);
    return std::tuple<string, string>(obuf, ebuf);
}


string runOut(string prog, string args, string input) {
    return std::get<0>(run(std::move(prog), std::move(args), std::move(input)));
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


void testExample(string pipe, string input, string output,
        bool lastOnly = false) {
    SECTION(pipe) {
        string prog = "mpirun";
        string args = "example/job_stream_example ../example/" + pipe;
        WHEN("one process") {
            string out = runOut(prog, "-np 1 " + args, input);
            if (lastOnly) {
                out = getLastLine(std::move(out));
            }
            REQUIRE(output == out);
        }
        WHEN("four processes") {
            string out = runOut(prog, "-np 4 " + args, input);
            if (lastOnly) {
                out = getLastLine(std::move(out));
            }
            REQUIRE(output == out);
        }
    }
}


TEST_CASE("example/job_stream_example/example1.yaml") {
    testExample("example1.yaml", "45\n7\n", "56\n");
}
TEST_CASE("example/job_stream_example/example2.yaml") {
    testExample("example2.yaml", "1\n2\n3\n", "4\n6\n8\n");
}
TEST_CASE("example/job_stream_example/example3.yaml") {
    testExample("example3.yaml", "1\n8\n12\n", "12\n10\n10\n");
}
TEST_CASE("example/job_stream_example/example4.yaml") {
    testExample("example4.yaml", "1\n", "188", true);
}

