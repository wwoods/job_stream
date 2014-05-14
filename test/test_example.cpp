/** Tests the 
    */

#include "catch.hpp"
#include "common.h"

#include <tuple>

using string = std::string;

void testExample(string pipe, string input, string output,
        bool lastOnly = false, bool ordered = true) {
    SECTION(pipe) {
        string prog = "mpirun";
        string args = "example/job_stream_example ../example/" + pipe;
        WHEN("one process") {
            runWithExpectedOut(prog, "-np 1 " + args, input, output, lastOnly,
                    ordered);
        }
        WHEN("four processes") {
            runWithExpectedOut(prog, "-np 4 " + args, input, output, lastOnly,
                    ordered);
        }
    }
}


TEST_CASE("example/job_stream_example/example1.yaml") {
    testExample("example1.yaml", "45\n7\n", "56\n");
}
TEST_CASE("example/job_stream_example/example2.yaml") {
    testExample("example2.yaml", "1\n2\n3\n", "4\n6\n8\n", false, false);
}
TEST_CASE("exampleRecur.yaml lots of rings") {
    string prog = "mpirun";
    string args = "example/job_stream_example ../example/exampleRecur.yaml";
    std::ostringstream input;
    for (int i = 0; i < 10; i++) {
        input << i << "\n";
    }
    run(std::move(prog), std::move(args), input.str());
}
TEST_CASE("example/job_stream_example/example3.yaml") {
    testExample("example3.yaml", "1\n8\n12\n", "12\n10\n10\n");
}
TEST_CASE("example/job_stream_example/example4.yaml") {
    testExample("example4.yaml", "1\n", "188", true);
}
TEST_CASE("example/job_stream_example/example5.yaml") {
    testExample("example5.yaml", "abba\nkadoodle\nj", "98\n105\n107\n", false, 
            false);
}

