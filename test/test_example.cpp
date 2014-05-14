/** Tests the 
    */

#include "catch.hpp"
#include "common.h"

#include <job_stream/job_stream.h>
#include <boost/regex.hpp>
#include <string>
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


/** Run some forced checkpoints and ensure that everything goes as planned...
    */
TEST_CASE("example/job_stream_example/checkpoint.yaml", "[checkpoint]") {
    boost::regex ms("took [0-9]+ ms");
    boost::regex end("(messages\\))(.*)");
    std::remove("test.chkpt");
    { //First run, shouldn't load from checkpoint
        auto r = runRetval(job_stream::RETVAL_CHECKPOINT_FORCED,
                "example/job_stream_example",
                " ../example/checkpoint.yaml -c test.chkpt 10", "");
        INFO("stdout1: " << std::get<0>(r));
        INFO("stderr1: " << std::get<1>(r));

        REQUIRE("" == std::get<0>(r));
        string stderr = boost::regex_replace(std::get<1>(r), ms,
                string("took X ms"));
        INFO("AHHH" << stderr);
        REQUIRE("Using test.chkpt as checkpoint file\n\
0 Checkpoint starting\n\
0 Checkpoint took X ms, resuming computation\n" == stderr);
    }

    { //Second run, should load with 3 messages (steal, ring 0, data)
        auto r = runRetval(job_stream::RETVAL_CHECKPOINT_FORCED,
                "example/job_stream_example",
                " ../example/checkpoint.yaml -c test.chkpt 10", "");
        INFO("stdout2: " << std::get<0>(r));
        INFO("stderr2: " << std::get<1>(r));

        REQUIRE("" == std::get<0>(r));
        string stderr = boost::regex_replace(std::get<1>(r), ms,
                string("took X ms"));
        REQUIRE("Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (3 pending messages)\n\
0 Checkpoint starting\n\
0 Checkpoint took X ms, resuming computation\n" == stderr);
    }

    { //Ensure checkpoint file still exists
        std::ifstream cpt("test.chkpt");
        REQUIRE(cpt);
    }

    { //Third run, should load with 4 messages (steal, ring 0, data)
        auto r = runRetval(job_stream::RETVAL_OK,
                "example/job_stream_example",
                " ../example/checkpoint.yaml -c test.chkpt 10", "");
        INFO("stdout3: " << std::get<0>(r));
        INFO("stderr3: " << std::get<1>(r));

        REQUIRE("15\n" == std::get<0>(r));
        string stderr = boost::regex_replace(std::get<1>(r), end,
                string("$1"));
        REQUIRE("Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (3 pending messages)" == stderr);
    }

    { //Ensure checkpoint file was cleaned up
        std::ifstream cpt("test.chkpt");
        REQUIRE(!cpt);
    }
}

