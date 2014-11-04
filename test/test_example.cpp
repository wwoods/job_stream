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
        string prog = "/usr/bin/mpirun";
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
    string prog = "/usr/bin/mpirun";
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
TEST_CASE("example/job_stream_exmaple/exampleHierarchy.yaml", "[hierarchy]") {
    testExample("exampleHierarchy.yaml", "10\n16\n90\n", "16\n22\n96\n", false,
            false);
}


/** Given the stderr of a job_stream application, parse out the number of
    pending messages loaded for each and replace it with an X.  Return the sum.
    */
int _countAndFixPending(std::string& stderr) {
    boost::regex pending("resumed from checkpoint \\(([0-9]+) pending");
    std::string dup = stderr;
    std::string fixed("resumed from checkpoint (X pending");
    boost::sregex_iterator end, cur(dup.begin(), dup.end(), pending);
    int r = 0, offset = 0;
    for (; cur != end; cur++) {
        const boost::smatch& m = *cur;
        r += std::atoi(string(m[1].first, m[1].second).c_str());
        stderr = stderr.replace(m.position() + offset, m[0].second - m[0].first,
                fixed);
        offset += fixed.size() - (m[0].second - m[0].first);
    }
    return r;
}


/** Run some forced checkpoints and ensure that everything goes as planned...
    */
TEST_CASE("example/job_stream_example/checkpoint.yaml", "[checkpoint]") {
    string numProcs;
    for (int np = 1; np <= 4; np += 3) {
        std::ostringstream secName;
        secName << "With " << np << " processes";
        SECTION(secName.str()) {
            boost::regex ms("(took|synced after) [0-9]+ms");
            boost::regex jobLogHeader("^(\\d+):[^:]+:[0-9]+");
            boost::regex mpiFooter("--------------------------------------------------------------------------\nmpirun has exited.*"
                    "|0_[a-zA-Z0-9_]+ [0-9]+% user time[ ,].*");
            boost::regex end("(messages\\))(.*)");
            boost::regex pending("\\(([0-9]+) pending messages");
            std::ostringstream args;
            args << "-np " << np << " example/job_stream_example "
                    "-c test.chkpt --check-sync 100 ../example/checkpoint.yaml 10";
            std::remove("test.chkpt");

            std::string stderrOld;
            { //First run, shouldn't load from checkpoint
                INFO("First run");
                auto r = runRetval("/usr/bin/mpirun", args.str(), "");
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout1: " << std::get<1>(r));
                INFO("stderr1: " << std::get<2>(r));

                REQUIRE(job_stream::RETVAL_CHECKPOINT_FORCED == std::get<0>(r));
                REQUIRE("" == std::get<1>(r));
                string stderr = boost::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                stderr = boost::regex_replace(stderr, mpiFooter, "--mpi--");
                stderr = boost::regex_replace(stderr, jobLogHeader, "$1");
                REQUIRE_CONTAINS_LINES("Using test.chkpt as checkpoint file\n\
0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, waiting 100ms then gathering data\n\
0 Checkpoint took Xms, resuming computation\n--mpi--", stderr);
                stderrOld = stderr;
            }

            bool sawResult = false;
            int run = 1;
            while (true) {
                //Second run, should load with 3 messages (steal, ring 0, data)
                run += 1;
                INFO("Run #" << run);
                auto r = runRetval("/usr/bin/mpirun", args.str(), "");
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout: " << std::get<1>(r));
                INFO("stderr: " << std::get<2>(r));
                INFO("First stderr: " << stderrOld);

                bool exitOk = (std::get<0>(r) == job_stream::RETVAL_OK
                        || std::get<0>(r) == job_stream::RETVAL_CHECKPOINT_FORCED);
                REQUIRE(exitOk);

                string stderr = boost::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                stderr = boost::regex_replace(stderr, mpiFooter, "--mpi--");
                stderr = boost::regex_replace(stderr, jobLogHeader, "$1");
                int p = _countAndFixPending(stderr);
                if (!sawResult) {
                    //Our action, steal, and ring 0
                    REQUIRE(3 == p);
                }
                else {
                    //Ring 0 and steal only
                    REQUIRE(2 == p);
                }
                std::ostringstream expected;
                expected << "Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (X pending messages)\n--mpi--";
                if (std::get<0>(r) != 0) {
                    expected << "\n0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, waiting 100ms then gathering data\n\
0 Checkpoint took Xms, resuming computation";
                }
                for (int n = 1; n < np; n++) {
                    expected << "\n" << n
                            << " resumed from checkpoint (X pending messages)";
                }
                REQUIRE_CONTAINS_LINES(expected.str(), stderr);

                if (!sawResult && std::get<1>(r).size() > 0) {
                    REQUIRE("15\n" == std::get<1>(r));
                    sawResult = true;
                }
                else {
                    //Shouldn't see any more output if we've seen it once.
                    REQUIRE("" == std::get<1>(r));
                }

                if (std::get<0>(r) == job_stream::RETVAL_OK) {
                    break;
                }

                { //Not final run, ensure checkpoint file still exists
                    std::ifstream cpt("test.chkpt");
                    REQUIRE(cpt);
                }
            }

            REQUIRE(sawResult);

            { //Ensure checkpoint file was cleaned up
                std::ifstream cpt("test.chkpt");
                REQUIRE(!cpt);
            }
        }
    }
}


TEST_CASE("example/job_stream_example/exampleRecurCheckpoint.yaml", "[checkpoint]") {
    const int numEmit = 2;

    //Some historic bugs with segmentation faults occur only with -np > 10, so
    //make 40 just to be sure.
    for (int np = 1; np <= 40; np += 39) {
        std::ostringstream secName;
        secName << "With " << np << " processes";
        SECTION(secName.str()) {
            boost::regex resultHarvester("^-?\\d+$");
            boost::regex ms("(took|synced after) [0-9]+ms");
            boost::regex jobLogHeader("^(\\d+):[^:]+:[0-9]+");
            boost::regex mpiFooter("--------------------------------------------------------------------------\nmpirun has exited.*"
                    "|0_[a-zA-Z0-9_]+ [0-9]+% user time[ ,].*");
            boost::regex end("(messages\\))(.*)");
            boost::regex pending("\\(([0-9]+) pending messages");
            std::ostringstream args;
            args << "-np " << np << " example/job_stream_example "
                    "-c test.chkpt --check-sync 100 ../example/exampleRecurCheckpoint.yaml";
            std::remove("test.chkpt");
            int resultsSeen = 0;
            { //First run, shouldn't load from checkpoint
                INFO("First run");
                std::ostringstream instream;
                for (int i = 0; i < numEmit; i++) {
                    instream << "-400000\n";
                }
                auto r = runRetval("/usr/bin/mpirun", args.str(),
                        instream.str());
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout1: " << std::get<1>(r));
                INFO("stderr1: " << std::get<2>(r));

                REQUIRE(job_stream::RETVAL_CHECKPOINT_FORCED == std::get<0>(r));
                REQUIRE("" == std::get<1>(r));
                string sstderr = boost::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                sstderr = boost::regex_replace(sstderr, jobLogHeader, "$1");
                sstderr = boost::regex_replace(sstderr, mpiFooter, "--mpi--");
                REQUIRE_CONTAINS_LINES("Using test.chkpt as checkpoint file\n\
0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, waiting 100ms then gathering data\n\
0 Checkpoint took Xms, resuming computation\n--mpi--", sstderr);
            }

            int run = 1;
            while (true) {
                //Second run, should load with 3 messages (steal, ring 0, data)
                run += 1;
                INFO("Run #" << run);
                auto r = runRetval("/usr/bin/mpirun", args.str(), "");
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout: " << std::get<1>(r));
                INFO("stderr: " << std::get<2>(r));

                bool exitOk = (std::get<0>(r) == job_stream::RETVAL_OK
                        || std::get<0>(r) == job_stream::RETVAL_CHECKPOINT_FORCED);
                REQUIRE(exitOk);

                string sstderr = boost::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                sstderr = boost::regex_replace(sstderr, jobLogHeader, "$1");
                sstderr = boost::regex_replace(sstderr, mpiFooter, "--mpi--");
                int p = _countAndFixPending(sstderr);
                REQUIRE(0 != p);
                std::ostringstream expected;
                expected << "Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (X pending messages)\n--mpi--";
                if (std::get<0>(r) != 0) {
                    expected << "\n0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, waiting 100ms then gathering data\n\
0 Checkpoint took Xms, resuming computation";
                }
                for (int n = 1; n < np; n++) {
                    expected << "\n" << n
                            << " resumed from checkpoint (X pending messages)";
                }
                REQUIRE_CONTAINS_LINES(expected.str(), sstderr);

                boost::smatch match;
                std::string rout = std::get<1>(r);
                std::string::const_iterator start = rout.begin(),
                        end = rout.end();
                while (boost::regex_search(start, end, match,
                        resultHarvester)) {
                    REQUIRE(resultsSeen < numEmit);
                    resultsSeen++;
                    REQUIRE(match.str() == "1442058676");
                    start = match[0].second;
                }

                if (std::get<0>(r) == job_stream::RETVAL_OK) {
                    break;
                }

                { //Not final run, ensure checkpoint file still exists
                    std::ifstream cpt("test.chkpt");
                    REQUIRE(cpt);
                }
            }

            REQUIRE(resultsSeen == numEmit);
            //Ensure sufficient complexity occurred
            REQUIRE(run > 10);

            { //Ensure checkpoint file was cleaned up
                std::ifstream cpt("test.chkpt");
                REQUIRE(!cpt);
            }
        }
    }
}
