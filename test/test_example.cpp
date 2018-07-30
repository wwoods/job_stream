/** Tests the
    */

#include "catch.hpp"
#include "common.h"

#include <boost/lexical_cast.hpp>
#include <job_stream/job_stream.h>
#include <regex>
#include <string>
#include <tuple>

using string = std::string;

#define CHECKPOINT_TRIALS 1

/** Since some MPI versions have non-blank stdout when a process returns
    non-zero error, the tests must be a bit more sophisticated.

    2018-08-30 - Now they do it on a zero error too.  Super annoying.
    You suck, Open MPI.
    */
string __processStdoutMpi(string output) {
    string exp(output);
    string rep("");

    const string mpiMatches[] = {
            "-+\nPrimary job.*normally.*\na non-zero exit code.*\n-+\n",
            "-+\nmpirun detected that one or more.*non-zero status.*\n(.*\n)+?----+\n",
            "-+\n.*A high-performance Open MPI(.*\n)+lower performance[.]\n-+\n",
    };

    for (auto& mRegex : mpiMatches) {
        std::regex mpi(mRegex);
        string expCopy(exp);
        std::sregex_iterator end, cur(expCopy.begin(), expCopy.end(), mpi);
        int offset = 0;
        for (; cur != end; cur++) {
            const auto& m = *cur;
            exp = exp.replace(m.position() + offset, m[0].second - m[0].first,
                    rep);
            offset += rep.size() - (m[0].second - m[0].first);
        }
    }
    return exp;
}


string processStdoutMpiNonzero(string a) { return __processStdoutMpi(a); }
string processStdoutMpiZero(string a) { return __processStdoutMpi(a); }


void testExample(string pipe, string input, string output,
        bool lastOnly = false, bool ordered = true) {
    SECTION(pipe) {
        string prog = "/usr/bin/mpirun";
        string args = "example/job_stream_example ../example/" + pipe;
        WHEN("one process") {
            runWithExpectedOut(prog, "-np 1 " + args, input, output, lastOnly,
                    ordered, processStdoutMpiZero);
        }
        WHEN("four processes") {
            runWithExpectedOut(prog, "-np 4 " + args, input, output, lastOnly,
                    ordered, processStdoutMpiZero);
        }
    }
}

TEST_CASE("should not have ERROR or ASSERT defined") {
    //Header already included
    #ifdef ERROR
    FAIL("job_stream/debug_internals.h erroneously included");
    #endif
    #ifdef ASSERT
    FAIL("job_stream/debug_internals.h erroneously included");
    #endif
}


TEST_CASE("example/job_stream_example/depthFirst.yaml") {
    string prog("/usr/bin/mpirun");
    string args("-np 1 example/job_stream_example ../example/depthFirst.yaml");
    string out, err;
    int nIn = 100;

    double pos1Avg = 0., pos2Avg = 0.;
    int trial = 0;
    for (; trial < 10; trial++) {
        std::tie(out, err) = run(prog, args,
                boost::lexical_cast<string>(nIn) + "\n");

        INFO("Full stderr: \n" << err);
        INFO("Full stdout: \n" << out);

        out = processStdoutMpiZero(out);

        double pos1 = 0., pos2 = 0.;
        int seen1 = 0, seen2 = 0, lineno = 0;
        std::regex outLine("(^|\n)([^\n]+)(?=$|\n)");
        std::sregex_iterator end, cur(out.begin(), out.end(), outLine);
        for (; cur != end; cur++) {
            const std::smatch& m = *cur;
            if (m[2].str() == "1") {
                seen1++;
                pos1 += lineno;
            }
            else if (m[2].str() == "Frame 2") {
                seen2++;
                pos2 += lineno;
            }
            else {
                FAIL("SAW '" << m[2].str() << "'");
            }
            lineno++;
        }

        pos1 /= seen1 * (2 * nIn);
        pos2 /= seen2 * (2 * nIn);
        INFO(
                "'1' Results: " << seen1 << " at " << pos1 << "\n"
                << "'Frame 2' Results: " << seen2 << " at " << pos2 << "\n"
        );
        REQUIRE(nIn == seen1);
        REQUIRE(nIn == seen2);
        pos1Avg += pos1;
        pos2Avg += pos2;
    }
    pos1Avg /= trial;
    pos2Avg /= trial;
    INFO("Avg finish of '1': " << pos1Avg << ", 'Frame 2': " << pos2Avg);
    REQUIRE(pos1Avg >= 0.3);
    REQUIRE(pos2Avg <= 0.7);
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
    std::regex pending("resumed from checkpoint \\(([0-9]+) pending");
    std::string dup = stderr;
    std::string fixed("resumed from checkpoint (X pending");
    std::sregex_iterator end, cur(dup.begin(), dup.end(), pending);
    int r = 0, offset = 0;
    for (; cur != end; cur++) {
        const std::smatch& m = *cur;
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
    for (int trial = 0; trial <= CHECKPOINT_TRIALS; trial++)
    for (int np = 1; np <= 4; np += 3) {
        std::ostringstream secName;
        secName << "With " << np << " processes, trial " << trial;
        SECTION(secName.str()) {
            uint64_t tStart = job_stream::message::Location::getCurrentTimeMs();
            std::regex ms("(took|synced after) [0-9]+ms");
            std::regex jobLogHeader("((^|\n)\\d+):[^:]+:[0-9]+:[0-9]+");
            std::regex mpiFooter("--------------------------------------------------------------------------\nmpirun has exited.*"
                    "|0_[a-zA-Z0-9_]+ [0-9]+% user time[ ,].*");
            std::regex end("(messages\\))(.*)");
            std::regex pending("\\(([0-9]+) pending messages");
            std::ostringstream args;
            args << "-np " << np << " example/job_stream_example "
                    "-c test.chkpt --check-sync 10 ../example/checkpoint.yaml 10";
            std::remove("test.chkpt");
            std::remove("test.chkpt.done");

            std::string stderrOld;
            { //First run, shouldn't load from checkpoint
                INFO("First run");
                auto r = runRetval("/usr/bin/mpirun", args.str(), "");
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout1: " << std::get<1>(r));
                INFO("stderr1: " << std::get<2>(r));

                REQUIRE(job_stream::RETVAL_CHECKPOINT_FORCED == std::get<0>(r));
                REQUIRE("" == processStdoutMpiNonzero(std::get<1>(r)));
                string stderr = std::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                stderr = std::regex_replace(stderr, mpiFooter, string(""));
                stderr = std::regex_replace(stderr, jobLogHeader, string("$1"));
                REQUIRE_CONTAINS_LINES("0 Using test.chkpt as checkpoint file\n\
0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, including mandatory 10ms quiet period\n\
0 Checkpoint took Xms, resuming computation", stderr);
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

                string stderr = std::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                stderr = std::regex_replace(stderr, mpiFooter, string(""));
                stderr = std::regex_replace(stderr, jobLogHeader, string("$1"));
                int p = _countAndFixPending(stderr);
                if (!sawResult) {
                    //Our action, and ring 0
                    REQUIRE(2 == p);
                }
                else {
                    //Ring 0 only
                    REQUIRE(1 == p);
                }
                std::ostringstream expected;
                expected << "0 Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (X pending messages)";
                if (std::get<0>(r) != 0) {
                    expected << "\n0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, including mandatory 10ms quiet period\n\
0 Checkpoint took Xms, resuming computation";
                }
                for (int n = 1; n < np; n++) {
                    expected << "\n" << n
                            << " resumed from checkpoint (X pending messages)";
                }
                REQUIRE_CONTAINS_LINES(expected.str(), stderr);

                string stdoutWoMpi = processStdoutMpiNonzero(std::get<1>(r));
                if (!sawResult && stdoutWoMpi.size() > 0) {
                    REQUIRE("15\n" == stdoutWoMpi);
                    sawResult = true;
                }
                else {
                    //Shouldn't see any more output if we've seen it once.
                    REQUIRE("" == stdoutWoMpi);
                }

                if (std::get<0>(r) == job_stream::RETVAL_OK) {
                    break;
                }

                { //Not final run, ensure checkpoint file still exists
                    std::ifstream cpt("test.chkpt");
                    REQUIRE(cpt);
                }

                INFO("Saw result? " << sawResult);
                uint64_t tElapsed =
                        job_stream::message::Location::getCurrentTimeMs()
                        - tStart;
                REQUIRE(tElapsed <= 20000);
            }

            REQUIRE(sawResult);

            { //Ensure checkpoint file was cleaned up
                std::ifstream cpt("test.chkpt");
                REQUIRE(!cpt);
            }
        }
    }
}


TEST_CASE("example/job_stream_example/exampleRecurBug.yaml", "[checkpoint]") {
    const int numEmit = 40;

    //Some historic bugs with segmentation faults occur only with -np > 10, so
    //make 40 just to be sure.
    for (int trial = 1; trial <= CHECKPOINT_TRIALS; trial++)
    for (int np = 1; np <= 4; np += 3) {
        uint64_t tStart = job_stream::message::Location::getCurrentTimeMs();
        std::ostringstream secName;
        secName << "With " << np << " processes, trial " << trial;
        SECTION(secName.str()) {
            std::regex resultHarvester("(^|\n)(-?\\d+)($|\n)");
            std::regex ms("(took|synced after) [0-9]+ms");
            std::regex jobLogHeader("((^|\n)\\d+):[^:]+:[0-9]+:[0-9]+");
            std::regex mpiFooter("--------------------------------------------------------------------------\nmpirun has exited.*"
                    "|0_[a-zA-Z0-9_]+ [0-9]+% user time[ ,].*");
            std::regex end("(messages\\))(.*)");
            std::regex pending("\\(([0-9]+) pending messages");
            std::ostringstream args;
            args << "-np " << np << " example/job_stream_example "
                    "-c test.chkpt --check-sync 100 ../example/exampleRecurBug.yaml";
            std::remove("test.chkpt");
            std::remove("test.chkpt.done");
            int resultsSeen = 0;
            std::ostringstream allErr, allOut;
            { //First run, shouldn't load from checkpoint
                INFO("First run");
                std::ostringstream instream;
                for (int i = 0; i < numEmit; i++) {
                    instream << "2\n";
                }
                auto r = runRetval("/usr/bin/mpirun", args.str(),
                        instream.str());
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout1: " << std::get<1>(r));
                allOut << "Trial 1\n=====\n" << std::get<1>(r);
                INFO("stderr1: " << std::get<2>(r));
                allErr << "Trial 1\n=====\n" << std::get<2>(r);

                REQUIRE(job_stream::RETVAL_CHECKPOINT_FORCED == std::get<0>(r));
                REQUIRE("" == processStdoutMpiNonzero(std::get<1>(r)));
                string sstderr = std::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                sstderr = std::regex_replace(sstderr, jobLogHeader,
                        string("$1"));
                INFO("PROCESSED TO:\n" << sstderr);
                sstderr = std::regex_replace(sstderr, mpiFooter, string(""));
                REQUIRE_CONTAINS_LINES("0 Using test.chkpt as checkpoint file\n\
0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, including mandatory 100ms quiet period\n\
0 Checkpoint took Xms, resuming computation", sstderr);
            }

            int run = 1;
            while (true) {
                //Second run, should load with 3 messages (steal, ring 0, data)
                run += 1;
                INFO("Run #" << run);
                auto r = runRetval("/usr/bin/mpirun", args.str(), "");
                INFO("retVal: " << std::get<0>(r));
                allOut << "\nTrial " << run << "\n=====\n" << std::get<1>(r);
                INFO("All stdout: " << allOut.str());
                allErr << "\nTrial " << run << "\n=====\n" << std::get<2>(r);
                INFO("All stderr: " << allErr.str());

                bool exitOk = (std::get<0>(r) == job_stream::RETVAL_OK
                        || std::get<0>(r) == job_stream::RETVAL_CHECKPOINT_FORCED);
                REQUIRE(exitOk);

                string sstderr = std::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                sstderr = std::regex_replace(sstderr, jobLogHeader,
                        string("$1"));
                sstderr = std::regex_replace(sstderr, mpiFooter, string(""));
                int p = _countAndFixPending(sstderr);
                REQUIRE(0 != p);
                std::ostringstream expected;
                expected << "0 Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (X pending messages)";
                if (std::get<0>(r) != 0) {
                    expected << "\n0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, including mandatory 100ms quiet period\n\
0 Checkpoint took Xms, resuming computation";
                }
                for (int n = 1; n < np; n++) {
                    expected << "\n" << n
                            << " resumed from checkpoint (X pending messages)";
                }
                REQUIRE_CONTAINS_LINES(expected.str(), sstderr);

                std::smatch match;
                std::string rout = std::get<1>(r);
                std::string::const_iterator start = rout.begin(),
                        end = rout.end();
                while (std::regex_search(start, end, match,
                        resultHarvester)) {
                    REQUIRE(resultsSeen < numEmit);
                    resultsSeen++;
                    REQUIRE(match[2].str() == "3");
                    start = match[0].second;
                }

                if (std::get<0>(r) == job_stream::RETVAL_OK) {
                    REQUIRE(resultsSeen != 0);
                    break;
                }

                { //Not final run, ensure checkpoint file still exists
                    std::ifstream cpt("test.chkpt");
                    REQUIRE(cpt);
                }

                INFO("Saw result? " << resultsSeen);
                uint64_t tElapsed =
                        job_stream::message::Location::getCurrentTimeMs()
                        - tStart;
                REQUIRE(tElapsed <= 20000);
            }

            INFO("All stdout: " << allOut.str());
            INFO("All stderr: " << allErr.str());
            REQUIRE(resultsSeen == numEmit);

            { //Ensure checkpoint file was cleaned up
                std::ifstream cpt("test.chkpt");
                REQUIRE(!cpt);
            }
        }
    }
}


TEST_CASE("example/job_stream_example/exampleRecurCheckpoint.yaml", "[checkpoint]") {
    const int numEmit = 4;

    //Some historic bugs with segmentation faults occur only with -np > 10, so
    //make 40 just to be sure.
    for (int trial = 1; trial <= CHECKPOINT_TRIALS; trial++)
    for (int np = 1; np <= 1; np += 39) {
        uint64_t tStart = job_stream::message::Location::getCurrentTimeMs();
        std::ostringstream secName;
        secName << "With " << np << " processes, trial " << trial;
        SECTION(secName.str()) {
            std::regex resultHarvester("(^|\n)(-?\\d+)($|\n)");
            std::regex ms("(took|synced after) [0-9]+ms");
            std::regex jobLogHeader("((^|\n)\\d+):[^:]+:[0-9]+:[0-9]+");
            std::regex mpiFooter("--------------------------------------------------------------------------\nmpirun has exited.*"
                    "|0_[a-zA-Z0-9_]+ [0-9]+% user time[ ,].*");
            std::regex end("(messages\\))(.*)");
            std::regex pending("\\(([0-9]+) pending messages");
            std::ostringstream args;
            //Note that stealing is disabled for this test since such rapid
            //checkpointing makes the work go really, really slow if you only
            //allow a minimum number of worker threads
            args << "-np " << np << " example/job_stream_example "
                    "-c test.chkpt --disable-steal --check-sync 100 ../example/exampleRecurCheckpoint.yaml";
            std::remove("test.chkpt");
            std::remove("test.chkpt.done");
            std::ostringstream allErr;
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
                allErr << "Trial 1\n=====\n" << std::get<2>(r);
                INFO("stderr1: " << allErr.str());

                REQUIRE(job_stream::RETVAL_CHECKPOINT_FORCED == std::get<0>(r));
                REQUIRE("" == processStdoutMpiNonzero(std::get<1>(r)));
                string sstderr = std::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                sstderr = std::regex_replace(sstderr, jobLogHeader,
                        string("$1"));
                sstderr = std::regex_replace(sstderr, mpiFooter, string(""));
                REQUIRE_CONTAINS_LINES("0 Using test.chkpt as checkpoint file\n\
0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, including mandatory 100ms quiet period\n\
0 Checkpoint took Xms, resuming computation", sstderr);
            }

            int run = 1;
            while (true) {
                //Second run, should load with 3 messages (steal, ring 0, data)
                run += 1;
                INFO("Run #" << run);
                auto r = runRetval("/usr/bin/mpirun", args.str(), "");
                INFO("retVal: " << std::get<0>(r));
                INFO("stdout: " << std::get<1>(r));
                allErr << "\nTrial " << run << "\n=====\n" << std::get<2>(r);
                INFO("stderr: " << allErr.str());

                bool exitOk = (std::get<0>(r) == job_stream::RETVAL_OK
                        || std::get<0>(r) == job_stream::RETVAL_CHECKPOINT_FORCED);
                REQUIRE(exitOk);

                string sstderr = std::regex_replace(std::get<2>(r), ms,
                        string("$1 Xms"));
                sstderr = std::regex_replace(sstderr, jobLogHeader,
                        string("$1"));
                sstderr = std::regex_replace(sstderr, mpiFooter, string(""));
                int p = _countAndFixPending(sstderr);
                REQUIRE(0 != p);
                std::ostringstream expected;
                expected << "0 Using test.chkpt as checkpoint file\n\
0 resumed from checkpoint (X pending messages)";
                if (std::get<0>(r) != 0) {
                    expected << "\n0 Checkpoint starting\n\
0 Checkpoint activity synced after Xms, including mandatory 100ms quiet period\n\
0 Checkpoint took Xms, resuming computation";
                }
                for (int n = 1; n < np; n++) {
                    expected << "\n" << n
                            << " resumed from checkpoint (X pending messages)";
                }
                REQUIRE_CONTAINS_LINES(expected.str(), sstderr);

                std::smatch match;
                std::string rout = std::get<1>(r);
                std::string::const_iterator start = rout.begin(),
                        end = rout.end();
                while (std::regex_search(start, end, match,
                        resultHarvester)) {
                    REQUIRE(resultsSeen < numEmit);
                    resultsSeen++;
                    REQUIRE(match[2].str() == "1442058676");
                    start = match[0].second;
                }

                if (std::get<0>(r) == job_stream::RETVAL_OK) {
                    break;
                }

                { //Not final run, ensure checkpoint file still exists
                    std::ifstream cpt("test.chkpt");
                    REQUIRE(cpt);
                }

                INFO("Saw result? " << resultsSeen);
                uint64_t tElapsed =
                        job_stream::message::Location::getCurrentTimeMs()
                        - tStart;
                REQUIRE(tElapsed <= 240000);
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
