
#include "catch.hpp"

#include <job_stream/job_stream.h>

using string = std::string;
namespace inv = job_stream::invoke;

TEST_CASE("invoke") {
    job_stream::invoke::_init();
    job_stream::JobLog::FakeHeaderForTests fakeHeader;
    job_stream::invoke::FakeInvokeWait fakeWaiter;
    SECTION("echo") {
        std::string output, error;
        std::vector<string> args;
        args.push_back("/bin/echo");
        args.push_back("Hello, world!");
        std::tie(output, error) = inv::run(args);
        REQUIRE(output == "Hello, world!\n");
    }
    SECTION("bad cat") {
        std::string output, error;
        std::vector<string> args;
        args.push_back("/bin/cat");
        args.push_back("/some/nonexistant/file/ok/right");
        REQUIRE_THROWS_AS(inv::run(args), std::runtime_error);
    }
    SECTION("transient errors") {
        std::string output, error;
        std::vector<string> args;
        args.push_back("/bin/bash");
        args.push_back("-c");
        //$RANDOM ranges 0-32767
        //So, if we have an X% chance of failure, then avg number of trials to
        //success: P(trialsToFirst <= n) = sum[k = 0 to n - 1](X^k * (1-X))
        //P(trialsToFirst <= n) = sum(X^(n - 1) * (1-x))
        //P(trialsToFirst <= n) = (1 - X) * (1 - X^(n)) / (1 - X) == 1 - X^(n)
        //n = log(1 - P(trialsToFirst <= n)) / log(X)
        //or, X = e^(log(1 - P(trialsToFirst <= n)) / n)
        //so, for n = 20, and a 99% probability of success, X = 0.767, scaled
        //to 32767...
        args.push_back("if [ $RANDOM -le 25132 ]; "
                "then echo 'Error: Bad Random' >&2; exit 1; "
                "else echo 'OK'; fi");
        std::vector<std::string> transientOk;
        transientOk.push_back("Bad Random");
        std::vector<std::string> transientBad;
        transientBad.push_back("bad Random");
        for (int i = 0, im = 10; i < im; i++) {
            std::string output, error;
            std::tie(output, error) = inv::run(args, transientOk);
            REQUIRE(output == "OK\n");
        }
        args.back() = "echo 'ERROR: Bad Random' >&2; exit 1;";
        REQUIRE_THROWS_AS(inv::run(args, transientBad), std::runtime_error);
    }
}
