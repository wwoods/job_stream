
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
    SECTION("stdin") {
        std::vector<string> args;
        args.push_back("/bin/cat");
        int status;
        std::string output, error;
        std::tie(status, output, error) = inv::runWithStdin(args, "monkeys");
        INFO(output);
        INFO(error);
        REQUIRE(status == 0);
        REQUIRE(output == "monkeys");
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
        //We want something that will fail the first run, and succeed the
        //second.
        args.push_back("(rm testj && echo OK) || (touch testj && failj)");
        //A good blacklist (will get to second, which succeeds)
        std::vector<std::string> transientOk;
        transientOk.push_back("failj: command not found");
        //A bad blacklist (will throw exception after first; case mismatch)
        std::vector<std::string> transientBad;
        transientBad.push_back("Failj: command not found");

        std::remove("testj");
        std::tie(output, error) = inv::run(args, transientOk);
        REQUIRE(output == "OK\n");

        std::remove("testj");
        //First fail should throw, since our blacklist is bad (case mismatch)
        REQUIRE_THROWS_AS(inv::run(args, transientBad), std::runtime_error);
    }
}
