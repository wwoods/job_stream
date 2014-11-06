#pragma once

#include <mutex>
#include <sstream>

namespace job_stream {
    typedef std::recursive_mutex Mutex;
    typedef std::lock_guard<Mutex> Lock;

    namespace processor {
        class Processor;
    }

    /** Helper class for debug code (see JOB_STREAM_DEBUG).  Outputs the message
        encoded to it (at destruction) to stderr with identifying information
        about which process this is.  Recommended usage:

        JobLog() << "My message: " << someCounter;

        That way the JobLog() gets destructed after the line.  If you know
        what you are doing and want to emit a message all at once, feel free
        to make a named JobLog() object.  Just scope it properly.
        */
    class JobLog {
    public:
        JobLog();
        ~JobLog();

        template<typename T>
        JobLog& operator<<(const T& obj) {
            this->stream << obj;
            return *this;
        }


        /** Test functionality only; since some internals use JobLog() when a
            processor is not initialized, use JobLog::FakeHeaderForTests to
            override. */
        struct FakeHeaderForTests {
            FakeHeaderForTests();
            ~FakeHeaderForTests();
        };

    private:
        friend class processor::Processor;
        /** The stream for this JobLog */
        std::ostringstream stream;

        /** The header for this machine, initialized by Processor. */
        static std::string header;
    };
}
