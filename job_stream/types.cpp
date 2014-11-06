
#include "types.h"

#include <cstdio>
#include <thread>

namespace job_stream {

std::string JobLog::header;


JobLog::JobLog() {
    if (JobLog::header.empty()) {
        throw std::runtime_error("Cannot use JobLog() before "
                "Processor::Processor()");
    }
}


JobLog::~JobLog() {
    std::ostringstream tid;
    tid << std::this_thread::get_id();
    fprintf(stderr, "%s %s\n", JobLog::header.c_str(),
            this->stream.str().c_str());
}



JobLog::FakeHeaderForTests::FakeHeaderForTests() {
    JobLog::header = "Fake Header";
}
JobLog::FakeHeaderForTests::~FakeHeaderForTests() {
    JobLog::header = "";
}

} //job_stream
