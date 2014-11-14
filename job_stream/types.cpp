
#include "types.h"

#include <cstdio>
#include <map>
#include <thread>

namespace job_stream {

std::string JobLog::header;
std::map<std::thread::id, int> workerMap;


JobLog::JobLog() {
    if (JobLog::header.empty()) {
        throw std::runtime_error("Cannot use JobLog() before "
                "Processor::Processor()");
    }
}


JobLog::~JobLog() {
    auto id = std::this_thread::get_id();
    auto m = workerMap.find(id);
    int tid;
    if (m == workerMap.end()) {
        workerMap[id] = tid = workerMap.size();
    }
    else {
        tid = m->second;
    }
    fprintf(stderr, "%s:%i %s\n", JobLog::header.c_str(), tid,
            this->stream.str().c_str());
}



JobLog::FakeHeaderForTests::FakeHeaderForTests() {
    JobLog::header = "Fake Header";
}
JobLog::FakeHeaderForTests::~FakeHeaderForTests() {
    JobLog::header = "";
}

} //job_stream
