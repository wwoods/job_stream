
#include "processor.h"

#include <chrono>

namespace job_stream {
namespace processor {

WorkerThread::WorkerThread(Processor* p, int index) : shouldRun(true),
        processor(p), workerIndex(index),
        thread(std::bind(&WorkerThread::main, this)) {
}


void WorkerThread::join() {
    this->shouldRun = false;
    this->thread.join();
}


void WorkerThread::main() {
    this->processor->localTimersInit();
    std::unique_ptr<Processor::WorkTimer> outerTimer(new Processor::WorkTimer(
            this->processor, Processor::TIME_IDLE));
    try {
        while (this->shouldRun) {
            if (!this->processor->processInThread(this->workerIndex)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
    catch (...) {
        this->processor->workerErrors.emplace_back(std::current_exception());
    }
    outerTimer.reset();
    this->processor->localTimersMerge();
}

} //processor
} //job_stream
