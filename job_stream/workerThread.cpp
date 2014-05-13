
#include "processor.h"

#include <chrono>

namespace job_stream {
namespace processor {

WorkerThread::WorkerThread(Processor* p) : shouldRun(true), processor(p), 
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
    while (this->shouldRun) {
        MpiMessagePtr work = this->processor->getWork();
        if (work) {
            this->processor->process(std::move(work));
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    outerTimer.reset();
    this->processor->localTimersMerge();
}

} //processor
} //job_stream
