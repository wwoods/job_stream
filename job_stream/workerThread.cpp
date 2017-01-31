
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

    while (this->shouldRun && this->processor->shouldRun) {
        //Actually do the job processing
        bool gotWork = this->processor->processInThread(this->workerIndex);
        if (!gotWork) {
            //Don't busy wait and pointlessly burn cycles
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    outerTimer.reset();
    this->processor->localTimersMerge();
}

} //processor
} //job_stream
