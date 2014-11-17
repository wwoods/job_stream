#pragma once

#include <thread>

namespace job_stream {
namespace processor {

class Processor;

class WorkerThread {
public:
    /** Starts a new WorkerThread that processes work from p. */
    WorkerThread(Processor* p, int workerIndex);

    /** Joins the thread. */
    void join();

    /** The main function that this WorkerThread runs. */
    void main();

private:
    bool shouldRun;
    Processor* processor;
    std::thread thread;
    int workerIndex;
};

}
}
