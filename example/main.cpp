
#include <job_stream/job_stream.h>

using std::unique_ptr;

/** Add one to any integer we receive */
class AddOneJob : public job_stream::Job<AddOneJob, int> {
public:
    static const char* NAME() { return "addOne"; }

    void handleWork(unique_ptr<int> work) {
        this->emit(*work + 1);
    }
} addOneJob;


class DuplicateJob : public job_stream::Job<DuplicateJob, int> {
public:
    static const char* NAME() { return "duplicate"; }

    void handleWork(unique_ptr<int> work) {
        this->emit(*work);
        this->emit(*work);
    }
} duplicateJob;


class GetToTenJob : public job_stream::Job<GetToTenJob, int> {
public:
    static const char* NAME() { return "getToTen"; }

    void handleWork(unique_ptr<int> work) {
        if (*work < 10) {
            this->emit(*work, "keep_going");
        }
        else {
            this->emit(*work, "done");
        }
    }
} getToTenJob;


class SumReducer : public job_stream::Reducer<SumReducer, int> {
public:
    static const char* NAME() { return "sum"; }

    /** Called to initialize the accumulator for this reduce.  May be called
        several times on different hosts, whose results will later be merged
        in handleJoin(). */
    void handleInit(int& current) {
        current = 0;
    }

    /** Used to add a new output to this Reducer */
    void handleAdd(int& current, unique_ptr<int> work) {
        current += *work;
    }

    /** Called to join this Reducer with the accumulator from another */
    void handleJoin(int& current, unique_ptr<int> other) {
        current += *other;
    }

    /** Called when the reduction is complete, or nearly - recur() may be used
        to keep the reduction alive (inject new work into this reduction). */
    void handleDone(int& current) {
        this->emit(current);
    }
} sumReducer;


class GetToValueReducer : public job_stream::Reducer<GetToValueReducer, int> {
public:
    static const char* NAME() { return "getToValue"; }

    void handleInit(int& current) {
        current = 0;
    }

    void handleAdd(int& current, unique_ptr<int> work) {
        //Everytime we get an output less than 2, we'll need to run it through
        //the system again.
        printf("Adding %i\n", *work);
        if (*work < 3) {
            this->recur(3);
        }
        current += *work;
    }

    void handleJoin(int& current, unique_ptr<int> other) {
        current += *other;
    }

    void handleDone(int& current) {
        printf("Maybe done at %i\n", current);
        if (current >= this->config["value"].as<int>()) {
            this->emit(current);
        }
        else {
            //Not really done, put work back in as our accumulated value.
            this->recur(current);
        }
    }
} getToValueReducer;


/** Frames are useful for when you want to start a reduction with a different
    type than the continuation.  For instance, here a string input (number of
    trials to run and initial int values by char ordinal) is used to run a 
    number of simulations in parallel that result in int values, which are 
    aggregated into an array and averaged as a result. */
class RunExperiments : public job_stream::Frame<RunExperiments,
        std::vector<int>, std::string, int> {
public:
    static const char* NAME() { return "runExperiments"; }

    void handleFirst(std::vector<int>& current, unique_ptr<std::string> work) {
        for (int i = 0; i < work->length(); i++) {
            this->recur((int)(*work)[i]);
        }
    }

    void handleWork(std::vector<int>& current, unique_ptr<int> work) {
        current.push_back(*work);
    }

    void handleJoin(std::vector<int>& current, 
            unique_ptr<std::vector<int>> other) {
        current.insert(current.end(), other->begin(), other->end());
    }

    void handleDone(std::vector<int>& current) {
        int sum = 0;
        for (int i : current) {
            sum += i;
        }
        this->emit(sum / current.size());
    }
} runExperimentsInst;


/** Example for testing / demonstrating checkpoints. */
class CheckpointTester : public job_stream::Job<CheckpointTester, int> {
public:
    static const char* NAME() { return "checkpointTester"; }

    void handleWork(unique_ptr<int> work) {
        //Forces a checkpoint after this work's completion.
        this->forceCheckpoint();
        this->emit(*work + 1);
    }
} checkpointTesterInst;


int main(int argc, char* argv []) {
    job_stream::runProcessor(argc, argv);
    return 0;
}
