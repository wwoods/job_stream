
#include <job_stream/job_stream.h>


/** Add one to any integer we receive */
class AddOneJob : public job_stream::Job<int> {
public:
    static AddOneJob* make() { return new AddOneJob(); }

    void handleWork(int& work) {
        this->emit(work + 1);
    }
};


class DuplicateJob : public job_stream::Job<int> {
public:
    static DuplicateJob* make() { return new DuplicateJob(); }

    void handleWork(int& work) {
        this->emit(work);
        this->emit(work);
    }
};


class GetToTenJob : public job_stream::Job<int> {
public:
    static GetToTenJob* make() { return new GetToTenJob(); }

    void handleWork(int& work) {
        if (work < 10) {
            this->emit(work, "keep_going");
        }
        else {
            this->emit(work, "done");
        }
    }
};


class SumReducer : public job_stream::Reducer<int> {
public:
    static SumReducer* make() { return new SumReducer(); }

    /** Called to initialize the accumulator for this reduce.  May be called
        several times on different hosts, whose results will later be merged
        in handleJoin(). */
    void handleInit(int& current) {
        current = 0;
    }

    /** Used to add a new output to this Reducer */
    void handleAdd(int& current, int& work) {
        current += work;
    }

    /** Called to join this Reducer with the accumulator from another */
    void handleJoin(int& current, int& other) {
        current += other;
    }

    /** Called when the reduction is complete, or nearly - recur() may be used
        to keep the reduction alive (inject new work into this reduction). */
    void handleDone(int& current) {
        this->emit(current);
    }
};


//Another way to write SumReducer, relying on defaults (operator+):
class Sum2Reducer : public job_stream::Reducer<int> {
    static Sum2Reducer* make() { return new Sum2Reducer(); }

    /** Init is needed just because int does not have an initializer.  User
        classes should specify an initializer rather than overloading 
        handleInit. */
    void handleInit(int& current) { current = 0; }
};


class GetToValueReducer : public job_stream::Reducer<int> {
public:
    static GetToValueReducer* make() { return new GetToValueReducer(); }

    void handleInit(int& current) {
        current = 0;
    }

    void handleAdd(int& current, int& work) {
        //Everytime we get an output less than 2, we'll need to run it through
        //the system again.
        printf("Adding %i\n", work);
        if (work < 3) {
            this->recur(3);
        }
        current += work;
    }

    void handleJoin(int& current, int& other) {
        current += other;
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
};


int main(int argc, char* argv []) {
    job_stream::addJob("addOne", AddOneJob::make);
    job_stream::addJob("duplicate", DuplicateJob::make);
    job_stream::addJob("getToTen", GetToTenJob::make);
    job_stream::addReducer("sum", SumReducer::make);
    job_stream::addReducer("getToValue", GetToValueReducer::make);
    job_stream::runProcessor(argc, argv);
    return 0;
}
