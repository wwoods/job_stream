
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


class SumReducer : public job_stream::Reducer<int> {
public:
    static SumReducer* make() { return new SumReducer(); }

    void handleInit(int& current) {
        //Must be callable multiple times.  That is, T_accum might be 
        //instantiated on different hosts, and later merged via handleMore.
        current = 0;
    }

    void handleMore(int& current, int& more) {
        current += more;
    }

    void handleDone(int& current) {
        this->emit(current);
    }
};


int main(int argc, char* argv []) {
    job_stream::addJob("addOne", AddOneJob::make);
    job_stream::addJob("duplicate", DuplicateJob::make);
    job_stream::addReducer("sum", SumReducer::make);
    job_stream::runProcessor(argc, argv);
    return 0;
}
