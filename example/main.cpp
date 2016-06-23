
#include <job_stream/job_stream.h>

using std::unique_ptr;

/** Add one to the integer input and forward it. */
class AddOneJob : public job_stream::Job<AddOneJob, int> {
public:
    /** The name used to describe this job in a YAML file */
    static const char* NAME() { return "addOne"; }
    void handleWork(unique_ptr<int> work) {
        this->emit(*work + 1);
    }
} addOneJob;


class PrintThenAddOne : public job_stream::Job<PrintThenAddOne, int> {
public:
    static const char* NAME() { return "printThenAddOne"; }
    void handleWork(unique_ptr<int> work) {
        printf("%i\n", *work);
        this->emit(*work + 1);
    }
} printThenAddOneJob;



struct PiCalculatorState {
    float precision;
    float piSum;
    int trials;

private:
    //All structures used for storage or emit()'d must be serializable
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & precision & piSum & trials;
    }
};

/** Calculates pi to the precision passed as the first work.  The template
    arguments for a Frame are: the Frame's class, the storage type, the
    first work's type, and subsequent (recurred) work's type. */
class PiCalculator : public job_stream::Frame<PiCalculator,
        PiCalculatorState, float, float> {
public:
    static const char* NAME() { return "piCalculator"; }

    void handleFirst(PiCalculatorState& current, unique_ptr<float> work) {
        current.precision = *work * 0.01;
        current.piSum = 0.0f;
        current.trials = 0;
        //Put work back into this Frame.  This will trigger whatever method
        //of pi approximation is defined in our YAML.  We'll pass the
        //current trial index as debug information.
        this->recur(current.trials++);
    }

    void handleNext(PiCalculatorState& current, unique_ptr<float> work) {
        current.piSum += *work;
    }

    void handleDone(PiCalculatorState& current) {
        //Are we done?
        float piCurrent = current.piSum / current.trials;
        if (fabsf((piCurrent - M_PI) / M_PI) < current.precision) {
            //We're within desired precision, emit trials count
            fprintf(stderr, "Pi found to be %f, +- %.1f%%\n", piCurrent,
                    current.precision * 100.f);
            this->emit(current.trials);
        }
        else {
            //We need more iterations.  Double our trial count
            for (int i = 0, m = current.trials; i < m; i++) {
                this->recur(current.trials++);
            }
        }
    }
} piCalculator;



class PiEstimate : public job_stream::Job<PiEstimate, int> {
public:
    static const char* NAME() { return "piEstimate"; }
    void handleWork(unique_ptr<int> work) {
        float x = rand() / (float)RAND_MAX;
        float y = rand() / (float)RAND_MAX;
        if (x * x + y * y <= 1.0) {
            //Estimate area as full circle
            this->emit(4.0f);
        }
        else {
            //Estimate area as nothing
            this->emit(0.0f);
        }
    }
} piEstimate;




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

    void handleNext(std::vector<int>& current, unique_ptr<int> work) {
        current.push_back(*work);
    }

    void handleDone(std::vector<int>& current) {
        int sum = 0;
        for (int i : current) {
            sum += i;
        }
        this->emit(sum / current.size());
    }
} runExperimentsInst;


/** Example for depth-first demonstration */
class DepthFirstFrame : public job_stream::Frame<DepthFirstFrame,
        int, int, int> {
public:
    static const char* NAME() { return "depthFirstFrame"; }

    void handleFirst(int& current, unique_ptr<int> work) {
        for (int i = 0; i < *work; i++) {
            this->recur(1);
        }
    }

    void handleNext(int& current, unique_ptr<int> work) {
        printf("Frame %i\n", *work);
    }

    void handleDone(int& current) {
    }
} depthFirstFrameInst;


/** Example for testing / demonstrating checkpoints. */
class CheckpointTester : public job_stream::Job<CheckpointTester, int> {
public:
    static const char* NAME() { return "checkpointTester"; }

    void handleWork(unique_ptr<int> work) {
        //Forces a checkpoint after this work's completion.  "true" makes the
        //program exit after the checkpoint.
        this->forceCheckpoint(true);
        this->emit(*work + 1);
    }
} checkpointTesterInst;


int main(int argc, char* argv []) {
    job_stream::runProcessor(argc, argv);
    return 0;
}
