
/** A simple worker / gather mechanism implemented in pipeline */

#include <job_stream/job_stream.h>

#include <cmath>
#include <memory>
#include <random>

using std::unique_ptr;

struct LoadedInt {
    LoadedInt() {}
    LoadedInt(int i, int loadBytes) : value(i) {
        this->load.resize(loadBytes, '=');
    }

    int value;
    std::string load;

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->value;
        ar & this->load;
    }
};

struct SystemCheck {
    SystemCheck() : iteration(0) {
    }

    int iteration;
    std::vector<LoadedInt> works;

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->iteration;
        ar & this->works;
    }
};


class MakeLoaded : public job_stream::Job<MakeLoaded, int> {
public:
    static const char* NAME() { return "makeLoaded"; }

    void handleWork(unique_ptr<int> input) {
        this->emit(LoadedInt(*input, this->globalConfig["dataSize"].as<int>()));
    }
} makeLoaded;


class UnmakeLoaded : public job_stream::Job<UnmakeLoaded, LoadedInt> {
public:
    static const char* NAME() { return "unmakeLoaded"; }

    void handleWork(unique_ptr<LoadedInt> input) {
        this->emit(input->value);
    }
} unmakeLoaded;


class MakeSystems : public job_stream::Job<MakeSystems, LoadedInt> {
public:
    static const char* NAME() { return "makeSystems"; }

    void handleWork(unique_ptr<LoadedInt> unused) {
        for (int i = 0; i < this->config["count"].as<int>(); i++) {
            this->emit(LoadedInt(this->globalConfig["sleepTime"].as<int>(),
                    this->globalConfig["dataSize"].as<int>()));
        }
    }
} makeSystems;


class EvalSystem : public job_stream::Job<EvalSystem, LoadedInt> {
public:
    static const char* NAME() { return "evalSystem"; }

    void handleWork(unique_ptr<LoadedInt> sleepTime) {
        for (int j = 0; j < 10; j++) {
            //this->checkMpi();
            const int max = sleepTime->value * 20000;
            volatile int i = 0;
            while (++i != max);
        }
        this->emit(sleepTime);
    }
} evalSystem;


class CheckSystems : public job_stream::Reducer<CheckSystems, SystemCheck, LoadedInt> {
public:
    static const char* NAME() { return "checkSystems"; }

    void handleAdd(SystemCheck& current, unique_ptr<LoadedInt> work) {
        current.works.push_back(*work);
    }

    void handleJoin(SystemCheck& current, unique_ptr<SystemCheck> other) {
        current.works.insert(current.works.end(), other->works.begin(), 
                other->works.end());
        current.iteration += other->iteration;
    }

    void handleDone(SystemCheck& current) {
        current.iteration += 1;
        printf("Iteration %i done\n", current.iteration);
        if (current.iteration >= this->config["iterations"].as<int>()) {
            printf("Done!\n");
            this->emit(current.works[0]);
            return;
        }

        for (int i = 0, m = current.works.size(); i < m; i++) {
            this->recur(current.works[i]);
        }

        //Wait for those to come through, maintain population size
        current.works.clear();
    }
} checkSystems;


int main(int argc, char* argv[]) {
    job_stream::runProcessor(argc, argv);
    return 0;
}

