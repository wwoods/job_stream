
/** A simple worker / gather mechanism implemented in pipeline */

#include <job_stream/job_stream.h>

#include <cmath>
#include <memory>
#include <random>

using std::unique_ptr;

class SystemCheck {
public:
    SystemCheck() : iteration(0) {
    }

    SystemCheck& operator+=(int work) {
        this->works.push_back(work);
    }

    SystemCheck& operator+=(unique_ptr<SystemCheck> other) {
        this->iteration += other->iteration;
        for (int i = 0, m = other->works.size(); i < m; i++) {
            this->works.push_back(other->works[i]);
        }
    }

    int iteration;
    std::vector<int> works;


private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->iteration;
        ar & this->works;
    }
};


class MakeSystems : public job_stream::Job<int> {
public:
    static MakeSystems* make() { return new MakeSystems(); }
    void handleWork(unique_ptr<int> networkCount) {
        for (int i = 0; i < *networkCount; i++) {
            this->emit(this->globalConfig["sleepTime"].as<int>());
        }
    }
};


class EvalSystem : public job_stream::Job<int> {
public:
    static EvalSystem* make() { return new EvalSystem(); }
    void handleWork(unique_ptr<int> sleepTime) {
        const int max = *sleepTime * 200000;
        volatile int i = 0;
        while (++i != max);
        this->emit(*sleepTime);
    }
};


class CheckSystems : public job_stream::Reducer<SystemCheck, int> {
public:
    static CheckSystems* make() { return new CheckSystems(); }

    void handleAdd(SystemCheck& current, unique_ptr<int> work) {
        current += *work;
    }

    void handleJoin(SystemCheck& current, unique_ptr<SystemCheck> other) {
        current += std::move(other);
    }

    void handleDone(SystemCheck& current) {
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
        current.iteration += 1;
        printf("Iteration %i\n", current.iteration);
    }
};


int main(int argc, char* argv[]) {
    job_stream::addJob("makeSystems", MakeSystems::make);
    job_stream::addJob("evalSystem", EvalSystem::make);
    job_stream::addReducer("checkSystems", CheckSystems::make);
    job_stream::runProcessor(argc, argv);
    return 0;
}

