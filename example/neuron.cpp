
/** A simple neuron / genetic algorithm implemented in pipeline language */

#include <job_stream/job_stream.h>

#include <cmath>
#include <memory>
#include <random>

std::mt19937 rngEngine;
std::uniform_real_distribution<float> rng(-1, 1);
float getRandom() {
    return rng(rngEngine);
}

class NeuralLayer {
public:
    NeuralLayer() {}
    NeuralLayer(const NeuralLayer& other) {
        this->inputs = other.inputs;
        this->neurons = other.neurons;
        this->weights = other.weights;
    }
    NeuralLayer(int inputs, int neurons) : inputs(inputs), neurons(neurons) {
        for (int i = 0; i < neurons * inputs; i++) {
            this->weights.push_back(getRandom());
        }
    }

    int inputs;
    int neurons;

    float* eval(float* inputs) {
        if (!this->lastResults) {
            this->lastResults.reset(new float[this->neurons]);
        }

        for (int i = 0; i < this->neurons; i++) {
            float v = 0.0;
            for (int j = 0; j < this->inputs; j++) {
                v += this->weights[i * this->inputs + j] * inputs[j];
            }
            this->lastResults.get()[i] = 1.f / (1.f + exp(-v));
        }

        return this->lastResults.get();
    }

    void initFrom(NeuralLayer* a, NeuralLayer* b) {
        this->inputs = a->inputs;
        this->neurons = a->neurons;
        std::uniform_int_distribution<> dist(1, a->weights.size() - 2);
        int split = dist(rngEngine);
        for (int i = 0; i < split; i++) {
            this->weights.push_back(a->weights[i]);
        }
        for (int i = split, m = a->weights.size(); i < m; i++) {
            this->weights.push_back(b->weights[i]);
        }

        //Mutate!
        std::uniform_int_distribution<> wdist(0, a->weights.size() / 2);
        std::uniform_int_distribution<> mut(0, this->weights.size());
        for (int j = 0, k = wdist(rngEngine); j < k; j++) {
            this->weights[mut(rngEngine)] += 0.2 * getRandom();
        }
    }

private:
    std::vector<float> weights;
    std::unique_ptr<float[]> lastResults;

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->inputs;
        ar & this->neurons;
        ar & this->weights;
    }
};


class NeuralNet {
public:
    NeuralNet() {}
    NeuralNet(int neurons, int inputs, int outputs) {
        std::unique_ptr<NeuralLayer> layer;
        layer.reset(new NeuralLayer(inputs, neurons));
        this->layers.push_back(std::move(layer));
        layer.reset(new NeuralLayer(neurons, neurons));
        this->layers.push_back(std::move(layer));
        layer.reset(new NeuralLayer(neurons, outputs));
        this->layers.push_back(std::move(layer));
    }
    NeuralNet(const NeuralNet& other) {
        std::unique_ptr<NeuralLayer> layer;
        for (int i = 0, m = other.layers.size(); i < m; i++) {
            layer.reset(new NeuralLayer(*other.layers[i]));
            this->layers.push_back(std::move(layer));
        }
        this->score = other.score;
    }
    NeuralNet(NeuralNet& a, NeuralNet& b) {
        //Cross a and b
        std::unique_ptr<NeuralLayer> layer;
        for (int i = 0, m = a.layers.size(); i < m; i++) {
            layer.reset(new NeuralLayer());
            layer->initFrom(a.layers[i].get(), b.layers[i].get());
            this->layers.push_back(std::move(layer));
        }
    }

    float score;

    float getError(YAML::Node& array) {
        std::unique_ptr<float[]> inputs(new float[array.size()]);
        for (int i = 0, m = array.size(); i < m; i++) {
            inputs[i] = array[i].as<float>();
        }

        float* layerInput = inputs.get();
        for (int i = 0, m = this->layers.size(); i < m; i++) {
            layerInput = this->layers[i]->eval(layerInput);
        }

        //layerInput now == output
        float score = 0;
        for (int base = this->layers[0]->inputs, i = 0, 
                m = array.size() - base; i < m; i++) {
            score += (layerInput[i] - inputs[base + i]) 
                    * (layerInput[i] - inputs[base + i]);
        }
        return score;
    }

private:
    std::vector<std::unique_ptr<NeuralLayer>> layers;

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->score;
        int m = this->layers.size();
        ar & m;
        for (int i = 0; i < m; i++) {
            if (Archive::is_loading::value) {
                std::unique_ptr<NeuralLayer> ptr(new NeuralLayer());
                ar & *ptr;
                this->layers.push_back(std::move(ptr));
            }
            else {
                ar & *this->layers[i];
            }
        }
    }
};


class NetworkPopulace { 
public:
    NetworkPopulace() {
    }

    NetworkPopulace(const NetworkPopulace& other) {
        for (size_t i = 0, m = other.networks.size(); i < m; i++) {
            *this += *other.networks[i];
        }
    }

    NetworkPopulace& operator+=(NeuralNet& network) {
        std::unique_ptr<NeuralNet> ptr(new NeuralNet(network));
        this->networks.push_back(std::move(ptr));
    }

    NetworkPopulace& operator+=(NetworkPopulace& other) {
        for (int i = 0, m = other.networks.size(); i < m; i++) {
            this->networks.push_back(std::move(other.networks.back()));
            other.networks.pop_back();
        }
    }

    NeuralNet& bestNetwork() {
        float best = this->bestScore();
        for (int i = 0, m = this->networks.size(); i < m; i++) {
            auto* net = this->networks[i].get();
            if (net->score == best) {
                return *net;
            }
        }
        throw std::runtime_error("No best network?");
    }

    float bestScore() {
        float min = 1e35f;
        for (int i = 0, m = this->networks.size(); i < m; i++) {
            auto* net = this->networks[i].get();
            if (net->score < min) {
                min = net->score;
            }
        }
        return min;
    }

    void clear() {
        this->networks.clear();
    }

    void cross() {
        std::vector<NeuralNet*> newNets;
        for (int i = 0, m = this->networks.size() - 1; i < m; i++) {
            NeuralNet& src1 = this->monteCarlo(0);
            NeuralNet& src2 = this->monteCarlo(&src1);
            newNets.push_back(new NeuralNet(src1, src2));
        }
        //Elite!
        for (int i = 0, m = this->networks.size(); i < m; i++) {
            if (this->networks[i]->score == this->bestScore()) {
                newNets.push_back(this->networks[i].release());
                break;
            }
        }
        this->networks.clear();
        for (int i = 0, m = newNets.size(); i < m; i++) {
            std::unique_ptr<NeuralNet> ptr(newNets[i]);
            this->networks.push_back(std::move(ptr));
        }
    }

    NeuralNet& monteCarlo(NeuralNet* dontChoose) {
        float max = 0.0;
        float total = 0.0;
        int m = this->networks.size();
        for (int i = 0; i < m; i++) {
            if (this->networks[i].get() == dontChoose) continue;
            float score = this->networks[i]->score;
            if (score > max) max = score;
        }
        max *= 1.01;
        for (int i = 0; i < m; i++) {
            if (this->networks[i].get() == dontChoose) continue;
            total += max - this->networks[i]->score;
        }

        std::uniform_real_distribution<float> rngMonte(0, total);
        float slice = rngMonte(rngEngine);
        for (int i = 0; i < m; i++) {
            if (this->networks[i].get() == dontChoose) continue;
            slice -= max - this->networks[i]->score;
            if (slice <= 0.0) {
                return *this->networks[i];
            }
        }

        throw std::runtime_error("monteCarlo fell out bottom");
    }

    int size() {
        return this->networks.size();
    }

    NeuralNet& operator[](int index) {
        return *this->networks[index];
    }

private:
    std::vector<std::unique_ptr<NeuralNet>> networks;

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        int m = this->networks.size();
        ar & m;
        for (int i = 0; i < m; i++) {
            if (Archive::is_loading::value) {
                std::unique_ptr<NeuralNet> ptr(new NeuralNet());
                ar & *ptr;
                this->networks.push_back(std::move(ptr));
            }
            else {
                ar & *this->networks[i];
            }
        }
    }
};

//TODO / FIXME!!! I don't want these specified...lexical_cast might not work.
//Could pidgeonhole it as per output... though.. hm.
std::istream &operator>>(std::istream &source, NeuralNet const &h) {
    return source;
}
std::istream &operator>>(std::istream &source, NetworkPopulace const &h) {
    return source;
} 


class MakeNetworks : public job_stream::Job<int> {
public:
    static MakeNetworks* make() { return new MakeNetworks(); }
    void handleWork(int& networkCount) {
        //Initialize networkCount networks
        for (int i = 0; i < networkCount; i++) {
            this->emit(NeuralNet(this->config["neurons"].as<int>(),
                    this->config["numInputs"].as<int>(),
                    this->config["numOutputs"].as<int>()));
        }
    }
};


class EvalNetwork : public job_stream::Job<NeuralNet> {
public:
    static EvalNetwork* make() { return new EvalNetwork(); }
    void handleWork(NeuralNet& network) {
        float score = 0.0;
        auto tests = this->globalConfig["tests"].as<std::vector<YAML::Node> >();
        for (int i = 0, m = tests.size(); i < m; i++) {
            score += network.getError(tests[i]);
        }
        network.score = score;
        this->emit(network);
    }
};


class CheckErrorAndBreed : public job_stream::Reducer<NetworkPopulace, 
        NeuralNet> {
public:
    static CheckErrorAndBreed* make() { return new CheckErrorAndBreed(); }

    void handleDone(NetworkPopulace& current) {
        float bestScore = current.bestScore();
        NeuralNet& best = current.bestNetwork();
        if (bestScore < this->config["error"].as<float>()) {
            std::ostringstream ss;
            ss << "Done!  Best error: " << boost::lexical_cast<std::string>(
                    current.bestScore());
            ss << ".";
            this->emit(ss.str());
            return;
        }
        else {
            printf("Best score: %.3f (", bestScore);
            auto tests = this->globalConfig["tests"]
                    .as<std::vector<YAML::Node>>();
            for (int i = 0, m = tests.size(); i < m; i++) {
                if (i != 0) {
                    printf(", ");
                }
                printf("%f", best.getError(tests[i]));
            }
            printf(")\n");
        }

        current.cross();
        for (int i = 0, m = current.size(); i < m; i++) {
            this->recur(current[i]);
        }

        //Wait for those crossed networks to come through, so we maintain
        //population size.
        current.clear();
    }
};



int main(int argc, char* argv[]) {
    job_stream::addJob("evalNetwork", EvalNetwork::make);
    job_stream::addJob("makeNetworks", MakeNetworks::make);
    job_stream::addReducer("checkErrorAndBreed", CheckErrorAndBreed::make);
    job_stream::runProcessor(argc, argv);
    return 0;
}
