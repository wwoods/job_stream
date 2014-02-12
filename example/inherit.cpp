/** Demo / test for polymorphic classes and inheritance */

#include <job_stream/job_stream.h>

using std::unique_ptr;

class Base {
public:
    int valueBase;

    virtual int getValue() {
        printf("BASE!\n");
        return valueBase;
    }

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->valueBase;
    }
};


class XorDerive : public Base {
public:
    int valueOther;

    virtual int getValue() {
        printf("XOR!\n");
        return valueBase ^ valueOther;
    }

    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::base_object<Base>(*this);
        ar & this->valueOther;
    }
};


class Bs {
public:
    int a, b, c;
private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & a & b & c;
    }
};


class MakeInstances : public job_stream::Job<int> {
public:
    static MakeInstances* make() { return new MakeInstances(); }
    void handleWork(unique_ptr<int> work) {
        for (int i = 0; i < *work; i++) {
            if (i == 9) {
                //Should error out!!!!  Bad type match.
                int i = 99;
                this->emit(i);
            }
            else if (i % 2 == 0) {
                Base b;
                b.valueBase = i + 1;
                this->emit(b);
            }
            else {
                XorDerive b;
                b.valueBase = i + 1;
                b.valueOther = 7;
                //Note that we're emitting the base class - job_stream will
                //resolve the derived type.
                this->emit(*(Base*)&b);
            }
        }
    }
};


class GetValue : public job_stream::Job<Base> {
public:
    static GetValue* make() { return new GetValue(); }
    void handleWork(unique_ptr<Base> work) {
        this->emit(work->getValue());
    }
};


int main(int argc, char* argv[]) {
    job_stream::addJob("makeInstances", MakeInstances::make);
    job_stream::addJob("getValue", GetValue::make);
    job_stream::serialization::registerType<Base, Base>();
    job_stream::serialization::registerType<XorDerive, Base>();
    job_stream::runProcessor(argc, argv);
    return 0;
}

