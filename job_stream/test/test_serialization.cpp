
#include <job_stream/serialization.h>

#include "test/catch.hpp"

using namespace job_stream::serialization;

struct NonPrimitive {
    int a;
    int b;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & a & b;
    }
};

class BaseClass {
public:
    int a;
    int b;

    virtual int func() { return a + b; }

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & a & b;
    }
};

class DerivedClass : public BaseClass {
public:
    int c;
    int func() { return a * b + c; }

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::base_object<BaseClass>(*this);
        ar & c;
    }
};

TEST_CASE("primitive types", "[serialization]") {
    { INFO("float");
        try {
            float f = 3.125;
            float f2;

            decode(encode(f), f2);
            REQUIRE(f == f2);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("float*");
        try {
            float* f = new float(3.125);
            std::unique_ptr<float> f2;

            decode(encode(f), f2);
            REQUIRE(*f == *f2);
            REQUIRE(f != f2.get());

            delete f;
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("unique_ptr<int>");
        try {
            std::unique_ptr<int> sp(new int(82));
            std::unique_ptr<int> np;
            decode(encode(sp), np);
            REQUIRE(*sp == *np);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("shared_ptr<int>");
        try {
            std::shared_ptr<int> sp(new int(8));
            std::shared_ptr<int> np;
            decode(encode(sp), np);
            REQUIRE(*sp == *np);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("vector<shared_ptr<int>>");
        try {
            std::vector<std::shared_ptr<int>> farr;
            farr.emplace_back(new int(8));
            farr.emplace_back(new int(4));
            std::vector<std::shared_ptr<int>> farr2;
            decode(encode(farr), farr2);

            REQUIRE(farr.size() == farr2.size());
            REQUIRE(*farr2[0] == 8);
            REQUIRE(*farr2[1] == 4);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }
}

TEST_CASE("non-primitive non-polymorphic", "[serialization]") {
    { INFO("Raw");
        try {
            NonPrimitive a, b;
            a.a = 8;
            a.b = 96;
            decode(encode(a), b);
            REQUIRE(b.a == 8);
            REQUIRE(b.b == 96);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("Pointer");
        try {
            NonPrimitive *a;
            std::unique_ptr<NonPrimitive> b;
            a = new NonPrimitive();
            a->a = 99;
            a->b = 101;
            decode(encode(a), b);
            REQUIRE(a != b.get());
            REQUIRE(a->a == b->a);
            REQUIRE(a->b == b->b);

            delete a;
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("unique_ptr");
        try {
            std::unique_ptr<NonPrimitive> a, b;
            a.reset(new NonPrimitive());
            a->a = 99;
            a->b = 101;
            decode(encode(a), b);
            REQUIRE(a->a == b->a);
            REQUIRE(a->b == b->b);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("shared_ptr");
        try {
            std::shared_ptr<NonPrimitive> a, b;
            a.reset(new NonPrimitive());
            a->a = 99;
            a->b = 101;
            decode(encode(a), b);
            REQUIRE(a->a == b->a);
            REQUIRE(a->b == b->b);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }
}

TEST_CASE("polymorphic types", "[serialization]") {
    DerivedClass a;
    a.a = 1;
    a.b = 96;
    a.c = 2;

    SECTION("Raw") {
        try {
            DerivedClass b;
            decode(encode(a), b);
            REQUIRE(b.a == 1);
            REQUIRE(b.b == 96);
            REQUIRE(b.c == 2);
            REQUIRE(b.func() == 98);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    SECTION("Raw mismatch") {
        BaseClass b;
        std::string aenc = encode(a);

        REQUIRE_THROWS(decode(aenc, b));
    }

    SECTION("Pointer pre-registration") {
        clearRegisteredTypes();
        std::unique_ptr<BaseClass> b;
        //Encoding fails because we know it's polymorphic, but it's unregistered
        REQUIRE_THROWS(encode(&a));
    }

    registerType<DerivedClass, BaseClass>();

    SECTION("Pointer") {
        try {
            std::unique_ptr<BaseClass> b;
            decode(encode(&a), b);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(b.get()));
            REQUIRE(96 == b->b);
            REQUIRE(98 == b->func());
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    SECTION("unique_ptr") {
        try {
            std::unique_ptr<BaseClass> b;
            decode(encode(&a), b);
            REQUIRE(a.a == b->a);
            REQUIRE(a.b == b->b);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(b.get()));
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    SECTION("shared_ptr") {
        try {
            std::shared_ptr<BaseClass> b;
            decode(encode(&a), b);
            REQUIRE(a.a == b->a);
            REQUIRE(a.b == b->b);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(b.get()));
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }
}
