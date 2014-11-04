
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

/** Ensure that pointers match, even across ownership. */
class ContainerClass {
public:
    std::unique_ptr<BaseClass> ptr;
    BaseClass* ptr2;

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & ptr;
        //SHOULD already be serialized.
        ar & ptr2;
    }
};

/** Ensure that pointers match, even across ownership. */
class ContainerSharedClass {
public:
    std::shared_ptr<BaseClass> ptr;
    std::shared_ptr<BaseClass> ptr2;

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & ptr;
        //SHOULD already be serialized.
        ar & ptr2;
    }
};


/** shared_ptr cannot save / load primitive types. */
struct Int {
    int value;

    Int() : value(0) {}
    Int(int v) : value(v) {}

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & value;
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

            decode(encodeAsPtr(*f), f2);
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

    { INFO("shared_ptr<Int>");
        try {
            std::shared_ptr<Int> sp(new Int(8));
            std::shared_ptr<Int> np;
            decode(encode(sp), np);
            REQUIRE(sp->value == np->value);
            REQUIRE(sp.get() != np.get());
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    { INFO("vector<shared_ptr<Int>>");
        try {
            std::vector<std::shared_ptr<Int>> farr;
            farr.emplace_back(new Int(8));
            farr.emplace_back(new Int(4));
            std::vector<std::shared_ptr<Int>> farr2;
            decode(encode(farr), farr2);

            REQUIRE(farr.size() == farr2.size());
            REQUIRE(farr2[0]->value == 8);
            REQUIRE(farr2[1]->value == 4);
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
            decode(encodeAsPtr(*a), b);
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

    try {
        /** It is now a compile error to encode a polymorphic class.
        SECTION("Raw") {
            DerivedClass b;
            decode(encode(a), b);
            REQUIRE(b.a == 1);
            REQUIRE(b.b == 96);
            REQUIRE(b.c == 2);
            REQUIRE(b.func() == 98);
        }

        SECTION("Raw mismatch") {
            BaseClass b;
            std::string aenc = encode(a);

            REQUIRE_THROWS(decode(aenc, b));
        }*/

        SECTION("Pointer pre-registration") {
            clearRegisteredTypes();
            std::unique_ptr<BaseClass> b;
            //Encoding fails because we know it's polymorphic, but it's unregistered
            REQUIRE_THROWS(encodeAsPtr(a));
        }

        registerType<BaseClass>();
        registerType<DerivedClass, BaseClass>();

        SECTION("Pointer") {
            std::unique_ptr<BaseClass> b;
            decode(encodeAsPtr(a), b);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(b.get()));
            REQUIRE(96 == b->b);
            REQUIRE(98 == b->func());
        }

        SECTION("unique_ptr") {
            std::unique_ptr<BaseClass> b;
            decode(encodeAsPtr(a), b);
            REQUIRE(a.a == b->a);
            REQUIRE(a.b == b->b);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(b.get()));
        }

        SECTION("unique_ptr mismatch") {
            std::unique_ptr<BaseClass> b;
            std::unique_ptr<DerivedClass> d;
            b.reset(new BaseClass());
            std::string s = encode(b);
            REQUIRE_THROWS(decode(s, d));

            //Ensure proper way works OK here...
            d.reset(new DerivedClass());
            decode(encode(d), b);
        }

        SECTION("shared_ptr") {
            std::shared_ptr<BaseClass> b;
            decode(encodeAsPtr(a), b);
            REQUIRE(a.a == b->a);
            REQUIRE(a.b == b->b);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(b.get()));
        }

        SECTION("shared_ptr mismatch") {
            std::shared_ptr<BaseClass> b;
            std::shared_ptr<DerivedClass> d;
            b.reset(new BaseClass());
            std::string s = encode(b);
            REQUIRE_THROWS(decode(s, d));

            //Ensure proper way works OK here...
            d.reset(new DerivedClass());
            decode(encode(d), b);
        }
    }
    catch (const std::exception& e) {
        FAIL(e.what());
    }
}


TEST_CASE("standard types", "[serialization]") {
    SECTION("vector") {
        try {
            std::vector<int> a, b;
            a.push_back(3);
            a.push_back(8);
            b.push_back(4);
            decode(encode(a), b);

            REQUIRE(b.size() == 2);
            REQUIRE(b[0] == 3);
            REQUIRE(b[1] == 8);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }

    SECTION("list") {
        try {
            std::list<int> a, b;
            a.push_back(88);
            a.push_front(3);
            b.push_back(27);
            decode(encode(a), b);

            REQUIRE(b.size() == 2);
            REQUIRE(b.front() == 3);
            REQUIRE(b.back() == 88);
        }
        catch (const std::exception& e) {
            FAIL(e.what());
        }
    }
}


TEST_CASE("unique_ptr sharing", "[serialization]") {
    registerType<BaseClass>();
    registerType<DerivedClass, BaseClass>();
    try {
        ContainerClass c1, c2;
        SECTION("base class") {
            c1.ptr.reset(new BaseClass());
            c1.ptr->a = 2;
            c1.ptr->b = 4;
            c1.ptr2 = c1.ptr.get();

            decode(encode(c1), c2);
            REQUIRE(c2.ptr->a == 2);
            REQUIRE(c2.ptr->b == 4);
            REQUIRE(c2.ptr->func() == 6);
            REQUIRE(c2.ptr.get() == c2.ptr2);
            REQUIRE(c2.ptr.get() != c1.ptr2);
        }
        SECTION("derived class") {
            c1.ptr.reset(new DerivedClass());
            c1.ptr->a = 5;
            c1.ptr->b = 3;
            ((DerivedClass*)c1.ptr.get())->c = 1;
            c1.ptr2 = c1.ptr.get();

            decode(encode(c1), c2);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(c1.ptr.get()));
            REQUIRE(c2.ptr->a == 5);
            REQUIRE(c2.ptr->b == 3);
            REQUIRE(c2.ptr->func() == 16);
            REQUIRE(c2.ptr.get() == c2.ptr2);
        }
    }
    catch (const std::exception& e) {
        FAIL(e.what());
    }
}


TEST_CASE("shared_ptr sharing", "[serialization]") {
    registerType<BaseClass>();
    registerType<DerivedClass, BaseClass>();
    try {
        ContainerSharedClass c1, c2;
        SECTION("base class") {
            c1.ptr.reset(new BaseClass());
            c1.ptr->a = 2;
            c1.ptr->b = 4;
            c1.ptr2 = c1.ptr;

            decode(encode(c1), c2);
            REQUIRE(c2.ptr->a == 2);
            REQUIRE(c2.ptr->b == 4);
            REQUIRE(c2.ptr->func() == 6);
            REQUIRE(c2.ptr.get() == c2.ptr2.get());
            REQUIRE(c2.ptr.get() != c1.ptr2.get());

            //Extra step for shared_ptr - refcount should be shared.  There
            //will be a segfault in here if it's bad.
            c2.ptr.reset();
            c2.ptr2->a = 2;
            c2.ptr2.reset();
        }
        SECTION("derived class") {
            c1.ptr.reset(new DerivedClass());
            c1.ptr->a = 5;
            c1.ptr->b = 3;
            dynamic_cast<DerivedClass*>(c1.ptr.get())->c = 1;
            c1.ptr2 = c1.ptr;

            decode(encode(c1), c2);
            REQUIRE(0 != dynamic_cast<DerivedClass*>(c1.ptr.get()));
            REQUIRE(c2.ptr->a == 5);
            REQUIRE(c2.ptr->b == 3);
            REQUIRE(c2.ptr->func() == 16);
            REQUIRE(c2.ptr.get() == c2.ptr2.get());

            //Extra step for shared_ptr - refcount should be shared.  There
            //will be a segfault in here if it's bad.
            c2.ptr.reset();
            c2.ptr2->a = 2;
            c2.ptr2.reset();
        }
    }
    catch (const std::exception& e) {
        FAIL(e.what());
    }
}
