#ifndef JOB_STREAM_SERIALIZATION_H_
#define JOB_STREAM_SERIALIZATION_H_

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/mpl/equal.hpp>
#include <boost/mpl/if.hpp>
#include <boost/mpl/not.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/extended_type_info_no_rtti.hpp>
#include <boost/serialization/level.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/type_traits/add_pointer.hpp>
#include <boost/type_traits/is_polymorphic.hpp>
#include <boost/type_traits/remove_const.hpp>
#include <boost/type_traits/remove_pointer.hpp>
#include <boost/utility/enable_if.hpp>
#include <exception>
#include <sstream>
#include <string>

/** Helper functions for job_stream serialization */

namespace job_stream {
namespace serialization {

typedef boost::archive::binary_oarchive OArchive;
typedef boost::archive::binary_iarchive IArchive;

//Actual public methods
/** An example signature only.  Decode takes either a non-pointer or a unique_ptr */
template<typename T, class ExampleOnly>
void decode(const std::string& message, T& dest);
template<typename T>
std::string encode(const T& src);
template<class T, class Base1>
void registerType();

/** Since job_stream supports smarter serialization of pointers with 
    polymorphic classes than boost... here's to it */
template<class T>
void serialize(OArchive& a, std::unique_ptr<T>& ptr);
template<class T>
void serialize(IArchive& a, std::unique_ptr<T>& ptr);

/** Fully polymorphic object copy - even if you specify the base pointer,
    the derived class will be copied. */
template<class T>
void copy(T&& src, std::unique_ptr<T>& dest);


//Implementations
class RegisteredTypeBase {
public:
    virtual const char* typeName() = 0;
    virtual const char* baseName() = 0;
    virtual bool tryDecode(IArchive& a, const std::string& archTypeName,
            const char* destTypeName, void** dest) = 0;
    virtual bool tryEncode(OArchive& a, const char* srcTypeName, void* src) = 0;

    /** Since we go through these in order, it is important that the
        most-derived classes are encountered first.  Otherwise, encode() will 
        find a base class before a potential better match, and since 
        dynamic_cast will return non-zero, encoding will stop.

        Returns -1 if this RegisteredTypeBase MUST go before other, and a 1 if 
        it MUST go after.  Returns 0 if it doesn't matter. */
    int getOrder(RegisteredTypeBase& other) {
        if (strcmp(other.baseName(), this->typeName()) == 0) {
            return 1;
        }
        if (strcmp(other.typeName(), this->baseName()) == 0) {
            return -1;
        }
        return 0;
    }
};



/** Relates a derivative type T to its base class B */
template<class T, class B>
class RegisteredType : public RegisteredTypeBase {
public:
    const char* typeName() { return typeid(T*).name(); }
    const char* baseName() { return typeid(B*).name(); }

    bool tryDecode(IArchive& a, const std::string& archTypeName, 
            const char* destTypeName, void** dest) {
        if ((strcmp(destTypeName, typeName()) != 0
                && strcmp(destTypeName, baseName()) != 0)
                || archTypeName != typeName()) {
            return false;
        }
        a >> *(T**)dest;
        return true;
    }

    bool tryEncode(OArchive& a, const char* srcTypeName, void* src) {
        if (strcmp(srcTypeName, typeName()) != 0
                && (strcmp(srcTypeName, baseName()) != 0
                    || dynamic_cast<T*>((B*)src) == 0)) {
            return false;
        }

        std::string myType = typeName();
        a << myType;
        T* srcT = (T*)src;
        a << srcT;
        return true;
    }
};



extern std::vector<std::unique_ptr<RegisteredTypeBase>> registeredTypes;
template<class T, class Base1>
void registerType() {
    std::unique_ptr<RegisteredTypeBase> ptr(new RegisteredType<T, Base1>());
    bool insertOverZero = false;
    auto it = registeredTypes.begin();
    for (; it != registeredTypes.end(); it++) {
        int order = ptr->getOrder(*it->get());
        if (order < 0) {
            registeredTypes.insert(it, std::move(ptr));
            return;
        }
        else if (order > 0) {
            insertOverZero = true;
        }
        else if (insertOverZero) {
            registeredTypes.insert(it, std::move(ptr));
            return;
        }
    }

    registeredTypes.insert(it, std::move(ptr));
}


struct _SerialHelperUtility {
    static void typeError(const std::string& expected,
            const std::string& actual) {
        std::ostringstream ss;
        ss << "Expected type '" << expected << "'; got '" << actual << "'";
        throw std::runtime_error(ss.str());
    }
};


/** To support encoding / decoding derived classes (virtual inheritance), we 
    serialize pointers.  But primitive types don't support that (as a pointer).
    So we use this to get around it.  Non-specialized is for non pointers or 
    pointers to non-polymorphic and non-primitive types. */
template<typename T, class Enable = void>
struct _SerialHelper {
    //T might be a pointer, though it will be a static type.  So we have to
    //come up with a const-stripped typed, regardless of pointer or not.
    typedef typename boost::mpl::if_<
            boost::is_pointer<T>,
            typename boost::add_pointer<typename boost::remove_const<
                typename boost::remove_pointer<T>::type>::type>::type,
            typename boost::remove_const<T>::type
            >::type T_noconst;
    static const char* typeName() { return typeid(T_noconst).name(); }

    static void encodeTypeAndObject(OArchive& a, const T& obj) {
        std::string tn = typeName();
        a << tn;
        a << obj;
    }

    static void decodeTypeAndObject(IArchive& a, T& obj) {
        std::string objTypeName;
        a >> objTypeName;
        if (objTypeName != typeName()) {
            _SerialHelperUtility::typeError(typeName(), objTypeName);
        }
        a >> obj;
    }
};


/** Pointers to primitive types have different handling because boost doesn't 
    allow serializing pointers to primitives by default. */
template<typename T>
struct _SerialHelper<T*, typename boost::enable_if<boost::mpl::bool_<
        boost::serialization::implementation_level<T>::type::value
        == boost::serialization::primitive_type>>::type> {
    typedef typename boost::remove_const<T>::type T_noconst;
    static const char* typeName() { return typeid(T_noconst*).name(); }

    static void encodeTypeAndObject(OArchive& a, T* const & obj) {
        std::string tn = typeName();
        a << tn;
        a << *obj;
    }

    static void decodeTypeAndObject(IArchive& a, T*& obj) {
        std::string type;
        a >> type;
        if (type != typeName()) {
            _SerialHelperUtility::typeError(typeName(), type);
        }
        obj = new T();
        a >> *obj;
    }
};


/** Polymorphic type specialization.  We have to look through registeredTypes on
    both encode and decode to ensure that we encode the most specific class, and
    decode the correct derived class (and a class that is a derivative at all). 
    */
template<typename T>
struct _SerialHelper<T*, 
        typename boost::enable_if<boost::is_polymorphic<T>>::type> {
    typedef typename boost::remove_const<T>::type T_noconst;
    static const char* typeName() { return typeid(T_noconst*).name(); }

    static void encodeTypeAndObject(OArchive& a, T* const & obj) {
        //Any polymorphic class MUST be registered.  Otherwise, we can't guarantee
        //type safety, and might end up encoding a base class representation of a non
        //base class.
        for (std::unique_ptr<RegisteredTypeBase>& m : registeredTypes) {
            if (m->tryEncode(a, typeName(), (void*)obj)) {
                return;
            }
        }

        std::ostringstream ss;
        ss << "Failed to encode polymorphic type '" << typeName() << "'.  ";
        ss << "Did you call job_stream::serialization::registerType<Cls, Base>()?";
        throw std::runtime_error(ss.str());
    }

    static void decodeTypeAndObject(IArchive& a, T*& obj) {
        std::string objTypeName;
        a >> objTypeName;
        for (std::unique_ptr<RegisteredTypeBase>& m : registeredTypes) {
            if (m->tryDecode(a, objTypeName, typeName(), (void**)&obj)) {
                return;
            }
        }

        std::ostringstream ss;
        ss << "Failed to decode type '" << objTypeName << "' into polymorphic type '";
        ss << typeName() << "'.  It is likely that these types are incompatible.  ";
        ss << "Did you call job_stream::serialization::registerType<Cls, Base>()?";
        throw std::runtime_error(ss.str());
    }
};


/** Non-pointer decode mechanism */
template<class T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type = 0>
void decode(const std::string& message, T& dest) {
    typedef _SerialHelper<T> sh;
    std::istringstream ss(message);
    IArchive ia(ss, boost::archive::no_header);
    sh::decodeTypeAndObject(ia, dest);
}


template<class T>
void decode(const std::string& message, std::unique_ptr<T>& dest) {
    typedef _SerialHelper<T*> sh;
    std::istringstream ss(message);
    IArchive ia(ss, boost::archive::no_header);
    T* toAllocate;
    sh::decodeTypeAndObject(ia, toAllocate);
    dest.reset(toAllocate);
}


template<class T>
std::string encode(const T& src) {
    typedef _SerialHelper<T> sh;
    std::ostringstream ss;
    OArchive oa(ss, boost::archive::no_header);
    sh::encodeTypeAndObject(oa, src);
    return ss.str();
}


/* Sharing is caring */
template<class T>
void serialize(OArchive& a, std::unique_ptr<T>& ptr) {
    std::string encoded = encode(ptr.get());
    a << encoded;
}

template<class T>
void serialize(IArchive& a, std::unique_ptr<T>& ptr) {
    std::string decoded;
    a >> decoded;
    decode(decoded, ptr);
}


template<class T>
void copy(const T& src, std::unique_ptr<T>& dest) {
    decode(encode(&src), dest);
}

}
}//job_stream

#endif//JOB_STREAM_SERIALIZATION_H_
