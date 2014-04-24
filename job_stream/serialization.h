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

/** Forward declared typedefs */
namespace job_stream {
namespace serialization {

typedef boost::archive::binary_oarchive OArchive;
typedef boost::archive::binary_iarchive IArchive;

} //serialization
} //job_stream

namespace boost {
namespace serialization {

/** Since job_stream supports smarter serialization of pointers with
    polymorphic classes than boost... here's to it */
template<class T>
void serialize(job_stream::serialization::OArchive& a, std::unique_ptr<T>& ptr,
        const unsigned int version);
template<class T>
void serialize(job_stream::serialization::IArchive& a, std::unique_ptr<T>& ptr,
        const unsigned int version);
template<class T>
void serialize(job_stream::serialization::OArchive& a, std::shared_ptr<T>& ptr,
        const unsigned int version);
template<class T>
void serialize(job_stream::serialization::IArchive& a, std::shared_ptr<T>& ptr,
        const unsigned int version);

} //serialization
} //boost

/** Helper functions for job_stream serialization */
namespace job_stream {
namespace serialization {

//Actual public methods
/** An example signature only.  Decode takes either a non-pointer or a unique_ptr */
template<typename T, class ExampleOnly>
void decode(const std::string& message, T& dest);
template<typename T>
void decode(const std::string& message, std::unique_ptr<T>& dest);
template<typename T>
void decode(const std::string& message, std::shared_ptr<T>& dest);
template<typename T>
std::string encode(const T& src);
template<typename T>
std::string encode(const std::unique_ptr<T>& src);
template<typename T>
std::string encode(const std::shared_ptr<T>& src);
/** Extract the typeid(T).name() field from the given encoded message. */
std::string getDecodedType(const std::string& message);
/** Register a polymorphic base class or a derivative with its bases */
template<class T, class Base1 = void, class Base2 = void, class Base3 = void,
        class Base4 = void, class Base5 = void, class Base6 = void>
void registerType();
/** Clear all registered types.  Used for testing. */
void clearRegisteredTypes();
/** Helper class for debugging registerType issues (unregistered class) */
void printRegisteredTypes();

/** Fully polymorphic object copy - even if you specify the base pointer,
    the derived class will be copied. */
template<class T>
void copy(const T& src, std::unique_ptr<T>& dest);
template<class T>
void copy(const std::unique_ptr<T>& src, std::unique_ptr<T>& dest);


/** A special serializable type that will preserve whatever the original type
    was. */
class AnyType {
public:
    AnyType() {}
    AnyType(std::string message) : data(std::move(message)) {}

    template<typename T>
    std::unique_ptr<T> as() {
        std::unique_ptr<T> ptr;
        decode(this->data, ptr);
        return std::move(ptr);
    }

private:
    std::string data;
    //Note - there should not need to be any serialize() on AnyType!  Otherwise
    //it's trying to go through the normal channels, which means it is
    //implemented wrong.
};


//Implementations
class RegisteredTypeBase {
public:
    virtual const char* typeName() = 0;
    virtual const char* baseName() = 0;
    virtual bool tryDecode(IArchive& a, const std::string& archTypeName,
            const char* destTypeName, void** dest) = 0;
    virtual bool tryEncode(OArchive& a, const char* srcTypeName, void* src) = 0;

    template<class U>
    bool isBaseClass() {
        return strcmp(typeid(U*).name(), baseName()) == 0;
    }

    template<class U>
    bool isDerivedClass() {
        return strcmp(typeid(U*).name(), typeName()) == 0;
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
        bool isNull = (src == 0);
        if (strcmp(srcTypeName, typeName()) != 0
                && (strcmp(srcTypeName, baseName()) != 0
                    || !isNull && dynamic_cast<T*>((B*)src) == 0)) {
            return false;
        }

        std::string myType = typeName();
        a << myType;
        T* srcT = (T*)src;
        a << srcT;
        return true;
    }
};



extern std::list<std::unique_ptr<RegisteredTypeBase>> registeredTypes;

template<class T, class Base>
class _RegisterType_impl {
public:
    static void doRegister() {
        std::unique_ptr<RegisteredTypeBase> ptr(new RegisteredType<T, Base>());
        auto it = registeredTypes.begin();
        for (; it != registeredTypes.end(); it++) {
            /** Since we go through these in order, it is important that the
                most-derived classes are encountered first.  Otherwise, encode()
                will find a base class before a potential better match, and 
                since dynamic_cast will return non-zero, encoding will stop. */
            if ((*it)->isDerivedClass<T>()) {
                //put with our people
                registeredTypes.insert(it, std::move(ptr));
                return;
            }
            else if ((*it)->isDerivedClass<Base>()) {
                //We derive from this class, so go above its block, and move all
                //of our other instances above it too
                registeredTypes.insert(it, std::move(ptr));
                auto insertIt = it;
                //it now points to one before the row that triggered this if
                //condition, after this decr and the first incr
                --it;
                while (++it != registeredTypes.end()) {
                    if ((*it)->isDerivedClass<T>()) {
                        auto toRemove = it++;
                        std::unique_ptr<RegisteredTypeBase> ptr = std::move(
                                *toRemove);
                        registeredTypes.erase(toRemove);
                        registeredTypes.insert(insertIt, std::move(ptr));
                        --it;
                    }
                }
                return;
            }
        }

        registeredTypes.insert(it, std::move(ptr));
    }
};


template<class T>
class _RegisterType_impl<T, void> {
public:
    static void doRegister() {
        //Nothing can derive from void
    }
};


template<class T, class Base1 = void, class Base2 = void, class Base3 = void,
        class Base4 = void, class Base5 = void, class Base6 = void>
void registerType() {
    _RegisterType_impl<T, T>::doRegister();
#define REGISTER(Base)\
    boost::mpl::if_< \
            std::is_same<Base, void>, \
            _RegisterType_impl<void, void>, \
            _RegisterType_impl<T, Base>>::type::doRegister()

    REGISTER(Base1);
    REGISTER(Base2);
    REGISTER(Base3);
    REGISTER(Base4);
    REGISTER(Base5);
    REGISTER(Base6);

#undef REGISTER
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
        bool isNull = (obj == 0);
        a << tn;
        a << isNull;
        if (!isNull) {
            a << *obj;
        }
    }

    static void decodeTypeAndObject(IArchive& a, T*& obj) {
        std::string type;
        bool isNull;
        a >> type;
        if (type != typeName()) {
            _SerialHelperUtility::typeError(typeName(), type);
        }
        a >> isNull;
        if (isNull) {
            obj = 0;
        }
        else {
            obj = new T();
            a >> *obj;
        }
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


/** Non-pointer decode mechanism.  Avoid memory leaks by explicitly disallowing
    decoding to a raw pointer type. */
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
void decode(const std::string& message, std::shared_ptr<T>& dest) {
    typedef _SerialHelper<T*> sh;
    std::istringstream ss(message);
    IArchive ia(ss, boost::archive::no_header);
    T* toAllocate;
    sh::decodeTypeAndObject(ia, toAllocate);
    dest.reset(toAllocate);
}


template<>
void decode(const std::string& message, std::unique_ptr<AnyType>& dest);


template<class T>
std::string encode(const T& src) {
    typedef _SerialHelper<T> sh;
    std::ostringstream ss;
    OArchive oa(ss, boost::archive::no_header);
    sh::encodeTypeAndObject(oa, src);
    return ss.str();
}


template<class T>
std::string encode(const std::unique_ptr<T>& src) {
    return encode(src.get());
}


template<class T>
std::string encode(const std::shared_ptr<T>& src) {
    return encode(src.get());
}


/** Lazy person's copy functionality */
template<class T>
void copy(const T& src, std::unique_ptr<T>& dest) {
    decode(encode(&src), dest);
}

template<class T>
void copy(const std::unique_ptr<T>& src, std::unique_ptr<T>& dest) {
    decode(encode(src.get()), dest);
}

}
}//job_stream


namespace boost {
namespace serialization {

/** Boost namespace capability to encode / decode std::unique_ptr */
template<class T>
void serialize(job_stream::serialization::OArchive& a, std::unique_ptr<T>& ptr,
        const unsigned int version) {
    std::string encoded = job_stream::serialization::encode(ptr.get());
    a << encoded;
}


/** Boost namespace capability to encode / decode std::unique_ptr */
template<class T>
void serialize(job_stream::serialization::IArchive& a, std::unique_ptr<T>& ptr, 
        const unsigned int version) {
    std::string decoded;
    a >> decoded;
    job_stream::serialization::decode(decoded, ptr);
}


/** Boost namespace capability to encode / decode std::shared_ptr */
template<class T>
void serialize(job_stream::serialization::OArchive& a, std::shared_ptr<T>& ptr,
        const unsigned int version) {
    std::string encoded = job_stream::serialization::encode(ptr.get());
    a << encoded;
}


/** Boost namespace capability to encode / decode std::shared_ptr */
template<class T>
void serialize(job_stream::serialization::IArchive& a, std::shared_ptr<T>& ptr,
        const unsigned int version) {
    std::string decoded;
    a >> decoded;
    std::unique_ptr<T> uptr;
    job_stream::serialization::decode(decoded, uptr);
    ptr.reset(uptr.release());
}

} //serialization
} //boost

#endif//JOB_STREAM_SERIALIZATION_H_
