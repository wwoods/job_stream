#ifndef JOB_STREAM_SERIALIZATION_H_
#define JOB_STREAM_SERIALIZATION_H_

//Guard against boost default serializations, which do not support unique_ptr
//correctly.

#ifdef BOOST_SERIALIZATION_VECTOR_HPP
#error Include job_stream serialization before boost/serialization/vector.hpp
#endif
#define BOOST_SERIALIZATION_VECTOR_HPP
#ifdef BOOST_SERIALIZATION_LIST_HPP
#error Include job_stream serialization before boost/serialization/list.hpp
#endif
#define BOOST_SERIALIZATION_LIST_HPP
#ifdef BOOST_SERIALIZATION_MAP_HPP
#error Include job_stream serialization before boost/serialization/map.hpp
#endif
#define BOOST_SERIALIZATION_MAP_HPP

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/mpl/equal.hpp>
#include <boost/mpl/if.hpp>
#include <boost/mpl/not.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/extended_type_info_no_rtti.hpp>
#include <boost/serialization/level.hpp>
#include <boost/serialization/serialization.hpp>
//Bad unique_ptr support: #include <boost/serialization/list.hpp>
//Bad unique_ptr support: #include <boost/serialization/map.hpp>
//Bad unique_ptr support: #include <boost/serialization/vector.hpp>
#include <boost/static_assert.hpp>
#include <boost/type_traits/add_pointer.hpp>
#include <boost/type_traits/is_polymorphic.hpp>
#include <boost/type_traits/remove_const.hpp>
#include <boost/type_traits/remove_pointer.hpp>
#include <boost/utility/enable_if.hpp>
#include <exception>
#include <list>
#include <map>
#include <sstream>
#include <string>
#include <unordered_set>

/** Forward declared typedefs */
namespace job_stream {
namespace serialization {

typedef boost::archive::binary_oarchive OArchive;
typedef boost::archive::binary_iarchive IArchive;

} //serialization
} //job_stream

namespace boost {
namespace serialization {

/** Since job_stream supports smarter (type checked) serialization of pointers
    with polymorphic classes than boost... here's to it */
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

/** job_stream map encoding supports maps of unique_ptr */
template<class Archive, class U, class Allocator>
inline void serialize(
    Archive & ar,
    std::list<U, Allocator> & t,
    const unsigned int file_version
){
    boost::serialization::split_free(ar, t, file_version);
}

/** job_stream map encoding supports maps of unique_ptr */
template<class Archive, class Type, class Key, class Compare, class Allocator >
inline void serialize(
    Archive & ar,
    std::map<Key, Type, Compare, Allocator> &t,
    const unsigned int file_version
){
    boost::serialization::split_free(ar, t, file_version);
}

/** job_stream map encoding supports maps of unique_ptr */
template<class Archive, class U, class Allocator>
inline void serialize(
    Archive & ar,
    std::vector<U, Allocator> & t,
    const unsigned int file_version
){
    boost::serialization::split_free(ar, t, file_version);
}

} //serialization
} //boost

/** Helper functions for job_stream serialization */
namespace job_stream {
namespace serialization {

/** Public encode-to-string method; dest can be non-pointer, unique_ptr, or
    shared_ptr. Raw pointers are not allowed to discourage memory leaks. */
template<typename T>
void decode(const std::string& message, T& dest);
/** Public encode-to-string method; src can be anything, but for decoding,
    it's best to use non-pointer, unique_ptr, or shared_ptr.  Raw pointers are
    not allowed to discourage memory leaks. */
template<typename T>
std::string encode(const T& src);
/** Lets you encode a pointer directly to a string.  Note that this is not
    recommended, as decode is asymmetric - you MUST decode into a unique_ptr or
    shared_ptr.  However, this is needed when you have an object that will be
    decoded into a pointer.  */
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type = 0>
std::string encodeAsPtr(const T& src);

/* Encode to archive */
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type = 0>
void decode(IArchive& a, T& dest);
template<typename T, int = 0>
void decode(IArchive& a, std::unique_ptr<T>& dest);
template<typename T, int = 0>
void decode(IArchive& a, std::shared_ptr<T>& dest);
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type = 0>
void encode(OArchive& a, const T& src);
template<typename T, int = 0>
void encode(OArchive& a, const std::unique_ptr<T>& src);
template<typename T, int = 0>
void encode(OArchive& a, const std::shared_ptr<T>& src);
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type = 0>
void encodeAsPtr(OArchive& a, const T& src);

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
void copy(const T& src, T& dest);

/** Base class for _SharedPtrs */
struct _SharedPtrsBase {
    virtual void clear() = 0;
};

/** Count of currently active decode() methods, so that we do not wipe the
    table of decoded pointers when we still have an archive open. */
extern thread_local int _activeDecodeCount;
/** Currently used _SharedPtr classes that need to be cleared when we no longer
    have an archive open; I'm using gcc 4.8, while the fix for extern
    thread_local is in 4.9... */
struct _activeSharedPtrs {
    /** 4.8 workaround... */
    static std::unordered_set<_SharedPtrsBase*>& set() {
        static thread_local std::unordered_set<_SharedPtrsBase*> val;
        return val;
    }
};

inline void _activeDecodeIncr() {
    _activeDecodeCount += 1;
}

inline void _activeDecodeDecr() {
    _activeDecodeCount -= 1;
    if (_activeDecodeCount < 0) {
        throw std::runtime_error("_activeDecodeCount decr'd below 0?");
    }
    if (_activeDecodeCount == 0) {
        for (auto* ptr : _activeSharedPtrs::set()) {
            ptr->clear();
        }
        _activeSharedPtrs::set().clear();
    }
}

/** shared_ptr tracking on a per-thread basis. */
template<class T>
struct _SharedPtrs : _SharedPtrsBase {
    /** Our instance */
    static _SharedPtrs<T> instance;

    _SharedPtrs() {
        this->_decoded.reset(new std::map<T*, std::shared_ptr<T>>());
    }

    /** Given a boost-decoded pptr, put it in our map and return the
        corresponding shared_ptr. */
    inline std::shared_ptr<T> decoded(T* pptr) {
        _activeSharedPtrs::set().insert(this);
        if (this->_decoded->count(pptr) == 0) {
            this->_decoded->emplace(pptr, std::shared_ptr<T>(pptr));
        }
        return (*this->_decoded)[pptr];
    }

    void clear() {
        //Free up OUR shared_ptr, leaving responsibility with application
        this->_decoded->clear();
    }

private:
    /** Map of pointer (boost managed) to shared_ptr (job_stream managed); due
        to static initialization order, this is a unique_ptr which is
        initialized in our constructor. */
    static thread_local std::unique_ptr<std::map<T*, std::shared_ptr<T>>> _decoded;
};

template<class T>
_SharedPtrs<T> _SharedPtrs<T>::instance;
template<class T>
thread_local std::unique_ptr<std::map<T*, std::shared_ptr<T>>> _SharedPtrs<T>::_decoded;


/** A special serializable type that will preserve whatever the original type
    was. */
class AnyType {
public:
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
    virtual const char* typeNameRaw() = 0;
    virtual const char* baseNameRaw() = 0;
    virtual bool tryDecode(IArchive& a, const std::string& archTypeName,
            const char* destTypeName, void** dest) = 0;
    virtual bool tryDecodeRaw(IArchive& a, const std::string& archTypeName,
            const char* destTypeName, void* dest) = 0;
    virtual bool tryEncode(OArchive& a, const char* srcTypeName, void* src) = 0;
    virtual bool tryEncodeRaw(OArchive& a, const char* srcTypeName,
            void* src) = 0;

    template<class U>
    bool isBaseClass() {
        return strcmp(typeid(U*).name(), baseName()) == 0;
    }

    template<class U>
    bool isDerivedClass() {
        return strcmp(typeid(U*).name(), typeName()) == 0;
    }
};

/** Returns list of registered types; put as a static in a function so that
    instantiation order supports self-registering jobs and reducers. */
std::list<std::unique_ptr<RegisteredTypeBase>>& registeredTypes();



/** Relates a derivative type T to its base class B */
template<class T, class B>
class RegisteredType : public RegisteredTypeBase {
public:
    const char* baseName() { return typeid(B*).name(); }
    const char* baseNameRaw() { return typeid(B).name(); }
    const char* typeName() { return typeid(T*).name(); }
    const char* typeNameRaw() { return typeid(T).name(); }

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

    bool tryDecodeRaw(IArchive& a, const std::string& archTypeName,
            const char* destTypeName, void* dest) {
        if ((strcmp(destTypeName, typeNameRaw()) != 0
                && strcmp(destTypeName, baseNameRaw()) != 0)
                || archTypeName != typeNameRaw()) {
            return false;
        }
        a >> *(T*)dest;
        return true;
    }

    bool tryEncode(OArchive& a, const char* srcTypeName, void* src) {
        bool isNull = (src == 0);
        if (strcmp(srcTypeName, typeName()) != 0
                && (strcmp(srcTypeName, baseName()) != 0
                    || (!isNull && dynamic_cast<T*>((B*)src) == 0))) {
            return false;
        }

        std::string myType = typeName();
        a << myType;
        T* srcT = (T*)src;
        a << srcT;
        return true;
    }

    bool tryEncodeRaw(OArchive& a, const char* srcTypeName, void* src) {
        bool isNull = (src == 0);
        if (strcmp(srcTypeName, typeNameRaw()) != 0
                && (strcmp(srcTypeName, baseNameRaw()) != 0
                    || (!isNull && dynamic_cast<T*>((B*)src) == 0))) {
            return false;
        }

        std::string myType = typeNameRaw();
        a << myType;
        T& srcT = *(T*)src;
        a << srcT;
        return true;
    }
};



template<class T, class Base>
class _RegisterType_impl {
public:
    static void doRegister() {
        std::unique_ptr<RegisteredTypeBase> ptr(new RegisteredType<T, Base>());
        auto& registered = registeredTypes();
        auto it = registered.begin();
        //Prevent double registration
        for (; it != registered.end(); it++) {
            if ((*it)->isDerivedClass<T>() && (*it)->isBaseClass<Base>()) {
                return;
            }
        }
        //Register with boost (this is really hard to find in the docs, btw)
        boost::serialization::void_cast_register<T, Base>();

        //Register internally
        it = registered.begin();
        for (; it != registered.end(); it++) {
            /** Since we go through these in order, it is important that the
                most-derived classes are encountered first.  Otherwise, encode()
                will find a base class before a potential better match, and
                since dynamic_cast will return non-zero, encoding will stop. */
            if ((*it)->isDerivedClass<T>()) {
                //put with our people
                registered.insert(it, std::move(ptr));
                return;
            }
            else if ((*it)->isDerivedClass<Base>()) {
                //We derive from this class, so go above its block, and move all
                //of our other instances above it too
                registered.insert(it, std::move(ptr));
                auto insertIt = it;
                //it now points to one before the row that triggered this if
                //condition, after this decr and the first incr
                --it;
                while (++it != registered.end()) {
                    if ((*it)->isDerivedClass<T>()) {
                        auto toRemove = it++;
                        std::unique_ptr<RegisteredTypeBase> ptr = std::move(
                                *toRemove);
                        registered.erase(toRemove);
                        registered.insert(insertIt, std::move(ptr));
                        --it;
                    }
                }
                return;
            }
        }

        registered.insert(it, std::move(ptr));
    }
};


template<class T>
class _RegisterType_impl<T, void> {
public:
    static void doRegister() {
        //Nothing can derive from void
    }
};


template<class T, class Base1, class Base2, class Base3, class Base4, class Base5,
        class Base6>
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


/** If a serialization attempt to a non-pointer of a polymorphic type happens,
    we complain.  Otherwise, there's a very unhelpful "Unregistered class"
    message at runtime, and no other help. */
template<typename T>
struct _SerialHelper<T, typename boost::enable_if<
        boost::is_polymorphic<T>>::type> {
    /*static_assert(sizeof(T) == 0, "When serializing polymorphic types, "
            "job_stream::serialization requires you to serialize a unique_ptr "
            "or shared_ptr instead of the direct object, since different "
            "types have different memory layouts, which will not be handled "
            "correctly.");*/
    typedef typename boost::mpl::if_<
            boost::is_pointer<T>,
            typename boost::add_pointer<typename boost::remove_const<
                typename boost::remove_pointer<T>::type>::type>::type,
            typename boost::remove_const<T>::type
            >::type T_noconst;
    static const char* typeName() { return typeid(T_noconst).name(); }


    static void encodeTypeAndObject(OArchive& a, T const & obj) {
        //Any polymorphic class MUST be registered.  Otherwise, we can't guarantee
        //type safety, and might end up encoding a base class representation of a non
        //base class.
        for (std::unique_ptr<RegisteredTypeBase>& m : registeredTypes()) {
            if (m->tryEncodeRaw(a, typeName(), (void*)&obj)) {
                return;
            }
        }

        std::ostringstream ss;
        ss << "Failed to encode polymorphic type '" << typeName() << "'.  ";
        ss << "Did you call job_stream::serialization::registerType<Cls, Base>()?";
        throw std::runtime_error(ss.str());
    }

    static void decodeTypeAndObject(IArchive& a, T& obj) {
        std::string objTypeName;
        a >> objTypeName;
        for (std::unique_ptr<RegisteredTypeBase>& m : registeredTypes()) {
            if (m->tryDecodeRaw(a, objTypeName, typeName(), (void*)&obj)) {
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
        for (std::unique_ptr<RegisteredTypeBase>& m : registeredTypes()) {
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
        for (std::unique_ptr<RegisteredTypeBase>& m : registeredTypes()) {
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

/** Encode to string; note that directly encoding a pointer is invalid.  You
    must use encodeAsPtr on a reference instead, and only if you're sure. */
template<typename T>
std::string encode(const T& src) {
    std::ostringstream ss;
    OArchive oa(ss, boost::archive::no_header);
    encode(oa, src);
    return ss.str();
}


/** Decode from string */
template<typename T>
void decode(const std::string& message, T& dest) {
    _activeDecodeIncr();
    std::istringstream ss(message);
    IArchive ia(ss, boost::archive::no_header);
    decode(ia, dest);
    _activeDecodeDecr();
}


/** Encode reference to a string, but encode it as a pointer.  Useful when
    you'll be decoding it to a unique_ptr or shared_ptr, as in a networking
    protocol.  */
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type>
std::string encodeAsPtr(const T& src) {
    std::ostringstream ss;
    OArchive oa(ss, boost::archive::no_header);
    encodeAsPtr(oa, src);
    return ss.str();
}


template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type>
void encodeAsPtr(OArchive& a, const T& src) {
    _SerialHelper<T*>::encodeTypeAndObject(a, const_cast<T* const>(&src));
}


/** Decode a previously encoded ANYTHING into AnyType. */
template<>
void decode(const std::string& message, std::unique_ptr<AnyType>& dest);


/** Non-pointer decode mechanism.  Avoid memory leaks by explicitly disallowing
    decoding to a raw pointer type. */
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type>
void decode(IArchive& ia, T& dest) {
    _activeDecodeIncr();
    _SerialHelper<T>::decodeTypeAndObject(ia, dest);
    _activeDecodeDecr();
}


/** Decode std::unique_ptr */
template<typename T, int>
void decode(IArchive& ia, std::unique_ptr<T>& dest) {
    _activeDecodeIncr();
    T* toAllocate;
    _SerialHelper<T*>::decodeTypeAndObject(ia, toAllocate);
    dest.reset(toAllocate);
    _activeDecodeDecr();
}


/** Decode std::shared_ptr */
template<typename T, int>
void decode(IArchive& ia, std::shared_ptr<T>& dest) {
    _activeDecodeIncr();
    //IF THIS ASSERTION FAILS - you must wrap your shared type in a struct.
    //Boost does not track primitives by default; we fix normal encoding and
    //decoding, but cannot fix it for shared_ptr.
    BOOST_STATIC_ASSERT(boost::serialization::tracking_level< T >::value
            != boost::serialization::track_never);

    T* pptr;
    _SerialHelper<T*>::decodeTypeAndObject(ia, pptr);
    dest = _SharedPtrs<T>::instance.decoded(pptr);
    _activeDecodeDecr();
}


/** Encode anything that isn't a raw pointer. */
template<typename T, typename boost::enable_if<boost::mpl::not_<
        boost::is_pointer<T>>, int>::type>
void encode(OArchive& oa, const T& src) {
    _SerialHelper<T>::encodeTypeAndObject(oa, src);
}


template<typename T, int>
void encode(OArchive& oa, const std::unique_ptr<T>& src) {
    _SerialHelper<T*>::encodeTypeAndObject(oa, src.get());
}


template<typename T, int>
void encode(OArchive& oa, const std::shared_ptr<T>& src) {
    //Save the object as-is; multiple shared_ptrs pointing to the same object
    //will be archived as the same object.  We'll resolve this when we load
    //them.

    //IF THIS ASSERTION FAILS - you must wrap your shared type in a struct.
    //Boost does not track primitives by default; we fix normal encoding and
    //decoding, but cannot fix it for shared_ptr.
    BOOST_STATIC_ASSERT(boost::serialization::tracking_level< T >::value
            != boost::serialization::track_never);

    _SerialHelper<T*>::encodeTypeAndObject(oa, src.get());
}


/** Lazy person's copy functionality */
template<class T>
void copy(const T& src, T& dest) {
    decode(encode(src), dest);
}

}
}//job_stream


namespace boost {
namespace serialization {

/** Boost namespace capability to encode / decode std::unique_ptr */
template<class T>
void serialize(job_stream::serialization::OArchive& a, std::unique_ptr<T>& ptr,
        const unsigned int version) {
    job_stream::serialization::encode(a, ptr);
}


/** Boost namespace capability to encode / decode std::unique_ptr */
template<class T>
void serialize(job_stream::serialization::IArchive& a, std::unique_ptr<T>& ptr,
        const unsigned int version) {
    job_stream::serialization::decode(a, ptr);
}


/** Boost namespace capability to encode / decode std::shared_ptr */
template<class T>
void serialize(job_stream::serialization::OArchive& a, std::shared_ptr<T>& ptr,
        const unsigned int version) {
    job_stream::serialization::encode(a, ptr);
}

/** Boost namespace capability to encode / decode std::shared_ptr */
template<class T>
void serialize(job_stream::serialization::IArchive& a, std::shared_ptr<T>& ptr,
        const unsigned int version) {
    job_stream::serialization::decode(a, ptr);
}


/** NOTE ABOUT WHY THIS USE job_stream::serialization::encode:
    bool& has no templated serialize, bool does.  job_stream automatically
    decodes all references as value types, which is what we want for these
    STL containers. */

template<class Archive, class U, class Allocator>
inline void save(
    Archive & ar,
    const std::list<U, Allocator> & t,
    const unsigned int file_version
){
    size_t sz = t.size();
    ar & sz;
    for (auto it = t.begin(); it != t.end(); it++) {
        job_stream::serialization::encode(ar, *it);
    }
}


template<class Archive, class U, class Allocator>
inline void load(
    Archive & ar,
    std::list<U, Allocator> & t,
    const unsigned int file_version
){
    size_t sz;
    ar & sz;
    //Ensure we're working with all default instances
    t.clear();
    t.resize(sz);

    for (auto it = t.begin(); it != t.end(); it++) {
        job_stream::serialization::decode(ar, *it);
    }
}


template<class Archive, class Type, class Key, class Compare, class Allocator >
inline void save(
    Archive & ar,
    const std::map<Key, Type, Compare, Allocator> &t,
    const unsigned int /* file_version */
){
    size_t sz = t.size();
    ar & sz;

    for (auto it = t.begin(); it != t.end(); it++) {
        job_stream::serialization::encode(ar, it->first);
        job_stream::serialization::encode(ar, it->second);
    }
}

template<class Archive, class Type, class Key, class Compare, class Allocator >
inline void load(
    Archive & ar,
    std::map<Key, Type, Compare, Allocator> &t,
    const unsigned int /* file_version */
){
    size_t sz;
    ar & sz;
    t.clear();

    Key k;

    for (size_t i = 0; i < sz; i++) {
        job_stream::serialization::decode(ar, k);
        job_stream::serialization::decode(ar, t[k]);
    }
}


template<class Archive, class U, class Allocator>
inline void save(
    Archive & ar,
    const std::vector<U, Allocator> & t,
    const unsigned int file_version
){
    size_t sz = t.size();
    ar & sz;
    for (size_t i = 0; i < sz; i++) {
        job_stream::serialization::encode(ar, t[i]);
    }
}


template<class Archive, class U, class Allocator>
inline void load(
    Archive & ar,
    std::vector<U, Allocator> & t,
    const unsigned int file_version
){
    size_t sz;
    ar & sz;
    //Ensure we get default objects
    t.clear();
    t.resize(sz);

    for (size_t i = 0; i < sz; i++) {
        //Weird cast needed for e.g. bool, which has its own _Bit_reference
        //reference type by default.
        job_stream::serialization::decode(ar, t[i]);
    }
}


/** bool has its own special override for vector. */
template<class Archive, class Allocator>
inline void save(Archive& ar, const std::vector<bool, Allocator>& t,
        const unsigned int version) {
    size_t sz = t.size();
    ar & sz;

    bool v;
    for (size_t i = 0; i < sz; i++) {
        //Weird cast needed for e.g. bool, which has its own _Bit_reference
        //reference type by default.
        v = t[i];
        job_stream::serialization::encode(ar, v);
    }
}


/** bool has its own special override for vector. */
template<class Archive, class Allocator>
inline void load(Archive& ar, std::vector<bool, Allocator>& t,
        const unsigned int version) {
    size_t sz;
    ar & sz;
    //Ensure we get default objects
    t.clear();
    t.resize(sz);

    bool v;
    for (size_t i = 0; i < sz; i++) {
        //Weird cast needed for e.g. bool, which has its own _Bit_reference
        //reference type by default.
        job_stream::serialization::decode(ar, v);
        t[i] = v;
    }
}

} //serialization
} //boost

#endif//JOB_STREAM_SERIALIZATION_H_
