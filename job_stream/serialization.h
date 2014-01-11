#ifndef JOB_STREAM_SERIALIZATION_H_
#define JOB_STREAM_SERIALIZATION_H_

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/extended_type_info_no_rtti.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/vector.hpp>
#include <exception>
#include <sstream>
#include <string>

/** Helper functions for job_stream serialization */

namespace job_stream {
namespace serialization {

template<class T>
void decode(const std::string& message, T& dest) {
    std::istringstream ss(message);
    boost::archive::text_iarchive ia(ss);
    std::string typeName;
    ia >> typeName;
    std::string tName = typeid(T).name();
    //It's amazing that boost doesn't do this for you.
    if (typeName != tName) {
        std::ostringstream oss;
        oss << "Decoded type '" << typeName << "'; expected '" << tName << "'";
        throw std::runtime_error(oss.str());
    }
    ia >> dest;
}


template<class T>
std::string encode(const T& src) {
    std::ostringstream ss;
    boost::archive::text_oarchive oa(ss);
    std::string typeName = typeid(T).name();
    oa << typeName;
    oa << src;
    return ss.str();
}

}
}//job_stream

#endif//JOB_STREAM_SERIALIZATION_H_
