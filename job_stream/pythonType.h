/** Header for the SerializedPython type so that it is integrated into decoding
    objects. */
#pragma once

#include <boost/serialization/serialization.hpp>

#include <iostream>

namespace job_stream {
namespace python {

/** Python data stored as a pickle string */
struct SerializedPython {
    std::string data;

    SerializedPython() {}
    explicit SerializedPython(std::string src) : data(std::move(src)) {}
    SerializedPython(SerializedPython&& other) : data(std::move(other.data)) {}

    SerializedPython& operator=(const SerializedPython& rhs) {
        this->data = rhs.data;
        return *this;
    }

    SerializedPython& operator=(SerializedPython&& rhs) {
        this->data = std::move(rhs.data);
        return *this;
    }

    /** Implemented in _job_stream.cpp.  Takes a string and turns it into a
        python pickled string. */
    friend std::istream& operator>>(std::istream& is,
            SerializedPython& sp);

    /** Print out the repr of the stored python object. */
    friend std::ostream& operator<<(std::ostream& os,
            const SerializedPython& sp);

private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & this->data;
    }
};

} //python
} //job_stream
