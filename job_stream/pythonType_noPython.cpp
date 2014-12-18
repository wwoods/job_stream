/** Used in job_stream static library (not python module) compilation to
    populate the SerializedPython methods without needing python. */

#include "pythonType.h"
#include "debug_internals.h"

namespace job_stream {
namespace python {

std::istream& operator>>(std::istream& is,
        SerializedPython& sp) {
    ERROR("SerializedPython without python support!");
}

/** Print out the repr of the stored python object. */
std::ostream& operator<<(std::ostream& os,
        const SerializedPython& sp) {
    ERROR("SerializedPython without python support!");
}

} //python
} //job_stream
