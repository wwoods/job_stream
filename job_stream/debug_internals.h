#ifndef JOB_STREAM_DEBUG_INTERNALS_H_
#define JOB_STREAM_DEBUG_INTERNALS_H_

#include <stdexcept>
#include <sstream>

/** Allows ERROR("Path not found: " << path) type throwing of errors. */
#define ERROR(msg) { std::ostringstream ss; ss << __FILE__ << ":" << __LINE__ \
        << ":" << __FUNCTION__ << ": " << msg; \
        throw std::runtime_error(ss.str()); }
/** Allows ASSERT(cond, "AHHH" << path) type throwing of errors. */
#define ASSERT(cond, msg) if (!(cond)) ERROR(msg)

#endif//JOB_STREAM_DEBUG_INTERNALS_H_
