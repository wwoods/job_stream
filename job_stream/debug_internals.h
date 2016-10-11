#ifndef JOB_STREAM_DEBUG_INTERNALS_H_
#define JOB_STREAM_DEBUG_INTERNALS_H_

#include <stdexcept>
#include <sstream>

#if defined(__linux__) || defined(unix) || defined(__unix__)
    #define OS_LINUX 1
#elif defined(__APPLE__)
    #define OS_APPLE 1
#elif defined(_WIN32) || defined(_WIN64)
    #define OS_WINDOWS 1
#else
    #error "NO OS DETECTED"
#endif

/** Allows ERROR("Path not found: " << path) type throwing of errors. */
#define ERROR(msg) { std::ostringstream ss; ss << __FILE__ << ":" << __LINE__ \
        << ":" << __FUNCTION__ << ": " << msg; \
        throw std::runtime_error(ss.str()); }
/** Allows ASSERT(cond, "AHHH" << path) type throwing of errors. */
#define ASSERT(cond, msg) if (!(cond)) ERROR(msg)

#endif//JOB_STREAM_DEBUG_INTERNALS_H_
