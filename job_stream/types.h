#pragma once

#include <mutex>

namespace job_stream {
    typedef std::recursive_mutex Mutex;
    typedef std::lock_guard<Mutex> Lock;
}
