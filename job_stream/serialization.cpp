#include "serialization.h"

namespace job_stream {
namespace serialization {

std::vector<std::unique_ptr<RegisteredTypeBase>> registeredTypes;

template<>
void decode(const std::string& message, std::unique_ptr<AnyType>& dest) {
    dest.reset(new AnyType(message));
}

} //serialization
} //job_stream

