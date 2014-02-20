#include "serialization.h"

namespace job_stream {
namespace serialization {

std::list<std::unique_ptr<RegisteredTypeBase>> registeredTypes;

void printRegisteredTypes() {
    printf("Printing job_stream::serialization::registeredTypes in order of "
            "resolution\n");
    for (auto it = registeredTypes.begin(); it != registeredTypes.end(); it++) {
        printf("%s from %s\n", (*it)->typeName(), (*it)->baseName());
    }
}


template<>
void decode(const std::string& message, std::unique_ptr<AnyType>& dest) {
    dest.reset(new AnyType(message));
}

} //serialization
} //job_stream

