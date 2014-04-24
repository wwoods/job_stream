#include "serialization.h"

namespace job_stream {
namespace serialization {

std::list<std::unique_ptr<RegisteredTypeBase>> registeredTypes;

void clearRegisteredTypes() {
    registeredTypes.clear();
}


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


std::string getDecodedType(const std::string& message) {
    std::istringstream ss(message);
    IArchive ia(ss, boost::archive::no_header);
    std::string type;
    ia >> type;
    return type;
}

} //serialization
} //job_stream

