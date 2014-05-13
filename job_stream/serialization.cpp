#include "serialization.h"

namespace job_stream {
namespace serialization {

std::list<std::unique_ptr<RegisteredTypeBase>>& registeredTypes() {
    static std::list<std::unique_ptr<RegisteredTypeBase>> result;
    return result;
}


void clearRegisteredTypes() {
    registeredTypes().clear();
}


void printRegisteredTypes() {
    printf("Printing job_stream::serialization::registeredTypes in order of "
            "resolution\n");
    auto& registered = registeredTypes();
    for (auto it = registered.begin(); it != registered.end(); it++) {
        printf("%s from %s\n", (*it)->typeName(), (*it)->baseName());
    }
}


template<>
void decode(IArchive& ia, std::unique_ptr<AnyType>& dest) {
    std::string encoded;
    ia >> encoded;
    dest.reset(new AnyType(encoded));
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

