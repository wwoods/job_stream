
#include "job_stream.h"
#include "processor.h"
#include "yaml.h"

#include <boost/algorithm/string.hpp>
#include <boost/mpi.hpp>
#include <boost/thread.hpp>
#include <exception>

namespace mpi = boost::mpi;

namespace job_stream {

void addJob(const std::string& typeName,
        std::function<job::JobBase* ()> allocator) {
    processor::Processor::addJob(typeName, allocator);
}


void addReducer(const std::string& typeName,
        std::function<job::ReducerBase* ()> allocator) {
    processor::Processor::addReducer(typeName, allocator);
}


void runProcessor(int argc, char** argv) {
    std::unique_ptr<mpi::environment> env(new mpi::environment(argc, argv));
    mpi::communicator world;

    if (argc < 2) {
        std::ostringstream ss;
        ss << "Usage: " << argv[0] 
                << " path/to/config [seed line; if omitted, stdin]";
        printf("%s\n\n", ss.str().c_str());
        printf("-f or seed line will only be used if 'input' from config is\n");
        printf("a string; if it is a map, (not implemented)\n");
        exit(-1);
    }
    std::string configPath = argv[1];
    YAML::Node config = YAML::LoadFile(configPath);

    std::string inputLine;
    for (int i = 2; i < argc; i++) {
        if (i != 2) {
            inputLine += " ";
        }
        inputLine += argv[i];
    }
    boost::algorithm::trim(inputLine);

    processor::Processor p(std::move(env), world, config);
    p.run(inputLine);
}

} //job_stream
