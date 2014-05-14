
#include "death_handler/death_handler.h"
#include "job_stream.h"
#include "processor.h"
#include "yaml.h"

#include <boost/algorithm/string.hpp>
#include <boost/mpi.hpp>
#include <boost/thread.hpp>
#include <cstdio>
#include <exception>

namespace mpi = boost::mpi;

namespace job_stream {


void runProcessor(int argc, char** argv) {
    Debug::DeathHandler dh;
    dh.set_color_output(false);

    std::unique_ptr<mpi::environment> env(new mpi::environment(argc, argv));
    mpi::communicator world;

    if (argc < 2) {
        std::ostringstream ss;
        ss << "Usage: " << argv[0] 
                << " path/to/config [-c checkpointFile] [seed line; if omitted, stdin]";
        printf("%s\n\n", ss.str().c_str());
        printf("If -c is specified and the file exists, stdin will be "
                "ignored.\n");
        exit(-1);
    }
    std::string configPath = argv[1];
    YAML::Node config = YAML::LoadFile(configPath);

    int inputStart = 2;
    std::string checkpoint;
    for (; inputStart < argc; inputStart++) {
        if (strcmp(argv[inputStart], "-c") == 0) {
            checkpoint = std::string(argv[inputStart + 1]);
            if (world.rank() == 0) {
                fprintf(stderr, "Using %s as checkpoint file\n",
                        checkpoint.c_str());
            }
            inputStart++;
        }
        else if (argv[inputStart][0] == '-'
                //Cheap hack to allow negative numbers
                && (argv[inputStart][1] < '0' || argv[inputStart][1] > '9')) {
            std::ostringstream ss;
            ss << "Unrecognized flag: " << argv[inputStart];
            throw std::runtime_error(ss.str());
        }
        else {
            //Unrecognized input that's not a flag; use as input line
            break;
        }
    }

    std::string inputLine;
    for (int i = inputStart; i < argc; i++) {
        if (i != inputStart) {
            inputLine += " ";
        }
        inputLine += argv[i];
    }
    boost::algorithm::trim(inputLine);

    processor::Processor p(std::move(env), world, config, checkpoint);
    p.run(inputLine);

    //If we get here, there were no errors.  If there were no errors, we should
    //delete the checkpoint.

    std::remove(checkpoint.c_str());
}

} //job_stream
