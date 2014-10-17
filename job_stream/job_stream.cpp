
#include "death_handler/death_handler.h"
#include "invoke.h"
#include "job_stream.h"
#include "processor.h"
#include "yaml.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/mpi.hpp>
#include <boost/thread.hpp>
#include <cstdio>
#include <exception>

namespace mpi = boost::mpi;

namespace job_stream {


void runProcessor(int argc, char** argv) {
    Debug::DeathHandler dh;
    dh.set_color_output(false);

    //Allow spawning (important that this happens before MPI initialization)
    job_stream::invoke::_init();

    if (argc < 2) {
        std::ostringstream ss;
        ss << "Usage: " << argv[0]
                << " path/to/config [flags] [seed line; if omitted, stdin]";
        printf("%s\n\n\
Flags:\n\
    -c filepath : File to use for checkpoints.  If file exists, seed will be \n\
            ignored and the system will resume from checkpoint.  Otherwise,\n\
            seed will be used and file will be created at checkpoint time.\n\
    -t number : Hours between checkpoints.  Default is 10 minutes.\n\
    --check-sync number : Additional milliseconds to wait during checkpoints.\n\
            Primarily used in tests.  Default 10000.\n", ss.str().c_str());
        exit(-1);
    }

    bool configLoaded = false;
    YAML::Node config;

    int inputStart = 1;
    std::string checkpoint;
    int checkpointMs = 600 * 1000;
    for (; inputStart < argc; inputStart++) {
        if (strcmp(argv[inputStart], "-c") == 0) {
            checkpoint = std::string(argv[inputStart + 1]);
            inputStart++;
        }
        else if (strcmp(argv[inputStart], "--check-sync") == 0) {
            processor::Processor::CHECKPOINT_SYNC_WAIT_MS
                    = boost::lexical_cast<int>(argv[inputStart + 1]);
            inputStart++;
        }
        else if (strcmp(argv[inputStart], "-t") == 0) {
            checkpointMs = (int)(
                    boost::lexical_cast<float>(argv[inputStart + 1])
                    * 3600 * 1000);
            inputStart++;
        }
        else if (argv[inputStart][0] == '-'
                //Cheap hack to allow negative numbers
                && (argv[inputStart][1] < '0' || argv[inputStart][1] > '9')) {
            std::ostringstream ss;
            ss << "Unrecognized flag: " << argv[inputStart];
            throw std::runtime_error(ss.str());
        }
        else if (!configLoaded) {
            //We have input that's not a flag, it's our config file
            config = YAML::LoadFile(argv[inputStart]);
            configLoaded = true;
        }
        else {
            //Unrecognized input that's not a flag; use as input line
            break;
        }
    }

    if (!configLoaded) {
        throw std::runtime_error("Config file not specified?");
    }

    std::string inputLine;
    for (int i = inputStart; i < argc; i++) {
        if (i != inputStart) {
            inputLine += " ";
        }
        inputLine += argv[i];
    }
    boost::algorithm::trim(inputLine);

    //Fire up MPI
    std::unique_ptr<mpi::environment> env(new mpi::environment(argc, argv));
    mpi::communicator world;

    if (world.rank() == 0 && !checkpoint.empty()) {
        fprintf(stderr, "Using %s as checkpoint file\n",
                checkpoint.c_str());
    }

    processor::Processor p(std::move(env), world, config, checkpoint);
    p.setCheckpointInterval(checkpointMs);
    p.run(inputLine);

    //If we get here, there were no errors.  If there were no errors, we should
    //delete the checkpoint.

    std::remove(checkpoint.c_str());
}

} //job_stream
