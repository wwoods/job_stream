
#include "death_handler/death_handler.h"
#include "debug_internals.h"
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

/** For e.g. interactive python, we keep the mpi environment alive as long as
    we possibly can.  In other words, when we start a processor, if anything
    goes wrong, we release the environment into this variable before crashing.
    This ensures that MPI_Finalize is not called, letting the interpreter keep
    running to report the error.  Additionally, this lets multiple job_stream
    jobs be run in series from an interactive interpreter. */
std::shared_ptr<mpi::environment> mpiEnvHolder;

void checkpointInfo(const std::string& path, processor::Processor& p) {
    std::ifstream cf(path);
    if (!cf) {
        throw std::runtime_error("Could not open specified checkpoint file");
    }

    serialization::IArchive ar(cf);
    int wsize;
    serialization::decode(ar, wsize);
    printf("%i processors\n", wsize);

    std::map<int, std::string> procData;
    for (int i = 0; i < wsize; i++) {
        int bufRank;
        std::string buffer;
        serialization::decode(ar, bufRank);
        serialization::decode(ar, buffer);
        procData[bufRank] = buffer;
    }

    //Gather data from each processor (and processor -1, which is cumulative
    //results.  Yes, it's inefficient to run the calculation twice.  However,
    //it saves me [the programmer] time).
    std::map<int, processor::CheckpointInfo> procInfo;
    for (int i = 0; i < wsize; i++) {
        p.populateCheckpointInfo(procInfo[i], procData[i]);
        //Yes, it's inefficient to run the calculation twice.  But it's
        //efficient dev time.
        p.populateCheckpointInfo(procInfo[-1], procData[i]);
    }

    for (int i = 0; i <= wsize; i++) {
        int j = i;
        if (j == wsize) {
            //Total
            j = -1;
        }

        printf("== %i ==\n%lu total bytes\n", j, procInfo[j].totalBytes);
        printf("%lu messages pending\n", procInfo[j].messagesWaiting);
        uint64_t messagesTotal = 0;
        for (int k = 0, km = processor::Processor::TAG_COUNT; k < km; k++) {
            messagesTotal += procInfo[j].bytesByTag[k];
        }
        printf("\n-- Initial breakdown --\nMessages (%lu total bytes):\n",
                messagesTotal);
        for (int k = 0, km = processor::Processor::TAG_COUNT; k < km; k++) {
            printf("%i - %lu messages, %lu bytes\n", k,
                    procInfo[j].countByTag[k], procInfo[j].bytesByTag[k]);
        }
        printf("\nJob tree: %lu bytes\nReduce map: %lu bytes\n",
                procInfo[j].jobTreeSize, procInfo[j].reduceMapSize);
        printf("\n-- Work Messages --\n");
        printf("%lu bytes of user data\n", procInfo[j].totalUserBytes);

        printf("\n-- Steal ring? --\n");
        if (procInfo[j].stealRing) {
            printf("Capacity, slots, work, load\n");
            auto& sr = *procInfo[j].stealRing;
            for (int k = 0; k < wsize; k++) {
                printf("%8i, %5i, %4i, %4f\n", sr.capacity[k], sr.slots[k],
                        sr.work[k], sr.load[k]);
            }
        }
        else {
            printf("no\n");
        }

        printf("\n\n");
    }
}


void help(char** argv) {
    std::ostringstream ss;
    ss << "Usage: " << argv[0]
            << " [flags] path/to/config [seed line; if omitted, stdin]";
    printf("%s\n\n\
Flags:\n\
    -c filepath : File to use for checkpoints.  If file exists, seed will be \n\
            ignored and the system will resume from checkpoint.  Otherwise,\n\
            seed will be used and file will be created at checkpoint time.\n\
    -t number : Hours between checkpoints.  Default is 10 minutes.\n\
    --check-sync number : Additional milliseconds to wait during checkpoints.\n\
            Primarily used in tests.  Default 10000.\n\
    --checkpoint-info checkpoint/file : Dumps information about the given \n\
            checkpoint file.\n\
    --disable-steal : Disables stealing as a means for sharing work.  Not\n\
            recommended, mostly here for speed tests.\n\
", ss.str().c_str());
    exit(-1);
}


void runProcessor(int argc, char** argv) {
    Debug::DeathHandler dh;
    dh.set_color_output(false);

    //Allow spawning (important that this happens before MPI initialization)
    job_stream::invoke::_init();

    if (argc < 2) {
        help(argv);
    }

    bool configLoaded = false;
    std::string config;

    int inputStart = 1;
    std::string checkpoint;
    int checkpointMs = 600 * 1000;
    bool stealOff = false;
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
        else if (strcmp(argv[inputStart], "-h") == 0
                || strcmp(argv[inputStart], "--help") == 0) {
            help(argv);
        }
        else if (strcmp(argv[inputStart], "--checkpoint-info") == 0) {
            std::unique_ptr<mpi::environment> env(new mpi::environment(argc,
                    argv));
            mpi::communicator world;
            processor::Processor p(std::move(env), world,
                    "__isCheckpointProcessorOnly: true", "");
            checkpointInfo(argv[inputStart + 1], p);
            exit(0);
        }
        else if (strcmp(argv[inputStart], "--disable-steal") == 0) {
            stealOff = true;
        }
        //END OF VALID FLAGS!!
        else if (argv[inputStart][0] == '-') {
            std::ostringstream ss;
            ss << "Unrecognized flag: " << argv[inputStart];
            throw std::runtime_error(ss.str());
        }
        else {
            //We have input that's not a flag, it's our config file
            std::ifstream confRead(argv[inputStart++]);
            std::stringstream configStream;
            configStream << confRead.rdbuf();
            config = configStream.str();
            configLoaded = true;
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

    processor::Processor p(std::move(env), world, config, checkpoint);
    p.setCheckpointInterval(checkpointMs);
    p.setStealEnabled(!stealOff);
    p.run(inputLine);

    //If we get here, there were no errors.  If there were no errors, we should
    //delete the checkpoint.

    std::remove(checkpoint.c_str());
}


void runProcessor(const SystemArguments& args) {
    //Fire up MPI and launch
    if (!mpiEnvHolder) {
        mpiEnvHolder.reset(new mpi::environment());
    }
    mpi::communicator world;

    processor::Processor p(mpiEnvHolder, world, args.config,
            args.checkpointFile);
    p.checkExternalSignals = args.checkExternalSignals;
    p.setCheckpointInterval(args.checkpointIntervalMs);
    if (args.checkpointSyncIntervalMs >= 0) {
        processor::Processor::CHECKPOINT_SYNC_WAIT_MS
                = args.checkpointSyncIntervalMs;
    }
    p.setStealEnabled(!args.disableSteal);
    p.run(args.inputLine);

    //If we get here, there were no errors, so we should delete the checkpoint
    std::remove(args.checkpointFile.c_str());
}

} //job_stream
