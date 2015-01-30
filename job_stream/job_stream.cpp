
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


void _startMpi() {
    if (!mpiEnvHolder) {
        //Important that this happen before MPI init!!!
        job_stream::invoke::_init();
        mpiEnvHolder.reset(new mpi::environment());
    }
}


std::string checkpointInfo(const std::string& path) {
    std::ifstream cf(path);
    if (!cf) {
        throw std::runtime_error("Could not open specified checkpoint file");
    }

    //Start a processor to decode into
    _startMpi();
    mpi::communicator world;
    processor::Processor p(mpiEnvHolder, world,
            "__isCheckpointProcessorOnly: true", "");

    std::ostringstream result;

    serialization::IArchive ar(cf);
    int wsize;
    serialization::decode(ar, wsize);
    result << wsize << " processors\n";

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

        result << "== " << j << " ==\n";
        result << procInfo[j].totalBytes << " total bytes\n";
        result << procInfo[j].messagesWaiting << " messages pending\n";
        uint64_t messagesTotal = 0;
        for (int k = 0, km = processor::Processor::TAG_COUNT; k < km; k++) {
            messagesTotal += procInfo[j].bytesByTag[k];
        }
        result << "\n-- Initial breakdown --\nMessages (" << messagesTotal << " total bytes):\n";
        for (int k = 0, km = processor::Processor::TAG_COUNT; k < km; k++) {
            result << procInfo[j].countByTag[k] << " - "
                    << procInfo[j].bytesByTag[k] << " bytes\n";
        }
        result << "\nJob tree: " << procInfo[j].jobTreeSize << " bytes\n";
        result << "Reduce map: " << procInfo[j].reduceMapSize << " bytes\n";
        result << "\n-- Work Messages --\n";
        result << procInfo[j].totalUserBytes << " bytes of user data\n";

        result << "\n-- Steal ring? --\n";
        if (procInfo[j].stealRing) {
            result << "Capacity, slots, work, load\n";
            auto& sr = *procInfo[j].stealRing;
            for (int k = 0; k < wsize; k++) {
                result << std::setw(8) << sr.capacity[k] << ", "
                        << std::setw(5) << sr.slots[k] << ", "
                        << std::setw(4) << sr.work[k] << ", "
                        << std::setw(4) << sr.load[k] << "\n";
            }
        }
        else {
            result << "no\n";
        }

        result << "\n\n";
    }

    return result.str();
}


int getRank() {
    _startMpi();
    mpi::communicator world;
    return world.rank();
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
            printf("%s\n", checkpointInfo(argv[inputStart + 1]).c_str());
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
    _startMpi();
    mpi::communicator world;

    processor::Processor p(mpiEnvHolder, world, config, checkpoint);
    p.setCheckpointInterval(checkpointMs);
    p.setStealEnabled(!stealOff);
    p.run(inputLine);

    //If we get here, there were no errors.  If there were no errors, we should
    //delete the checkpoint.

    std::remove(checkpoint.c_str());
}


void runProcessor(const SystemArguments& args) {
    //Fire up MPI and launch
    _startMpi();
    mpi::communicator world;

    processor::Processor p(mpiEnvHolder, world, args.config,
            args.checkpointFile);
    p.checkExternalSignals = args.checkExternalSignals;
    p.handleOutputCallback = args.handleOutputCallback;
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
