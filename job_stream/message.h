
#ifndef JOB_STREAM_MESSAGE_H_
#define JOB_STREAM_MESSAGE_H_

#include "serialization.h"
#include <string>
#include <vector>

namespace job_stream {
namespace message {
    /** Record of location
        Work flow:  generation -> tsSent -> tsReceived -> tsWorkStart
      */
    class Location {
    public:
        /* Original location */
        std::string hostname;

        /* Target job */
        std::vector<std::string> target;

        /* Timestamp (ms since epoch) received from this location; 0 for creation
           location. */
        uint64_t tsReceived;

        /* Timestamp (ms since epoch) queued up to be sent from this location */
        uint64_t tsSent;

        /* Timestamp (ms since epoch) work started from this location (not at 
           this location, necessarily) */
        uint64_t tsWorkStart;

        /** Returns current time since epoch, in ms */
        static uint64_t getCurrentTimeMs();

        Location() : hostname("unknown host"), tsReceived(0), tsSent(0),
                tsWorkStart(0) {}

        Location(const Location& other) : hostname(other.hostname),
                tsReceived(other.tsReceived), tsSent(other.tsSent),
                tsWorkStart(other.tsWorkStart) {}

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->hostname;
            ar & this->target;
            ar & this->tsSent;
            ar & this->tsReceived;
            ar & this->tsWorkStart;
        }
    };


    /** Record of work to be done / in progress */
    class WorkRecord {
    public:
        static const int TAG_ADDRESS_BITS = 16;

        /* Restores a WorkRecord that has been serialized(), filling in 
           tsReceived on source */
        WorkRecord(const std::string& serialized);

        /** Instantiates a new _WorkRecord with source of localhost. */
        WorkRecord(const std::vector<std::string>& target, 
                const std::string& work);

        /* Fill in this WorkRecord as though it is a continuation of wr */
        void chainFrom(const WorkRecord& wr);

        int getReduceHomeRank() const { return this->reduceHomeRank; }
        uint64_t getReduceTag() const { return this->reduceTag; }
        const std::vector<std::string>& getTarget() const { 
                return this->source.target; }
        const std::string& getWork() const { return this->work; }

        /** Get work as a printable string; used for output. */
        std::string getWorkAsString() const;

        void markStarted() {
            this->source.tsWorkStart = Location::getCurrentTimeMs();
        }


        /* Called when our target is a module; redirects us to first job. */
        void redirectTo(const std::string& id) {
            this->source.target.push_back(id);
        }


        std::string serialized() const {
            return serialization::encode(*this);
        }


        void setReduce(int rank, uint64_t tag) {
            this->reduceHomeRank = rank;
            this->reduceTag = tag;
        }


        template<class T> void putWorkInto(T& dest) const {
            serialization::decode(this->work, dest);
        }


    protected:
        Location source;
        std::vector<Location> route;

        /** Rank of client that will receive output from this message.  That 
            client has the original WorkRecord that started the reduction.  All
            results need to end up there.  By default, all output goes to 0. */
        int reduceHomeRank;
        /** The tag assigned to this WorkRecord's current reduction */
        uint64_t reduceTag;

        std::string work;

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->source;
            ar & this->reduceHomeRank;
            ar & this->reduceTag;
            ar & this->route;
            ar & this->work;
        }
    };



    class DeadRingTestMessage {
    public:
        uint64_t allWork;
        int pass;
        uint64_t passWork;
        uint64_t reduceTag;
        int sentryRank;
        uint64_t tsTestStarted;

        DeadRingTestMessage(const std::string& serialized) {
            serialization::decode(serialized, *this);
        }


        DeadRingTestMessage(int rank, uint64_t reduceTag, uint64_t workCount)
                : sentryRank(rank), pass(0), allWork(0), passWork(workCount), 
                    reduceTag(reduceTag),  
                    tsTestStarted(Location::getCurrentTimeMs()) {
        }


        std::string serialized() {
            return serialization::encode(*this);
        }

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->sentryRank;
            ar & this->reduceTag;
            ar & this->pass;
            ar & this->allWork;
            ar & this->passWork;
            ar & this->tsTestStarted;
        }
    };
}
}

#endif//JOB_STREAM_MESSAGE_H_