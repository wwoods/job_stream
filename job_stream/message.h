
#ifndef JOB_STREAM_MESSAGE_H_
#define JOB_STREAM_MESSAGE_H_

#include "serialization.h"
#include <string>
#include <vector>

namespace job_stream {
namespace message {
    /** Meta data passed with any message. */
    struct Header {
        /** For boost */
        Header() {}
        Header(Header&& other) : tag(other.tag), dest(other.dest) {}
        Header(int tag, int dest) : tag(tag), dest(dest) {}

        int tag;
        int dest;

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & tag & dest;
        }
    };


    /** Used as actual vessel of send / receive over the network. */
    struct _Message {
        _Message() {}
        _Message(Header header, std::string payload)
                : header(std::move(header)), buffer(std::move(payload)) {}

        Header header;
        std::string buffer;

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & header & buffer;
        }
    };


    /** A group of serialized messages */
    class GroupMessage {
    public:
        GroupMessage() {}

        void add(int tag, std::string&& message) {
            this->messageTags.emplace_back(tag);
            this->messages.emplace_back(message);
        }


        std::string serialized() {
            return serialization::encode(*this);
        }


        std::vector<int> messageTags;
        std::vector<std::string> messages;
    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            int m = messageTags.size();
            ar & m;
            if (Archive::is_saving::value) {
                for (int i = 0; i < m; i++) {
                    ar & this->messageTags[i];
                    ar & this->messages[i];
                }
            }
            else {
                for (int i = 0; i < m; i++) {
                    int tag;
                    std::string msg;

                    ar & tag;
                    ar & msg;

                    this->messageTags.emplace_back(tag);
                    this->messages.emplace_back(std::move(msg));
                }
            }
        }
    };



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
        WorkRecord(const std::vector<std::string>& target,
                void* work);

        ~WorkRecord() {}

        /* Fill in this WorkRecord as though it is a continuation of wr */
        void chainFrom(const WorkRecord& wr);

        int getReduceHomeRank() const { return this->reduceHomeRank; }
        uint64_t getReduceTag() const { return this->reduceTag; }
        const std::vector<std::string>& getTarget() const {
                return this->source.target; }

        /** Get work as a printable string; used for output. */
        std::string getWorkAsString() const;
        size_t getWorkSize() const { return this->work.size(); }

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


        /** Steal our work away into an appropriately typed unique_ptr. */
        template<class T> void putWorkInto(std::unique_ptr<T>& dest) {
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
        WorkRecord() {}

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
        uint64_t reduceTag;
        int sentryRank;
        int pass;
        uint64_t tsTestStarted;

        uint64_t processed;
        uint64_t created;
        //Used to see if there is more work being generated.  While there is,
        //we won't pronounce this ring as dead.
        uint64_t createdLast;

        DeadRingTestMessage(const std::string& serialized) {
            serialization::decode(serialized, *this);
        }


        DeadRingTestMessage(int rank, uint64_t reduceTag)
                : reduceTag(reduceTag), sentryRank(rank), pass(0),
                    tsTestStarted(Location::getCurrentTimeMs()),
                    processed(0), created(0), createdLast(0) {
        }


        std::string serialized() {
            return serialization::encode(*this);
        }

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & reduceTag & sentryRank & pass & tsTestStarted;
            ar & processed & created & createdLast;
        }
    };



    /** A special message that gets passed around.  Each processor will update
        its state in the ring, and possibly send some of its work to nodes that
        have no work.

        Note that on checkpoint resume, StealRing() will be re-initialized with
        the new world size.  Thus, none of this data should be important.  Loss
        must be tolerated.
        */
    class StealRing {
    public:
        /** For each rank, the max concurrency, current work quantity, and
            current machine load. */
        std::vector<int> capacity, slots, work;
        std::vector<double> load;

        StealRing(const std::string& buffer) {
            serialization::decode(buffer, *this);
        }


        StealRing(int worldCount) : capacity(worldCount), slots(worldCount),
                work(worldCount), load(worldCount) {
            for (int i = 0; i < worldCount; i++) {
                this->capacity[i] = 0;
                this->slots[i] = 0;
                this->work[i] = 0;
                this->load[i] = -1;
            }
        }


        std::string serialized() {
            return serialization::encode(*this);
        }

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->capacity & this->slots & this->work & this->load;
        }
    };
}
}

#endif//JOB_STREAM_MESSAGE_H_
