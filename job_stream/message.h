
#ifndef JOB_STREAM_MESSAGE_H_
#define JOB_STREAM_MESSAGE_H_

#include "serialization.h"
#include <string>
#include <vector>

namespace job_stream {
namespace message {
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

        virtual ~WorkRecord() {}

        /* Fill in this WorkRecord as though it is a continuation of wr */
        void chainFrom(const WorkRecord& wr);

        int getReduceHomeRank() const { return this->reduceHomeRank; }
        uint64_t getReduceTag() const { return this->reduceTag; }
        const std::vector<std::string>& getTarget() const { 
                return this->source.target; }

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


        /** Steal our work away into an appropriately typed unique_ptr. */
        template<class T> void putWorkInto(std::unique_ptr<T>& dest) {
            if (this->isTyped()) {
                dest.reset((T*)this->getTypedWork());
            }
            else {
                T* value = new T();
                serialization::decode(this->work, *value);
                dest.reset(value);
            }
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

        mutable std::string work;


        /** Returns true if this WorkRecord is Typed (meaning it has its own
            allocated version of our work, no string necessary). */
        virtual bool isTyped() {
            return false;
        }


        virtual void* getTypedWork() {
            throw std::runtime_error("Default impl has no typed work");
        }

        
        virtual void serializeTypedWork() const {
            //Default impl has work string already encoded and ready to go
        }

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->source;
            ar & this->reduceHomeRank;
            ar & this->reduceTag;
            ar & this->route;

            if (Archive::is_saving::value) {
                //We're being saved out, so sync up work with our work
                this->serializeTypedWork();
            }
            ar & this->work;
        }
    };



    template<typename T>
    class TypedWorkRecord : public WorkRecord {
    public:
        TypedWorkRecord(const std::vector<std::string>& target, T* work)
                : WorkRecord(target, work), typedWork(work) {}
        virtual ~TypedWorkRecord() {}

    protected:
        virtual bool isTyped() {
            return true;
        }


        virtual void* getTypedWork() {
            if (this->typedWork.get() == 0) {
                throw std::runtime_error("Work already cast?");
            }
            return (void*)this->typedWork.release();
        }


        virtual void serializeTypedWork() const {
            if (this->typedWork.get() == 0) {
                throw std::runtime_error("Work already cast?");
            }
            this->work = serialization::encode(*this->typedWork);
        }

    private:
        std::unique_ptr<T> typedWork;
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



    /** A special message that gets passed around.  Each processor will update
        its state in the ring, and possibly send some of its work to nodes that
        have no work. */
    class StealRing {
    public:
        std::vector<bool> needsWork;

        StealRing(const std::string& buffer) {
            serialization::decode(buffer, *this);
        }


        StealRing(int worldCount) : needsWork(worldCount) {
            for (int i = 0; i < worldCount; i++) {
                this->needsWork[i] = false;
            }
        }


        std::string serialized() {
            return serialization::encode(*this);
        }

    private:
        friend class boost::serialization::access;
        template<class Archive>
        void serialize(Archive& ar, const unsigned int version) {
            ar & this->needsWork;
        }
    };
}
}

#endif//JOB_STREAM_MESSAGE_H_