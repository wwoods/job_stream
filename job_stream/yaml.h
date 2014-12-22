#ifndef YAML_PROTO_H_
#define YAML_PROTO_H_

#include "types.h"

#include <yaml-cpp/yaml.h>

namespace YAML {
    class GuardedNode;
    //Allows Node.as<NodeList>();
    typedef std::vector<Node> NodeList;

    template<>
    struct convert<NodeList> {
        static Node encode(const NodeList& rhs) {
            Node node;
            for (int i = 0, s = rhs.size(); i < s; i++) {
                node.push_back(rhs[i]);
            }
            return node;
        }

        static bool decode(const Node& node, NodeList& rhs) {
            if (node.IsSequence()) {
                for (int i = 0, s = node.size(); i < s; i++) {
                    rhs.push_back(node[i]);
                }
            }
            else {
                rhs.push_back(node);
            }
            return true;
        }
    };

    /** A locked YAML::Node - that is, it locks the mutex of the closest
        GuardedNode.  Note that since YAML::Node uses plenty of mutables (hence
        our issue with thread-safety even though we're read-only...), we don't
        have a const version of most methods. */
    class LockedNode {
    public:
        LockedNode(const LockedNode& other) : mutex(other.mutex),
                node(other.node), lock(other.mutex) {}
        ~LockedNode() {}

        template<typename T> const T as() const { return this->node.as<T>(); }
        template<typename T> LockedNode& operator=(const T& rhs) {
                this->node = rhs; return *this; }
        template<typename Key> LockedNode operator[](const Key& key) {
                return LockedNode(this->node[key], this->mutex); }
        template<typename Key> const LockedNode operator[](const Key& key) const {
                return LockedNode(this->node[key], this->mutex); }
        template<typename Key> bool remove(const Key& key) {
                return this->node.remove(key); }

        std::size_t size() const { return this->node.size(); }

        NodeType::value Type() const { return this->node.Type(); }
        bool IsDefined() const { return this->node.IsDefined(); }
        bool IsNull() const { return this->node.IsNull(); }
        bool IsScalar() const { return this->node.IsScalar(); }
        bool IsSequence() const { return this->node.IsSequence(); }
        bool IsMap() const { return this->node.IsMap(); }

        const std::string& Scalar() const { return this->node.Scalar(); }
        const std::string& Tag() const { return this->node.Tag(); }

        //Unsafe, but... Unless the iterator gets passed between functions,
        //the lock on this node should outlast the begin()..end() iteration.
        Node::const_iterator begin() const { return this->node.begin(); }
        Node::const_iterator end() const { return this->node.end(); }
        Node::iterator begin() { return this->node.begin(); }
        Node::iterator end() { return this->node.end(); }

        operator bool() const { return this->node; }
        bool operator!() const { return !this->node; }

        /** Use with extreme caution - returns the underlying node.  Shouldn't
            be used in user code. */
        Node _getNode() { return this->node; }

    private:
        friend class GuardedNode;
        friend Node Clone(const LockedNode& other);
        LockedNode(Node n, job_stream::Mutex& mutex) : mutex(mutex), node(n),
                lock(mutex) {}

        job_stream::Mutex& mutex;
        Node node;
        job_stream::Lock lock;
    };

    /** yaml-cpp is not thread safe.  An unlocked node DOES NOT HAVE THE LOCK
        but only exposes array access methods.  When accessed, it accesses its
        GuardedNode and returns the aquired version. */
    class UnlockedNode {
    public:
        template<typename Key> LockedNode operator[](const Key& key);
        inline LockedNode get();
        template<typename Key> bool remove(const Key& key);

        /** Points this UnlockedNode at the given GuardedNode, which must be
            maintained elsewhere. */
        void set(GuardedNode& node) {
            this->node = &node;
        }

    private:
        GuardedNode* node;
    };

    /** yaml-cpp is not thread safe.  This class is constructed based on a YAML
        node and conveys functionality that is compatible with yaml-cpp yet is
        thread safe.  Essentially just puts a mutex around the given YAML::Node.
        */
    class GuardedNode {
    public:
        /** A thread-safe proxy for a YAML::Node and its descendents. */
        GuardedNode() {}

        /** Sets the node to be controlled. */
        void set(Node n) {
            job_stream::Lock locker(this->mutex);
            this->node = n;
        }

        /** Returns a LockedNode object, which is a Node proxy with the caveat
            that between its constructor and destructor, it holds our mutex.
            */
        LockedNode acquire() { return LockedNode(this->node, this->mutex); }

    private:
        Node node;
        static job_stream::Mutex mutex;
    };


    /** Allows cloning of a LockedNode.  Creates an unassociated instance. */
    inline Node Clone(const LockedNode& other) {
        return YAML::Clone(other.node);
    }

    /** Now that GuardedNode is defined, implement UnlockedNode. */
    template<typename Key> LockedNode UnlockedNode::operator[](const Key& key) {
            return this->node->acquire()[key]; }
    LockedNode UnlockedNode::get() { return this->node->acquire(); }
    template<typename Key> bool UnlockedNode::remove(const Key& key) {
            return this->node->acquire().remove(key); }
}

#endif//YAML_PROTO_H_
