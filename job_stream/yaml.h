#ifndef YAML_PROTO_H_
#define YAML_PROTO_H_

#include <yaml-cpp/yaml.h>

//Allow any node to be cast as a NodeList (sequence)
namespace YAML {
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
}

#endif//YAML_PROTO_H_
