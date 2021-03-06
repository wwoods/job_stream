#include "yaml-cpp/node/detail/memory.h"
#include "yaml-cpp/node/detail/node.h"

namespace YAML
{
	namespace detail
	{
		void memory_holder::merge(memory_holder& rhs)
		{
			if(m_pMemory == rhs.m_pMemory)
				return;

			m_pMemory->merge(*rhs.m_pMemory);
			rhs.m_pMemory = m_pMemory;
		}

		node& memory::create_node(const Mark& mark)
		{
			shared_node pNode(new node(mark));
			m_nodes.insert(pNode);
			return *pNode;
		}

		void memory::merge(const memory& rhs)
		{
			m_nodes.insert(rhs.m_nodes.begin(), rhs.m_nodes.end());
		}
	}
}
