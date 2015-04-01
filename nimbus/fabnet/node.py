from operator import itemgetter
import binascii
import heapq


class DHTNode:
    def __init__(self, node_id, host=None, port=None,
                 ext_host=None, ext_port=None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.ext_host = ext_host
        self.ext_port = ext_port
        self.long_id = int.from_bytes(node_id, byteorder='big')

    def __hash__(self):
        return hash(self.node_id)

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, DHTNode):
            raise RuntimeError('"%s" should be an instance of DHTNode class'
                               % node)

        return self.host == other.host and self.port == other.port

    def distance(self, node):
        """
        Get the distance between this node and another.
        """
        if not isinstance(node, DHTNode):
            raise RuntimeError('"%s" should be an instance of DHTNode class'
                               % node)

        return self.long_id ^ node.long_id

    def hex_id(self):
        return binascii.b2a_hex(self.node_id).decode()

    def __iter__(self):
        yield self.node_id
        yield self.host
        yield self.port
        yield self.ext_host
        yield self.ext_port

    def __repr__(self):
        return '{%s}%s:%s' % (self.hex_id(), self.host, self.port)

    def __str__(self):
        return "%s:%s[%s]" % (self.host, self.port, self.ext_port)


class NodeHeap(object):
    """
    A heap of nodes ordered by distance to a given node.
    """
    def __init__(self, node, maxsize):
        """
        Constructor.

        @param node: The node to measure all distnaces from.
        @param maxsize: The maximum size that this heap can grow to.
        """
        self.node = node
        self.heap = []
        self.heap_ids = set()
        self.removed_ids = set()
        self.contacted = set()
        self.maxsize = maxsize

    def push(self, nodes):
        """
        Push nodes onto heap.

        @param nodes: This can be a single item or a C{list}.
        """
        if not isinstance(nodes, list):
            nodes = [nodes]

        for node in nodes:
            if node.node_id in self.removed_ids:
                continue
            distance = self.node.distance(node)
            if node.node_id in self.heap_ids:
                continue
            heapq.heappush(self.heap, (distance, node))
            self.heap_ids.add(node.node_id)

    def remove(self, peer_ids):
        """
        Remove a list of peer ids from this heap.  Note that while this
        heap retains a constant visible size (based on the iterator), it's
        actual size may be quite a bit larger than what's exposed.  Therefore,
        removal of nodes may not change the visible size as previously added
        nodes suddenly become visible.
        """
        peer_ids = set(peer_ids)
        if len(peer_ids) == 0:
            return
        nheap = []
        for distance, node in self.heap:
            if node.node_id not in peer_ids:
                heapq.heappush(nheap, (distance, node))
            else:
                self.heap_ids.remove(node.node_id)
        self.heap = nheap
        self.removed_ids = self.removed_ids.union(peer_ids)

    def get_node_by_id(self, node_id):
        for _, node in self.heap:
            if node.node_id == node_id:
                return node
        return None

    def all_been_contacted(self):
        return len(self.get_uncontacted()) == 0

    def get_ids(self):
        return [n.node_id for n in self]

    def mark_contacted(self, node):
        self.contacted.add(node.node_id)

    def popleft(self):
        if len(self) > 0:
            return heapq.heappop(self.heap)[1]
        return None

    def __len__(self):
        return min(len(self.heap), self.maxsize)

    def __iter__(self):
        nodes = heapq.nsmallest(self.maxsize, self.heap)
        return iter(map(itemgetter(1), nodes))

    def get_uncontacted(self):
        return [n for n in self if n.node_id not in self.contacted]
