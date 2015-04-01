import heapq
import time
import operator
from collections import OrderedDict

from .utils import OrderedSet, shared_prefix


class KBucket(object):
    def __init__(self, range_lower, range_upper, ksize):
        self.range = (range_lower, range_upper)
        self.nodes = OrderedDict()
        self.replacement_nodes = OrderedSet()
        self.touch_last_updated()
        self.ksize = ksize

    def touch_last_updated(self):
        self.last_updated = time.time()

    def get_nodes(self):
        return list(self.nodes.values())

    def split(self):
        midpoint = self.range[1] - ((self.range[1] - self.range[0]) / 2)
        one = KBucket(self.range[0], midpoint, self.ksize)
        two = KBucket(midpoint + 1, self.range[1], self.ksize)
        for node in self.nodes.values():
            bucket = one if node.long_id <= midpoint else two
            bucket.nodes[node.node_id] = node
        return (one, two)

    def remove_node(self, node):
        if node.node_id not in self.nodes:
            return

        # delete node, and see if we can add a replacement
        del self.nodes[node.node_id]
        if len(self.replacement_nodes) > 0:
            newnode = self.replacement_nodes.pop()
            self.nodes[newnode.node_id] = newnode

    def has_in_range(self, node):
        return self.range[0] <= node.long_id <= self.range[1]

    def is_new_node(self, node):
        c_node = self.nodes.get(node.node_id, None)
        if c_node is None:
            return True
        return node != c_node

    def add_node(self, node):
        """
        Add a C{Node} to the C{KBucket}.  Return True if successful,
        False if the bucket is full.

        If the bucket is full, keep track of node in a replacement list,
        per section 4.1 of the paper.
        """
        if node.node_id in self.nodes:
            del self.nodes[node.node_id]
            self.nodes[node.node_id] = node
        elif len(self) < self.ksize:
            self.nodes[node.node_id] = node
        else:
            self.replacement_nodes.push(node)
            return False
        return True

    def depth(self):
        sp = shared_prefix([n.node_id for n in self.nodes.values()])
        return len(sp)

    def head(self):
        return list(self.nodes.values())[0]

    def __getitem__(self, node_id):
        return self.nodes.get(node_id, None)

    def __len__(self):
        return len(self.nodes)

    def __repr__(self):
        return '[{0:040x}-{1:040x}]=({2})' \
               .format(int(self.range[0]), int(self.range[1]),
                       ', '.join([str(n) for n in self.nodes.values()]))


class TableTraverser:
    def __init__(self, table, start_node):
        index = table.get_bucket_for(start_node)
        table.buckets[index].touch_last_updated()
        self.current_nodes = table.buckets[index].get_nodes()
        self.left_buckets = table.buckets[:index]
        self.right_buckets = table.buckets[(index + 1):]
        self.left = True

    def __iter__(self):
        return self

    def __next__(self):
        """
        Pop an item from the left subtree, then right, then left, etc.
        """
        if len(self.current_nodes) > 0:
            return self.current_nodes.pop()

        if self.left and len(self.left_buckets) > 0:
            self.current_nodes = self.left_buckets.pop().get_nodes()
            self.left = False
            return next(self)

        if len(self.right_buckets) > 0:
            self.current_nodes = self.right_buckets.pop().get_nodes()
            self.left = True
            return next(self)

        raise StopIteration


class RoutingTable(object):
    def __init__(self, ksize, node):
        """
        @param node: The node that represents this server.  It won't
        be added to the routing table, but will be needed later to
        determine which buckets to split or not.
        """
        self.node = node
        self.ksize = ksize
        self.flush()

    def flush(self):
        self.buckets = [KBucket(0, 2 ** 160, self.ksize)]

    def split_bucket(self, index):
        one, two = self.buckets[index].split()
        self.buckets[index] = one
        self.buckets.insert(index + 1, two)

    def get_lonely_buckets(self):
        """
        Get all of the buckets that haven't been updated in over
        an hour.
        """
        return [b for b in self.buckets
                if b.last_updated < (time.time() - 3600)]

    def remove_contact(self, node):
        index = self.get_bucket_for(node)
        self.buckets[index].remove_node(node)

    def is_new_node(self, node):
        index = self.get_bucket_for(node)
        return self.buckets[index].is_new_node(node)

    def add_contact(self, node):
        index = self.get_bucket_for(node)
        bucket = self.buckets[index]

        # this will succeed unless the bucket is full
        if bucket.add_node(node):
            return

        # Per section 4.2 of paper, split if the bucket has the node
        # in its range or if the depth is not congruent to 0 mod 5
        if bucket.has_in_range(self.node) or bucket.depth() % 5 != 0:
            self.split_bucket(index)
            self.add_contact(node)
        else:
            return bucket.head()

    def get_bucket_for(self, node):
        """
        Get the index of the bucket that the given node would fall into.
        """
        for index, bucket in enumerate(self.buckets):
            if node.long_id < bucket.range[1]:
                return index

    def iterate(self):
        for neighbour in TableTraverser(self, self.node):
            if self.node == neighbour:
                continue
            yield neighbour

    def find_neighbors(self, node, k=None, exclude=None):
        k = k or self.ksize
        nodes = []
        if exclude != self.node:
            heapq.heappush(nodes, (node.distance(self.node), self.node))

        for neighbor in TableTraverser(self, node):
            if neighbor.node_id != node.node_id and \
                    (exclude is None or neighbor != exclude):
                heapq.heappush(nodes, (node.distance(neighbor), neighbor))
            if len(nodes) == k:
                break

        return list(map(operator.itemgetter(1), heapq.nsmallest(k, nodes)))
