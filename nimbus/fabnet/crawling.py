from collections import Counter

from .node import DHTNode, NodeHeap
from .utils import logger
from .utils import future_dict


class SpiderCrawl(object):
    """
    Crawl the network and look for given 160-bit keys.
    """
    def __init__(self, protocol, node, peers, ksize, alpha):
        self.protocol = protocol
        self.ksize = ksize
        self.alpha = alpha
        self.node = node
        self.nearest = NodeHeap(self.node, self.ksize)
        self.last_ids_crawled = []
        self.nearest.push(peers)

    def _find(self, rpcmethod):
        """
        Get either a value or list of nodes.

        Args:
            rpcmethod: The protocol's callfindValue or callFindNode.

        The process:
          1. calls find_* to current ALPHA nearest not already queried nodes,
             adding results to current nearest list of k nodes.
          2. current nearest list needs to keep track of who has been queried
             already sort by nearest, keep KSIZE
          3. if list is same as last time, next call should be to everyone not
             yet queried
          4. repeat, unless nearest list has all been queried, then ur done
        """
        count = self.alpha
        cur_ids = self.nearest.get_ids()
        if cur_ids == self.last_ids_crawled:
            logger.debug("last iteration same as current - "
                         "checking all in list now")
            count = len(self.nearest)
        self.last_ids_crawled = cur_ids

        ds = {}
        next_uncontacted = self.nearest.get_uncontacted()[:count]
        logger.debug("crawling with uncontacted nearest: %s", next_uncontacted)
        for peer in next_uncontacted:
            ds[peer] = rpcmethod(peer, self.node)
            self.nearest.mark_contacted(peer)
        return future_dict(ds, self._nodes_found)


class ValueSpiderCrawl(SpiderCrawl):
    def __init__(self, protocol, node, peers, ksize, alpha):
        SpiderCrawl.__init__(self, protocol, node, peers, ksize, alpha)
        # keep track of the single nearest node without value - per
        # section 2.3 so we can set the key there if found
        self.nearest_without_value = NodeHeap(self.node, 1)

    def find(self):
        """
        Find either the closest nodes or the value requested.
        """
        return self._find(self.protocol.call_find_value)

    def _nodes_found(self, responses):
        """
        Handle the result of an iteration in _find.
        """
        toremove = []
        found_values = []
        for peer, response in responses.items():
            response = RPCFindResponse(response)
            if not response.happened():
                toremove.append(peer.node_id)
            elif response.has_value():
                found_values.append((peer, response.value()))
            else:
                peer = self.nearest.get_node_by_id(peer.node_id)
                self.nearest_without_value.push(peer)
                self.nearest.push(response.node_list())
        self.nearest.remove(toremove)

        if len(found_values) > 0:
            return self._handle_found_values(found_values)
        if self.nearest.all_been_contacted():
            # not found!
            return []
        return self.find()

    def _handle_found_values(self, values):
        """
        We got some values!  Exciting.  But let's make sure
        they're all the same or freak out a little bit.  Also,
        make sure we tell the nearest node that *didn't* have
        the value to store it.
        """
        return [peer for peer, has_value in values if has_value]


class NodeSpiderCrawl(SpiderCrawl):
    def find(self):
        """
        Find the closest nodes.
        """
        return self._find(self.protocol.call_find_node)

    def _nodes_found(self, responses):
        """
        Handle the result of an iteration in _find.
        """
        toremove = []
        for peer, response in responses.items():
            response = RPCFindResponse(response)
            if not response.happened():
                toremove.append(peer.node_id)
            else:
                self.nearest.push(response.node_list())
        self.nearest.remove(toremove)

        if self.nearest.all_been_contacted():
            return list(self.nearest)
        return self.find()


class RPCFindResponse(object):
    def __init__(self, response):
        """
        A wrapper for the result of a RPC find.

        Args:
            response: This will be a tuple of (<response received>, <value>)
                      where <value> will be a list of tuples if not found or
                      a dictionary of {'value': v} where v is the value desired
        """
        self.response = response

    def happened(self):
        """
        Did the other host actually respond?
        """
        return self.response is not None

    def has_value(self):
        return isinstance(self.response, dict)

    def value(self):
        return self.response['value']

    def node_list(self):
        """
        Get the node list in the response.  If there's no value, this should
        be set.
        """
        nodelist = list(self.response) or []
        return [DHTNode(*nodeple) for nodeple in nodelist]
