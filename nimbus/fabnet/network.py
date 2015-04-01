"""
Package for interacting on the network at a high level.
"""
import asyncio
import random
import pickle

from .protocol import ManagementProtocol
from .utils import digest, logger, future_list, future_dict
from .storage import ForgetfulStorage
from .node import DHTNode
from .crawling import ValueSpiderCrawl
from .crawling import NodeSpiderCrawl
from .ext_api import ExternalAPI
from .routing import RoutingTable


class Server(object):
    """
    High level view of a node instance.
    This is the object that should be created
    to start listening as an active node on the network.
    """

    def __init__(self, ksize=10, alpha=3, node_id=None, storage=None):
        """
        Create a server instance.  This will start listening on the given port.

        Args:
            ksize (int): The k parameter from the paper
            alpha (int): The alpha parameter from the paper
            node_id: The id for this node on the network.
            storage: An instance that implements `storage.IStorage`
        """
        self.ksize = ksize
        self.alpha = alpha
        self.storage = storage or ForgetfulStorage()
        self.node = DHTNode(node_id or digest(random.getrandbits(255)))
        self.protocol = None
        self.ext_api = None
        self.refresh_loop = asyncio.async(self.refresh_table())
        self.loop = asyncio.get_event_loop()
        self.__transport = None
        self.port = None

    def listen(self, port, ext_port):
        """
        Start listening on the given port.
        """
        self.port = port
        self.node.host = '127.0.0.1'
        self.node.ext_host = '0.0.0.0'
        self.node.port = port
        self.node.ext_port = ext_port
        router = RoutingTable(self.ksize, self.node)
        self.protocol = ManagementProtocol(router, self.node, self.storage,
                                           self.ksize, self.new_node_signal)
        self.ext_api = ExternalAPI(self.protocol, self.storage)

        bind_addr = ('0.0.0.0', port)
        listen = self.loop.create_datagram_endpoint(lambda: self.protocol,
                                                    local_addr=bind_addr)
        self.__transport, _ = self.loop.run_until_complete(listen)

        self.ext_api.listen(self.loop, '0.0.0.0', ext_port)

    def stop(self):
        if self.__transport:
            self.__transport.close()
        if self.ext_api:
            self.ext_api.close()

    def new_node_signal(self, new_node):
        asyncio.async(self.transfer_key_values(new_node))

    @asyncio.coroutine
    def transfer_key_values(self, node):
        """
        Given a new node, send it all the keys/values it should be storing.

        @param node: A new node that just joined (or that we just found out
        about).

        Process:
        For each key in storage, get k closest nodes.  If newnode is closer
        than the furtherst in that list, and the node for this server
        is closer than the closest in that list, then store the key/value
        on the new node (per section 2.5 of the paper)
        """
        ds = []
        for key, value in self.storage.iteritems():
            keynode = DHTNode(digest(key))
            neighbors = self.protocol.router.find_neighbors(keynode)
            if len(neighbors) > 0:
                new_node_close = node.distance(keynode) < \
                    neighbors[-1].distance(keynode)

                this_node_closest = self.node.distance(keynode) < \
                    neighbors[0].distance(keynode)

            if len(neighbors) == 0 or (new_node_close and this_node_closest):
                res = yield from self.call_store(node, key, value)  # FIXME
                ds.append(res)
        return ds

    def bootstrap(self, addrs):
        """
        Bootstrap the server by connecting to other known nodes in the network.

        Args:
            addrs: A `list` of (ip, port) `tuple` pairs.
            Note that only IP addresses
            are acceptable - hostnames will cause an error.
        """
        # if the transport hasn't been initialized yet, wait a second
        if self.protocol.transport is None:
            return asyncio.call_later(1, self.bootstrap, addrs)

        def init_table(results):
            nodes = []
            for addr, result in results.items():
                if result is None:
                    continue

                if result:
                    nodes.append(DHTNode(*result))
            spider = NodeSpiderCrawl(self.protocol, self.node, nodes,
                                     self.ksize, self.alpha)
            return spider.find()

        ds = {}
        for addr in addrs:
            ds[addr] = self.protocol.ping(addr, self.node)

        if not ds:
            ds[None] = asyncio.Future()
            ds[None].set_result(None)

        return future_dict(ds, init_table)

    @asyncio.coroutine
    def get_data_block(self, key):
        key = digest(key)
        stream = self.storage.get(key, None)
        return stream

    @asyncio.coroutine
    def put_data_block(self, key, stream):
        key = digest(key)
        yield from self.storage.save(key, stream)

    # FIXME REMOVE ME
    def refresh_table(self):
        """
        Refresh buckets that haven't had any lookups in the last hour
        (per section 2.3 of the paper).
        """
        while True:
            yield from asyncio.sleep(3600)

            ds = []
            for node_id in self.protocol.get_refresh_ids():
                node = DHTNode(node_id)
                nearest = self.protocol.router.find_neighbors(node, self.alpha)
                spider = NodeSpiderCrawl(self.protocol, node, nearest)
                ds.append(spider.find())

            for future in ds:
                res = yield from future

            ds = []
            # Republish keys older than one hour
            for key, value in self.storage.iteritems_older_than(3600):
                ds.append(self.set(key, value))

            for future in ds:
                res = yield from future

    def bootstrappable_neighbors(self):
        """
        Get a :class:`list` of (ip, port) :class:`tuple` pairs suitable
        for use as an argument to the bootstrap method.

        The server should have been bootstrapped
        already - this is just a utility for getting some neighbors and then
        storing them if this server is going down for a while.  When it comes
        back up, the list of nodes can be used to bootstrap.
        """
        neighbors = self.protocol.router.find_neighbors(self.node)
        return [tuple(n)[-2:] for n in neighbors]

    def inet_visible_ip(self):
        """
        Get the internet visible IP's of this node as other nodes see it.

        Returns:
            A `list` of IP's.
            If no one can be contacted, then the `list` will be empty.
        """
        def handle(results):
            ips = [result[1][0] for result in results if result[0]]
            logger.debug("other nodes think our ip is %s", ips)
            return ips

        ds = []
        for neighbor in self.bootstrappable_neighbors():
            ds.append(self.protocol.stun(neighbor))
        future_list(ds, handle)

    def get(self, key):
        """
        Get a key if the network has it.

        Returns:
            :class:`None` if not found, the value otherwise.
        """
        node = DHTNode(digest(key))
        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            logger.warning("There are no known neighbors to get key %s", key)
            future = asyncio.Future()
            future.set_result(None)
            return future
        spider = ValueSpiderCrawl(self.protocol, node, nearest, self.ksize,
                                  self.alpha)
        return spider.find()

    def find_node(self, key):
        """
        Get a key if the network has it.

        Returns:
            :class:`None` if not found, the value otherwise.
        """
        node = DHTNode(digest(key))
        logger.info('finding node for key: %s', node.hex_id())
        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            logger.warning("There are no known neighbors to find node %s", key)
            future = asyncio.Future()
            future.set_result(None)
            return future
        spider = NodeSpiderCrawl(self.protocol, node, nearest, self.ksize,
                                 self.alpha)
        return spider.find()

    def set(self, key, value):
        """
        Set the given key to the given value in the network.
        """
        logger.debug("setting '%s' = '%s' on network", key, value)
        dkey = digest(key)

        def store(nodes):
            logger.debug("setting '%s' on %s", key, nodes)
            ds = [self.protocol.call_store(node, dkey, value)
                  for node in nodes]
            return future_list(ds, self._any_respond_success)

        node = DHTNode(dkey)
        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            logger.warning("There are no known neighbors to set key %s", key)
            future = asyncio.Future()
            future.set_result(False)
            return future
        spider = NodeSpiderCrawl(self.protocol, node, nearest,
                                 self.ksize, self.alpha)
        nodes = spider.find()
        while type(nodes) != list:
            nodes = yield from nodes

        return store(nodes)

    def _any_respond_success(self, responses):
        """
        Given the result of a DeferredList of calls to peers,
        ensure that at least one of them was contacted
        and responded with a Truthy result.
        """
        if True in responses:
            return True
        return False

    def save_state(self, fname):
        """
        Save the state of this node (the alpha/ksize/id/immediate neighbors)
        to a cache file with the given fname.
        """
        data = {'ksize': self.ksize,
                'alpha': self.alpha,
                'id': self.node.node_id,
                'neighbors': self.bootstrappable_neighbors()}
        if len(data['neighbors']) == 0:
            logger.warning("No known neighbors, so not writing to cache.")
            return
        with open(fname, 'w') as f:
            pickle.dump(data, f)

    @classmethod
    def load_state(self, fname):
        """
        Load the state of this node (the alpha/ksize/id/immediate neighbors)
        from a cache file with the given fname.
        """
        with open(fname, 'r') as f:
            data = pickle.load(f)
        s = Server(data['ksize'], data['alpha'], data['id'])
        if len(data['neighbors']) > 0:
            s.bootstrap(data['neighbors'])
        return s

    def save_state_regularly(self, fname, frequency=600):
        """
        Save the state of node with a given regularity to the given
        filename.

        Args:
            fname: File name to save retularly to
            frequencey: Frequency in seconds that the state should be saved.
                        By default, 10 minutes.
        """
        def _save_cycle(fname, freq):
            while True:
                yield from asyncio.sleep(freq)
                self.save_state(fname)
        return asyncio.async(_save_cycle(fname, frequency))
