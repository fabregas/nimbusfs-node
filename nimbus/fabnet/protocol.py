
import functools
import random
import asyncio

from .asyncio_rpc import UDPRPC, TCPRPC
from .node import DHTNode
from .utils import logger, digest


class ManagementProtocol(UDPRPC):
    def __init__(self, router, source_node, storage, ksize, new_node_signal):
        super().__init__()
        self.router = router
        self.storage = storage
        self.source_node = source_node
        self.new_node_signal = new_node_signal

    def add_contact(self, source):
        if self.router.is_new_node(source):
            self.new_node_signal(source)
        else:
            # this node is already exists...
            return
        logger.debug("adding to router %s", source)
        bucket_head = self.router.add_contact(source)
        if bucket_head:
            asyncio.async(self.call_ping(bucket_head))

    def check_contact(self, node_id, node_addr):
        node = DHTNode(node_id, node_addr[0], node_addr[1])
        if not self.router.is_new_node(node):
            asyncio.async(self.call_ping(node))
        return node

    def api_stun(self, sender):
        return sender

    def api_ping(self, sender, source):
        source.host = sender[0]
        source.port = sender[1]
        self.add_contact(source)
        return tuple(self.source_node)

    def api_bye(self, sender, source):
        source.host = sender[0]
        source.port = sender[1]
        logger.info('node %s is down now. removing it...', source)
        self.router.remove_contact(source)

    def api_find_node(self, sender, source_node_id, key):
        source = self.check_contact(source_node_id, sender)
        node = DHTNode(key)
        logger.debug("finding neighbors of %s in local table (%s)",
                     node.hex_id(), self.source_node.hex_id())
        return list(map(tuple,
                        self.router.find_neighbors(node, exclude=source)))

    def api_find_value(self, sender, source_node_id, key):
        source = self.check_contact(source_node_id, sender)
        value = self.storage.get(key, None)
        if value is None:
            return self.api_find_node(sender, source_node_id, key)
        return {'value': True}

    def handle_call_response(self, node, result):
        """
        If we get a response, add the node to the routing table.  If
        we get no response, make sure it's removed from the routing table.
        """
        if result is not None:
            self.add_contact(node)
        else:
            logger.debug("no response from %s, removing from router", node)
            self.router.remove_contact(node)
        return result

    def get_refresh_ids(self):
        """
        Get ids to search for to keep old buckets up to date.
        """
        ids = []
        for bucket in self.router.get_lonely_buckets():
            ids.append(random.randint(*bucket.range))
        return ids

    @asyncio.coroutine
    def call_ping(self, node_to_ask):
        address = (node_to_ask.host, node_to_ask.port)
        resp = yield from self.ping(address, self.source_node)
        return self.handle_call_response(node_to_ask, resp)

    @asyncio.coroutine
    def call_bye(self, node_to_ask):
        address = (node_to_ask.host, node_to_ask.port)
        resp = yield from self.bye(address, self.source_node, nowait=True)
        return resp

    @asyncio.coroutine
    def call_find_node(self, node_to_ask, node_to_find):
        address = (node_to_ask.host, node_to_ask.port)
        resp = yield from self.find_node(address,
                                         self.source_node.node_id,
                                         node_to_find.node_id)
        return self.handle_call_response(node_to_ask, resp)

    @asyncio.coroutine
    def call_find_value(self, node_to_ask, node_to_find):
        address = (node_to_ask.host, node_to_ask.port)
        resp = yield from self.find_value(address,
                                          self.source_node.node_id,
                                          node_to_find.node_id)

        return self.handle_call_response(node_to_ask, resp)

    # FIXME REMOVE ME!
    @asyncio.coroutine
    def call_store(self, node_to_ask, key, value):
        address = (node_to_ask.host, node_to_ask.port)
        resp = yield from self.store(address,
                                     self.source_node,
                                     key, value)
        return self.handle_call_response(node_to_ask, resp)

    def api_store(self, sender, source, key, value):
        self.add_contact(source)
        logger.debug("got a store request from %s, storing value", sender)
        self.storage[key] = value
        return True
