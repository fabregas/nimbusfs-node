import json
import asyncio
import inspect
import hashlib
from aiohttp import web

from .crawling import NodeSpiderCrawl, ValueSpiderCrawl
from .node import DHTNode
from .utils import digest, logger

class ExternalAPI:
    def __init__(self, mgmt_protocol, storage, ksize=20, alpha=3):
        self.protocol = mgmt_protocol
        self.storage = storage
        self.ksize = ksize
        self.alpha = alpha
        self.__app= web.Application()
        self.__app.router.add_route('GET', '/find_keys', self.find_keys)
        self.__app.router.add_route('GET', '/find_values', self.find_values)
        self.__app.router.add_route('GET', '/get_data_block',
                                    self.get_data_block)
        self.__app.router.add_route('POST', '/put_data_block',
                                    self.put_data_block)
        self.__server = None

    def listen(self, loop, host, port):
        server = loop.create_server(self.__app.make_handler(), host, int(port))
        server = loop.run_until_complete(server)
        logger.info('External API serving on {}'
                    .format(server.sockets[0].getsockname()))
        self.__server = server
        return server

    def close(self):
        if self.__server:
            self.__server.close()
        self.__server = None

    @asyncio.coroutine
    def find_keys(self, request):
        key = request.GET['key']
        node = DHTNode(digest(key))
        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            raise web.HTTPInternalServerError("There are no known neighbors "
                                              "to get key %s" % key)

        spider = NodeSpiderCrawl(self.protocol, node, nearest, self.ksize,
                                  self.alpha)

        nodes = spider.find()
        while inspect.isgenerator(nodes):
            nodes = yield from nodes

        ret_list = []
        for node_id, _, _, e_host, e_port in nodes:
            ret_list.append((e_host, e_port))

        return web.Response(text=json.dumps(ret_list),
                            content_type='application/json')

    @asyncio.coroutine
    def find_values(self, request):
        key = request.GET['key']
        node = DHTNode(digest(key))
        nearest = self.protocol.router.find_neighbors(node)
        if len(nearest) == 0:
            raise web.HTTPInternalServerError("There are no known neighbors "
                                              "to get key %s" % key)

        spider = ValueSpiderCrawl(self.protocol, node, nearest, self.ksize,
                                  self.alpha)
        values = spider.find()
        while inspect.isgenerator(values):
            values = yield from values

        ret_list = []
        for node in values:
            ret_list.append((node.ext_host, node.ext_port))

        return web.Response(text=json.dumps(ret_list),
                            content_type='application/json')


    @asyncio.coroutine
    def put_data_block(self, request):
        stream = request.content
        key = request.GET['key']
        key = digest(key)

        out_stream = self.storage.stream_for_write()
        h = hashlib.sha1()
        while True:
            chunk = yield from stream.readany()
            if not chunk:
                break
            out_stream.write(chunk.decode())
            h.update(chunk)
        self.storage.save_stream(key, out_stream)
        #self.storage[key] = h.hexdigest()
        return web.Response()

    @asyncio.coroutine
    def get_data_block(self, request):
        key = request.GET['key']
        key = digest(key)

        stream = self.storage.get_stream(key, None)
        if stream is None:
            raise web.HTTPNotFound()

        resp = web.StreamResponse()
        resp.start(request)
        while True:
            buf = stream.read(1024)
            if not buf:
                break
            resp.write(buf.encode())
            yield from resp.drain()
        resp.write_eof()
        return resp

