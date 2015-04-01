import random
import unittest
import binascii

import asyncio
import aiohttp
from nimbus.fabnet.network import Server
from nimbus.fabnet.node import DHTNode
from nimbus.fabnet.utils import digest 

loop = asyncio.get_event_loop()

class ServersManager:
    def __init__(self):
        self.servers = set()

    def start_at(self, port, ext_port, node_id=None):
        server = Server(node_id=node_id)
        server.listen(port, ext_port)
        self.servers.add(server)
        return server

    def wait_result(self, resp):
        while asyncio.iscoroutine(resp):
            resp = loop.run_until_complete(resp)
        return resp

    def bootstrap(self, server, rem_port):
        if not rem_port:
            peers = []
        else:
            peers = [("127.0.0.1", rem_port)]

        resp = server.bootstrap(peers)
        return self.wait_result(resp)

    def print_nodes(self):
        for server in self.servers:
            print('NODE-{} {}'.format(server.port, server.node.hex_id()))
            print(' ==> buckets: {}'.format(server.protocol.router.buckets))

    def iter(self):
        for s in self.servers:
            yield s


    def stop_all(self):
        for server in self.servers:
            server.stop()

        self.servers = set()


class ExtClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def get_keys(self, key):
        req = aiohttp.request('get', 'http://%s:%s/find_keys?key=%s'%(self.host, self.port, key))
        req = loop.run_until_complete(req)
        data = req.json()
        return loop.run_until_complete(data)

    def get_values(self, key):
        req = aiohttp.request('get', 'http://%s:%s/find_values?key=%s'%(self.host, self.port, key))
        req = loop.run_until_complete(req)
        data = req.json()
        return loop.run_until_complete(data)

    def put_data(self, host, port, key, fobj):
        req = aiohttp.request('post', 'http://%s:%s/put_data_block?key=%s'%(host, port, key), data = fobj)
        req = loop.run_until_complete(req)
        return req.status
        #data = req.json()

    def get_data(self, host, port, key):
        req = aiohttp.request('get', 'http://%s:%s/get_data_block?key=%s'%(host, port, key))
        req = loop.run_until_complete(req)
        data = req.content
        resp = ''
        while True:
            chunk = loop.run_until_complete(data.read(100))
            if not chunk:
                break
            resp += chunk.decode()
        return resp



class TestDHTNetwork(unittest.TestCase):
    def start_network(self, nodes_count):
        pass

    def test1_node_start(self):
        sm = ServersManager()
        try:
            server = sm.start_at(1900, 1800)

            res = sm.bootstrap(server, None)
            self.assertEqual(res, [])

            server2 = sm.start_at(1901, 1801)
            res = sm.bootstrap(server2, 1900)
            self.assertEqual(len(res), 1)
            node = res[0]
            self.assertTrue(isinstance(node, DHTNode))
            self.assertEqual(node.host, '127.0.0.1')
            self.assertEqual(node.port, 1900)
            self.assertTrue(node.long_id > 0)

            server3 = sm.start_at(1902, 1802)
            res = sm.bootstrap(server3, 1901)
            self.assertEqual(len(res), 2)

            server4 = sm.start_at(1903, 1803)
            res = sm.bootstrap(server4, 1900)
            self.assertEqual(len(res), 3)

            peers_count = 4
            for i in range(60):
                new_server = sm.start_at(1904+i, 1804+i)
                res = sm.bootstrap(new_server, 1900)
                #self.assertEqual(len(res), peers_count if peers_count < 20 else 20)
                peers_count += 1


            print('======FINDING key...')
            ret = server2.find_node('test key')
            ret = sm.wait_result(ret)
            print(ret)
            print(len(ret))
            print("FIND KEY: ", binascii.b2a_hex(digest('test key')))

            print('==========STORING VALUE...')
            ret = server3.set('test key', 'VALUE')
            ret = sm.wait_result(ret)
            print(ret)
            print('=========================')

            sm.print_nodes()
            
            node = DHTNode(digest('test key'))
            print ('FIND NEAREST FOR ', binascii.b2a_hex(digest('test key')))
            nearest = server3.protocol.router.find_neighbors(node)
            print('NEAREST ==== ', nearest)

            print('==========GETING VALUE...')
            ret = server2.get('test key')
            ret = sm.wait_result(ret)
            print(ret)
            print('=========================')

            """
            #profiling...
            def test():
                print('===PROFILE===FINDING key...')
                for i in range(100):
                    ret = server2.get('test key %s'%i)
                    ret = sm.wait_result(ret)
                print(ret)
                print('===PROFILE===END')

            import cProfile
            cProfile.runctx('test()', {'test': test}, locals(), 'restats')
            import pstats
            p = pstats.Stats('restats')
            p.strip_dirs().sort_stats('cumtime').print_stats()
            """
        except Exception as err: 
            raise err
        finally:
            sm.stop_all()

        
    def test2_ext_api(self):
        sm = ServersManager()
        try:
            server = sm.start_at(1900, 1800)
            res = sm.bootstrap(server, None)

            server2 = sm.start_at(1901, 1801)
            res = sm.bootstrap(server2, 1900)

            server3 = sm.start_at(1902, 1802)
            res = sm.bootstrap(server3, 1901)

            server4 = sm.start_at(1903, 1803)
            res = sm.bootstrap(server4, 1900)

            print('=========EXT CLIENT TEST')
            print("FIND KEY: ", binascii.b2a_hex(digest('test')))
            ext = ExtClient('127.0.0.1', 1802)
            data = ext.get_keys('test')
            nodes_for_put = data
            self.assertEqual(len(data), 4, data)
            print('=========RESULT: ', data)
            data = ext.get_values('test')
            self.assertEqual(data, [])

            for host, port in nodes_for_put:
                f = open(__file__, 'rb')
                resp = ext.put_data(host, port, 'test', f)
                f.close()
                self.assertEqual(resp, 200)

            data = ext.get_values('test')
            self.assertEqual(len(data), 3)
            nodes_for_get = data
            for host, port in nodes_for_get:
                data = ext.get_data(host, port, 'test')
                self.assertEqual(data, open(__file__, 'rb').read().decode())
        except Exception as err: 
            raise err
        finally:
            sm.stop_all()

    def test3_predefined_ids(self):
        sm = ServersManager()
        NODES = []
        n_count = 4
        prev = None
        import binascii
        for i in range(n_count):
            node_id = '%040x' % (i * (int( pow(2, 160)/n_count ))) 
            NODES.append((1900+i,1800+i, binascii.a2b_hex(node_id), prev))
            prev = 1900+i
        
        try:

            for port, e_port, node_id, n_port in NODES:
                server = sm.start_at(port, e_port, node_id)
                res = sm.bootstrap(server, n_port)

            prev = None
            for server in sm.iter(): 
                print('======FINDING key...')
                ret = server.find_node('test key')
                ret = sm.wait_result(ret)
                print(ret)
                if prev:
                    self.assertEqual(ret, prev)
                prev = ret
        finally:
            sm.stop_all()


if __name__ == '__main__':
    unittest.main()


