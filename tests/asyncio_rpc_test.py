
import time
import unittest
import asyncio
from datetime import datetime
from multiprocessing import Process

from nimbus.fabnet.asyncio_rpc import UDPRPC, TCPRPC

class TestTCPRPC(TCPRPC):
    def api_sum(self, addr, a, b):
        return a+b

    def api_test(self, addr):
        return 'TEST', 3

    def api_exeption_test(self, addr):
        raise RuntimeError('Some exception')

class TestUDPRPC(UDPRPC):
    def api_sum(self, addr, a, b):
        return a+b

    def api_test(self, addr):
        return 'TEST', 3

    def api_echo(self, addr, value):
        return value

loop = asyncio.get_event_loop()

def start_server(rpc_class, port):
    print("Starting server at 127.0.0.1:%s ..."%port)
    loop = asyncio.get_event_loop()
    listen = loop.create_server(rpc_class, '127.0.0.1', port)
    return loop.run_until_complete(listen)

def start_server_forever(rpc_class, port):
    server = start_server(rpc_class, port)
    try:
        loop.run_forever()
    finally:
        print('==============END')

def start_udp_client(port):
    connect = loop.create_datagram_endpoint(
              TestUDPRPC, local_addr=('127.0.0.1', port))
    transport, api = loop.run_until_complete(connect)   
    return api


class TestDHTNetwork(unittest.TestCase):
    def test_asyncio_TCP_rpc(self):        
        server = start_server(TestTCPRPC, 5656)
        try:
            ADDR = ('127.0.0.1', 5656)
            client = TestTCPRPC()
            future = client.sum(ADDR, 2,3)
            ret = loop.run_until_complete(future)
            self.assertEqual(ret, 5)

            future = client.test(ADDR)
            ret = loop.run_until_complete(future)
            self.assertEqual(ret, ('TEST', 3))
        finally:
            server.close()


    def test_asyncio_UDP_rpc(self):        
        server = start_server(TestUDPRPC, 5657)
        try:
            ADDR = ('127.0.0.1', 5657)

            client = start_udp_client(5657)
            future = client.sum(ADDR, 2,3)
            ret = loop.run_until_complete(future)
            self.assertEqual(ret, 5)

            future = client.test(ADDR)
            ret = loop.run_until_complete(future)
            self.assertEqual(ret, ('TEST', 3))
        finally:
            server.close()

    def test_performance_rpc(self):
        server = Process(target=start_server_forever, args=(TestUDPRPC, 5677))
        server.start()
        time.sleep(.5)

        #server = start_server(TestUDPRPC, 5678)
        N = 10000
        M = 2
        try:
            ADDR = ('127.0.0.1', 5678)
            client = start_udp_client(5678)
            t0 = datetime.now()
            for i in range(N):
                val = 'item#%s'%i
                future = client.echo(ADDR, val)
                ret = loop.run_until_complete(future)
                self.assertEqual(ret, val) 
            print('%s in one thread proc time: %s'%(N, datetime.now()-t0))


            '''
            #profiling...
            def test():
                print('===PROFILE===')
                for i in range(N):
                    val = 'item#%s'%i
                    future = client.echo(ADDR, val)
                    ret = loop.run_until_complete(future)
                    self.assertEqual(ret, val) 
                print('===PROFILE===END')

            import cProfile
            cProfile.runctx('test()', {'test': test}, locals(), 'restats')
            import pstats
            p = pstats.Stats('restats')
            p.strip_dirs().sort_stats('cumtime').print_stats()
            '''


            tasks = []
            def testf(num):
                for i in range(int(N/M)):
                    val = 'item#%s.%s'%(num, i)
                    ret = yield from client.echo(ADDR, val)
                    self.assertEqual(ret, val) 

            t0 = datetime.now()
            for i in range(M):
                tasks.append(asyncio.async(testf(i)))
            loop.run_until_complete(asyncio.wait(tasks))  
            print('%s in %s threads proc time: %s'%(N, M, datetime.now()-t0))

            t0 = datetime.now()
            M = 10
            for i in range(M):
                tasks.append(asyncio.async(testf(i)))
            loop.run_until_complete(asyncio.wait(tasks))  
            print('%s in %s threads proc time: %s'%(N, M, datetime.now()-t0))
        finally:
            server.terminate()
            server.join()

if __name__ == '__main__':
    unittest.main()

