import asyncio
import random
import uuid
import sys
import traceback
import pickle
import inspect
from base64 import b64encode
from hashlib import sha1

from .utils import logger


class TimeoutException(Exception):
    pass


class RemoteError(Exception):
    pass


def serializable_error(msg):
    traceback.print_exc(file=sys.stderr)
    return {'__error__': str(msg)}


def check_remote_error(obj):
    if isinstance(obj, dict):
        if '__error__' in obj:
            return RemoteError(obj['__error__'])


# packet's markers
PM_REQUEST = 0
PM_RESPONSE = 1
PM_END = bytes([66, 99, 66])


class AbstractRPC(asyncio.Protocol):
    def __init__(self, wait_response_time=5):
        super(asyncio.Protocol, self).__init__()
        self.wait_response_time = wait_response_time
        self._outstanding = {}
        self.buf = bytes()

    def connection_made(self, transport):
        self.transport = transport

    def _accept_request(self, msg_id, data, address):
        if not isinstance(data, list) or len(data) != 2:
            raise RuntimeError("Could not read packet: %s" % data)

        funcname, args = data
        api_method = getattr(self, "api_%s" % funcname, None)
        if api_method is None or not callable(api_method):
            logger.info("%s has no callable method api_%s; ignoring request",
                        self.__class__.__name__, funcname)
            return

        @asyncio.coroutine
        def proc_request(address, *args):
            if inspect.isgeneratorfunction(api_method):
                ret = yield from api_method(address, *args)
                return ret
            else:
                return api_method(address, *args)

        def resp_func(task):
            try:
                resp = task.result()
            except Exception as err:
                resp = serializable_error(err)
            txdata = b'\x01' + msg_id + pickle.dumps(resp) + PM_END
            self._send_response(address, txdata)

        resp = asyncio.async(proc_request(address, *args))
        resp.add_done_callback(resp_func)

    def _accept_response(self, msg_id, data, address):
        if msg_id not in self._outstanding:
            msgargs = (b64encode(msg_id), address)
            logger.info("received unknown message %s from %s; ignoring"
                        % msgargs)
            return

        future, timeout = self._outstanding[msg_id]
        timeout.cancel()

        err = check_remote_error(data)
        if err:
            future.set_exception(err)
        else:
            future.set_result(data)
        del self._outstanding[msg_id]

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            pass

        @asyncio.coroutine
        def func(address, *args):
            msg_id = sha1(str(random.getrandbits(255)).encode()).digest()
            data = pickle.dumps([name, args])
            if len(data) > 8192:
                raise RuntimeError('RPC message is too long! Max is 8K')

            txdata = b'\x00' + msg_id + data + PM_END

            isok = yield from self._send_request(address, txdata)
            if isok is False:
                future = asyncio.Future()
                future.set_result(None)
                return future

            loop = asyncio.get_event_loop()
            timeout = loop.call_later(self.wait_response_time,
                                      self._timeout, msg_id)
            future = asyncio.Future()
            self._outstanding[msg_id] = (future, timeout)
            ret = yield from future
            return ret
        return func

    def _send_response(self, address, data):
        raise RuntimeError('not implemented')

    def _send_request(self, address, txdata):
        raise RuntimeError('not implemented')

    def _timeout(self, msg_id):
        args = (b64encode(msg_id), self.wait_response_time)
        logger.info("Did not received reply for msg id %s within %i seconds"
                    % args)
        # self._outstanding[msg_id][0].set_exception(TimeoutException())
        self._outstanding[msg_id][0].set_result(None)

        del self._outstanding[msg_id]

    def _on_received(self, data, addr):
        if self.buf:
            data = self.buf + data

        while True:
            found = data.find(PM_END)
            if found == -1:
                self.buf += data
                return

            self.buf = data[found+3:]
            datagram = data[:found]

            msg_id = datagram[1:21]

            data = pickle.loads(datagram[21:])

            if datagram[0] == PM_REQUEST:
                self._accept_request(msg_id, data, addr)
            elif datagram[0] == PM_RESPONSE:
                self._accept_response(msg_id, data, addr)
            else:
                logger.info("Received unknown message from %s, ignoring",
                            repr(addr))

            data = self.buf


class UDPRPC(AbstractRPC):
    def datagram_received(self, data, addr):
        self._on_received(data, addr)

    @asyncio.coroutine
    def _send_request(self, address, data):
        self.transport.sendto(data, address)

    def _send_response(self, address, data):
        self.transport.sendto(data, address)


class TCPRPC(AbstractRPC):
    def __init__(self, wait_response_time=5):
        super().__init__(wait_response_time)
        self.connections = {}
        self.buf = bytes()
        self._keep_alive_timeout = 30

    def data_received(self, data):
        addr = self.transport.get_extra_info('peername')
        self._on_received(data, addr)

    def connection_lost(self, exc):
        logger.info('connection is lost (%s)' % exc)

    @asyncio.coroutine
    def __connect(self, address):
        loop = asyncio.get_event_loop()
        transport, protocol = yield from \
            loop.create_connection(lambda: self,
                                   address[0], address[1])
        return transport

    @asyncio.coroutine
    def _send_request(self, address, data):
        transport = self.connections.get(address, None)
        try:
            if transport is None:
                self.connections[address] = asyncio.Future()
                transport = yield from self.__connect(address)
                self.connections[address].set_result(transport)
                self.connections[address] = transport

            elif isinstance(transport, asyncio.Future):
                transport = yield from transport
        except OSError as err:
            del self.connections[address]
            logger.info(err)
            return False

        transport.write(data)

    def _send_response(self, address, data):
        self.transport.write(data)
