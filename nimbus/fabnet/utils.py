
import hashlib
import logging
import logging.handlers
import socket
import sys
import asyncio
import operator


def digest(s):
    if not isinstance(s, str):
        s = str(s)
    return hashlib.sha1(s.encode()).digest()


@asyncio.coroutine
def future_list(ds, callback):
    results = []
    for future in ds:
        res = yield from future
        results.append(res)

    return callback(results)


@asyncio.coroutine
def future_dict(ds, callback):
    results = {}
    for key, future in ds.items():
        results[key] = yield from future

    return callback(results)


class OrderedSet(list):
    def push(self, thing):
        """
        1. If the item exists in the list, it's removed
        2. The item is pushed to the end of the list
        """
        if thing in self:
            self.remove(thing)
        self.append(thing)


def shared_prefix(args):
    """
    Find the shared prefix between the strings.

    For instance:

        sharedPrefix(['blahblah', 'blahwhat'])

    returns 'blah'.
    """
    i = 0
    while i < min(map(len, args)):
        if len(set(map(operator.itemgetter(i), args))) != 1:
            break
        i += 1
    return args[0][:i]


def init_logger(logger_name='localhost', to_console=True):
    logger = logging.getLogger(logger_name)

    logger.setLevel(logging.INFO)

    if sys.platform == 'darwin':
        log_path = '/var/run/syslog'
    else:
        log_path = '/dev/log'

    formatter = logging.Formatter('%(name)s %(levelname)s '
                                  '[%(threadName)s] %(message)s')
    hdlr = logging.handlers.SysLogHandler(log_path)
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)

    if to_console:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger

logger = init_logger('fabnet')
