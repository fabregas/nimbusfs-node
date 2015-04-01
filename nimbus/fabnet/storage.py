import time
from io import StringIO, BufferedReader

from itertools import takewhile
import operator
from collections import OrderedDict


class IStorage(dict):
    """
    Local storage for this node.
    """

    def __setitem__(key, value):
        """
        Set a key to the given value.
        """

    def __getitem__(key):
        """
        Get the given key.  If item doesn't exist, raises C{KeyError}
        """

    def get(key, default=None):
        """
        Get given key.  If not found, return default.
        """

    def iteritems_older_than(secondsOld):
        """
        Return the an iterator over (key, value) tuples
        for items older than the given secondsOld.
        """

    def iteritems():
        """
        Get the iterator for this storage, should yield tuple of (key, value)
        """


class FSBasedStorage(object):
    def __init__(self, homedir):
        if not homedir.endswith('/'):
            homedir += '/'
        self.__home = homedir
        self.__rjournal = os.path.join(self.__home, '__rjournal__')

    def exists(self, key):
        return os.path.exists(self.__home + key)

    def stream_for_write(self):
        pass

    def save_stream(self, key, stream, user_id, rcount):
        pass

    def get_db_stream(self, key):
        pass

    def update_metadata(self, key, status, rcount, user_id):
        pass

    def remove_db(self, key):
        pass


class ForgetfulStorage(IStorage):
    def __init__(self, ttl=604800):
        """
        By default, max age is a week.
        """
        super(IStorage, self).__init__()
        self.data = OrderedDict()
        self.ttl = ttl

    def __setitem__(self, key, value):
        if key in self.data:
            del self.data[key]
        self.data[key] = (time.time(), value)
        self.cull()

    def stream_for_write(self):
        return StringIO()

    def save_stream(self, key, stream):
        self[key] = stream.getvalue()
        stream.close()

    def get_stream(self, key, default):
        if key in self.data:
            sio = StringIO(self.data[key][1])
            return sio

        return default

    def cull(self):
        for k, v in self.iteritems_older_than(self.ttl):
            self.data.popitem(last=False)

    def get(self, key, default=None):
        self.cull()
        if key in self.data:
            return self[key]
        return default

    def __getitem__(self, key):
        self.cull()
        return self.data[key][1]

    def __iter__(self):
        self.cull()
        return iter(self.data)

    def __repr__(self):
        self.cull()
        return repr(self.data)

    def iteritems_older_than(self, secondsOld):
        minBirthday = time.time() - secondsOld
        zipped = self._tripleIterable()
        matches = takewhile(lambda r: minBirthday >= r[1], zipped)
        return map(operator.itemgetter(0, 2), matches)

    def _tripleIterable(self):
        ikeys = self.data.keys()
        ibirthday = map(operator.itemgetter(0), self.data.values())
        ivalues = map(operator.itemgetter(1), self.data.values())
        return zip(ikeys, ibirthday, ivalues)

    def iteritems(self):
        self.cull()
        ikeys = self.data.keys()
        ivalues = map(operator.itemgetter(1), self.data.values())
        return zip(ikeys, ivalues)
