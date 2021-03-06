# coding: UTF-8
import pickle

from werkzeug.contrib.cache import BaseCache
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from time import time
from bson.binary import Binary


class MongoCache(BaseCache):

    """Cache that uses MongoDB to store data.
    :param default_timeout: the default timeout (in seconds) that is used if no
                            timeout is specified on :meth:`set`. A timeout of 0
                            indicates that the cache never expires.
    """

    def __init__(self, default_timeout=300):
        super(MongoCache, self).__init__(default_timeout)
        _connection = MongoClient()
        _database = _connection['TestCache']
        self.collection = _database['Cache']

    def _pickle(self, obj):
        if not str(obj).isdigit():
            _bytes = pickle.dumps(obj)
            obj = Binary(_bytes)
        return obj

    def _unpickle(self, binary):
        if isinstance(binary, Binary):
            return pickle.loads(binary)
        return binary

    def _get_expiration(self, timeout):
        if timeout is None:
            timeout = self.default_timeout
        if timeout > 0:
            timeout = self._time() + timeout
        return timeout

    def _time(self):
        """
        Wrapper funcion for time.time() for easier mocking
        """
        return time()

    def _verify_timeout(self, doc):
        """Verifies if a document has expired.
        :param doc: document to verify.
        :returns: Whether the document has expired or not.
        :rtype: boolean
        """
        expires = doc['expires']
        if expires == 0:
            return False
        if expires >= self._time():
            return False
        return True

    def _get_doc(self, key, value, timeout):
        return {
            '_id': key,
            'value': self._pickle(value),
            'expires': self._get_expiration(timeout)
        }

    def get(self, key):
        """Look up key in the cache and return the value for it.
        :param key: the key to be looked up.
        :returns: The value if it exists and is readable, else ``None``.
        """
        _filter = {'_id': key}
        doc = self.collection.find_one(_filter)

        if doc and not self._verify_timeout(doc):
            return self._unpickle(doc['value'])

    def delete(self, key):
        """Delete `key` from the cache.
        :param key: the key to delete.
        :returns: Whether the key existed and has been deleted.
        :rtype: boolean
        """
        _filter = {'_id': key}
        count = self.collection.count(_filter)

        if count:
            self.collection.remove(_filter)
            return True
        return False

    def get_many(self, *keys):
        """Returns a list of values for the given keys.
        For each key a item in the list is created::
            foo, bar = cache.get_many("foo", "bar")
        Has the same error handling as :meth:`get`.
        :param keys: The function accepts multiple keys as positional
                     arguments.
        """
        key_x_value = self.get_dict(*keys)
        return [key_x_value[key] for key in keys]

    def get_dict(self, *keys):
        """Like :meth:`get_many` but return a dict::
            d = cache.get_dict("foo", "bar")
            foo = d["foo"]
            bar = d["bar"]
        :param keys: The function accepts multiple keys as positional
                     arguments.
        """
        result = {}
        documents = self.collection.find({'_id': {'$in': keys}})
        for document in documents:
            if self._verify_timeout(document):
                result[document['_id']] = None
            else:
                result[document['_id']] = self._unpickle(document['value'])
        return result

    def set(self, key, value, timeout=None):
        """Add a new key/value to the cache (overwrites value, if key already
        exists in the cache).
        :param key: the key to set
        :param value: the value for the key
        :param timeout: the cache timeout for the key (if not specified,
                        it uses the default timeout). A timeout of 0 idicates
                        that the cache never expires.
        :returns: ``True`` if key has been updated, ``False`` for backend
                  errors. Pickling errors, however, will raise a subclass of
                  ``pickle.PickleError``.
        :rtype: boolean
        """
        doc = self._get_doc(key, value, timeout)

        inserted = self.collection.save(doc)
        return True

    def add(self, key, value, timeout=None):
        """Works like :meth:`set` but does not overwrite the values of already
        existing keys.
        :param key: the key to set
        :param value: the value for the key
        :param timeout: the cache timeout for the key or the default
                        timeout if not specified. A timeout of 0 indicates
                        that the cache never expires.
        :returns: Same as :meth:`set`, but also ``False`` for already
                  existing keys.
        :rtype: boolean
        """
        if self.has(key):
            return False
        return self.set(key, value, timeout)

    def set_many(self, mapping, timeout=None):
        """Sets multiple keys and values from a mapping.
        :param mapping: a mapping with the keys/values to set.
        :param timeout: the cache timeout for the key (if not specified,
                        it uses the default timeout). A timeout of 0
                        indicates tht the cache never expires.
        :returns: Whether all given keys have been set.
        :rtype: boolean
        """
        values = [self._get_doc(key, value, timeout) for key, value in mapping.iteritems()]
        self.collection.insert_many(values)
        return True

    def delete_many(self, *keys):
        """Deletes multiple keys at once.
        :param keys: The function accepts multiple keys as positional
                     arguments.
        :returns: Whether all given keys have been deleted.
        :rtype: boolean
        """
        self.collection.remove({'_id': {'$in': keys}})
        return True

    def has(self, key):
        """Checks if a key exists in the cache without returning it. This is a
        cheap operation that bypasses loading the actual data on the backend.
        This method is optional and may not be implemented on all caches.
        :param key: the key to check
        """
        return self.collection.find_one({'_id': key}) is not None

    def clear(self):
        """Clears the cache.  Keep in mind that not all caches support
        completely clearing the cache.
        :returns: Whether the cache has been cleared.
        :rtype: boolean
        """
        self.collection.drop()
        return True

    def inc(self, key, delta=1):
        """Increments the value of a key by `delta`.  If the key does
        not yet exist it is initialized with `delta`.
        For supporting caches this is an atomic operation.
        :param key: the key to increment.
        :param delta: the delta to add.
        :returns: The new value or ``None`` for backend errors.
        """
        if self.has(key):
            _filter = {'_id': key}
            document = {'$inc': {'value': delta}}
            try:
                self.collection.update(_filter, document)
            except PyMongoError:
                return None
        else:
            self.add(key, delta)
        return self.get(key)

    def dec(self, key, delta=1):
        """Decrements the value of a key by `delta`.  If the key does
        not yet exist it is initialized with `-delta`.
        For supporting caches this is an atomic operation.
        :param key: the key to increment.
        :param delta: the delta to subtract.
        :returns: The new value or `None` for backend errors.
        """
        return self.inc(key, -delta)
