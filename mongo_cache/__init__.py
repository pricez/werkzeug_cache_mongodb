# coding: UTF-8
import pickle

from werkzeug.contrib.cache import BaseCache
from pymongo import MongoClient

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
        _bytes = pickle.dumps(obj)
        return Binary(_bytes)

    def _unpickle(self, binary):
        return pickle.loads(binary)

    def get(self, key):
        """Look up key in the cache and return the value for it.
        :param key: the key to be looked up.
        :returns: The value if it exists and is readable, else ``None``.
        """
        _filter = {'_id': key}
        doc = self.collection.find_one(_filter)

        if doc:
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
            self.collection.delete(_filter)
            return True
        return False

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
        value = self._pickle(value)

        doc = {
            '_id': key,
            'value': value
        }

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
        _filter = {'_id': key}

        document = self.collection.find_one(_filter)
        if not document:
            return self.set(key, value, timeout)
        return False

    def clear(self):
        """Clears the cache.  Keep in mind that not all caches support
        completely clearing the cache.
        :returns: Whether the cache has been cleared.
        :rtype: boolean
        """
        self.collection.drop()
        return True
