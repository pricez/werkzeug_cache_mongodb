# coding: UTF-8
from __future__ import absolute_import

import unittest

from pymongo import MongoClient

from mongo_cache import MongoCache


class MockData(object):

    def __init__(self, x):
        self.x = x


class TestCache(unittest.TestCase):

    def setUp(self):
        self.mongo = MongoClient()
        self.db = self.mongo['test-db']
        self.cache_coll = self.db['cache']
        self.cache = MongoCache()

    def tearDown(self):
        self.cache.delete_many({})

    def test_get(self):
        x = MockData(1)
        data = self.cache._pickle(x)
        self.cache_coll.test.insert_one({'key': 'key-1', 'data': data})
        xc = self.cache.get('key-1')

        self.assertEqual(x, xc)

    def test_delete_existing(self):
        pass

    def test_delete_not_existing(self):
        pass

    def test_set(self):
        pass

    def test_add_not_existing(self):
        pass

    def test_add_existing(self):
        pass

    def test_clear(self):
        pass
