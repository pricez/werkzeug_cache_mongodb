# coding: UTF-8
from __future__ import absolute_import

import unittest

from pymongo import MongoClient

from mongo_cache import MongoCache


class MockData(object):

    def __init__(self, x):
        self.x = x

    def __eq__(self, mock_data):
        print mock_data
        return self.x == mock_data.x



class TestCache(unittest.TestCase):

    def setUp(self):
        self.cache = MongoCache(default_timeout=0)

    def tearDown(self):
        self.cache.collection.delete_many({})

    def test_get(self):
        x = MockData(1)

        self.cache.set('key-1', x)

        xc = self.cache.get('key-1')
        self.assertEqual(x, xc)

    def test_delete_existing(self):
        x = MockData(1)

        self.cache.set('key-1', x)

        self.assertTrue(self.cache.delete('key-1'))

    def test_delete_not_existing(self):
        self.assertFalse(self.cache.delete('key-1'))

    def test_set(self):
        x = MockData(1)
        self.cache.set('key-1', x)

        xc = self.cache.get('key-1')

        self.assertEqual(x, xc)

    def test_add_not_existing(self):
        x = MockData(1)

        added = self.cache.add('key-1', x)

        self.assertTrue(added)

    def test_add_existing(self):
        x = MockData(1)
        self.cache.set('key-1', x)

        y = MockData(2)
        added = self.cache.add('key-1', y)

        self.assertFalse(added)

    def test_clear(self):
        x = MockData(1)
        self.cache.set('key-1', x)

        cleared = self.cache.clear()
        xc = self.cache.get('key-1')

        self.assertTrue(cleared)
        self.assertIsNone(xc)

    def test_set_overwrite(self):
        x1 = MockData(1)
        key = 'key-set-overwrite'
        self.cache.set(key, x1)

        x2 = MockData(2)
        self.cache.set(key, x2)

        _filter = {'_id': key}
        count_keys = self.cache.collection.count(_filter)

        self.assertEqual(1, count_keys)


