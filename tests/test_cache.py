# coding: UTF-8
from __future__ import absolute_import

import unittest
import mock

from pymongo import MongoClient

from mongo_cache import MongoCache


class MockData(object):

    def __init__(self, x):
        self.x = x

    def __eq__(self, mock_data):
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

    def test_inc_with_exist_key(self):
        value = 10
        key = 'key-inc-with-exist-key'
        self.cache.set(key, value)

        delta = 9
        new_value = self.cache.inc(key, delta)

        value_cache = self.cache.get(key)

        result = delta + value

        self.assertEqual(result, value_cache)
        self.assertEqual(result, new_value)

    def test_inc_witho_exist_key(self):
        key = 'key-inc-without-exist-key'
        delta = 9
        new_value = self.cache.inc(key, delta)

        value_cache = self.cache.get(key)

        self.assertEqual(delta, value_cache)
        self.assertEqual(delta, new_value)

    def test_inc_with_error(self):
        value = MockData(1)
        key = 'key-inc-with-error'
        self.cache.add(key, value)

        delta = 9
        new_value = self.cache.inc(key, delta)

        value_cache = self.cache.get(key)

        self.assertEqual(value, value_cache)
        self.assertEqual(None, new_value)

    def test_has_with_add_key(self):
        key = 'key-has-with-add-key'
        value = MockData(1)

        self.cache.add(key, value)

        has_key = self.cache.has(key)

        self.assertTrue(has_key)

    def test_has_without_add_key(self):
        key = 'key-has-without-add-key'

        has_key = self.cache.has(key)

        self.assertFalse(has_key)

    def test_dec_with_exist_key(self):
        value = 10
        key = 'key-dec-with-exist-key'
        self.cache.set(key, value)

        delta = 9
        new_value = self.cache.dec(key, delta)

        value_cache = self.cache.get(key)

        result = value - delta

        self.assertEqual(result, value_cache)
        self.assertEqual(result, new_value)

    def test_dec_witho_exist_key(self):
        key = 'key-dec-without-exist-key'
        delta = 9
        new_value = self.cache.dec(key, delta)

        value_cache = self.cache.get(key)

        self.assertEqual(-delta, value_cache)
        self.assertEqual(-delta, new_value)

    def test_dec_with_error(self):
        value = MockData(1)
        key = 'key-dec-with-error'
        self.cache.add(key, value)

        delta = 9
        new_value = self.cache.dec(key, delta)

        value_cache = self.cache.get(key)

        self.assertEqual(value, value_cache)
        self.assertEqual(None, new_value)

    def test_get_many(self):
        key_x_value = {'key-%s' % i: MockData(i) for i in range(1, 11)}

        for key, value in key_x_value.items():
            self.cache.add(key, value)

        values = self.cache.get_many(*key_x_value.keys())

        self.assertEqual(10, len(values))
        for _return, _value in zip(values, key_x_value.values()):
            self.assertEqual(_value, _return)

    def test_get_dict(self):
        key_x_value = {'key-%s' % i: MockData(i) for i in range(1, 6)}

        for key, value in key_x_value.items():
            self.cache.add(key, value)

        results = self.cache.get_dict(*key_x_value.keys())

        self.assertIsInstance(results, dict)
        for key, value in key_x_value.items():
            self.assertIn(key, results)
            self.assertEqual(key_x_value[key], results[key])

    def test_delete_many(self):
        key_x_value = {'key-%s' % i: MockData(i) for i in range(1, 6)}

        for key, value in key_x_value.items():
            self.cache.add(key, value)

        self.assertEqual(5, self.cache.collection.count({'_id': {'$in': key_x_value.keys()}}))

        result = self.cache.delete_many(*key_x_value.keys())

        self.assertTrue(result)
        self.assertEqual(0, self.cache.collection.count({'_id': {'$in': key_x_value.keys()}}))

    def test_set_many(self):
        key_x_value = {'key-set-many-%s' % i: MockData(i) for i in range(1, 6)}

        result = self.cache.set_many(key_x_value)

        self.assertTrue(result)
        self.assertEqual(5, self.cache.collection.count({'_id': {'$in': key_x_value.keys()}}))

@mock.patch('mongo_cache.MongoCache._time')
class TestTimeout(unittest.TestCase):

    def setUp(self):
        self.cache = MongoCache()

    def tearDown(self):
        self.cache.collection.delete_many({})

    def test_set(self, mock_time):
        key = 'key-set'
        mock_time.return_value = 100

        self.cache.set(key, MockData(1), timeout=300)

        doc = self.cache.collection.find_one({'_id': key})

        self.assertIn('expires', doc)
        self.assertEqual(400, doc['expires'])
        self.assertIn('value', doc)

    def test_get_not_expired(self, mock_time):
        key = 'key-not-expired'
        mock_time.return_value = 100

        self.cache.set(key, MockData(1), timeout=100)

        mock_time.return_value = 150
        result = self.cache.get(key)

        self.assertIsNotNone(result)
        self.assertEqual(result, MockData(1))

    def test_get_timeout_0(self, mock_time):
        key = 'key-not-expired'
        mock_time.return_value = 100

        self.cache.set(key, MockData(1), timeout=0)

        result = self.cache.get(key)

        self.assertIsNotNone(result)
        self.assertEqual(result, MockData(1))

    def test_get_expired(self, mock_time):
        key = 'key-get-expired'
        mock_time.return_value = 100

        self.cache.set(key, MockData(1), timeout=100)

        mock_time.return_value = 201
        result = self.cache.get(key)

        self.assertIsNone(result)

    def test_get_many_expired(self, mock_time):
        key_timeout_1 = 'key-timeout-1'
        key_timeout_100 = 'key-timeout-100'

        mock_time.return_value = 100

        self.cache.set(key_timeout_1, MockData(1), timeout=1)
        self.cache.set(key_timeout_100, MockData(1), timeout=100)

        mock_time.return_value = 150

        results = self.cache.get_many(*[key_timeout_1, key_timeout_100])

        self.assertIsNone(results[0])
        self.assertIsNotNone(results[1])
