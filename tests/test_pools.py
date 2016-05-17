import unittest
import time
import os
from unittest.mock import patch

from redis import Redis
from pyrq.pools import Pool

POOL_NAME = os.getenv('POOL_NAME', 'test-pool')

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASS', None)

TEST_TIME = 1444222459.0


@patch('pyrq.helpers.wait_for_synced_slaves')
class TestPool(unittest.TestCase):

    def setUp(self):
        synced_slaves_count = 1
        synced_slaves_timeout = 2
        self.client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD,
                            decode_responses=True)
        self.client.delete(POOL_NAME)
        self.pool_instance = Pool(POOL_NAME, self.client, synced_slaves_enabled=True,
                                  synced_slaves_count=synced_slaves_count,
                                  synced_slaves_timeout=synced_slaves_timeout)

    def tearDown(self):
        self.client.delete(POOL_NAME)

    def test_get_count(self, slaves_mock):
        self._load_items_to_pool('a', 'b')

        self.assertEquals(2, self.pool_instance.get_count())
        self.assertEquals([POOL_NAME], self.client.keys())

    def test_get_count_to_process(self, slaves_mock):
        timestamp = int(time.time())
        self.client.zadd(POOL_NAME, 'a', timestamp - 5, 'b', timestamp - 3, c=timestamp + 5)

        self.assertEquals(2, self.pool_instance.get_count_to_process())
        self.assertEquals([POOL_NAME], self.client.keys())

    def test_is_in_pool(self, slaves_mock):
        self._load_items_to_pool('a', 'b')
        self.assertTrue(self.pool_instance.is_in_pool('a'))
        self.assertTrue(self.pool_instance.is_in_pool('b'))
        self.assertFalse(self.pool_instance.is_in_pool('whatever'))

    @patch('pyrq.pools.time.time')
    def test_add_item(self, time_mock, slaves_mock):
        time_mock.return_value = TEST_TIME

        self.pool_instance.add_item('test1')
        self.pool_instance.add_item('test2')
        self.pool_instance.add_item('test3')
        self.pool_instance.add_item('test2')

        self.assertEquals([('test1', TEST_TIME), ('test2', TEST_TIME), ('test3', TEST_TIME)],
                          self.client.zrange(POOL_NAME, 0, 5, withscores=True))
        self.assertEquals([POOL_NAME], self.client.keys())

    @patch('pyrq.pools.time.time')
    def test_add_items(self, time_mock, slaves_mock):
        time_mock.return_value = TEST_TIME

        self.pool_instance.add_items(['test1', 'test2', 'test3', 'test2'])

        self.assertEquals([('test1', TEST_TIME), ('test2', TEST_TIME), ('test3', TEST_TIME)],
                          self.client.zrange(POOL_NAME, 0, 5, withscores=True))
        self.assertEquals([POOL_NAME], self.client.keys())

    def test_get_items(self, slaves_mock):
        self._load_items_to_pool('a', 'b', 'c')

        self.assertEquals(['a', 'b'], self.pool_instance.get_items(2))
        self.assertEquals(['c'], self.pool_instance.get_items(2))
        self.assertEquals([], self.pool_instance.get_items(2))

        self.assertEquals([POOL_NAME], self.client.keys())

    def test_get_all_items(self, slaves_mock):
        items = ['a', 'b', 'c']
        self._load_items_to_pool(*items)

        self.assertEquals(items, self.pool_instance.get_all_items())

        self.assertEquals([POOL_NAME], self.client.keys())

    @patch('pyrq.pools.time.time')
    def test_ack_item(self, time_mock, slaves_mock):
        time_mock.return_value = TEST_TIME
        self._load_test_data_to_pool()

        self.pool_instance.ack_item('a')
        self.pool_instance.ack_item('c')
        self.pool_instance.ack_item('b')

        self.assertEquals(5, self.client.zcard(POOL_NAME))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 'a')))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 'b')))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 'c')))
        self.assertEquals(TEST_TIME + 600.1, self.client.zscore(POOL_NAME, 'd'))
        self.assertEquals(TEST_TIME + 5, int(self.client.zscore(POOL_NAME, 'e')))

        self.assertEquals([POOL_NAME], self.client.keys())

    @patch('pyrq.pools.time.time')
    def test_ack_items(self, time_mock, slaves_mock):
        time_mock.return_value = TEST_TIME
        self._load_test_data_to_pool()

        self.pool_instance.ack_items(['a'])
        self.pool_instance.ack_items(['c', 'b'])

        self.assertEquals(5, self.client.zcard(POOL_NAME))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 'a')))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 'b')))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 'c')))
        self.assertEquals(TEST_TIME + 600.1, self.client.zscore(POOL_NAME, 'd'))
        self.assertEquals(TEST_TIME + 5, int(self.client.zscore(POOL_NAME, 'e')))

        self.assertEquals([POOL_NAME], self.client.keys())

    def test_remove_item(self, slaves_mock):
        self._load_test_data_to_pool()

        self.pool_instance.remove_item('a')
        self.pool_instance.remove_item('d')
        self.pool_instance.remove_item('c')

        self.assertEquals(2, self.client.zcard(POOL_NAME))
        self.assertEquals(['e', 'b'], self.client.zrange(POOL_NAME, 0, 5))

        self.assertEquals([POOL_NAME], self.client.keys())

    def test_remove_items(self, slaves_mock):
        self._load_test_data_to_pool()

        self.pool_instance.remove_items(['a'])
        self.pool_instance.remove_items(['d', 'c'])

        self.assertEquals(2, self.client.zcard(POOL_NAME))
        self.assertEquals(['e', 'b'], self.client.zrange(POOL_NAME, 0, 5))

        self.assertEquals([POOL_NAME], self.client.keys())

    def test_clear_pool(self, slaves_mock):
        self._load_test_data_to_pool()

        self.pool_instance.clear_pool()

        self.assertEquals([], self.client.keys())

    @patch('pyrq.pools.time.time')
    def test_real_use_case_example(self, time_mock, slaves_mock):
        time_mock.return_value = TEST_TIME

        test_data = [1, 2, 3, 4, 5, 6, 7]
        self.pool_instance.add_items(test_data)

        self.assertEquals(7, self.client.zcard(POOL_NAME))
        self.assertEquals([str(item) for item in test_data], self.client.zrange(POOL_NAME, 0, 10))

        self.client.zadd(POOL_NAME, 7, TEST_TIME + 5)

        self.assertEquals(['1', '2', '3'], self.pool_instance.get_items(3))

        self.pool_instance.ack_item(1)
        self.pool_instance.ack_items([3])

        self.assertEquals(['4', '5', '6'], self.pool_instance.get_items(3))

        self.pool_instance.ack_items([2, 4, 5])

        self.assertEquals(7, self.client.zcard(POOL_NAME))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 1)))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 2)))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 3)))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 4)))
        self.assertEquals(TEST_TIME + 129600, int(self.client.zscore(POOL_NAME, 5)))
        self.assertEquals(TEST_TIME + 600.1, self.client.zscore(POOL_NAME, 6))
        self.assertEquals(TEST_TIME + 5, int(self.client.zscore(POOL_NAME, 7)))

        self.pool_instance.remove_item(7)

        self.assertEquals(7, self.client.zcard(POOL_NAME))

        self.pool_instance.remove_items([6])

        self.assertEquals(6, self.client.zcard(POOL_NAME))

        self.assertEquals([POOL_NAME], self.client.keys())

    def _load_test_data_to_pool(self):
        prepared_items = [
            'a', TEST_TIME - 10 + 600.1,
            'b', TEST_TIME - 5 + 600.1,
            'c', TEST_TIME - 2 + 600.1,
            'd', TEST_TIME + 600.1,
            'e', TEST_TIME + 5
        ]
        self.client.zadd(POOL_NAME, *prepared_items)

    def _load_items_to_pool(self, *args):
        timestamp = int(time.time())
        prepared_items = []
        for item in args:
            prepared_items.append(item)
            prepared_items.append(timestamp)
        self.client.zadd(POOL_NAME, *prepared_items)

