import unittest
import time
import socket
import os
from unittest.mock import patch

from redis import Redis
from pyrq.queues import Queue

QUEUE_NAME = os.getenv('QUEUE_NAME', 'test-queue')
PROCESSING_QUEUE_SCHEMA = QUEUE_NAME + '-processing-{}[{}][{}]'
TIMEOUT_QUEUE = QUEUE_NAME + '-timeouts'

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASS', None)


@patch('pyrq.helpers.wait_for_synced_slaves')
class TestQueue(unittest.TestCase):

    def setUp(self):
        synced_slaves_count = 1
        synced_slaves_timeout = 2
        self.client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD,
                            decode_responses=True)
        self.client.delete(QUEUE_NAME)
        self.queue_instance = Queue(QUEUE_NAME, self.client, synced_slaves_enabled=True,
                                    synced_slaves_count=synced_slaves_count,
                                    synced_slaves_timeout=synced_slaves_timeout)
        self.processing_queue = self.queue_instance.processing_queue_name
        self.timeouts_hash = self.queue_instance.timeouts_hash_name

    def tearDown(self):
        self.client.eval("""
            local keys = unpack(redis.call("keys", ARGV[1]))
            if keys then
                return redis.call("del", keys)
            end
        """, 0, QUEUE_NAME + '*')

    def test_add_items(self, slaves_mock):
        items = ['first-message', 'second-message']
        self.queue_instance.add_items(items)
        self.assertEqual(items[0], self.client.rpop(QUEUE_NAME))
        self.assertEqual(items[1], self.client.rpop(QUEUE_NAME))
        self.assertEqual(None, self.client.rpop(QUEUE_NAME))
        self.assertEqual(1, slaves_mock.call_count)

    def test_add_item(self, slaves_mock):
        for i in [3, 5, 3, 1]:
            self.queue_instance.add_item(i)
        self.assertEqual(['1', '3', '5', '3'], self.client.lrange(QUEUE_NAME, 0, 5))
        self.assertEqual(4, slaves_mock.call_count)

    def test_get_items(self, slaves_mock):
        for i in [3, 5, 3, 1]:
            self.client.lpush(QUEUE_NAME, i)
        self.assertEqual(['3', '5', '3'], self.queue_instance.get_items(3))
        self.assertEqual(['1'], self.queue_instance.get_items(1))
        self.assertEqual([], self.queue_instance.get_items(1))
        self.client.delete(self.queue_instance.processing_queue_name)
        self.client.delete(self.timeouts_hash)
        self.assertEqual(0, slaves_mock.call_count)

    def test_ack_item(self, slaves_mock):
        self.client.lpush(self.processing_queue, *[1, 5, 5, 3])

        saved_time = int(time.time())
        self.client.hset(self.timeouts_hash, self.processing_queue, saved_time)
        for i in [1, 5, 1]:
            self.queue_instance.ack_item(i)

        self.assertEqual(['3', '5'], self.client.lrange(self.processing_queue, 0, 5))
        self.assertEqual({self.processing_queue: str(saved_time)}, self.client.hgetall(self.timeouts_hash))

        for i in [5, 3]:
            self.queue_instance.ack_item(i)

        self.assertEqual(0, self.client.llen(self.processing_queue))
        self.assertEqual(5, slaves_mock.call_count)

    def test_ack_items(self, slaves_mock):
        self.client.lpush(self.processing_queue, *[1, 5, 5, 3, 6, 7])
        saved_time = int(time.time())
        self.client.hset(self.timeouts_hash, self.processing_queue, saved_time)
        self.queue_instance.ack_items([1, 5])
        self.queue_instance.ack_items([1])

        self.assertEqual(['7', '6', '3', '5'], self.client.lrange(self.processing_queue, 0, 5))
        self.assertEqual({self.processing_queue: str(saved_time)}, self.client.hgetall(self.timeouts_hash))

        self.queue_instance.ack_items([5, 3, 6])
        self.queue_instance.ack_items([7])
        self.assertEqual(0, self.client.llen(self.processing_queue))
        self.assertEqual(4, slaves_mock.call_count)

    def test_reject_item(self, slaves_mock):
        self.client.lpush(self.processing_queue, *[1, 5, 5, 3])
        saved_time = int(time.time())
        self.client.hset(self.timeouts_hash, self.processing_queue, saved_time)

        self.queue_instance.reject_item(1)
        self.queue_instance.reject_item(5)
        self.queue_instance.reject_item(1)

        self.assertEqual(['1', '5'], self.client.lrange(QUEUE_NAME, 0, 5))
        self.assertEqual(['3', '5'], self.client.lrange(self.processing_queue, 0, 5))
        self.assertEqual({self.processing_queue: str(saved_time)}, self.client.hgetall(self.timeouts_hash))

        self.queue_instance.reject_item(3)
        self.queue_instance.reject_item(5)
        self.assertEqual(['1', '5', '3', '5'], self.client.lrange(QUEUE_NAME, 0, 5))
        self.assertEqual(0, self.client.llen(self.processing_queue))
        self.assertEqual(5, slaves_mock.call_count)

    def test_reject_items(self, slaves_mock):
        self.client.lpush(self.processing_queue, *[1, 5, 5, 3, 6, 7])
        saved_time = int(time.time())
        self.client.hset(self.timeouts_hash, self.processing_queue, saved_time)

        self.queue_instance.reject_items([1, 5])
        self.queue_instance.reject_items([5])
        self.queue_instance.reject_items([9])

        self.assertEqual(['5', '1', '5'], self.client.lrange(QUEUE_NAME, 0, 5))
        self.assertEqual(['7', '6', '3'], self.client.lrange(self.processing_queue, 0, 5))
        self.assertEqual({self.processing_queue: str(saved_time)}, self.client.hgetall(self.timeouts_hash))

        self.queue_instance.reject_items([3, 6, 7])
        self.assertEqual(['5', '1', '5', '7', '6', '3'], self.client.lrange(QUEUE_NAME, 0, 10))
        self.assertEqual(0, self.client.llen(self.processing_queue))
        self.assertEqual(4, slaves_mock.call_count)

    def test_integration(self, slaves_mock):
        self.queue_instance.add_items([1, 5, 2, 6, 7])
        self.assertEqual(['1', '5', '2', '6', '7'], self.queue_instance.get_items(5))
        self.assertEqual([], self.queue_instance.get_items(1))
        self.queue_instance.ack_items([1, 5])
        self.assertEqual([], self.queue_instance.get_items(1))
        self.queue_instance.reject_items([2, 6, 7])
        self.assertEqual(['2', '6', '7'], self.queue_instance.get_items(5))
        self.queue_instance.ack_items([2, 6, 7])
        self.assertEqual(0, self.client.llen(QUEUE_NAME))
        self.assertEqual(4, slaves_mock.call_count)

    def test_re_enqueue_timeout_items(self, slaves_mock):
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        self.client.lpush(processing_queue1, 1, 5, 3)
        self.client.hset(TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        self.client.lpush(processing_queue2, 1, 4, 6)
        self.client.hset(TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        self.client.lpush(processing_queue3, 4, 7, 8)
        self.client.hset(TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        self.queue_instance.re_enqueue_timeout_items(7)

        self.assertEqual(['6', '4', '1', '3', '5', '1'], self.client.lrange(QUEUE_NAME, 0, 10))
        self.assertEqual(['8', '7', '4'], self.client.lrange(processing_queue3, 0, 5))
        self.assertEqual({processing_queue3: str(microtimestamp - 5)}, self.client.hgetall(TIMEOUT_QUEUE))
        self.assertEqual([QUEUE_NAME, processing_queue3, TIMEOUT_QUEUE], sorted(self.client.keys(QUEUE_NAME + '*')))

        self.queue_instance.re_enqueue_timeout_items(0)

        self.assertEqual(['6', '4', '1', '3', '5', '1', '8', '7', '4'], self.client.lrange(QUEUE_NAME, 0, 10))
        self.assertEqual([QUEUE_NAME], self.client.keys(QUEUE_NAME + '*'))

        self.assertEqual(2, slaves_mock.call_count)

    def test_re_enqueue_all_times(self, slaves_mock):
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        self.client.lpush(processing_queue1, 1, 5, 3)
        self.client.hset(TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        self.client.lpush(processing_queue2, 1, 4, 6)
        self.client.hset(TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        self.client.lpush(processing_queue3, 4, 7, 8)
        self.client.hset(TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        self.queue_instance.re_enqueue_all_items()

        self.assertEqual(['8', '7', '4', '6', '4', '1', '3', '5', '1'], self.client.lrange(QUEUE_NAME, 0, 10))
        self.assertEqual([QUEUE_NAME], self.client.keys(QUEUE_NAME + '*'))

        self.assertEqual(1, slaves_mock.call_count)

    def test_drop_timeout_items(self, slaves_mock):
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        self.client.lpush(processing_queue1, 1, 5, 3)
        self.client.hset(TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        self.client.lpush(processing_queue2, 1, 4, 6)
        self.client.hset(TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        self.client.lpush(processing_queue3, 4, 7, 8)
        self.client.hset(TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        self.queue_instance.drop_timeout_items(7)

        self.assertEqual([], self.client.lrange(QUEUE_NAME, 0, 5))
        self.assertEqual(['8', '7', '4'], self.client.lrange(processing_queue3, 0, 5))
        self.assertEqual({processing_queue3: str(microtimestamp - 5)}, self.client.hgetall(TIMEOUT_QUEUE))
        self.assertEqual([processing_queue3, TIMEOUT_QUEUE], sorted(self.client.keys(QUEUE_NAME + '*')))

        self.queue_instance.drop_timeout_items(0)

        self.assertEqual([], self.client.lrange(QUEUE_NAME, 0, 10))
        self.assertEqual([], self.client.keys(QUEUE_NAME + '*'))

        self.assertEqual(2, slaves_mock.call_count)

    def test_drop_all_items(self, slaves_mock):
        microtimestamp = time.time()
        timestamp = int(microtimestamp)

        processing_queue1 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 15)
        self.client.lpush(processing_queue1, 1, 5, 3)
        self.client.hset(TIMEOUT_QUEUE, processing_queue1, microtimestamp - 15)

        processing_queue2 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 10)
        self.client.lpush(processing_queue2, 1, 4, 6)
        self.client.hset(TIMEOUT_QUEUE, processing_queue2, microtimestamp - 10)

        processing_queue3 = PROCESSING_QUEUE_SCHEMA.format(socket.gethostname(), os.getpid(), timestamp - 5)
        self.client.lpush(processing_queue3, 4, 7, 8)
        self.client.hset(TIMEOUT_QUEUE, processing_queue3, microtimestamp - 5)

        self.queue_instance.drop_all_items()

        self.assertEqual([], self.client.lrange(QUEUE_NAME, 0, 10))
        self.assertEqual([], self.client.keys(QUEUE_NAME + '*'))

        self.assertEqual(1, slaves_mock.call_count)


if __name__ == 'main':
    unittest.main()
