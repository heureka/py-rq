import time
import socket
import os

from pyrq import helpers

CHUNK_SIZE = 10
SET_QUEUE_SUFFIX = '-unique'
PROCESSING_SUFFIX = '-processing'
PROCESSING_TIMEOUT_SUFFIX = '-timeouts'
PROCESSING_TIMEOUT = 7200  # seconds

DEFAULT_SYNC_SLAVES_COUNT = 0
DEFAULT_SYNC_SLAVES_TIMEOUT = 100


def _create_chunks(items):
    for chunk in [items[i:i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)]:
        yield chunk


class UniqueQueue(object):
    """
    UniqueQueue is a queue implemented using List as the queue, multiple Lists as the processing queues and a Hash as
    a storage for processing queue timeouts (so you can tell which processing queue is expired).
    UniqueQueue items are always unique, adding the same item more than once will be ignored.
    There is no priority whatsoever - items are processed as they were inserted into the queue.

    Queue needs a garbage collector process because the queue creates a processing queues every time you request items
    from it. This process is implemented by the methods reEnqueue* and drop* of this class and they should be called
    before getting the items or periodically (if you don't care about the order of the items).

    author: Jakub Chábek <jakub.chabek@heureka.cz>
    author: Vladimír Kašpar <vladimir.kaspar@heureka.cz>
    author: Heureka.cz <vyvoj@heureka.cz>
    """

    def __init__(self, queue_name: str, redis, **kwargs):
        """
        :param queue_name: Name of the queue
        :param redis: Redis client
        :param **kwargs: [
            synced_slaves_enabled: bool Enables slave synchronous syncing
            synced_slaves_count: int Number of slaves that need to be synced in order to continue
            synced_slaves_timeout: int Timeout for syncing slaves. If reached, exception is raised
        ]
        :return:
        """
        self.client_id = '{0}[{1}][{2}]'.format(socket.gethostname(), os.getpid(), int(time.time()))
        self.redis = redis
        self.queue_name = queue_name
        self.options = kwargs
        self._register_commands()

    def _register_commands(self):
        self.add_command = self.redis.register_script(self.QueueCommand.add())
        self.ack_command = self.redis.register_script(self.QueueCommand.ack())
        self.get_command = self.redis.register_script(self.QueueCommand.get())
        self.reject_command = self.redis.register_script(self.QueueCommand.reject())
        self.re_enqueue_command = self.redis.register_script(self.QueueCommand.re_enqueue())

    def get_count(self) -> int:
        """
        :return: Number of items in the queue
        """
        return self.redis.llen(self.queue_name)

    def add_item(self, item) -> bool:
        """
        :param item: Anything that is convertible to str
        """
        self.add_command(keys=[self.queue_name, self.set_name], args=[str(item)])

        self._wait_for_synced_slaves()

    def add_items(self, items: list):
        """
        :param items: List of items to be added via pipeline
        """
        pipeline = self.redis.pipeline()

        for chunk in helpers.create_chunks(items, CHUNK_SIZE):
            for item in chunk:
                self.add_command(keys=[self.queue_name, self.set_name], args=[str(item)], client=pipeline)
            pipeline.execute()

        self._wait_for_synced_slaves()

    def get_items(self, count: int) -> list:
        """
        :param count: Number of items to be returned
        :return: List of items
        """
        return self.get_command(keys=[self.queue_name, self.set_name, self.processing_queue_name,
                                      self.timeouts_hash_name],
                                args=[count, int(time.time())])

    def ack_item(self, item):
        """
        :param item: Anything that is convertible to str
        :return: Success
        """
        self.ack_command(keys=[self.processing_queue_name, self.timeouts_hash_name],
                         args=[str(item)])
        self._wait_for_synced_slaves()

    def ack_items(self, items: list):
        """
        :param items: List of items that are convertible to str
        """
        pipeline = self.redis.pipeline()
        for item in items:
            self.ack_command(keys=[self.processing_queue_name, self.timeouts_hash_name],
                             args=[str(item)],
                             client=pipeline)
        pipeline.execute()
        self._wait_for_synced_slaves()

    def reject_item(self, item):
        """
        :param item: Anything that is convertible to str
        """
        self.reject_command(keys=[self.queue_name, self.set_name, self.processing_queue_name, self.timeouts_hash_name],
                            args=[str(item)])
        self._wait_for_synced_slaves()

    def reject_items(self, items: list):
        """
        :param items: List of items that are convertible to str
        """
        pipeline = self.redis.pipeline()
        for item in reversed(items):
            self.reject_command(keys=[self.queue_name, self.set_name, self.processing_queue_name,
                                      self.timeouts_hash_name],
                                args=[str(item)],
                                client=pipeline)
        pipeline.execute()
        self._wait_for_synced_slaves()

    def re_enqueue_timeout_items(self, timeout: int=PROCESSING_TIMEOUT):
        """
        :param timeout: int seconds
        """
        for queue, value_time in self._get_sorted_processing_queues():
            if int(float(value_time)) + timeout < int(time.time()):
                self.re_enqueue_command(keys=[self.queue_name, self.set_name, queue, self.timeouts_hash_name])
        self._wait_for_synced_slaves()

    def re_enqueue_all_items(self):
        for queue, value_time in self._get_sorted_processing_queues():
            self.re_enqueue_command(keys=[self.queue_name, self.set_name, queue, self.timeouts_hash_name])
        self._wait_for_synced_slaves()

    def drop_timeout_items(self, timeout: int=PROCESSING_TIMEOUT):
        """
        :param timeout: int seconds
        """
        for queue, value_time in self._get_sorted_processing_queues():
            if int(float(value_time)) + timeout < int(time.time()):
                self.redis.delete(queue)
                self.redis.hdel(self.timeouts_hash_name, queue)
        self._wait_for_synced_slaves()

    def drop_all_items(self):
        for queue, value_time in self._get_sorted_processing_queues():
            self.redis.delete(queue)
            self.redis.hdel(self.timeouts_hash_name, queue)
        self._wait_for_synced_slaves()

    def _get_sorted_processing_queues(self):
        return sorted(self.redis.hscan(self.timeouts_hash_name)[1].items(), reverse=True)

    @property
    def set_name(self):
        """
        :return: Name of the set queue
        """
        return self.queue_name + SET_QUEUE_SUFFIX

    @property
    def processing_queue_name(self):
        """
        :return: Name of the processing queue
        """
        return self.queue_name + PROCESSING_SUFFIX + '-' + self.client_id

    @property
    def timeouts_hash_name(self):
        """
        :return: Name of the timeouts hash
        """
        return self.queue_name + PROCESSING_TIMEOUT_SUFFIX

    def _wait_for_synced_slaves(self):
        if self.options.get('synced_slaves_enabled'):
            count = self.options['synced_slaves_count'] if self.options['synced_slaves_count'] \
                else DEFAULT_SYNC_SLAVES_COUNT
            timeout = self.options['synced_slaves_timeout'] if self.options['synced_slaves_timeout'] \
                else DEFAULT_SYNC_SLAVES_TIMEOUT
            helpers.wait_for_synced_slaves(self.redis, count, timeout)

    class QueueCommand(object):

        @staticmethod
        def add():
            """
            :return: LUA Script for ACK command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local item = ARGV[1]

            local inQueue = redis.call('sismember', set, item)
            if inQueue == 0 then
                redis.call('lpush', queue, item)
                redis.call('sadd', set, item)
            end
            """

        @staticmethod
        def ack():
            """
            :return: LUA Script for ACK command
            """
            return """
            local processing = KEYS[1]
            local timeouts = KEYS[2]
            local item = ARGV[1]
            local result = redis.call('lrem', processing, -1, item)
            local count = redis.call('llen', processing)
            if count == 0 then
                redis.call('hdel', timeouts, processing)
            end
            """

        @staticmethod
        def get():
            """
            :return: LUA Script for GET command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local processing = KEYS[3]
            local timeouts = KEYS[4]
            local size = ARGV[1]
            local time = ARGV[2]
            redis.call('hset', timeouts, processing, time)
            local item
            local items = {}
            for i = 1, size, 1 do
                item = redis.call('rpoplpush', queue, processing)
                if not item then
                    break
                end
                redis.call('srem', set, item)
                table.insert(items, item)
            end
            return items
            """

        @staticmethod
        def reject():
            """
            :return: LUA Script for REJECT command
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local processing = KEYS[3]
            local timeouts = KEYS[4]
            local item = ARGV[1]
            local removed = redis.call('lrem', processing, -1, item)
            if removed == 1 then
                local inQueue = redis.call('sismember', set, item)
                if inQueue == 0 then
                    redis.call('rpush', queue, item)
                    redis.call('sadd', set, item)
                end
            end
            local count = redis.call('llen', processing)
            if count == 0 then
                redis.call('hdel', timeouts, processing)
            end
            """

        @staticmethod
        def re_enqueue():
            """
            :return: LUA Script for reject queue
            """
            return """
            local queue = KEYS[1]
            local set = KEYS[2]
            local processing = KEYS[3]
            local timeouts = KEYS[4]
            local item
            local inQueue
            while true do
                item = redis.call('lpop', processing);
                if not item then
                    break
                end
                inQueue = redis.call('sismember', set, item)
                if inQueue == 0 then
                    redis.call('rpush', queue, item)
                    redis.call('sadd', set, item)
                else
                    redis.call('lrem', queue, -1, item)
                    redis.call('rpush', queue, item)
                end
            end
            redis.call('hdel', timeouts, processing)
            """
