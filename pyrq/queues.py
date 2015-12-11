import time
import socket
import os

import pyrq.helpers

CHUNK_SIZE = 10
PROCESSING_SUFFIX = '-processing'
PROCESSING_TIMEOUT_SUFFIX = '-timeouts'
PROCESSING_TIMEOUT = 7200  # seconds

DEFAULT_SYNC_SLAVES_COUNT = 0
DEFAULT_SYNC_SLAVES_TIMEOUT = 100


def _create_chunks(items):
    for chunk in [items[i:i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)]:
        yield chunk


class Queue(object):
    """
    Queue is a simple queue implemented using List as the queue, multiple Lists as the processing queues and a Hash as
    a storage for processing queue timeouts (so you can tell which processing queue is expired).
    There is no priority whatsoever - items are processed as they were inserted into the queue.

    Queue needs a garbage collector process because the queue creates a processing queues every time you request items
    from it. This process is implemented by the methods reEnqueue* and drop* of this class and they should be called
    before getting the items or periodically (if you don't care about the order of the items).

    author: Jakub Chábek <jakub.chabek@heureka.cz>
    author: Jan Chmelíček <jan.chmelicek@heureka.cz>
    author: Vladimír Kašpar <vladimir.kaspar@heureka.cz>
    author: Heureka.cz <vyvoj@heureka.cz>
    """

    def __init__(self, name: str, redis, **kwargs):
        """
        :param name: Name of the queue
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
        self.name = name
        self.options = kwargs
        self._register_commands()

    def _register_commands(self):
        self.ack_command = self.redis.register_script(self.QueueCommand.ack())
        self.get_command = self.redis.register_script(self.QueueCommand.get())
        self.reject_command = self.redis.register_script(self.QueueCommand.reject())
        self.re_enqueue_command = self.redis.register_script(self.QueueCommand.re_enqueue())

    def get_count(self) -> int:
        """
        :return: Number of items in the queue
        """
        return self.redis.llen(self.name)

    def add_item(self, item) -> bool:
        """
        :param item: Anything that is convertible to str
        :return: Returns true if item was inserted into queue, false otherwise
        """
        result = self.redis.lpush(self.name, item)
        self._wait_for_synced_slaves()
        return result

    def add_items(self, items: list):
        """
        :param items: List of items to be added via pipeline
        """
        pipeline = self.redis.pipeline()
        for chunk in _create_chunks(items):
            pipeline.lpush(self.name, *chunk)
        pipeline.execute()
        self._wait_for_synced_slaves()

    def get_items(self, count: int) -> list:
        """
        :param count: Number of items to be returned
        :return: List of items
        """
        return self.get_command(keys=[self.name, self.processing_queue_name, self.timeouts_hash_name],
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
            self.ack_command(keys=[self.processing_queue_name, self.timeouts_hash_name], args=[str(item)],
                             client=pipeline)
        pipeline.execute()
        self._wait_for_synced_slaves()

    def reject_item(self, item):
        """
        :param item: Anything that is convertible to str
        """
        self.reject_command(keys=[self.name, self.processing_queue_name, self.timeouts_hash_name],
                                     args=[str(item)])
        self._wait_for_synced_slaves()

    def reject_items(self, items: list):
        """
        :param items: List of items that are convertible to str
        """
        pipeline = self.redis.pipeline()
        for item in reversed(items):
            self.reject_command(keys=[self.name, self.processing_queue_name, self.timeouts_hash_name], args=[str(item)],
                                client=pipeline)
        pipeline.execute()
        self._wait_for_synced_slaves()

    def re_enqueue_timeout_items(self, timeout: int=PROCESSING_TIMEOUT):
        """
        :param timeout: int seconds
        """
        for queue, value_time in self._get_sorted_processing_queues():
            if int(float(value_time)) + timeout < int(time.time()):
                self.re_enqueue_command(keys=[self.name, queue, self.timeouts_hash_name])
        self._wait_for_synced_slaves()

    def re_enqueue_all_items(self):
        for queue, value_time in self._get_sorted_processing_queues():
            self.re_enqueue_command(keys=[self.name, queue, self.timeouts_hash_name])
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
    def processing_queue_name(self):
        """
        :return: Name of the processing queue
        """
        return self.name + PROCESSING_SUFFIX + '-' + self.client_id

    @property
    def timeouts_hash_name(self):
        """
        :return: Name of the timeouts hash
        """
        return self.name + PROCESSING_TIMEOUT_SUFFIX

    def _wait_for_synced_slaves(self):
        if self.options.get('synced_slaves_enabled'):
            count = self.options['synced_slaves_count'] if self.options['synced_slaves_count'] \
                else DEFAULT_SYNC_SLAVES_COUNT
            timeout = self.options['synced_slaves_timeout'] if self.options['synced_slaves_timeout'] \
                else DEFAULT_SYNC_SLAVES_TIMEOUT
            pyrq.helpers.wait_for_synced_slaves(self.redis, count, timeout)

    class QueueCommand(object):

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
            local processing = KEYS[2]
            local timeouts = KEYS[3]
            local size = ARGV[1]
            local time = ARGV[2]

            local item
            local items = {}

            redis.call('hset', timeouts, processing, time)

            for i = 1, size, 1 do
                item = redis.call('rpoplpush', queue, processing)

                if not item then
                    break
                end

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
            local processing = KEYS[2]
            local timeouts = KEYS[3]
            local item = ARGV[1]

            local removed = redis.call('lrem', processing, -1, item)

            if removed == 1 then
                redis.call('rpush', queue, item)
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
            local processing = KEYS[2]
            local timeouts = KEYS[3]

            local item
            while true do
                item = redis.call('lpop', processing);

                if not item then
                    break
                end

                redis.call('rpush', queue, item)
            end

            redis.call('hdel', timeouts, processing)
            """
