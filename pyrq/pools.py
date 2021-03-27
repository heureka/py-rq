import time
import socket
import os

from pyrq import helpers

DEFAULT_CHUNK_SIZE = 100
DEFAULT_SYNC_SLAVES_COUNT = 0
DEFAULT_SYNC_SLAVES_TIMEOUT = 100
DEFAULT_ACK_TTL = 600  # seconds
DEFAULT_ACK_VALID_FOR = 129600  # seconds


class Pool(object):
    def __init__(self, name: str, redis, **kwargs):
        """
        Pool is intended for "queues" which are most of the time the same - the items need to be processed periodically.
        This is exactly how the pool works - it processes items that are "outdated". When the item is processed (ACKed),
        the validity of the item is set accordingly to the options (e.g. for 36 hours as of default).

        There is no need for a garbage collector process - items that were failed to process are automatically processed
        after the ACK_TTL time has passed.

        :param name: Name of the pool
        :param redis: Redis client
        :param **kwargs: [
            chunk_size: int Size of chunks
            synced_slaves_enabled: bool Enables slave synchronous syncing
            synced_slaves_count: int Number of slaves that need to be synced in order to continue
            synced_slaves_timeout: int Timeout for syncing slaves. If reached, exception is raised
            ack_ttl: int Acknowledge timeout of the just processed items
        ]
        """
        self.client_id = '{0}[{1}][{2}]'.format(socket.gethostname(), os.getpid(), int(time.time()))
        self.redis = redis
        self.name = name
        self.options = self._load_options(kwargs)
        self._register_commands()

    @staticmethod
    def _load_options(kwargs):
        return {
            'chunk_size': kwargs.get('chunk_size', DEFAULT_CHUNK_SIZE),
            'synced_slaves_enabled': kwargs.get('synced_slaves_enabled', False),
            'synced_slaves_count': kwargs.get('synced_slaves_count', DEFAULT_SYNC_SLAVES_COUNT),
            'synced_slaves_timeout': kwargs.get('synced_slaves_timeout', DEFAULT_SYNC_SLAVES_TIMEOUT),
            'ack_ttl': kwargs.get('ack_ttl', DEFAULT_ACK_TTL),
            'ack_valid_for': kwargs.get('ack_valid_for', DEFAULT_ACK_VALID_FOR)
        }

    def _register_commands(self):
        self.ack_command = self.redis.register_script(self.PoolCommand.ack())
        self.get_command = self.redis.register_script(self.PoolCommand.get())
        self.remove_command = self.redis.register_script(self.PoolCommand.remove())

    def get_count(self) -> int:
        """
        :return: Number of items in the pool
        """
        return self.redis.zcard(self.name)

    def get_count_to_process(self) -> int:
        """
        :return: Number of items in the pool which should be processed
        """
        return self.redis.zcount(self.name, '-inf', int(time.time()))

    def is_in_pool(self, item) -> bool:
        """
        :return: Checks if the given item is present in the pool
        """
        return self.redis.zscore(self.name, item) is not None

    def add_item(self, item):
        """
        :param item: Anything that is convertible to str
        """
        self.redis.zadd(self.name, {item: int(time.time())})
        self._wait_for_synced_slaves()

    def add_items(self, items):
        """
        :param items: List of items to be added via pipeline
        """
        pipeline = self.redis.pipeline()
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            current_time = int(time.time())
            prepared_items = {
                item: current_time
                for item in chunk
            }
            pipeline.zadd(self.name, prepared_items)
        pipeline.execute()
        self._wait_for_synced_slaves()

    def get_items(self, count: int) -> list:
        """
        :param count: Number of items to be returned
        :return: List of items
        """
        return self.get_command(keys=[self.name],
                                args=[count, int(time.time()), self.options['ack_ttl']])

    def get_all_items(self) -> list:
        """
        :return: List of all items
        """
        result = []
        while True:
            chunk = self.get_items(self.options['chunk_size'])
            result += chunk

            if len(chunk) < self.options['chunk_size']:
                break
        return result

    def ack_item(self, item):
        """ Acknowledges an item that was processed correctly
        :param item: Anything that is convertible to str
        """
        self.ack_command(keys=[self.name],
                         args=[item, int(time.time()) + self.options['ack_valid_for']])
        self._wait_for_synced_slaves()

    def ack_items(self, items):
        """ Acknowledges items that were processed correctly
        :param items: List of items that are convertible to str
        """
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            pipeline = self.redis.pipeline()
            for item in chunk:
                self.ack_command(keys=[self.name],
                                 args=[item, int(time.time()) + self.options['ack_valid_for']])
            pipeline.execute()
            self._wait_for_synced_slaves()

    def remove_item(self, item):
        """ Removes an item that is no longer valid
        :param item: Anything that is convertible to str
        """
        self.remove_command(keys=[self.name],
                            args=[item])
        self._wait_for_synced_slaves()

    def remove_items(self, items):
        """ Removes an item that is no longer valid
        :param items: List of items that are convertible to str
        """
        for chunk in helpers.create_chunks(items, self.options['chunk_size']):
            pipeline = self.redis.pipeline()
            for item in chunk:
                self.remove_command(keys=[self.name],
                                    args=[item])
            pipeline.execute()
            self._wait_for_synced_slaves()

    def clear_pool(self):
        """ Clears all the items from the pool """
        while True:
            removed = self.redis.zremrangebyrank(self.name, 0, self.options['chunk_size'])
            if not removed:
                break

    def _wait_for_synced_slaves(self):
        if self.options['synced_slaves_enabled']:
            helpers.wait_for_synced_slaves(self.redis, self.options['synced_slaves_count'],
                                           self.options['synced_slaves_timeout'])

    class PoolCommand(object):

        @staticmethod
        def ack():
            """
            :return: LUA Script for ACK command
            """
            return """
            local pool = KEYS[1]
            local item = ARGV[1]
            local validUntil = ARGV[2]

            local score = redis.call('zscore', pool, item)
            if score and score - math.floor(score) > 0.01 then
                redis.call('zadd', pool, validUntil, item)
            end
            """

        @staticmethod
        def get():
            """
            :return: LUA Script for GET command
            """
            return """
            local pool = KEYS[1]
            local size = ARGV[1]
            local time = ARGV[2]
            local ackTTL = ARGV[3]

            local result = redis.call('zrangebyscore', pool, '-inf', time, 'WITHSCORES', 'LIMIT', 0, size)
            local finalResult = {}
            local i
            local value
            local score
            for i = 1, #result, 2 do
                value = result[i]
                score = math.floor(result[i + 1])
                redis.call('zadd', pool, score + tonumber(ackTTL) + 0.1, value)
                table.insert(finalResult, value)
            end

            return finalResult
            """

        @staticmethod
        def remove():
            """
            :return: LUA Script for REMOVE command
            """
            return """
            local pool = KEYS[1]
            local item = ARGV[1]

            local score = redis.call('zscore', pool, item)
            if score and score - math.floor(score) > 0.01 then
                redis.call('zrem', pool, item)
            end
            """
