def wait_for_synced_slaves(redis, count: int, timeout: int):
    synced = redis.execute_command('WAIT', count, timeout)
    if synced < count:
        raise NotEnoughSyncedSlavesError('There are only {} synced slaves. Required {}'.format(synced, count))


class NotEnoughSyncedSlavesError(Exception):
    pass

