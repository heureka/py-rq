def wait_for_synced_slaves(redis, count: int, timeout: int):
    synced = redis.execute_command('WAIT', count, timeout)
    if synced < count:
        raise NotEnoughSyncedSlavesError('There are only {} synced slaves. Required {}'.format(synced, count))


def create_chunks(items, chunk_size):
    for chunk in [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]:
        yield chunk


class NotEnoughSyncedSlavesError(Exception):
    pass

