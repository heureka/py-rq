"""
Copyright (c) 2021 Heureka Group a.s. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


def wait_for_synced_slaves(redis, count: int, timeout: int):
    synced = redis.execute_command('WAIT', count, timeout)
    if synced < count:
        raise NotEnoughSyncedSlavesError('There are only {} synced slaves. Required {}'.format(synced, count))


def create_chunks(items, chunk_size):
    for chunk in [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]:
        yield chunk


class NotEnoughSyncedSlavesError(Exception):
    pass
