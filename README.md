#Py-RQ#

This library is set of Python and Lua scripts which enables you to easily implement queuing system based on Redis. 
All the queues works well in multi-threaded environment. The only thing you have to keep in mind is that with multiple consumers the order of the items is impossible to preserve. 
E.g. if multiple consumers exits unexpectedly and then you use re-enqueue method to get the items back to the queue then you will most probably lose the order of the items. 
If you want to rely on the order of the items then you are required to use only one consumer at a time, reject whole batch after failure and re-enqueue everything before getting another chunk of items.
Currently, it contains only one data structure called **Queue**.

##Requirements##
 - Python 3.x
 - Virutalenv
 - Redis

##Building##
Just use `make`.

##Tests##
Just use `make test`.

##Basic usage##
###Queue###
Use `from pyrq import Queue`.

If you need slave synchronization, use *synced_slaves_count* and *synced_slaves_timeout* arguments.

This data structure serves for managing queues of any **stringable** items. Typical data flow consist of several phases:
 1. Adding item (via `add_item(s)`)
 2. Getting item (via `get_items`)
 3. Acknowledging item (via `ack_item(s)`) when item was successfully processed **OR** rejecting item (via `reject_item(s)`) when error occurs.

**BEWARE!**. You must either acknowledge item or reject item. If you fail to do this, you have to clean internal processing queues created by **py-RQ**.

##Example##

```python
from pyrq import Queue

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=True)
queue = Queue(QUEUE_NAME, self.client, synced_slaves_enabled=True, synced_slaves_count=COUNT_OF_SLAVES, synced_slaves_timeout=TIMEOUT)

queue.add_item(value) # adding item

list_of_values = queue.get_items(10) # getting items
queue.ack_items(list_of_values) # acknowledging items or
queue.revert_items(list_of_values) # reverting item to queue
```
