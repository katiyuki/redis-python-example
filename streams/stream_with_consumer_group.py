import random
import sys
import threading
import time

import redis

'''
This example will show how to use consumer groups with redis streams. When 
using consumer groups, redis will share parts of the stream among each consumer
in the consumer group. After processing of a message, your consumer needs to 
execute an "XACK" on the message processed, otherwise that message will put 
in a "pending" list, and will be available to other consumers, in the 
consumer group, to claim.

NOTE: When running multiple times, I will execute a FLUSHALL on my local
redis (this deletes ALL keys, consumer group names, and stream names 
from redis so make sure that is exactly what you want).
'''
STREAM_NAME = 'tasks-1'
CONSUMER_GROUP_NAME = 'consumer-group-1'
CONSUMERS = [
    'consumer-1',
    'consumer-2',
    'consumer-3'
]
r_conn = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

try:
    # can't create a consumer group of the same name more than once. if attempted,
    # an exception is thrown with the message containing "BUSYGROUP"
    r_conn.xgroup_create(STREAM_NAME, CONSUMER_GROUP_NAME, id="0", mkstream=True)
except Exception as ex:
    if "BUSYGROUP" in str(ex):
        print(f"Consumer group already created. skipping...")
    else:
        print(f"Exception thrown: {str(ex)}")
        sys.exit(1)

# this gives info on the state of the consumer group:
# example: [{'name': 'consumer-group-1', 'consumers': 0, 'pending': 0, 'last-delivered-id': '0-0'}]
print(f"info on consumer group = {r_conn.xinfo_groups(STREAM_NAME)}")


# producer: will be a separate thread/location/project/container
def message_producer():
    '''Creating a simple message that contains a worker id'''
    start_value = 2000
    total_ids = 10
    worker_ids = range(start_value, start_value + total_ids)
    for wid in worker_ids:
        # https://redis.io/commands/xadd
        r_conn.xadd(STREAM_NAME, {"worker_id": wid})
        print(f"added worker id {wid}")
        # sleeping between adding to redis to simulate
        # the producer project taking some time to create
        # messages as it is processing its own tasks
        time.sleep(random.randrange(1, 3, 1))


# consumer: will be a separate thread/location/project/container
def message_consumer(consumer_name):
    '''
    Consuming messages, on consumer_name (in consumer group)
    as they are added to the stream.
    :param consumer_name: name of this consumer
    :type consumer_name: str
    :return: True
    :rtype: bool
    '''
    count = 0
    print(f"on {consumer_name}...")

    while True:
        # NOTE: Need to keep the ID (specified in the STREAMS option) to ">" to continuously receive
        # messages that were "never delivered to any other consumer".
        # https://redis.io/commands/xreadgroup
        task = r_conn.xreadgroup(CONSUMER_GROUP_NAME, consumer_name, streams={STREAM_NAME: ">"}, count=1, block=5000)
        print(f"[{consumer_name}] data from stream = {task}")
        # example of what to expect to get back from the stream
        # task[0][0] = stream name
        # task[0][1] = (1626817438793-0, {'worker_id': '1000'})
        if not task:
            print(f"[{consumer_name}] empty task received")
            # try getting non-ack'd/pending entries before attempting to close;
            # BLOCK and NOACK options are ignored here
            # set the ID (specified in the STREAMS option) to "0" to get any pending messages
            task = r_conn.xreadgroup(CONSUMER_GROUP_NAME, consumer_name, streams={STREAM_NAME: "0"})
            if not task or not task[0][1]:
                print(f"[{consumer_name}] consumed all tasks. closing down.")
                break
            else:
                # move on to the for-loop below to process this pending task
                print(f"[{consumer_name}] retrieved pending message from stream = {task}")
                pass

        for msg_id, msg in task[0][1]:
            print(f"[{consumer_name}] Processing: message id = {msg_id}, task = {msg}")
            # simulate some processing time
            time.sleep(random.randrange(1, 3, 1))
            if random.randint(1, 3) > 1:
                count += 1
                r_conn.xack(STREAM_NAME, CONSUMER_GROUP_NAME, msg_id)
                print(f"[{consumer_name}] MESSAGED ACKED. message id = {msg_id}")
            else:
                print(f"[{consumer_name}] ** No ack for message id {msg_id} **")

    print(f"[{consumer_name}] count = {count}")
    print(f"[{consumer_name}] info on consumer group = {r_conn.xinfo_groups(STREAM_NAME)}")
    print(f"[{consumer_name}] no more tasks.")
    return True


if __name__ == "__main__":

    producer = threading.Thread(target=message_producer)
    producer.start()
    print(f"producer started")

    # time.sleep(5)
    c_threads = []
    for c in range(len(CONSUMERS)):
        consumer = threading.Thread(target=message_consumer, args=(CONSUMERS[c],))
        consumer.start()
        print(f"{CONSUMERS[c]} started")
        c_threads.append(consumer)

    producer.join()
    print(f"producer done")

    for consumer in c_threads:
        consumer.join()

    print(f"consumers done")
