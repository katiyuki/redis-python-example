import random
import sys
import threading
import time

import redis

STREAM_NAME = 'aix-tasks-1'
CONSUMER_GROUP_NAME = 'consumer-aix-group-1'
CONSUMERS = [
    'aix-consumer-1',
    'aix-consumer-2',
    'aix-consumer-3'
]
r_conn = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

try:
    r_conn.xgroup_create(STREAM_NAME, CONSUMER_GROUP_NAME, id="0", mkstream=True)
except Exception as ex:
    if "BUSYGROUP" in str(ex):
        print(f"Consumer group already created. skipping...")
    else:
        print(f"Exception thrown: {str(ex)}")
        sys.exit(0)

print(f"info on consumer group = {r_conn.xinfo_groups(STREAM_NAME)}")


# producer: will be a separate thread/location
def message_producer():
    start_value = 2000
    total_ids = 10
    worker_ids = range(start_value, start_value + total_ids)
    for wid in worker_ids:
        r_conn.xadd(STREAM_NAME, {"worker_id": wid})
        print(f"added worker id {wid}")
        time.sleep(random.randrange(1, 3, 1))


# consumer: will be a separate thread/location
def message_consumer(consumer_name):
    count = 0
    print(f"on {consumer_name}...")

    while True:
        # need to keep the ID (specified in the STREAMS option) to ">" to continuously receive
        # messages that were never delivered to any other consumer.
        task = r_conn.xreadgroup(CONSUMER_GROUP_NAME, consumer_name, streams={STREAM_NAME: ">"}, block=5000)
        print(f"[{consumer_name}] data from stream = {task}")
        # task[0][0] = stream name
        # task[0][1] = (1626817438793-0, {'worker_id': '1000'})
        if not task:
            print(f"[{consumer_name}] empty task received")
            # try getting non-ack'd/pending entries; BLOCK and NOACK are ignored here
            task = r_conn.xreadgroup(CONSUMER_GROUP_NAME, consumer_name, streams={STREAM_NAME: "0"})
            if not task[0][1]:
                print(f"[{consumer_name}] consumed all tasks. closing down.")
                break
            else:
                # move on to the for-loop below to process this pending task
                pass

        for msg_id, msg in task[0][1]:
            count += 1
            print(f"[{consumer_name}] Processing: message id = {msg_id}, task = {msg}")
            time.sleep(random.randrange(1, 3, 1))
            r_conn.xack(STREAM_NAME, CONSUMER_GROUP_NAME, msg_id)
            print(f"[{consumer_name}] MESSAGED ACKED. message id = {msg_id}")

    print(f"[{consumer_name}] count = {count}")
    print(f"[{consumer_name}] info on consumer group = {r_conn.xinfo_groups(STREAM_NAME)}")
    print(f"[{consumer_name}] no more tasks.")
    return True


if __name__ == "__main__":

    producer = threading.Thread(target=message_producer)
    producer.start()
    print(f"producer started")

    time.sleep(5)
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
