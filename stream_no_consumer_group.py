import random
import sys
import threading
import time

import redis

STREAM_NAME = 'aix-tasks-1'
r_conn = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


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
def message_consumer():
    count = 0
    last_id = "0"
    print(f"consuming messages started...")

    while True:
        print(f"last_id={last_id}")
        task = r_conn.xread({STREAM_NAME: last_id}, count=1, block=20000)
        print(f"data from stream = {task}")
        # task[0][0] = stream name
        # task[0][1] = (1626817438793-0, {'worker_id': '1000'})
        if not task:
            print(f"empty task. stopping...")
            break

        for msg_id, msg in task[0][1]:
            count += 1
            print(f"Processing: message id = {msg_id}, task = {msg}")
            time.sleep(random.random())
            last_id = msg_id

    print(f"count = {count}")
    print(f"consumer closed")
    return True


if __name__ == "__main__":

    producer = threading.Thread(target=message_producer)
    producer.start()
    print(f"producer started")

    consumer = threading.Thread(target=message_consumer)
    consumer.start()
    print(f"consumer started")

    # c_threads = []
    # for c in range(len(CONSUMERS)):
    #     consumer = threading.Thread(target=message_consumer, args=(CONSUMERS[c],))
    #     consumer.start()
    #     print(f"{CONSUMERS[c]} started")
    #     c_threads.append(consumer)

    producer.join()
    print(f"producer done")
    consumer.join()
    print(f"consumer done")
    # for consumer in c_threads:
    #     consumer.join()
    #
    # print(f"consumers done")

