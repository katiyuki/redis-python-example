import random
import sys
import threading
import time
import redis

'''
This is more straightforward use of redis streams, in that you don't
have to worry about consumer groups. Do note, however, because consumer 
groups are not used, if you have multiple consumers on the same stream, 
each consumer will receive the full stream of messages. There is also no 
ack'ing of messages, therefore no monitoring of pending messages.
'''
STREAM_NAME = 'task-stream-1'
r_conn = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


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
def message_consumer():
    '''Consuming messages as they are added to the stream'''
    count = 0
    last_id = "0" # start consuming from the first message in the stream
    print(f"consuming messages started...")

    while True:
        # last_id needs to change to the id of the last message processed by this consumer
        print(f"last_id={last_id}")
        # https://redis.io/commands/xread
        task = r_conn.xread({STREAM_NAME: last_id}, count=1, block=5000)
        print(f"data from stream = {task}")
        # example of what to expect to get back from the stream
        # task[0][0] = stream name
        # task[0][1] = (1626817438793-0, {'worker_id': '1000'})
        if not task:
            # when task is None/Null, it means there are no more messages
            # and blocking (in the xread call) has timed out
            print(f"empty task. stopping...")
            break

        for msg_id, msg in task[0][1]:
            count += 1
            print(f"Processing: message id = {msg_id}, task = {msg}")
            # simulate some processing time
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

    producer.join()
    print(f"producer done")
    consumer.join()
    print(f"consumer done")

    # clean up everything! uncomment if you really want EVERYTHING to be erased
    # r_conn.flushall()


