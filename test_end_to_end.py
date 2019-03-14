from contextlib import contextmanager
import datetime
import dateutil.parser
import json
import logging
import pika
import random
import threading
import uuid

import delay_agent


# maybe just up the level to 'warning'
logging.getLogger("delay_agent.py").addHandler(logging.NullHandler())
logging.getLogger("").addHandler(logging.NullHandler())
# for the big boy tests, the delay agent is running
# (just like postgres and rabbitmq)
t_main = threading.Thread(target=delay_agent.main, kwargs={"pgport": 5434})
t_main.daemon = True
t_main.start()


@contextmanager
def amqp_publisher(host, exchange):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type="topic")

    def publish(routing_key, message):
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=json.dumps(message)
        )

    yield publish
    channel.close()
    connection.close()


@contextmanager
def amqp_listener(host, exchange):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type="topic")
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=exchange, queue=queue_name, routing_key="#")

    def consume():
        return channel.consume(queue=queue_name, inactivity_timeout=1)

    yield consume
    channel.cancel()
    connection.close()


def test_end_to_end():
    """Push messages and make sure the same messages are published to the
    delayed queue."""

    MESSAGES = 10000

    # create messages
    send_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=20)
    messages = []
    for __ in range(MESSAGES):
        message = {"uuid": str(uuid.uuid4()), "time": send_time.isoformat()}
        messages.append(message)
    random.seed(1)  # reproducable shuffling
    random.shuffle(messages)

    # open consumer connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange="moq_delayed", exchange_type="topic")
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="moq_delayed", queue=queue_name, routing_key="#")

    # publish messages
    with amqp_publisher("localhost", "moq") as publish:
        for msg in messages:
            publish("employee.create.itsystem", msg)

    messages_received = []
    for method, prop, body in channel.consume(queue=queue_name, inactivity_timeout=1):
        if (method, prop, body) == (None, None, None):
            # no new messages, timeout
            break
        msg = json.loads(body)
        messages_received.append(msg)

    sort_by_uuid = lambda m: m["uuid"]
    assert len(messages) == len(messages_received)
    for m1, m2 in zip(sorted(messages, key=sort_by_uuid), sorted(messages_received, key=sort_by_uuid)):
        assert m1 == m2

    channel.cancel()
    connection.close()
