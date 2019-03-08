from contextlib import contextmanager
import datetime
import dateutil.parser
import logging
import pika
import random
import threading

import delay_agent


# maybe just up the level to 'warning'
logging.getLogger("delay_agent.py").addHandler(logging.NullHandler())
logging.getLogger("").addHandler(logging.NullHandler())
# for the big boy tests, the delay agent is running
# (just like postgres and rabbitmq)
t_main = threading.Thread(target=delay_agent.main)
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


def test_message_order():
    """Make sure that messages are produced in the correct order."""
    # (1) make 20000 messages with ``time`` = n + 3sec and publish them
    #     in a random order.
    # (2) verify that they are published to delayed queue in the
    #     correct order (we cannot verify that they are published at the
    #     correct time as it is mocked and does not correspond to now).

    MESSAGES = 20000

    # create messages
    increasing_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=20)
    messages = []
    for __ in range(MESSAGES):
        message = {"uuid": str(uuid.uuid4()), "time": increasing_time.isoformat()}
        increasing_time += datetime.timedelta(seconds=3)
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

    prev_time = datetime.datetime.min
    messages_received = 0
    for method, properties, body in channel.consume(
        queue=queue_name, inactivity_timeout=1
    ):
        if (method, properties, body) == (None, None, None):
            # no new messages, timeout
            assert messages_received == MESSAGES, "Too few messages received"
            break
        msg = json.loads(body)
        this_time = dateutil.parser.isoparse(msg["time"])
        assert prev_time < this_time, "Messages received in wrong order"
        prev_time = this_time
        messages_received += 1

    channel.cancel()
    connection.close()
