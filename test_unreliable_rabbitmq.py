"""The purpose of this test is to make sure delay_agent can recover,
when the connection to rabbitmq iis dropped.
"""

import datetime
import json
import logging
import subprocess
import threading
import time
import uuid

import pika

import delay_agent


MQPORT = 5673

# for the big boy tests, the delay agent is running
# (just like postgres and rabbitmq)
t_main = threading.Thread(target=delay_agent.main, kwargs={"mqport": MQPORT})
t_main.daemon = True
t_main.start()

# maybe just up the level to 'warning'
logging.getLogger("delay_agent.py").setLevel(logging.WARNING)
logging.getLogger("").setLevel(logging.WARNING)
logging.getLogger("pika").addHandler(logging.NullHandler())


def producer():
    """For 3 seconds, produce messages."""

    def new_channel():
        while True:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host="localhost", port=MQPORT)
                )
                channel = connection.channel()
                channel.exchange_declare(exchange="moq", exchange_type="topic")
            except pika.exceptions.AMQPError:
                time.sleep(0.5)
            else:
                return channel

    channel = new_channel()
    messages = []
    timestamp = datetime.datetime.utcnow() + datetime.timedelta(seconds=120)
    for __ in range(1000):
        message = {"uuid": str(uuid.uuid4()), "time": timestamp.isoformat()}
        messages.append(message)
    for message in messages:
        try:
            channel.basic_publish(
                exchange="moq",
                routing_key="employee.create.itsystem",
                body=json.dumps(message),
            )
        except pika.exceptions.AMQPError:
            channel = new_channel()
        time.sleep(3 / len(messages))

    channel.close()


t = threading.Thread(target=producer)
t.daemon = True
t.start()


def test_unreliable_rabbitmq1():
    assert t_main.is_alive()
    assert t.is_alive()
    time.sleep(0.3)
    subprocess.run(["docker", "rm", "-f", "amqp_unreliable"])
    time.sleep(0.3)
    subprocess.run(["docker-compose", "up", "-d"])
    time.sleep(0.3)
    assert t_main.is_alive()
    assert t.is_alive()


def test_unreliable_rabbitmq2():
    assert t_main.is_alive()
    assert t.is_alive()
    subprocess.run(["docker", "rm", "-f", "amqp_unreliable"])
    time.sleep(0.6)
    subprocess.run(["docker-compose", "up", "-d"])
    time.sleep(0.6)
    assert t_main.is_alive()
    assert t.is_alive()
