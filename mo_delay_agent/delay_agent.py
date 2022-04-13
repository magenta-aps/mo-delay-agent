# Copyright (C) 2019 Magenta ApS, https://magenta.dk.
# Contact: info@magenta.dk.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import json
import logging
import random
import threading
import time
from functools import partial

import dateutil.parser
import pika
import psycopg2
from mo_delay_agent.config import Settings


def new_backoff_gen():
    """In case rabbitmq or postgres restarts or crashes, we do not want
    all services to connect at the same time."""
    yield random.randrange(0, 2)
    yield random.randrange(0, 4)
    yield random.randrange(0, 8)
    while True:
        yield random.randrange(0, 300)


def get_new_producer_channel(amqp_url, delayed_exchange):
    """Return a channel for publishing messages to the delayed queue."""
    backoff = new_backoff_gen()
    while True:
        logging.info("Make a new producer connection to RabbitMQ")
        try:
            conn = pika.BlockingConnection(pika.URLParameters(amqp_url))
            channel = conn.channel()
            channel.exchange_declare(exchange=delayed_exchange, exchange_type="topic")
        except pika.exceptions.AMQPError:
            logging.error("Failed to connect to producer RabbitMQ", exc_info=True)
            time.sleep(next(backoff))
        else:
            logging.info("Successfully connected to producer RabbitMQ")
            return channel


def producer(get_pg_conn, amqp_url, delayed_exchange, timeout=2):
    """Push due messages to the delayed queue."""

    def pg_reconnect():
        backoff = new_backoff_gen()
        while True:
            logging.info("Trying to connect to PostgreSQL")
            try:
                conn = get_pg_conn()
            except psycopg2.Error:
                logging.error("Failed to connect to PostgreSQL")
                time.sleep(next(backoff))
                continue
            logging.info("Successfully connected to PostgreSQL")
            return conn

    # this can hang indefinitely, but that is fine as the producer is not
    # useful without it anyway.
    channel = get_new_producer_channel(amqp_url, delayed_exchange)
    pgconn = pg_reconnect()
    while True:
        # we user this inner ``timeout_`` because we do not want to timeout in
        # the iteration after a successful call to ``get_due_messages``.
        timeout_ = timeout
        try:
            with pgconn.cursor() as curs:
                curs.callproc("get_due_messages")
                for id, message, topic in curs.fetchall():
                    timeout_ = 0
                    try:
                        channel.basic_publish(
                            exchange=delayed_exchange,
                            routing_key=topic,
                            body=message,
                        )
                    except pika.exceptions.AMQPError:
                        logging.error("Failed to publish", exc_info=True)
                        channel = get_new_producer_channel(
                            settings.amqp_delayed_exchange
                        )
                    else:
                        curs.execute("delete from messages where id = %s;", (id,))
            pgconn.commit()
        except psycopg2.Error:
            # rollback? perhaps if "delete from ..." fails?
            logging.error("PostgreSQL error", exc_info=True)
            pgconn = pg_reconnect()
        finally:
            time.sleep(timeout_)


def consumer(conn, channel, method, properties, body):
    """Insert the rabbitmq message in the database."""
    logging.info(" [%s] %r", method.routing_key, body)

    try:
        message = json.loads(body)
    except json.JSONDecodeError:
        logging.error("Failed to decode body: %s", body)
        # we still acknowledge, because we do not want this message ever again
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        message["time"]
        message["uuid"]
    except (ValueError, TypeError, KeyError, IndexError):
        # loading json can give us an int, list, string... Also checks that
        # ``message`` is not missing keys.
        logging.error("Invalid message: %s", message)
        # we still acknowledge, because we do not want this message ever again
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    try:
        time = dateutil.parser.isoparse(message["time"])
    except ValueError:
        logging.error("Failed to parse time: %s", message["time"])
        # we still acknowledge, because we do not want this message ever again
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    if time > datetime.datetime.utcnow():
        try:
            with conn.cursor() as curs:
                curs.execute(
                    """
                insert into messages
                       (message, topic, produce_at)
                values (%s, %s, %s);
                """,
                    (json.dumps(message), method.routing_key, time),
                )
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
            logging.error(message, exc_info=True)
            return  # no ack

    channel.basic_ack(delivery_tag=method.delivery_tag)


def get_new_consumer_channel(get_pg_conn, amqp_url, exchange):
    """Return a channel for consuming messages from MO's queue."""
    backoff = new_backoff_gen()
    while True:
        logging.info("Trying to connect to PostgreSQL")
        try:
            pgconn = get_pg_conn()
        except psycopg2.Error:
            logging.error("Failed to connect to PostgreSQL")
            time.sleep(next(backoff))
            continue
        else:
            logging.info("Successfully connected to PostgreSQL")

        backoff = new_backoff_gen()
        while True:
            logging.info("Trying to make a new consumer connection to RabbitMQ")
            try:
                conn = pika.BlockingConnection(pika.URLParameters(amqp_url))
                channel = conn.channel()
                channel.exchange_declare(exchange=exchange, exchange_type="topic")
                queue_name = channel.queue_declare("delayed", exclusive=True).method.queue
                channel.queue_bind(exchange=exchange, queue=queue_name, routing_key="#")
                channel.basic_consume(queue=queue_name, on_message_callback=partial(consumer, pgconn), auto_ack=False
                )
            except pika.exceptions.AMQPError:
                logging.error("Failed to connect to consumer RabbitMQ")
                time.sleep(next(backoff))
            else:
                logging.info("Successfully connected to consumer RabbitMQ")
                return channel


def main(settings):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(threadName)-8s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)

    def get_pg_conn():
        return psycopg2.connect(settings.postgresurl)

    threading.current_thread().name = "consumer"
    t = threading.Thread(
        target=producer,
        name="producer",
        args=(get_pg_conn, settings.amqp_url, settings.amqp_delayed_exchange),
    )
    t.daemon = True
    t.start()

    channel = get_new_consumer_channel(
        get_pg_conn, settings.amqp_url, settings.amqp_exchange
    )

    logging.info(" [*] Waiting for messages. To exit press CTRL+C")

    while True:
        try:
            channel.start_consuming()
        except pika.exceptions.AMQPError:
            logging.error("AMQPError while consuming", exc_info=True)
            channel = get_new_consumer_channel(
                get_pg_conn, settings.amqp_url, settings.amqp_exchange
            )
        except psycopg2.Error:
            logging.error("psycopg2.Error while consuming", exc_info=True)
            channel = get_new_consumer_channel(
                get_pg_conn, settings.amqp_url, settings.amqp_exchange
            )
        except KeyboardInterrupt:
            channel.stop_consuming()
            break

    channel.close()


if __name__ == "__main__":
    settings = Settings()
    main(settings)
