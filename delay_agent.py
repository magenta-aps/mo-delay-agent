import datetime
import dateutil.parser
from functools import partial
import json
import logging
import os
import threading
import time

import pika
import psycopg2


MQ_HOST = os.getenv("AMQP_HOST", "localhost")
MQ_PORT = int(os.getenv("AMQP_PORT", "5672"))
MQ_MO_EXCHANGE = os.getenv("AMQP_MO_EXCHANGE", "moq")
MQ_DELAYED_EXCHANGE = os.getenv("AMQP_DELAYED_EXCHANGE", "delayed_moq")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "delay_agent")
PG_USER = os.getenv("POSTGRES_USER", "delay_agent")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "delay_agent")


def get_new_producer_channel():
    """Return a channel for publishing messages to the delayed queue."""
    while True:
        logging.info(
            "Trying to make a new producer connection to RabbitMQ on port %s", MQ_PORT
        )
        try:
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host=MQ_HOST, port=MQ_PORT)
            )
            channel = conn.channel()
            channel.exchange_declare(exchange=MQ_DELAYED_EXCHANGE, exchange_type="topic")
        except pika.exceptions.AMQPError:
            logging.error("Failed to connect to producer RabbitMQ", exc_info=True)
            time.sleep(4)
        else:
            logging.info("Successfully connected to producer RabbitMQ")
            return channel


def producer(get_pg_conn, timeout=2):
    """Push due messages to the delayed queue."""

    def pg_reconnect():
        while True:
            logging.info("Trying to connect to PostgreSQL")
            try:
                conn = get_pg_conn()
            except psycopg2.Error:
                logging.error("Failed to connect to PostgreSQL")
                time.sleep(4)
                continue
            logging.info("Successfully connected to PostgreSQL")
            return conn

    # this can hang indefinitely, but that is fine as the producer is not
    # useful without it anyway.
    channel = get_new_producer_channel()
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
                            exchange=MQ_DELAYED_EXCHANGE, routing_key=topic, body=message
                        )
                    except pika.exceptions.AMQPError as e:
                        logging.error("Failed to publish", exc_info=True)
                        channel = get_new_producer_channel()
                    else:
                        curs.execute("delete from messages where id = %s;", (id,))
            pgconn.commit()
        except psycopg2.Error:
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


def get_new_consumer_channel(get_pg_conn):
    """Return a channel for consuming messages from MO's queue."""
    while True:
        logging.info("Trying to connect to PostgreSQL")
        try:
            pgconn = get_pg_conn()
        except psycopg2.Error:
            logging.error("Failed to connect to PostgreSQL")
            time.sleep(4)
            continue
        else:
            logging.info("Successfully connected to PostgreSQL")

        while True:
            logging.info(
                "Trying to make a new consumer connection to RabbitMQ on port %s", MQ_PORT
            )
            try:
                conn = pika.BlockingConnection(
                    pika.ConnectionParameters(host=MQ_HOST, port=MQ_PORT)
                )
                channel = conn.channel()
                channel.exchange_declare(exchange=MQ_MO_EXCHANGE, exchange_type="topic")
                queue_name = channel.queue_declare(exclusive=True).method.queue
                channel.queue_bind(exchange=MQ_MO_EXCHANGE, queue=queue_name, routing_key="#")
                channel.basic_consume(
                    partial(consumer, pgconn), queue=queue_name, no_ack=False
                )
            except pika.exceptions.AMQPError:
                logging.error("Failed to connect to consumer RabbitMQ")
                time.sleep(4)
            else:
                logging.info("Successfully connected to consumer RabbitMQ")
                return channel


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)

    def get_pg_conn():
        return psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )

    t = threading.Thread(target=producer, args=(get_pg_conn,))
    t.daemon = True
    t.start()

    channel = get_new_consumer_channel(get_pg_conn)

    logging.info(" [*] Waiting for messages. To exit press CTRL+C")

    while True:
        try:
            channel.start_consuming()
        except pika.exceptions.AMQPError:
            logging.error("AMQPError while consuming", exc_info=True)
            channel = get_new_consumer_channel(get_pg_conn)
        except psycopg2.Error:
            logging.error("psycopg2.Error while consuming", exc_info=True)
            channel = get_new_consumer_channel(get_pg_conn)
        except KeyboardInterrupt:
            channel.stop_consuming()
            break

    channel.close()


if __name__ == "__main__":
    main()
