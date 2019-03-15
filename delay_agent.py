import datetime
import dateutil.parser
from functools import partial
import json
import logging
import threading
import time

import pika
import psycopg2


def get_new_producer_channel(port):
    """Return a channel for publishing messages to the delayed queue."""
    while True:
        try:
            logging.info(
                "Trying to make a new producer connection to RabbitMQ on port %s", port
            )
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host="localhost", port=port)
            )
            channel = conn.channel()
            channel.exchange_declare(exchange="moq_delayed", exchange_type="topic")
        except pika.exceptions.AMQPError:
            logging.error("Failed to connect to producer RabbitMQ", exc_info=True)
            time.sleep(0.5)
        else:
            logging.info("Successfully connected to producer RabbitMQ")
            return channel


def producer(pgconn, mqport, timeout=2):
    """Push due messages to the delayed queue."""
    # this can hang indefinitely, but that is fine as the producer is not
    # useful without it anyway.
    channel = get_new_producer_channel(mqport)
    while True:
        # we user this inner ``timeout_`` because we do not want to timeout in
        # the iteration after a successful call to ``get_due_messages``.
        timeout_ = timeout
        with pgconn.cursor() as curs:
            curs.callproc("get_due_messages")
            for id, message, topic in curs.fetchall():
                timeout_ = 0
                try:
                    channel.basic_publish(
                        exchange="moq_delayed", routing_key=topic, body=message
                    )
                except pika.exceptions.AMQPError as e:
                    logging.error("Failed to publish", exc_info=True)
                    channel = get_new_producer_channel()
                else:
                    curs.execute("delete from messages where id = %s;", (id,))
        pgconn.commit()
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


def get_new_consumer_channel(pgconn, port):
    """Return a channel for consuming messages from MO's queue."""
    while True:
        try:
            logging.info(
                "Trying to make a new consumer connection to RabbitMQ on port %s", port
            )
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(host="localhost", port=port)
            )
            channel = conn.channel()
            channel.exchange_declare(exchange="moq", exchange_type="topic")
            queue_name = channel.queue_declare(exclusive=True).method.queue
            channel.queue_bind(exchange="moq", queue=queue_name, routing_key="#")
            channel.basic_consume(
                partial(consumer, pgconn), queue=queue_name, no_ack=False
            )
        except pika.exceptions.AMQPError:
            logging.error("Failed to connect to consumer RabbitMQ")
            time.sleep(0.5)
        else:
            logging.info("Successfully connected to consumer RabbitMQ")
            return channel


def main(*, pgport=5432, mqport=5672):
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)

    pgconn = psycopg2.connect(
        database="delay_agent",
        user="delay_agent",
        password="delay_agent",
        host="127.0.0.1",
        port=pgport,
    )

    t = threading.Thread(target=producer, args=(pgconn, mqport))
    t.daemon = True
    t.start()

    channel = get_new_consumer_channel(pgconn, mqport)

    logging.info(" [*] Waiting for messages. To exit press CTRL+C")

    while True:
        try:
            channel.start_consuming()
        except pika.exceptions.AMQPError:
            logging.error("AMQPError while consuming", exc_info=True)
            channel = get_new_consumer_channel(pgconn, mqport)
        except KeyboardInterrupt:
            channel.stop_consuming()
            break

    channel.close()
    pgconn.close()


if __name__ == "__main__":
    main()
