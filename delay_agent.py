import datetime
import dateutil.parser
from functools import partial
import json
import logging
import threading
import time

import pika
import psycopg2


def producer(pgconn, amqp_channel, timeout=2):
    """Push due messages to the delayed queue."""
    while True:
        with pgconn.cursor() as curs:
            curs.execute(
                """
            select id, message, topic
              from messages
             where now() > produce_at
             limit 10;
            """
            )
            for id, message, topic in curs.fetchall():
                try:
                    amqp_channel.basic_publish(
                        exchange="moq_delayed", routing_key=topic, body=message
                    )
                except pika.exceptions.AMQPError as e:
                    logging.error("Failed to publish", exc_info=True)
                else:
                    curs.execute("delete from messages where id = %s;", (id,))
        pgconn.commit()
        time.sleep(timeout)


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
        time = dateutil.parser.isoparse(message["time"])
    except ValueError:
        logging.error("Failed to parse time: %s", message["time"])
        # we still acknowledge, because we do not want this message ever again
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    if time > datetime.datetime.now():
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
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(message, exc_info=True)
            return  # no ack

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)

    mqconn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    pgconn = psycopg2.connect(
        database="delay_agent",
        user="delay_agent",
        password="delay_agent",
        host="127.0.0.1",
    )

    consumer_channel = mqconn.channel()
    consumer_channel.exchange_declare(exchange="moq", exchange_type="topic")
    queue_name = consumer_channel.queue_declare(exclusive=True).method.queue
    consumer_channel.queue_bind(exchange="moq", queue=queue_name, routing_key="#")

    producer_channel = mqconn.channel()
    producer_channel.exchange_declare(exchange="moq_delayed", exchange_type="topic")
    t = threading.Thread(target=producer, args=(pgconn, producer_channel))
    t.daemon = True
    t.start()

    logging.info(" [*] Waiting for messages. To exit press CTRL+C")
    consumer_channel.basic_consume(
        partial(consumer, pgconn), queue=queue_name, no_ack=False
    )

    try:
        consumer_channel.start_consuming()
    except KeyboardInterrupt:
        consumer_channel.stop_consuming()
    finally:
        mqconn.close()
        pgconn.close()


if __name__ == "__main__":
    main()
