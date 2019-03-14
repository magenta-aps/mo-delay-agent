import datetime
import dateutil.parser
from functools import partial
import json
import logging
import threading
import time

import pika
import psycopg2


def producer(pgconn, timeout=2):
    """Push due messages to the delayed queue."""
    mqconn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = mqconn.channel()
    channel.exchange_declare(exchange="moq_delayed", exchange_type="topic")

    while True:
        timeout_ = timeout
        with pgconn.cursor() as curs:
            curs.execute(
                """
              select id, message, topic
                from messages
               where now() > produce_at
                  -- Do not rely on messages being ordered, but know that we
                  -- produced the "most due" we knew of at the time. Also
                  -- remember that rabbitmq is nondetermistic, so the queue
                  -- may not have them in ascending order, even if the messages
                  -- were published so.
            order by produce_at asc
                  -- We have to limit, so the application can run with (low)
                  -- memory allocated without crashing.
               limit 10;
            """
            )
            for id, message, topic in curs.fetchall():
                timeout_ = 0
                try:
                    channel.basic_publish(
                        exchange="moq_delayed", routing_key=topic, body=message
                    )
                except pika.exceptions.AMQPError as e:
                    logging.error("Failed to publish", exc_info=True)
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
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(message, exc_info=True)
            return  # no ack

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main(*, pgport=5432):
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
        port=pgport,
    )

    consumer_channel = mqconn.channel()
    consumer_channel.exchange_declare(exchange="moq", exchange_type="topic")
    queue_name = consumer_channel.queue_declare(exclusive=True).method.queue
    consumer_channel.queue_bind(exchange="moq", queue=queue_name, routing_key="#")

    t = threading.Thread(target=producer, args=(pgconn,))
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
