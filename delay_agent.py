import datetime
import dateutil.parser
from functools import partial
import json
import logging

import pika
import psycopg2


def select():
    cursor.execute(
        """
    select message, topic
      from messages
     where now() > produce_at
     limit 10;
    """
    )
    yield from cursor.fetchall()


def worker(conn, channel, method, properties, body):
    """Insert the rabbitmq message in the database."""
    logging.info(" [%s] %r", method.routing_key, body)

    try:
        message = json.loads(body)
    except json.JsonDecodeError:
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
    channel = mqconn.channel()

    channel.exchange_declare(exchange="moq", exchange_type="topic")
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange="moq", queue=queue_name, routing_key="#")

    logging.info(" [*] Waiting for messages. To exit press CTRL+C")
    channel.basic_consume(partial(worker, pgconn), queue=queue_name, no_ack=False)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        mqconn.close()
        pgconn.close()


if __name__ == "__main__":
    main()
