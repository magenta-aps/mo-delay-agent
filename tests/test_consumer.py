# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""
This file implements a classic unit test for delay_agent.consumer.

It does not require anything to run. PostgreSQL and RabbitMQ are
mocked.
"""
import datetime
import json
import re
import uuid

from mo_delay_agent import delay_agent


class Mock:
    def __init__(self, *args, **kwargs):
        for arg in args:
            setattr(self, arg, None)
        for name, value in kwargs.items():
            setattr(self, name, value)

    def __getattr__(self, o):
        if o in self.__dict__:
            return self.__dict__[o]
        m = Mock()
        setattr(self, o, m)
        return m

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    def __repr__(self):
        return "%s" % self.__dict__

    def __enter__(self):
        self.context_manager = Mock()
        return self.context_manager

    def __exit__(self, *args):
        return self


def string_equal_ignore_whitespace(s1, s2):
    return re.sub(r"\s", "", s1) == re.sub(r"\s", "", s2)


def test_invalid_json():
    channel = Mock()
    method = Mock("routing_key", delivery_tag="invalid_json")
    delay_agent.consumer(None, channel, method, None, "INVALID JSON))")
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag


def test_invalid_time():
    channel = Mock()
    method = Mock("routing_key", delivery_tag="invalid_time")
    bad_time_json = {
        "uuid": str(uuid.uuid4()),
        "time": "2019-03-28iuuuu13:41:48.036681",
    }
    delay_agent.consumer(None, channel, method, None, json.dumps(bad_time_json))
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag


def test_before_now():
    channel = Mock()
    method = Mock("routing_key", delivery_tag="before_now")
    message = {"uuid": str(uuid.uuid4()), "time": "2019-02-28 13:41:48.036681"}
    delay_agent.consumer(None, channel, method, None, json.dumps(message))
    # this test works, because we give None as conn, which means it would
    # crash if it tries to access the database
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag


def test_valid_message():
    conn = Mock()
    channel = Mock()
    topic = "valid_message"
    method = Mock("routing_key", delivery_tag=topic)
    now = datetime.datetime.utcnow()
    future = datetime.datetime(now.year + 1, now.month, now.day)
    message = {"uuid": str(uuid.uuid4()), "time": str(future)}
    delay_agent.consumer(conn, channel, method, None, json.dumps(message))
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag
    sql = conn.cursor.context_manager.execute.args[0]

    assert string_equal_ignore_whitespace(
        sql,
        "insert into messages (message, topic, produce_at) values (%s, %s, %s);",
    )
    assert conn.commit.args is not None


def test_bad_type_str():
    channel = Mock()
    method = Mock("routing_key", delivery_tag="bad_type_str")
    bad_json = "asd"
    delay_agent.consumer(None, channel, method, None, json.dumps(bad_json))
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag


def test_bad_type_int():
    channel = Mock()
    method = Mock("routing_key", delivery_tag="bad_type_int")
    bad_json = 1337
    delay_agent.consumer(None, channel, method, None, json.dumps(bad_json))
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag


def test_bad_type_list():
    channel = Mock()
    method = Mock("routing_key", delivery_tag="bad_type_list")
    bad_json = ["time", "uuid"]
    delay_agent.consumer(None, channel, method, None, json.dumps(bad_json))
    assert channel.basic_ack.kwargs["delivery_tag"] == method.delivery_tag
