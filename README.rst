==============
mo-delay-agent
==============

``mo-delay-agent`` is a component in the MO eco system. MO_ sends messages to
an AMQP_ broker every time an object is created, changed or deleted with a
timestamp of when said event is scheduled. This agent saves all those messages
and sends them to a new queue when they are due. This makes it possible to
write integrations for MO_, that react to changes when they are scheduled to occur.
Please refer to the MO_ docs_ for more information about the messages.


Architecture
------------

``mo-delay-agent`` consists of the ``consumer`` and the ``producer``.
``consumer`` is responsible for receiving the messages from MO_ and writing
them to postgresql. ``producer`` is responsible for reading the due messages
from postgresql and writing them to the delayed queue.


Developing
----------

``mo-delay-agent`` depends on ``PostgreSQL`` and ``rabbitmq``. The easiest way
to run these locally is with ``docker`` and ``docker-compose``::

    $ docker-compose up --build -d


Testing
-------

It has proven very hard to test the application, because it is undeterministic.
There are unit tests for the consumer, but not the producer. If you aspire
to write integration tests, try digging through the git history for
inspiration. Lessons learned: it is hard to test undeterministic and time
sensitive applications.


Configuration
-------------

These are the environment variables used to configure ``mo-delay-agent`` listed
with their default values.

+----------------------+---------------+
| Environment variable | Default value |
+======================+===============+
| AMQP_HOST            | localhost     |
+----------------------+---------------+
| AMQP_PORT            | 5672          |
+----------------------+---------------+
| AMQP_MO_EXCHANGE     | moq           |
+----------------------+---------------+
| AMQP_DELAYED_QUEUE   | delayed_moq   |
+----------------------+---------------+
| POSTGRES_HOST        | localhost     |
+----------------------+---------------+
| POSTGRES_PORT        | 5432          |
+----------------------+---------------+
| POSTGRES_DB          | delay_agent   |
+----------------------+---------------+
| POSTGRES_USER        | delay_agent   |
+----------------------+---------------+
| POSTGRES_PASSWORD    | delay_agent   |
+----------------------+---------------+


License
-------

``mo-delay-agent`` is free software. You are entitled to use, study, modify and
share it under the provisions of Version 2.0 of the Mozilla Public License as
specified in the LICENSE file.

This software was developed by Magenta_ ApS.


.. _MO: https://os2mo.readthedocs.io/
.. _docs: https://os2mo.readthedocs.io/en/1.16.1/api/amqp.html
.. _AMQP: https://www.rabbitmq.com/
.. _Magenta: https://magenta.dk
