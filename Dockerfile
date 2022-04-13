# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
FROM python:3.10

ENV PYTHONUNBUFFERED 1

WORKDIR /code/

RUN pip install poetry

COPY pyproject.toml poetry.lock ./

RUN poetry install

COPY mo_delay_agent mo_delay_agent

CMD ["poetry", "run", "python", "-m",  "mo_delay_agent.delay_agent"]
