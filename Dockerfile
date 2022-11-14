# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
FROM python:3.10

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION="1.2.0" \
    POETRY_HOME=/opt/poetry \
    VIRTUAL_ENV="/venv"
ENV PATH="$VIRTUAL_ENV/bin:$POETRY_HOME/bin:$PATH"

# Install poetry in an isolated environment
RUN python -m venv $POETRY_HOME \
    && pip install --no-cache-dir poetry==${POETRY_VERSION}

# Install project in another isolated environment
WORKDIR /code/
RUN python -m venv $VIRTUAL_ENV
COPY poetry.lock pyproject.toml ./
RUN poetry install --no-root

COPY mo_delay_agent mo_delay_agent

CMD ["poetry", "run", "python", "-m",  "mo_delay_agent.delay_agent"]
