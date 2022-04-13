# We do not use alpine. The resulting image is smaller, but there is currently
# no support for pip installation of wheels (binary) packages. It falls back
# to installing from source which is very time consuming. See
# https://github.com/pypa/manylinux/issues/37 and
# https://github.com/docker-library/docs/issues/904
FROM python:3.10

# Force the stdout and stderr streams from python to be unbuffered. See
# https://docs.python.org/3/using/cmdline.html#cmdoption-u
ENV PYTHONUNBUFFERED 1

WORKDIR /code/

RUN pip install poetry

COPY pyproject.toml poetry.lock ./

RUN poetry install

COPY mo_delay_agent mo_delay_agent

CMD ["poetry", "run", "python", "-m",  "mo_delay_agent.delay_agent"]
