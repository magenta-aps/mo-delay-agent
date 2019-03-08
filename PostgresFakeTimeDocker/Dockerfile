FROM postgres:9.6

RUN apt-get update
RUN apt-get install -y git make gcc

RUN git clone https://github.com/wolfcw/libfaketime/
RUN make -C /libfaketime/ install

COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT []
ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["postgres"]
