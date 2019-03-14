drop table if exists messages;
create table messages(
    id         serial primary key,
    message    text not null,
    topic      text not null,
    produce_at timestamptz not null
);


-- presumably a lot of messages will accumulate
create index on messages (produce_at);


-- return <= 100 messages that should be published to the delayed queue.
create or replace function get_due_messages() returns
    table(id integer, message text, topic text)
as $$
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
       limit 100;
$$ language sql;
