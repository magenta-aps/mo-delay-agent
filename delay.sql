drop table if exists messages;
create table messages(
    id         serial primary key,
    message    text not null,
    topic      text not null,
    produce_at timestamptz not null
);

-- presumably a lot of messages will accumulate
create index on messages (produce_at);
