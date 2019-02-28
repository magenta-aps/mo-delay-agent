drop table if exists messages;
create table messages(
    id         serial primary key,
    message    text not null,
    topic      text not null,
    produce_at timestamp not null
);
