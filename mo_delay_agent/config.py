from pydantic import AmqpDsn
from pydantic import BaseSettings
from pydantic import parse_obj_as
from pydantic import PostgresDsn


class Settings(BaseSettings):

    amqp_url: AmqpDsn = parse_obj_as(AmqpDsn, "amqp://guest:guest@localhost:5672")
    amqp_exchange: str = "os2mo"
    amqp_delayed_exchange: str = "os2mo_delayed"
    postgresurl: PostgresDsn = parse_obj_as(
        PostgresDsn, "postgres://delay_agent:delay_agent@localhost:5432/delay_agent"
    )
