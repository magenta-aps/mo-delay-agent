import pytest
from mo_delay_agent.config import Settings
from pydantic import ValidationError
from parameterized import parameterized

env_exchange = "dummy_exchange"
env_amqp_url = "amqp://This:is@aTest:5672"

@pytest.fixture
def mock_env(monkeypatch):
    monkeypatch.setenv("amqp_url", env_amqp_url)
    monkeypatch.setenv("amqp_exchange", env_exchange)


class TestSettings:

    def test_default_settings(self):
        settings = Settings()
        assert settings.amqp_url == "amqp://guest:guest@localhost:5672"
        assert settings.amqp_exchange == "os2mo"
        assert settings.amqp_delayed_exchange == "os2mo_delayed"
        assert settings.postgresurl == "postgres://delay_agent:delay_agent@localhost:5432/delay_agent"

    
    def test_env_override(self, mock_env):
        settings = Settings()
        assert settings.amqp_exchange == env_exchange
        assert settings.amqp_url == env_amqp_url

    @parameterized.expand([
        ("amqp_url","Not a amqp url"),
        ("postgres_url","Not a postgres url"),
        ("amqp_exchange", 1),
        ("amqp_delayed_exchange", 1),
        ("not_a_setting", "Should also fail"),
    ])
    def test_wrong_settings_type(self, setting, value):
        with pytest.raises(ValidationError):
            Settings(setting=value)   