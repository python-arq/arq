import pytest
from arq import ConnectionSettings


def test_settings_unchanged():
    settings = ConnectionSettings()
    assert settings.R_PORT == 6379


def test_settings_changed():
    settings = ConnectionSettings(R_PORT=123)
    assert settings.R_PORT == 123


def test_settings_invalid():
    with pytest.raises(TypeError):
        ConnectionSettings(FOOBAR=123)
