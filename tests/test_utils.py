from collections import OrderedDict

import pytest
from arq import ConnectionSettings

from .fixtures import CustomSettings


def test_settings_unchanged():
    settings = ConnectionSettings()
    assert settings.R_PORT == 6379


def test_settings_changed():
    settings = ConnectionSettings(R_PORT=123)
    assert settings.R_PORT == 123
    d = OrderedDict([('R_HOST', 'localhost'), ('R_PORT', 6379), ('R_DATABASE', 0), ('R_PASSWORD', None)])
    assert isinstance(settings.dict, OrderedDict)
    assert settings.dict == d
    assert dict(settings) == dict(d)


def test_settings_invalid():
    with pytest.raises(TypeError):
        ConnectionSettings(FOOBAR=123)


def test_custom_settings():
    settings = CustomSettings()
    assert settings.dict == OrderedDict([('R_HOST', 'localhost'), ('R_PORT', 6379),
                                         ('R_DATABASE', 0), ('R_PASSWORD', None),
                                         ('X_THING', 2), ('A_THING', 1)])
