"""Config loading and environment overlay tests."""

import os
import tempfile

import pytest

from lacelang_executor.config import ConfigError, load_config


def _write_config(content: str) -> str:
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".config", delete=False, encoding="utf-8"
    )
    f.write(content)
    f.close()
    return f.name


def test_defaults_when_no_file():
    cfg = load_config()
    assert cfg["executor"]["maxRedirects"] == 10
    assert cfg["executor"]["maxTimeoutMs"] == 300_000
    assert cfg["executor"]["extensions"] == []


def test_explicit_path():
    path = _write_config("""
[executor]
maxRedirects = 5
""")
    try:
        cfg = load_config(explicit_path=path)
        assert cfg["executor"]["maxRedirects"] == 5
        assert cfg["executor"]["maxTimeoutMs"] == 300_000  # default
    finally:
        os.unlink(path)


def test_explicit_path_not_found():
    with pytest.raises(ConfigError, match="not found"):
        load_config(explicit_path="/nonexistent/lace.config")


def test_env_overlay():
    path = _write_config("""
[executor]
maxRedirects = 10
maxTimeoutMs = 300000

[lace.config.staging]
[lace.config.staging.executor]
maxTimeoutMs = 60000
""")
    try:
        cfg = load_config(explicit_path=path, env_selector="staging")
        assert cfg["executor"]["maxTimeoutMs"] == 60000
        assert cfg["executor"]["maxRedirects"] == 10  # inherited
    finally:
        os.unlink(path)


def test_env_overlay_nonexistent_section():
    """Selecting a non-existent env section is not an error — base is returned."""
    path = _write_config("""
[executor]
maxRedirects = 10
""")
    try:
        cfg = load_config(explicit_path=path, env_selector="nonexistent")
        assert cfg["executor"]["maxRedirects"] == 10
    finally:
        os.unlink(path)


def test_env_var_substitution():
    path = _write_config("""
[executor]
user_agent = "env:LACE_TEST_UA:fallback-ua"
""")
    try:
        cfg = load_config(explicit_path=path)
        assert cfg["executor"]["user_agent"] == "fallback-ua"

        os.environ["LACE_TEST_UA"] = "custom-ua"
        try:
            cfg = load_config(explicit_path=path)
            assert cfg["executor"]["user_agent"] == "custom-ua"
        finally:
            del os.environ["LACE_TEST_UA"]
    finally:
        os.unlink(path)


def test_env_var_unset_no_default():
    path = _write_config("""
[executor]
user_agent = "env:LACE_MISSING_VAR_12345"
""")
    try:
        with pytest.raises(ConfigError, match="not set"):
            load_config(explicit_path=path)
    finally:
        os.unlink(path)


def test_extension_config_forwarded():
    path = _write_config("""
[executor]
extensions = ["laceNotifications"]

[extensions.laceNotifications]
level = "all"
""")
    try:
        cfg = load_config(explicit_path=path)
        assert cfg["extensions"]["laceNotifications"]["level"] == "all"
    finally:
        os.unlink(path)
