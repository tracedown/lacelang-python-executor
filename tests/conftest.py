"""Shared fixtures and markers for the executor test suite."""

import pytest

from lacelang_executor.executor import _Env
from lacelang_executor.laceext.registry import ExtensionRegistry


def pytest_addoption(parser):
    parser.addoption(
        "--network",
        action="store_true",
        default=False,
        help="Run integration tests that require network access.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "network: marks tests that require network access"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--network"):
        skip = pytest.mark.skip(reason="needs --network")
        for item in items:
            if "network" in item.keywords:
                item.add_marker(skip)


@pytest.fixture
def env():
    """A minimal runtime env with no variables."""
    return _Env({}, None, "/tmp", ExtensionRegistry())


@pytest.fixture
def env_with_vars():
    """Factory for an env with custom script variables."""
    def _make(**kwargs):
        return _Env(kwargs, None, "/tmp", ExtensionRegistry())
    return _make
