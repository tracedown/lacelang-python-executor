"""Tests for the high-level LaceExecutor / LaceProbe API."""

import os
import tempfile

import pytest

from lacelang_executor import LaceExecutor, LaceProbe


@pytest.fixture
def lace_root(tmp_path):
    """Create a minimal lace/ project layout."""
    scripts = tmp_path / "scripts" / "ping"
    scripts.mkdir(parents=True)
    (scripts / "ping.lace").write_text(
        'get("https://httpbin.org/status/200")\n'
        '    .expect(status: 200)\n'
    )
    (scripts / "vars.json").write_text('{"base_url": "https://httpbin.org"}')

    config = tmp_path / "lace.config"
    config.write_text(
        "[executor]\n"
        "maxRedirects = 10\n"
        "maxTimeoutMs = 300000\n"
    )
    return str(tmp_path)


def test_executor_discovers_config(lace_root):
    executor = LaceExecutor(lace_root)
    assert executor.config["executor"]["maxRedirects"] == 10
    assert executor.root == os.path.abspath(lace_root)


def test_executor_no_root():
    executor = LaceExecutor()
    assert executor.root is None
    assert executor.config["executor"]["maxRedirects"] == 10  # defaults


def test_probe_name_resolution(lace_root):
    executor = LaceExecutor(lace_root)
    probe = executor.probe("ping")
    assert probe.name == "ping"
    assert probe.script_path is not None
    assert probe.script_path.endswith("ping.lace")


def test_probe_bound_to_executor(lace_root):
    executor = LaceExecutor(lace_root)
    probe = executor.probe("ping")
    assert probe._executor is executor


def test_probe_inline_source():
    executor = LaceExecutor()
    probe = executor.probe(
        'get("https://httpbin.org/status/200")\n'
        '    .expect(status: 200)\n'
    )
    assert probe.name is None
    assert probe.script_path is None


def test_probe_file_path(lace_root):
    executor = LaceExecutor(lace_root)
    path = os.path.join(lace_root, "scripts", "ping", "ping.lace")
    probe = executor.probe(path)
    assert probe.script_path == os.path.abspath(path)
    assert probe.name is None  # file path → no name


def test_probe_invalid_script():
    executor = LaceExecutor()
    with pytest.raises(Exception):
        executor.probe("this is not valid lace syntax at all {{{")


def test_extension_registration_file(tmp_path):
    ext_file = tmp_path / "myext.laceext"
    ext_file.write_text(
        '[extension]\nname = "myext"\nversion = "1.0.0"\n'
    )
    executor = LaceExecutor()
    ext = executor.extension(str(ext_file))
    assert ext.name == "myext"
    assert ext.path == str(ext_file)
    assert str(ext_file) in executor._extension_paths


def test_extension_registration_directory(tmp_path):
    ext_dir = tmp_path / "myext"
    ext_dir.mkdir()
    (ext_dir / "myext.laceext").write_text(
        '[extension]\nname = "myext"\nversion = "1.0.0"\n'
    )
    (ext_dir / "myext.config").write_text(
        '[extension]\nname = "myext"\nversion = "1.0.0"\n\n'
        '[config]\nkey = "value"\n'
    )
    executor = LaceExecutor()
    ext = executor.extension(str(ext_dir))
    assert ext.name == "myext"
    assert ext.config_path is not None


def test_extension_not_found():
    executor = LaceExecutor()
    with pytest.raises(FileNotFoundError):
        executor.extension("/nonexistent/ext.laceext")


def test_track_prev_default(lace_root):
    executor = LaceExecutor(lace_root)
    assert executor.track_prev is True


def test_track_prev_disabled(lace_root):
    executor = LaceExecutor(lace_root, track_prev=False)
    assert executor.track_prev is False
