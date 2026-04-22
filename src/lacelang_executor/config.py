"""lace.config TOML loader — spec §11 + §15.13.

Resolution order (first match wins unless ``explicit_path`` is provided):

1. ``explicit_path`` (from ``--config``), if set.
2. ``lace.config`` in the script's directory.
3. ``lace.config`` in the current working directory.
4. Defaults-only (no file).

If ``LACE_ENV`` or ``env_selector`` (from ``--env``) is set, any
``[lace.config.{env}]`` section is merged on top of the base sections
(base over defaults, env over base).

Every string value supports ``env:VARNAME`` and ``env:VARNAME:default``
substitution — resolved against ``os.environ`` at load time.

Returns a flat dict::

    {
        "executor": {
            "extensions":   [...],
            "maxRedirects": int,
            "maxTimeoutMs": int,
        },
        "result": {
            "path": str | bool,
            "bodies": {"dir": str | bool},
        },
        "extensions": { "<name>": {...}, ... },
    }
"""

from __future__ import annotations

import os
from typing import Any

try:  # Python 3.11+
    import tomllib as _toml  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised only on 3.10
    import tomli as _toml  # type: ignore[import-not-found, no-redef]


# ──────────────────────────────────────────────────────────────────────
# Defaults (spec §11 table)
# ──────────────────────────────────────────────────────────────────────

_DEFAULT_MAX_REDIRECTS = 10
_DEFAULT_MAX_TIMEOUT_MS = 300_000
_DEFAULT_RESULT_PATH = "."


class ConfigError(RuntimeError):
    """Raised for malformed configs or unresolved ``env:`` references."""


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────

def load_config(
    script_path: str | None = None,
    explicit_path: str | None = None,
    env_selector: str | None = None,
) -> dict[str, Any]:
    """Resolve the effective lace.config for a run.

    For library usage, pass ``explicit_path`` directly::

        config = load_config(explicit_path="path/to/lace.config")
        config = load_config()  # auto-discover in cwd, or defaults

    Parameters
    ----------
    explicit_path:
        Direct path to a ``lace.config`` file. Bypasses auto-discovery
        (error if the file doesn't exist). This is the recommended
        parameter for library usage.
    script_path:
        (CLI use) Path to the ``.lace`` source. Used to locate
        ``lace.config`` in the script's directory as a fallback.
    env_selector:
        Selects ``[lace.config.{env}]`` section. Falls back to
        ``LACE_ENV`` env var when unset.

    Returns a dict with an additional ``_meta`` key indicating whether a
    config file was located (callers use this to distinguish "user provided
    config" from "pure defaults").
    """
    found_path = _resolve_path(script_path, explicit_path)
    raw = _read_toml(found_path)

    env_name = env_selector or os.environ.get("LACE_ENV")
    merged = _merge_with_env(raw, env_name)
    resolved = _resolve_env_refs(merged)
    cfg = _apply_defaults(resolved)
    cfg["_meta"] = {"source_path": found_path}
    return cfg


# ──────────────────────────────────────────────────────────────────────
# File discovery + parse
# ──────────────────────────────────────────────────────────────────────

def _resolve_path(
    script_path: str | None,
    explicit_path: str | None,
) -> str | None:
    if explicit_path:
        if not os.path.isfile(explicit_path):
            raise ConfigError(f"config file not found: {explicit_path}")
        return explicit_path

    candidates: list[str] = []
    if script_path:
        script_dir = os.path.dirname(os.path.abspath(script_path))
        candidates.append(os.path.join(script_dir, "lace.config"))
    candidates.append(os.path.join(os.getcwd(), "lace.config"))

    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


def _read_toml(path: str | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        with open(path, "rb") as f:
            return _toml.load(f)
    except OSError as e:
        raise ConfigError(f"failed to read config {path}: {e}") from e
    except Exception as e:  # tomllib.TOMLDecodeError
        raise ConfigError(f"failed to parse config {path}: {e}") from e


# ──────────────────────────────────────────────────────────────────────
# Section merging (base + [lace.config.{env}])
# ──────────────────────────────────────────────────────────────────────

def _merge_with_env(raw: dict[str, Any], env_name: str | None) -> dict[str, Any]:
    """Strip the ``[lace.config.{env}]`` nest and overlay the selected env.

    The ``[lace.config.{env}]`` TOML section parses into nested dicts under
    ``raw["lace"]["config"][env_name]``. Its contents override top-level
    sections when ``env_name`` is selected.
    """
    # Copy top-level (excluding the lace.config.* namespace).
    base = {k: _deep_copy(v) for k, v in raw.items() if k != "lace"}

    if env_name is None:
        return base

    env_section: Any = (
        raw.get("lace", {}).get("config", {}).get(env_name)
        if isinstance(raw.get("lace"), dict)
        else None
    )
    if not isinstance(env_section, dict):
        return base

    return _deep_merge(base, env_section)


def _deep_copy(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: _deep_copy(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_deep_copy(x) for x in v]
    return v


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    out = _deep_copy(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = _deep_copy(v)
    return out


# ──────────────────────────────────────────────────────────────────────
# env: substitution
# ──────────────────────────────────────────────────────────────────────

def _resolve_env_refs(node: Any) -> Any:
    if isinstance(node, dict):
        return {k: _resolve_env_refs(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_env_refs(v) for v in node]
    if isinstance(node, str):
        return _resolve_env_string(node)
    return node


def _resolve_env_string(s: str) -> str:
    if not s.startswith("env:"):
        return s
    body = s[4:]
    if ":" in body:
        var, default = body.split(":", 1)
        return os.environ.get(var, default)
    val = os.environ.get(body)
    if val is None:
        raise ConfigError(
            f"config references env var {body!r} but it is not set "
            f"(use 'env:{body}:default' to supply a fallback)"
        )
    return val


# ──────────────────────────────────────────────────────────────────────
# Defaults
# ──────────────────────────────────────────────────────────────────────

def _apply_defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    executor = cfg.get("executor") or {}
    if not isinstance(executor, dict):
        raise ConfigError("config [executor] must be a table")

    extensions_list = executor.get("extensions", [])
    if not isinstance(extensions_list, list):
        raise ConfigError("config executor.extensions must be an array")
    extensions_list = [str(x) for x in extensions_list]

    max_redirects = int(executor.get("maxRedirects", _DEFAULT_MAX_REDIRECTS))
    max_timeout_ms = int(executor.get("maxTimeoutMs", _DEFAULT_MAX_TIMEOUT_MS))
    user_agent = executor.get("user_agent")
    if user_agent is not None and not isinstance(user_agent, str):
        raise ConfigError("config executor.user_agent must be a string")

    result = cfg.get("result") or {}
    if not isinstance(result, dict):
        raise ConfigError("config [result] must be a table")

    result_path: Any = result.get("path", _DEFAULT_RESULT_PATH)
    # $LACE_RESULT_PATH overrides config when set (useful for test harnesses).
    env_result_path = os.environ.get("LACE_RESULT_PATH")
    if env_result_path is not None:
        result_path = env_result_path
    # Accept string, bool false, or the literal string "false".
    if isinstance(result_path, str) and result_path.lower() == "false":
        result_path = False

    bodies = result.get("bodies") or {}
    if not isinstance(bodies, dict):
        raise ConfigError("config [result.bodies] must be a table")
    # result.bodies.dir: path string = save there, false = don't save (default).
    bodies_dir = bodies.get("dir", False)
    if isinstance(bodies_dir, str) and bodies_dir.lower() == "false":
        bodies_dir = False
    env_bodies_dir = os.environ.get("LACE_BODIES_DIR")
    if env_bodies_dir is not None:
        bodies_dir = env_bodies_dir

    ext_block = cfg.get("extensions") or {}
    if not isinstance(ext_block, dict):
        raise ConfigError("config [extensions] must be a table")

    return {
        "executor": {
            "extensions": extensions_list,
            "maxRedirects": max_redirects,
            "maxTimeoutMs": max_timeout_ms,
            "user_agent": user_agent,
        },
        "result": {
            "path": result_path,
            "bodies": {"dir": bodies_dir},
        },
        "extensions": ext_block,
    }
