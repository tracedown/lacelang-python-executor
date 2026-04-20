"""High-level library API for running Lace scripts programmatically.

Provides ``LaceExecutor`` (config + extension holder) and ``LaceProbe``
(prepared script with auto-prev tracking, bound to its executor).

Default project layout::

    my-project/
      lace/
        lace.config                   # executor config (auto-discovered)
        config.staging.json           # env-specific variable overrides
        extensions/                   # third-party .laceext files
          custom.laceext
          custom.config
        scripts/
          health/
            health.lace
            vars.json                 # default variables for this script
            vars.staging.json         # env-specific variables

Typical usage::

    from lacelang_executor import LaceExecutor

    executor = LaceExecutor("my-project/lace")
    probe = executor.probe("health")
    result = probe.run(vars={"base_url": "https://api.example.com"})
    result = probe.run()  # prev from last run injected automatically
"""

from __future__ import annotations

import json
import os
from typing import Any

from lacelang_validator.parser import parse
from lacelang_validator.validator import validate

from lacelang_executor.config import load_config
from lacelang_executor.executor import run_script


# ─── Helpers ────────────────────────────────────────────────────────

def _load_json(path_or_dict: str | dict[str, Any]) -> dict[str, Any]:
    """Accept a file path (str) or an already-loaded dict."""
    if isinstance(path_or_dict, dict):
        return path_or_dict
    with open(path_or_dict, encoding="utf-8") as f:
        return json.load(f)


def _read_source(path_or_source: str) -> tuple[str, str | None]:
    """Return (source_text, file_path).

    If the string ends with ``.lace`` or exists on disk, read it as a
    file. Otherwise treat it as inline Lace source code.
    """
    if path_or_source.rstrip().endswith(".lace") or os.path.isfile(path_or_source):
        with open(path_or_source, encoding="utf-8") as f:
            return f.read(), os.path.abspath(path_or_source)
    return path_or_source, None


# ─── LaceProbe ──────────────────────────────────────────────────────

class LaceProbe:
    """A parsed Lace script bound to its parent ``LaceExecutor``.

    Created by ``LaceExecutor.probe()`` — not intended for direct
    construction. Tracks its own ``prev`` result across runs so
    sequential executions of the same probe automatically chain.
    """

    def __init__(
        self,
        executor: LaceExecutor,
        ast: dict[str, Any],
        *,
        script_path: str | None = None,
        name: str | None = None,
    ) -> None:
        self._executor = executor
        self._ast = ast
        self._prev: dict[str, Any] | None = None
        self.script_path = script_path
        self.name = name

    @property
    def prev(self) -> dict[str, Any] | None:
        """Last result from ``run()``, or ``None`` if not yet run."""
        return self._prev

    @prev.setter
    def prev(self, value: dict[str, Any] | None) -> None:
        self._prev = value

    def run(
        self,
        vars: str | dict[str, Any] | None = None,
        prev: str | dict[str, Any] | None = None,
        *,
        reparse: bool = False,
    ) -> dict[str, Any]:
        """Execute the probe and return a ProbeResult dict.

        Parameters
        ----------
        vars:
            Script variables — a dict or a path to a JSON file.
        prev:
            Previous run result — a dict or a path to a JSON file.
            When omitted and ``track_prev`` is enabled on the executor,
            the result from the last ``run()`` call is used.
        reparse:
            When ``True``, re-read and re-parse the script from disk
            before running. Only works for file-backed probes.
        """
        if (reparse or getattr(self, "_always_reparse", False)) and self.script_path:
            with open(self.script_path, encoding="utf-8") as f:
                self._ast = parse(f.read())

        script_vars = _load_json(vars) if vars is not None else {}

        if prev is not None:
            prev_result = _load_json(prev)
        elif self._executor.track_prev and self._prev is not None:
            prev_result = self._prev
        else:
            prev_result = None

        result = run_script(
            self._ast,
            script_vars=script_vars,
            prev=prev_result,
            active_extensions=self._executor._active_ext_names or None,
            extension_paths=self._executor._extension_paths or None,
            config=self._executor._config,
            user_agent=self._executor._config["executor"].get("user_agent"),
        )

        if self._executor.track_prev:
            self._prev = result

        return result


# ─── LaceExtension ──────────────────────────────────────────────────

class LaceExtension:
    """A registered extension with its manifest and optional config.

    Created by ``LaceExecutor.extension()`` — not intended for direct
    construction.
    """

    def __init__(
        self,
        path: str,
        config_path: str | None = None,
    ) -> None:
        self.path = os.path.abspath(path)
        self.config_path = os.path.abspath(config_path) if config_path else None
        # Derive name from the .laceext filename.
        self.name = os.path.splitext(os.path.basename(path))[0]


# ─── LaceExecutor ──────────────────────────────────────────────────

class LaceExecutor:
    """Holds resolved config and extensions; creates and runs probes.

    Parameters
    ----------
    root:
        Path to the ``lace/`` directory. When set, the executor
        discovers ``lace.config`` inside it and resolves script names
        relative to ``{root}/scripts/``.  Pass ``None`` to skip
        directory-based discovery (use ``config`` kwarg instead).
    config:
        Explicit path to a ``lace.config`` file, overriding
        ``{root}/lace.config`` discovery.
    env:
        Selects ``[lace.config.{env}]`` section (overrides ``LACE_ENV``).
    extensions:
        Built-in extension names to activate (e.g.
        ``["laceNotifications"]``). Merged with config-declared ones.
    track_prev:
        When ``True`` (default), each ``LaceProbe`` stores its last
        result and injects it as ``prev`` on the next ``run()``.
    """

    def __init__(
        self,
        root: str | None = None,
        *,
        config: str | None = None,
        env: str | None = None,
        extensions: list[str] | None = None,
        track_prev: bool = True,
    ) -> None:
        # Resolve the lace root directory.
        if root is not None:
            self._root: str | None = os.path.abspath(root)
        else:
            self._root = None

        # Config: explicit path > {root}/lace.config > cwd discovery.
        config_path = config
        if config_path is None and self._root:
            candidate = os.path.join(self._root, "lace.config")
            if os.path.isfile(candidate):
                config_path = candidate

        self._config = load_config(explicit_path=config_path, env_selector=env)
        self.track_prev = track_prev

        # Built-in extensions from config + constructor arg.
        cfg_exts = list(self._config["executor"]["extensions"])
        extra_exts = list(extensions or [])
        merged: list[str] = []
        for name in extra_exts + cfg_exts:
            if name not in merged:
                merged.append(name)
        self._active_ext_names = merged

        # User-registered extensions (paths to .laceext files).
        self._extension_paths: list[str] = []
        self._extensions: list[LaceExtension] = []

        # Prepared probes, keyed by name.
        self._probes: dict[str, LaceProbe] = {}

    @property
    def root(self) -> str | None:
        """The lace root directory, or ``None`` if not set."""
        return self._root

    @property
    def config(self) -> dict[str, Any]:
        """The resolved config dict."""
        return self._config

    # ── Extension registration ──────────────────────────────────────

    def extension(
        self,
        path: str,
        config_path: str | None = None,
    ) -> LaceExtension:
        """Register a third-party extension.

        Parameters
        ----------
        path:
            Path to a ``.laceext`` file or a directory containing one
            (the directory's name is used to find ``{name}.laceext``).
        config_path:
            Optional path to the extension's ``.config`` file. When
            omitted and ``path`` is a directory, the executor looks for
            ``{name}.config`` alongside the manifest.
        """
        # If path is a directory, resolve the manifest inside it.
        if os.path.isdir(path):
            name = os.path.basename(os.path.abspath(path))
            manifest = os.path.join(path, f"{name}.laceext")
            if not os.path.isfile(manifest):
                raise FileNotFoundError(
                    f"no {name}.laceext found in directory {path}")
            if config_path is None:
                candidate = os.path.join(path, f"{name}.config")
                if os.path.isfile(candidate):
                    config_path = candidate
            path = manifest

        if not os.path.isfile(path):
            raise FileNotFoundError(f"extension not found: {path}")

        ext = LaceExtension(path, config_path)
        self._extensions.append(ext)
        self._extension_paths.append(ext.path)

        # Merge extension config into the executor config's
        # [extensions] block so the registry can forward it.
        if ext.config_path:
            self._merge_extension_config(ext)

        return ext

    def _merge_extension_config(self, ext: LaceExtension) -> None:
        """Load extension .config and merge into the executor config."""
        if not ext.config_path:
            return
        try:
            import sys
            if sys.version_info >= (3, 11):
                import tomllib
            else:
                import tomli as tomllib  # type: ignore[import-not-found]
            with open(ext.config_path, "rb") as f:
                raw = tomllib.load(f)
            # The [config] section of the extension config maps to
            # config["extensions"][ext.name].
            ext_section = raw.get("config") or {}
            if ext_section:
                exts = self._config.setdefault("extensions", {})
                exts[ext.name] = ext_section
        except Exception:
            pass  # Config loading is best-effort for .config files.

    # ── Probe creation ──────────────────────────────────────────────

    def probe(
        self,
        script: str,
        *,
        vars: str | dict[str, Any] | None = None,
        always_reparse: bool = False,
    ) -> LaceProbe:
        """Create or retrieve a prepared probe for a script.

        Parameters
        ----------
        script:
            One of:

            - A script **name** (e.g. ``"health"``). Resolved to
              ``{root}/scripts/{name}/{name}.lace``.
            - A file **path** ending in ``.lace``.
            - Inline Lace **source code**.
        vars:
            Default variables for this probe — a dict or path to a JSON
            file. Can be overridden per-run in ``probe.run(vars=...)``.
        always_reparse:
            When ``True``, the probe re-reads the script file on every
            ``run()`` call. Useful during development. Default is
            ``False`` (parse once, reuse the AST).
        """
        source, script_path, name = self._resolve_script(script)
        ast = parse(source)

        # Validate at preparation time.
        ctx = {
            "maxRedirects": self._config["executor"]["maxRedirects"],
            "maxTimeoutMs": self._config["executor"]["maxTimeoutMs"],
        }
        sink = validate(
            ast,
            variables=None,
            context=ctx,
            active_extensions=self._active_ext_names or None,
        )
        if sink.errors:
            codes = ", ".join(d.code for d in sink.errors)
            raise ValueError(f"validation failed: {codes}")

        p = LaceProbe(
            self,
            ast,
            script_path=script_path,
            name=name,
        )
        if always_reparse:
            # Stash a flag so run() re-parses each time.
            p._always_reparse = True  # type: ignore[attr-defined]

        if name:
            self._probes[name] = p

        return p

    def run(
        self,
        script: str,
        vars: str | dict[str, Any] | None = None,
        prev: str | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """One-shot: parse, validate, and run a script.

        Does not track prev across calls — use ``probe()`` for that.
        """
        source, script_path, _ = self._resolve_script(script)
        ast = parse(source)

        script_vars = _load_json(vars) if vars is not None else {}
        prev_result = _load_json(prev) if prev is not None else None

        return run_script(
            ast,
            script_vars=script_vars,
            prev=prev_result,
            active_extensions=self._active_ext_names or None,
            extension_paths=self._extension_paths or None,
            config=self._config,
            user_agent=self._config["executor"].get("user_agent"),
        )

    # ── Internal ────────────────────────────────────────────────────

    def _resolve_script(self, script: str) -> tuple[str, str | None, str | None]:
        """Resolve a script reference to (source, path, name).

        Resolution order:
        1. If ``script`` ends with ``.lace`` → treat as file path.
        2. If ``root`` is set and ``{root}/scripts/{script}/{script}.lace``
           exists → use that (name-based lookup).
        3. If ``script`` exists as a file on disk → read it.
        4. Otherwise → treat as inline source code.
        """
        # 1. Explicit .lace path
        if script.rstrip().endswith(".lace"):
            with open(script, encoding="utf-8") as f:
                return f.read(), os.path.abspath(script), None

        # 2. Name-based lookup: {root}/scripts/{name}/{name}.lace
        if self._root:
            candidate = os.path.join(
                self._root, "scripts", script, f"{script}.lace")
            if os.path.isfile(candidate):
                with open(candidate, encoding="utf-8") as f:
                    return f.read(), os.path.abspath(candidate), script

        # 3. Existing file on disk
        if os.path.isfile(script):
            with open(script, encoding="utf-8") as f:
                return f.read(), os.path.abspath(script), None

        # 4. Inline source
        return script, None, None
