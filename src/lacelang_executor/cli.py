"""CLI for lacelang-executor — supports the full `parse` / `validate` / `run`
testkit contract. Parse and validate delegate to the `lacelang-validator`
dependency; `run` is the executor-specific entry point.

Exit codes:
  0 on processed request (errors are in the JSON body)
  2 on tool/arg errors
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Any

from lacelang_validator.cli import (
    add_common_flags,
    cmd_parse,
    cmd_validate,
    emit,
    read_json,
    read_text,
)
from lacelang_validator.parser import ParseError, parse
from lacelang_validator.validator import validate

from lacelang_executor import __version__
from lacelang_executor.config import ConfigError, load_config
from lacelang_executor.executor import _default_bodies_dir


def _parse_var_kv(raw: str) -> tuple[str, Any]:
    """Parse a ``KEY=VALUE`` pair from --var.

    VALUE is interpreted as JSON when it parses; otherwise kept as a string.
    This matches how scripts generally want to inject scalars (numbers, bools)
    without quoting gymnastics.
    """
    if "=" not in raw:
        raise ValueError(f"--var expects KEY=VALUE, got {raw!r}")
    key, _, value = raw.partition("=")
    if not key:
        raise ValueError(f"--var KEY must be non-empty: {raw!r}")
    try:
        parsed: Any = json.loads(value)
    except json.JSONDecodeError:
        parsed = value
    return key, parsed


def _save_result(result: dict[str, Any], target: str | bool, pretty: bool) -> None:
    """Persist ``result`` to disk per spec §11 result.path semantics."""
    if target is False or (isinstance(target, str) and target.lower() == "false"):
        return
    if not isinstance(target, str):
        return

    # Directory: {dir}/{YYYY-MM-DD_HH-MM-SS}.json
    if os.path.isdir(target):
        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_path = os.path.join(target, f"{stamp}.json")
    else:
        # Ensure parent dir exists when writing to a fresh file path.
        parent = os.path.dirname(os.path.abspath(target))
        if parent:
            os.makedirs(parent, exist_ok=True)
        out_path = target

    with open(out_path, "w", encoding="utf-8") as f:
        if pretty:
            json.dump(result, f, indent=2)
        else:
            json.dump(result, f)


def cmd_run(args: argparse.Namespace) -> int:
    from lacelang_executor.executor import run_script

    try:
        source = read_text(args.script)
    except OSError as e:
        print(f"error reading script: {e}", file=sys.stderr)
        return 2

    # ─── lace.config loading ─────────────────────────────────────────
    try:
        config = load_config(
            script_path=args.script,
            explicit_path=args.config,
            env_selector=args.env,
        )
    except ConfigError as e:
        # Spec §11: unset `env:VARNAME` is a startup error. Surface as a
        # failure-shaped result so conformance tooling (and downstream
        # pipelines) can diff against structured output instead of parsing
        # stderr.
        now = datetime.now().isoformat(timespec="milliseconds") + "Z"
        emit({"outcome": "failure", "error": f"config error: {e}",
              "startedAt": now, "endedAt": now, "elapsedMs": 0,
              "runVars": {}, "calls": [], "actions": {}},
             args.pretty)
        return 0

    script_vars: dict[str, Any] = {}
    prev: dict[str, Any] | None = None
    try:
        if args.vars:
            script_vars = dict(read_json(args.vars))
        # --var KEY=VALUE overrides --vars entries.
        for raw in args.var or []:
            try:
                k, v = _parse_var_kv(raw)
            except ValueError as e:
                print(f"error: {e}", file=sys.stderr)
                return 2
            script_vars[k] = v
        if args.prev_results:
            prev = read_json(args.prev_results)
    except (OSError, json.JSONDecodeError) as e:
        print(f"error reading aux input: {e}", file=sys.stderr)
        return 2

    try:
        ast = parse(source)
    except ParseError as e:
        emit({"outcome": "failure",
              "error": f"parse error on line {e.line}: {e.message}"},
             args.pretty)
        return 0

    # Merge CLI-enabled extensions with config-declared extensions (dedup,
    # preserve order: CLI-first so --enable-extension wins on precedence).
    cli_exts = list(args.enable_extensions or [])
    cfg_exts = list(config["executor"]["extensions"])
    merged_exts: list[str] = []
    for name in cli_exts + cfg_exts:
        if name not in merged_exts:
            merged_exts.append(name)

    # Spec §12: validate the parsed AST before executing. An invalid script
    # must not be run. The validator is shared with the `validate` subcommand
    # — same diagnostic codes, same semantics. `variables=None` means the
    # registry is unknown at startup: per §15.2 a $var absent from the
    # registry resolves to null at runtime, so unknown refs are non-fatal.
    ctx = {
        "maxRedirects": config["executor"]["maxRedirects"],
        "maxTimeoutMs": config["executor"]["maxTimeoutMs"],
    }
    sink = validate(
        ast,
        variables=None,
        context=ctx,
        prev_results_available=(prev is not None),
        active_extensions=merged_exts or None,
    )
    if sink.errors:
        now = datetime.now().isoformat(timespec="milliseconds") + "Z"
        codes = ",".join(d.code for d in sink.errors)
        failure = {
            "outcome": "failure",
            "error": f"validation failed: {codes}",
            "startedAt": now,
            "endedAt": now,
            "elapsedMs": 0,
            "runVars": {},
            "calls": [],
            "actions": {},
        }
        emit(failure, args.pretty)
        return 0

    # --save-body CLI flag: set bodies.dir to result path (or temp default).
    if args.save_body and not args.bodies_dir:
        result_path = config["result"].get("path", ".")
        if isinstance(result_path, str):
            config["result"]["bodies"]["dir"] = result_path
        else:
            config["result"]["bodies"]["dir"] = _default_bodies_dir()

    result = run_script(
        ast,
        script_vars=script_vars,
        prev=prev,
        bodies_dir=args.bodies_dir,
        active_extensions=merged_exts or None,
        config=config,
        user_agent=config["executor"].get("user_agent"),
    )
    # Surface validator warnings as a top-level `validationWarnings` array.
    # We keep them separate from per-call `warnings` (which are plain strings
    # emitted at runtime) because validator diagnostics are structured dicts
    # with `code` / `callIndex` / `chainMethod` fields — mixing shapes into
    # the first call's warnings array would make consumers parse two kinds
    # of entries. A dedicated top-level key also survives when the script
    # has no successfully-executed calls.
    if sink.warnings:
        result["validationWarnings"] = [d.to_dict() for d in sink.warnings]
    emit(result, args.pretty)

    # Persist to disk when explicitly requested: either --save-to is set, or
    # a lace.config file was actually loaded with a result.path directive.
    # With no flags and no config file we preserve the historical behavior of
    # emitting only to stdout.
    if args.save_to is not None:
        save_target: Any = args.save_to
        should_save = True
    elif config.get("_meta", {}).get("source_path"):
        save_target = config["result"]["path"]
        should_save = True
    else:
        save_target = False
        should_save = False

    if should_save:
        try:
            _save_result(result, save_target, args.pretty)
        except OSError as e:
            # Surface the save error but don't fail the run — stdout already
            # has the authoritative result.
            print(f"warning: failed to save result: {e}", file=sys.stderr)

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lacelang-executor",
        description="Reference Python executor for the Lace probe scripting language.",
    )
    p.add_argument("--version", action="version",
                   version=f"lacelang-executor {__version__}")

    common = argparse.ArgumentParser(add_help=False)
    add_common_flags(common)

    sub = p.add_subparsers(dest="command", required=True)

    pp = sub.add_parser("parse", parents=[common],
                        help="Parse a script (delegates to lacelang-validator).")
    pp.add_argument("script")
    pp.set_defaults(func=cmd_parse)

    pv = sub.add_parser("validate", parents=[common],
                        help="Validate a script (delegates to lacelang-validator).")
    pv.add_argument("script")
    pv.add_argument("--vars-list", dest="vars_list",
                    help="JSON array of declared variable names.")
    pv.add_argument("--context",
                    help="JSON object with validator context.")
    pv.set_defaults(func=cmd_validate)

    pr = sub.add_parser("run", parents=[common],
                        help="Run a script and emit a ProbeResult.")
    pr.add_argument("script")
    pr.add_argument("--vars", help="JSON object of variable values.")
    pr.add_argument("--var", action="append", default=[],
                    help="Inject a single variable as KEY=VALUE "
                         "(repeatable; overrides --vars). VALUE is parsed as "
                         "JSON when valid, otherwise a raw string.")
    # --prev is a short alias for --prev-results.
    pr.add_argument("--prev-results", "--prev", dest="prev_results",
                    help="JSON of previous run results (spec §15.13). "
                         "--prev is a short alias.")
    pr.add_argument("--config", dest="config",
                    help="Explicit path to a lace.config TOML file.")
    pr.add_argument("--env", dest="env",
                    help="Selects [lace.config.{env}] section "
                         "(overrides LACE_ENV).")
    pr.add_argument("--save-to", dest="save_to",
                    help="Overrides result.path: directory → timestamped "
                         "JSON file, file path → overwrites, 'false' → skip.")
    pr.add_argument("--bodies-dir", dest="bodies_dir",
                    help="Directory for request/response body files. "
                         "Defaults to $LACE_BODIES_DIR or <tmp>/lacelang-bodies.")
    pr.add_argument("--save-body", dest="save_body",
                    action="store_true", default=False,
                    help="Enable response body file saving "
                         "(sets result.bodies.dir to the result path).")
    pr.set_defaults(func=cmd_run)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)
