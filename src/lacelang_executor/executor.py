"""Spec-compliant Lace runtime executor.

Implements lace-spec.md §7 (execution model), §9 (ProbeResult wire
format), §3.2 (redirects), §3.3 (cookie jars), §3.4 (timing),
§4.4–4.6 (chain methods).

Covers the full core spec: variable resolution ($var, $$var), string
interpolation, all HTTP methods, redirect following, TLS verification,
timeout handling (fail/warn/retry), cookie jars (all modes), scope
evaluation (.expect/.check), custom assertions (.assert), store with
run-scope/writeback distinction ($var write-back), .wait, failure
cascade, per-phase timing, body storage, and schema() validation.
"""

from __future__ import annotations

import json
import mimetypes
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit

from lacelang_executor import __version__
from lacelang_validator.ast_fmt import fmt as fmt_expr
from lacelang_executor.http_timing import HttpResult, Timings, send_request
from lacelang_executor.laceext.registry import ExtensionRegistry

try:
    from importlib.resources import files as _pkg_files
except ImportError:  # pragma: no cover
    _pkg_files = None  # type: ignore[assignment]

# Bundled reference extensions. Spec: "Bundled with every Lace executor as
# builtin:laceNotifications". More builtins slot in here as they're added.
BUILTIN_EXTENSIONS: dict[str, str] = {
    "laceNotifications":  "laceNotifications.laceext",
    "laceBaseline":       "laceBaseline.laceext",
    "notifCounter":       "notifCounter.laceext",
    "notifWatch":         "notifWatch.laceext",
    "notifRelay":         "notifRelay.laceext",
    "hookTrace":          "hookTrace.laceext",
    "badNamespace":       "badNamespace.laceext",
    "configDemo":         "configDemo.laceext",
}

# Matches variable interpolation: $var, $$var, ${$var}, ${$$var}.
# Braced forms per spec §3.5 for disambiguation (e.g. "${$host}name").
_INTERP_RE = re.compile(
    r"\$\{(\$\$[a-zA-Z_][a-zA-Z0-9_]*)\}"   # ${$$runvar}
    r"|\$\{(\$[a-zA-Z_][a-zA-Z0-9_]*)\}"     # ${$scriptvar}
    r"|\$\$([a-zA-Z_][a-zA-Z0-9_]*)"         # $$runvar
    r"|\$([a-zA-Z_][a-zA-Z0-9_]*)"           # $scriptvar
)

# Default User-Agent header per lace-spec.md §3.6. Executors default to
# `lace-probe/<version> (<implementation-name>)`; host platforms override
# via `lace.config [executor].user_agent` when they want a fleet-specific
# UA. Scripts can override per-request by setting `headers: { "User-Agent": ... }`.
DEFAULT_USER_AGENT = f"lace-probe/{__version__} (lacelang-python)"

# Spec §3.2: per-call timeout default when the call omits `timeout.ms`.
# This is the execution-context default, NOT the system ceiling
# (`executor.maxTimeoutMs` = 300000 per §11).
DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_TIMEOUT_ACTION = "fail"
DEFAULT_TIMEOUT_RETRIES = 0

# Spec §3.2: security defaults.
DEFAULT_REJECT_INVALID_CERTS = True

# Spec §3.2: redirect defaults (max comes from env.default_max_redirects
# at runtime, which itself defaults to 10 per §11).
DEFAULT_FOLLOW_REDIRECTS = True


# ═════════════════════════════════════════════════════════════════
# Runtime state
# ═════════════════════════════════════════════════════════════════

class _Env:
    def __init__(
        self,
        script_vars: dict[str, Any],
        prev: dict[str, Any] | None,
        bodies_dir: str,
        registry: ExtensionRegistry,
        user_agent: str | None = None,
        save_bodies: bool = False,
    ) -> None:
        self.script_vars = script_vars
        self.run_vars: dict[str, Any] = {}
        self.prev = prev or {}
        self.this: dict[str, Any] | None = None
        self.bodies_dir = bodies_dir
        self.save_bodies = save_bodies
        # Cookie jars keyed by jar name. "__default__" is the inherited jar.
        self.cookie_jars: dict[str, dict[str, str]] = {"__default__": {}}
        self.registry = registry
        # Tag constructors from loaded extensions, cached once per run.
        self.tag_ctors = registry.tag_constructors()
        # Configured UA override (spec §3.6). None → use DEFAULT_USER_AGENT.
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        # Default redirect cap when a call omits `redirects.max`. Populated
        # from lace.config's `executor.maxRedirects` in `run_script`. The
        # attribute is set post-construction so tests / direct callers that
        # bypass run_script still work with the spec default of 10.
        self.default_max_redirects: int = 10


# ═════════════════════════════════════════════════════════════════
# Public entry point
# ═════════════════════════════════════════════════════════════════

def run_script(
    ast: dict[str, Any],
    script_vars: dict[str, Any] | None = None,
    prev: dict[str, Any] | None = None,
    bodies_dir: str | None = None,
    active_extensions: list[str] | None = None,
    extension_paths: list[str] | None = None,
    user_agent: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Resolve bodies dir: --bodies-dir arg > config result.bodies.dir > false.
    # A path string means save bodies; False means don't save.
    if bodies_dir:
        resolved_bodies_dir = bodies_dir
    elif config and isinstance(config.get("result"), dict):
        cfg_dir = (config["result"].get("bodies") or {}).get("dir")
        resolved_bodies_dir = cfg_dir if isinstance(cfg_dir, str) and cfg_dir else False
    else:
        resolved_bodies_dir = False

    save_bodies = isinstance(resolved_bodies_dir, str)
    if save_bodies:
        os.makedirs(resolved_bodies_dir, exist_ok=True)
    else:
        resolved_bodies_dir = _default_bodies_dir()  # placeholder, won't be used

    # Spec §11: forward the `[extensions]` subtree of lace.config so each
    # rule's `config` base sees per-extension settings.
    ext_cfg = (config or {}).get("extensions") or {}
    registry = _load_extensions(
        active_extensions or [],
        extension_paths or [],
        extension_config=ext_cfg,
    )
    env = _Env(script_vars or {}, prev, resolved_bodies_dir, registry,
               user_agent=user_agent, save_bodies=save_bodies)
    # Spec §11: executor.maxRedirects is the default when a call omits
    # redirects.max. Stash on env so _run_call can consult it without being
    # threaded through every helper.
    env.default_max_redirects = _default_max_redirects_from(config)
    started_at = _now_iso()
    started_mono = time.perf_counter()

    script_calls = ast.get("calls", [])

    # Spec / lace-extensions.md §8.2: `on before script` fires once after
    # extensions are loaded and before any call is issued. Context carries
    # the call count and run timestamp; no per-call state yet.
    registry.fire_hook("before script", {
        "script": {
            "callCount": len(script_calls),
            "startedAt": started_at,
        },
        "prev": prev,
    })

    calls: list[dict[str, Any]] = []
    writeback: dict[str, Any] = {}
    overall: str = "success"
    cascade_outcome: str | None = None  # "failure" or "timeout" once tripped

    for i, call in enumerate(script_calls):
        if cascade_outcome is not None:
            calls.append(_skipped_record(i))
            continue

        record = _run_call(call, i, env, writeback)
        calls.append(record)

        # A call records outcome=timeout when the HTTP call timed out. Only
        # cascade when the timeout.action is "fail" or "retry" (exhausted);
        # action=warn records the timeout on the call but the run continues.
        call_action = record["config"].get("timeout", {}).get("action", DEFAULT_TIMEOUT_ACTION)
        if record["outcome"] == "failure":
            cascade_outcome = "failure"
            overall = "failure"
        elif record["outcome"] == "timeout" and call_action != "warn":
            cascade_outcome = "timeout"
            overall = "timeout"

        wait = (call.get("chain") or {}).get("wait")
        if isinstance(wait, int) and wait > 0 and cascade_outcome is None:
            time.sleep(wait / 1000.0)

    ended_at = _now_iso()
    elapsed_ms = int(round((time.perf_counter() - started_mono) * 1000))

    actions: dict[str, Any] = {}
    if writeback:
        actions["variables"] = writeback
    # Extension-emitted action arrays (e.g. `actions.notifications`).
    for key, events in registry.actions.items():
        actions[key] = events

    # Merge extension-emitted run_vars (prefixed with extension name per §9).
    merged_run_vars = dict(env.run_vars)
    merged_run_vars.update(registry.ext_run_vars)

    # Spec / lace-extensions.md §8.2: `on script` is the last extension
    # hook point in a run. It fires after every call record is finalised
    # and before the result is returned, so extensions can summarise state
    # or emit run-level events. Any emits here are still included in the
    # returned result (actions/runVars refreshed below).
    registry.fire_hook("script", {
        "script": {
            "callCount": len(script_calls),
            "startedAt": started_at,
            "endedAt": ended_at,
        },
        "result": {
            "outcome": overall,
            "calls": calls,
            "runVars": merged_run_vars,
            "actions": actions,
        },
        "prev": prev,
    })

    # Re-merge any emits that happened during the `on script` hook.
    for key, events in registry.actions.items():
        actions[key] = events
    merged_run_vars = dict(env.run_vars)
    merged_run_vars.update(registry.ext_run_vars)

    return {
        "outcome": overall,
        "startedAt": started_at,
        "endedAt": ended_at,
        "elapsedMs": elapsed_ms,
        "runVars": merged_run_vars,
        "calls": calls,
        "actions": actions,
    }


def _load_extensions(
    names: list[str],
    paths: list[str],
    *,
    extension_config: dict | None = None,
) -> ExtensionRegistry:
    """Resolve extension names + explicit paths into a populated registry.

    Names are matched against BUILTIN_EXTENSIONS; unknown names raise so the
    caller gets a clear diagnostic. Paths load arbitrary .laceext files from
    disk (useful for third-party extensions not yet bundled).

    `extension_config` is the `[extensions]` subtree of `lace.config` —
    forwarded into the registry so each rule's `config` base sees the
    caller's per-extension settings (spec §11).
    """
    reg = ExtensionRegistry(config=extension_config)
    for name in names:
        if name not in BUILTIN_EXTENSIONS:
            raise RuntimeError(f"unknown builtin extension: {name!r}")
        reg.load(_builtin_path(BUILTIN_EXTENSIONS[name]))
    for p in paths:
        reg.load(p)
    reg.finalize()
    return reg


def _builtin_path(filename: str) -> str:
    """Resolve a builtin extension file. Checks subdir layout first
    (extensions/name/name.laceext), then flat (extensions/name.laceext)."""
    name = filename.removesuffix(".laceext")
    if _pkg_files is not None:
        pkg = _pkg_files("lacelang_executor")
        subdir = pkg.joinpath("extensions", name, filename)
        try:
            if subdir.is_file():
                return str(subdir)
        except (AttributeError, FileNotFoundError):
            pass
        return str(pkg.joinpath("extensions", filename))
    # Fallback — locate relative to this module file.
    base = os.path.join(os.path.dirname(__file__), "extensions")
    subdir = os.path.join(base, name, filename)
    if os.path.isfile(subdir):
        return subdir
    return os.path.join(base, filename)


# ═════════════════════════════════════════════════════════════════
# Per-call execution
# ═════════════════════════════════════════════════════════════════

def _run_call(
    call: dict[str, Any],
    idx: int,
    env: _Env,
    writeback: dict[str, Any],
) -> dict[str, Any]:
    call_started = _now_iso()
    cfg = call.get("config") or {}
    method = call["method"]

    # Spec §9.2: compute resolved config (with all defaults applied) once.
    # Used for execution logic, the emitted call record, and hook contexts.
    resolved_cfg = _resolve_call_config(cfg, env)

    # Fire `on before call` before any wire activity.
    env.registry.fire_hook("before call", {
        "call": {"index": idx, "config": resolved_cfg},
        "prev": env.prev,
    })

    warnings: list[str] = []

    url = _interp(call["url"], env, warnings)
    headers = {_to_header_name(k): _interp_header_value(v, env, warnings)
               for k, v in (cfg.get("headers") or {}).items()}

    body_bytes, body_ct = _resolve_body(cfg.get("body"), env, warnings)
    if body_ct and not any(k.lower() == "content-type" for k in headers):
        headers["Content-Type"] = body_ct

    # Spec §3.6: script-set User-Agent wins; otherwise use env.user_agent
    # (which the executor populated from config or the default).
    if not any(k.lower() == "user-agent" for k in headers):
        headers["User-Agent"] = env.user_agent

    # Apply cookie jar to outgoing request
    active_jar = _apply_cookies_to_request(cfg, env, url, headers)

    # Timeout + retries
    timeout_s, action, retries = _resolve_timeout(cfg)

    # TLS verification — read from resolved config (defaults already applied).
    verify = resolved_cfg["security"]["rejectInvalidCerts"]
    if not verify:
        # Spec §3.2: when rejectInvalidCerts=false, TLS errors become a
        # warning (instead of hard-failing). Detect by probing with
        # verification; on SSLError, warn and let the real request fall
        # through with verify disabled.
        if url.startswith("https://"):
            try:
                from .http_timing import probe_tls_verify
                probe_tls_verify(url, timeout_s)
            except Exception as e:
                warnings.append(f"TLS certificate invalid: {e}; proceeding with rejectInvalidCerts=false")

    # Redirect policy — read from resolved config (defaults already applied).
    follow = resolved_cfg["redirects"]["follow"]
    max_redirects = int(resolved_cfg["redirects"]["max"])

    # Issue request, handling redirects + retries
    http_result, final_url, redirect_hops, redirect_exceeded = \
        _issue_with_redirects_and_retries(
            method=method.upper(),
            url=url,
            headers=headers,
            body=body_bytes,
            timeout_s=timeout_s,
            verify=verify,
            follow=follow,
            max_redirects=max_redirects,
            retries=retries if action == "retry" else 0,
            jar_name=active_jar,
            env=env,
        )

    # Build request record now that we know final URL
    request_rec: dict[str, Any] = {
        "url": url,
        "method": method,
        "headers": headers,
    }

    # Spec §3.7: redirects array is every hop *after* the initial URL.
    # Populated even on REDIRECTS_MAX_LIMIT hard-fail.
    redirects_list: list[str] = list(redirect_hops[1:]) if redirect_hops else []

    # Determine call outcome from transport layer first.
    call_outcome: str = "success"
    response_rec: dict[str, Any] | None = None
    error: str | None = None

    if http_result.timed_out:
        # Spec §3.2 + §7: timeout always marks the call outcome "timeout".
        # action distinguishes whether the failure cascades:
        #   fail / retry-exhausted → cascade (handled by caller via outcome)
        #   warn                   → record timeout, run continues
        call_outcome = "timeout"
        # Per spec §9.2 the timeout is encoded in `outcome`; the call's
        # `error` and `warnings` fields stay clean (the message is implied
        # by outcome=timeout).
        error = None if action == "warn" else http_result.error
    elif http_result.error is not None:
        call_outcome = "failure"
        error = http_result.error
    elif http_result.response is None:
        call_outcome = "failure"
        error = "no response and no error — internal inconsistency"
    elif redirect_exceeded:
        # Spec §3.2 / §15.5: redirect limit exceeded is a hard fail and the
        # response is not surfaced (treat as connection-level failure).
        call_outcome = "failure"
        error = f"redirect limit {max_redirects} exceeded"
    else:
        # Success path — capture response + store cookies + evaluate chain.
        resp = http_result.response
        resp_ct_raw = resp.headers.get("content-type")
        resp_ct = resp_ct_raw if isinstance(resp_ct_raw, str) else (
            resp_ct_raw[0] if isinstance(resp_ct_raw, list) and resp_ct_raw else None
        )
        # Spec §4.2 + §15.12: the `bodySize` scope also gates capture —
        # if the response exceeds the declared threshold, skip writing
        # the body file and record bodyNotCapturedReason="bodyTooLarge".
        # When save_bodies is False, body files are never written.
        body_cap = _body_capture_limit(call.get("chain") or {}, env)
        body_too_large = body_cap is not None and len(resp.body) > body_cap
        if not env.save_bodies:
            body_path = None
        elif body_too_large:
            body_path = None
        else:
            body_path = _write_body_file(env, resp.body, request=False,
                                         content_type=resp_ct,
                                         call_index=idx)
        response_rec = _build_response_rec(resp, body_path)
        if not env.save_bodies:
            response_rec["bodyNotCapturedReason"] = "notRequested"
        elif body_too_large:
            response_rec["bodyNotCapturedReason"] = "bodyTooLarge"
        elif body_path is None:
            response_rec["bodyNotCapturedReason"] = "notRequested"
        _absorb_response_cookies(active_jar, env, resp.headers)
        env.this = _build_this(resp, response_rec, redirects_list)

    # Evaluate chain — .expect / .check / .assert / .store
    assertions: list[dict[str, Any]] = []
    scope_hard_fail = False
    chain = call.get("chain") or {}

    if response_rec is not None:
        scope_hard_fail, assertions = _evaluate_scope_blocks(chain, env, response_rec, idx)
        cond_hard_fail, cond_asserts = _evaluate_assert_block(chain, env, idx)
        assertions.extend(cond_asserts)
        if cond_hard_fail and not scope_hard_fail:
            scope_hard_fail = True

        if scope_hard_fail:
            call_outcome = "failure"

        if not scope_hard_fail and "store" in chain:
            _apply_store({**chain["store"], "__call_index": idx}, env, writeback, warnings)

    record = {
        "index": idx,
        "outcome": call_outcome,
        "startedAt": call_started,
        "endedAt": _now_iso(),
        "request": request_rec,
        "response": response_rec,
        "redirects": redirects_list,
        "assertions": assertions,
        "config": resolved_cfg,
        "warnings": warnings,
        "error": error,
    }
    # Fire `on call` post-hook — hooks see the full resolved config
    # (e.g. timeout_notifications rule inspects call.outcome + call.config).
    env.registry.fire_hook("call", {
        "call": {
            "index": idx,
            "outcome": call_outcome,
            "response": response_rec,
            "assertions": assertions,
            "config": resolved_cfg,
        },
        "prev": env.prev,
    })
    return record


def _skipped_record(idx: int) -> dict[str, Any]:
    return {
        "index": idx,
        "outcome": "skipped",
        "startedAt": None,
        "endedAt": None,
        "request": None,
        "response": None,
        "redirects": [],
        "assertions": [],
        "config": {},
        "warnings": [],
        "error": None,
    }


# ═════════════════════════════════════════════════════════════════
# Request dispatch (redirects + retries)
# ═════════════════════════════════════════════════════════════════

def _issue_with_redirects_and_retries(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout_s: float,
    verify: bool,
    follow: bool,
    max_redirects: int,
    retries: int,
    jar_name: str = "__default__",
    env: _Env | None = None,
) -> tuple[HttpResult, str, list[str], bool]:
    """Returns (result, final_url, hops, redirect_limit_exceeded)."""
    attempt = 0
    while True:
        hops: list[str] = [url]
        cur_url = url
        cur_method = method
        cur_body = body
        redirects = 0
        while True:
            r = send_request(cur_method, cur_url, headers, cur_body, timeout_s, verify_tls=verify)
            if r.response is None:
                break
            status = r.response.status
            if follow and status in (301, 302, 303, 307, 308):
                # Absorb Set-Cookie from redirect responses into the
                # cookie jar so subsequent hops (and future calls)
                # carry the cookies. Spec §3.3: cookies persist in the
                # jar across redirects within a call.
                if env is not None:
                    _absorb_response_cookies(jar_name, env, r.response.headers)
                    # Update Cookie header for the next hop.
                    jar = env.cookie_jars.get(jar_name, {})
                    if jar:
                        headers["Cookie"] = "; ".join(
                            f"{k}={v}" for k, v in jar.items())
                if redirects >= max_redirects:
                    # Exceeded — caller treats as hard fail.
                    return r, cur_url, hops, True
                redirects += 1
                loc = r.response.headers.get("location")
                if isinstance(loc, list):
                    loc = loc[0]
                if not loc:
                    break
                cur_url = urljoin(cur_url, loc)
                hops.append(cur_url)
                if status == 303 or (status in (301, 302) and cur_method == "POST"):
                    cur_method = "GET"
                    cur_body = None
                continue
            # final response
            return r, cur_url, hops, False

        # transport error — possibly retry
        if r.timed_out and attempt < retries:
            attempt += 1
            continue
        return r, cur_url, hops, False


# ═════════════════════════════════════════════════════════════════
# Body handling
# ═════════════════════════════════════════════════════════════════

def _resolve_body(
    body_node: dict[str, Any] | None,
    env: _Env,
    warnings: list[str] | None = None,
) -> tuple[bytes | None, str | None]:
    if not body_node:
        return None, None
    t = body_node.get("type")
    v = body_node.get("value")
    if t == "json":
        return json.dumps(_eval(v, env)).encode("utf-8"), "application/json"
    if t == "form":
        data = _eval(v, env)
        if not isinstance(data, dict):
            return b"", "application/x-www-form-urlencoded"
        return urlencode({k: _stringify(val) for k, val in data.items()}).encode("utf-8"), \
               "application/x-www-form-urlencoded"
    if t == "raw":
        return _interp(v, env, warnings).encode("utf-8"), None
    return None, None


def _write_body_file(
    env: _Env,
    body: bytes | None,
    request: bool,
    content_type: str | None = None,
    call_index: int = 0,
) -> str | None:
    """Persist a request/response body to the shared bodies volume.

    Spec §15.12 mandates the filename convention:
        ``{run_base_dir}/call_{index}_{request|response}.{ext}``

    The call index uniquely identifies the call within a run, so no UUID is
    needed — the per-run bodies directory is unique per execution.
    """
    if body is None or len(body) == 0:
        return None
    kind = "request" if request else "response"
    ext = _ext_for_content_type(content_type)
    name = f"call_{call_index}_{kind}{ext}"
    path = os.path.join(env.bodies_dir, name)
    with open(path, "wb") as f:
        f.write(body)
    return path


# Frequently-seen MIME → extension map. mimetypes.guess_extension falls back
# to oddities on some systems (e.g. returns `.htm` for text/html, or nothing
# at all for application/x-www-form-urlencoded). We override the common cases
# and defer to stdlib for the long tail.
_MIME_EXT: dict[str, str] = {
    "application/json":                   ".json",
    "application/ld+json":                ".json",
    "application/problem+json":           ".json",
    "text/html":                          ".html",
    "application/xhtml+xml":              ".html",
    "text/xml":                           ".xml",
    "application/xml":                    ".xml",
    "text/plain":                         ".txt",
    "text/css":                           ".css",
    "text/javascript":                    ".js",
    "application/javascript":             ".js",
    "text/csv":                           ".csv",
    "application/x-www-form-urlencoded":  ".form",
    "application/pdf":                    ".pdf",
    "application/zip":                    ".zip",
    "application/gzip":                   ".gz",
    "application/octet-stream":           ".bin",
    "image/png":                          ".png",
    "image/jpeg":                         ".jpg",
    "image/gif":                          ".gif",
    "image/webp":                         ".webp",
    "image/svg+xml":                      ".svg",
    "image/x-icon":                       ".ico",
}


def _ext_for_content_type(ct: str | None) -> str:
    if not ct:
        return ".bin"
    # Strip parameters like `; charset=utf-8` and lowercase for lookup.
    base = ct.split(";", 1)[0].strip().lower()
    if base in _MIME_EXT:
        return _MIME_EXT[base]
    # Structured suffixes (RFC 6839): e.g. application/vnd.api+json → .json
    if "+" in base:
        suffix = "+" + base.split("+", 1)[1]
        compound = {"+json": ".json", "+xml": ".xml", "+yaml": ".yaml", "+zip": ".zip"}
        if suffix in compound:
            return compound[suffix]
    guessed = mimetypes.guess_extension(base)
    return guessed or ".bin"


# ═════════════════════════════════════════════════════════════════
# Response shaping
# ═════════════════════════════════════════════════════════════════

def _build_response_rec(resp: Any, body_path: str | None) -> dict[str, Any]:
    t = resp.timings
    dns_obj = {
        "resolvedIps": list(resp.dns.resolved_ips) if resp.dns else [],
        "resolvedIp":  resp.dns.resolved_ip if resp.dns else None,
    }
    tls_obj: dict[str, Any] | None = None
    if resp.tls is not None:
        tls_obj = {
            "protocol":    resp.tls.protocol,
            "cipher":      resp.tls.cipher,
            "alpn":        resp.tls.alpn,
            "certificate": resp.tls.certificate,
        }
    return {
        "status": resp.status,
        "statusText": resp.status_text,
        "headers": _lower_headers(resp.headers),
        "bodyPath": body_path,
        "responseTimeMs": t.response_time_ms,
        "dnsMs": t.dns_ms,
        "connectMs": t.connect_ms,
        "tlsMs": t.tls_ms,
        "ttfbMs": t.ttfb_ms,
        "transferMs": t.transfer_ms,
        "sizeBytes": len(resp.body),
        "dns": dns_obj,
        "tls": tls_obj,
    }


def _lower_headers(h: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in h.items():
        out[k.lower()] = v
    return out


def _build_this(resp: Any, rec: dict[str, Any], redirects: list[str]) -> dict[str, Any]:
    body: Any
    ctype = ""
    for k, v in resp.headers.items():
        if k.lower() == "content-type":
            ctype = v if isinstance(v, str) else (v[0] if v else "")
            break
    decoded: str | None = None
    try:
        decoded = resp.body.decode("utf-8")
    except UnicodeDecodeError:
        decoded = None

    body = decoded
    if decoded is not None and "application/json" in ctype.lower():
        try:
            body = json.loads(decoded)
        except ValueError:
            body = decoded

    return {
        "status": rec["status"],
        "statusText": rec["statusText"],
        "headers": rec["headers"],
        "body": body,
        "size": rec["sizeBytes"],
        "redirects": redirects,
        # Short-name aliases for `this.<field>` expressions (spec §3.4).
        "responseTime": rec["responseTimeMs"],
        "responseTimeMs": rec["responseTimeMs"],
        "totalDelayMs": rec["responseTimeMs"],
        "connect": rec["connectMs"],
        "ttfb": rec["ttfbMs"],
        "transfer": rec["transferMs"],
        # Metadata (spec §3.4.1 / §3.4.2) — structured objects.
        "dns": rec["dns"],
        "tls": rec["tls"],
        # Timing scalars are exposed separately.
        "dnsMs": rec["dnsMs"],
        "tlsMs": rec["tlsMs"],
    }


# ═════════════════════════════════════════════════════════════════
# Cookie jar handling
# ═════════════════════════════════════════════════════════════════

def _apply_cookies_to_request(
    cfg: dict[str, Any],
    env: _Env,
    url: str,
    headers: dict[str, str],
) -> str:
    jar_spec = cfg.get("cookieJar", "inherit")
    jar_name, fresh, selective, clear_list = _resolve_jar_spec(
        jar_spec, cfg.get("clearCookies") or [],
    )
    if fresh:
        env.cookie_jars[jar_name] = {}
    elif selective:
        jar = env.cookie_jars.setdefault(jar_name, {})
        for c in clear_list:
            jar.pop(c, None)
    else:
        env.cookie_jars.setdefault(jar_name, {})

    # Static per-request cookies from cfg.cookies are merged on top.
    static_cookies: dict[str, str] = {}
    for name, expr in (cfg.get("cookies") or {}).items():
        static_cookies[name] = _stringify(_eval(expr, env))

    combined = {**env.cookie_jars[jar_name], **static_cookies}
    if combined:
        cookie_header = "; ".join(f"{k}={v}" for k, v in combined.items())
        headers["Cookie"] = cookie_header
    return jar_name


def _resolve_jar_spec(spec: str, clear_list: list[str]) -> tuple[str, bool, bool, list[str]]:
    """Return (jar_name, is_fresh, is_selective, clear_list)."""
    if spec == "inherit":
        return "__default__", False, False, []
    if spec == "fresh":
        return "__default__", True, False, []
    if spec == "selective_clear":
        return "__default__", False, True, clear_list
    if spec.startswith("named:"):
        return spec[len("named:"):], False, False, []
    if spec.endswith(":selective_clear"):
        return spec[: -len(":selective_clear")], False, True, clear_list
    return "__default__", False, False, []


def _absorb_response_cookies(jar_name: str, env: _Env, headers: dict[str, Any]) -> None:
    for k, v in headers.items():
        if k.lower() != "set-cookie":
            continue
        values = v if isinstance(v, list) else [v]
        for raw in values:
            c = SimpleCookie()
            try:
                c.load(raw)
            except Exception:
                continue
            for name, morsel in c.items():
                env.cookie_jars[jar_name][name] = morsel.value


# ═════════════════════════════════════════════════════════════════
# Timeout / retries
# ═════════════════════════════════════════════════════════════════

def _resolve_timeout(cfg: dict[str, Any]) -> tuple[float, str, int]:
    t = cfg.get("timeout") or {}
    ms = int(t.get("ms", DEFAULT_TIMEOUT_MS))
    action = t.get("action", DEFAULT_TIMEOUT_ACTION)
    retries = int(t.get("retries", DEFAULT_TIMEOUT_RETRIES)) if action == "retry" else 0
    return ms / 1000.0, action, retries


def _resolve_call_config(cfg: dict[str, Any], env: _Env) -> dict[str, Any]:
    """Build the fully-resolved call config with all spec defaults applied.

    Spec §9.2: the call record `config` field must include resolved defaults.
    Extension fields are preserved under `config.extensions` (spec §3.2).
    """
    resolved = _resolve_node(cfg, env)

    # Timeout defaults (§3.2)
    timeout_section = dict(resolved.get("timeout") if isinstance(resolved.get("timeout"), dict) else {})
    timeout_section.setdefault("ms", DEFAULT_TIMEOUT_MS)
    timeout_section.setdefault("action", DEFAULT_TIMEOUT_ACTION)
    timeout_section.setdefault("retries", DEFAULT_TIMEOUT_RETRIES)
    resolved["timeout"] = timeout_section

    # Redirect defaults (§3.2, §11)
    redirect_section = dict(resolved.get("redirects") if isinstance(resolved.get("redirects"), dict) else {})
    redirect_section.setdefault("follow", DEFAULT_FOLLOW_REDIRECTS)
    redirect_section.setdefault("max", env.default_max_redirects)
    resolved["redirects"] = redirect_section

    # Security defaults (§3.2)
    security_section = dict(resolved.get("security") if isinstance(resolved.get("security"), dict) else {})
    security_section.setdefault("rejectInvalidCerts", DEFAULT_REJECT_INVALID_CERTS)
    resolved["security"] = security_section

    return resolved


# ═════════════════════════════════════════════════════════════════
# Scope / assertion evaluation (AssertionRecord shape)
# ═════════════════════════════════════════════════════════════════

_DEFAULT_OP: dict[str, str] = {
    # Spec §4.4: exact-match scopes vs threshold scopes.
    "status":     "eq",
    "body":       "eq",
    "headers":    "eq",
    "size":       "eq",
    "bodySize":    "lt",
    "totalDelayMs": "lt",
    "dns":        "lt",
    "connect":    "lt",
    "tls":        "lt",
    "ttfb":       "lt",
    "transfer":   "lt",
}

_SCOPE_ACTUAL_KEY: dict[str, str] = {
    "status":       "status",
    "body":         "body",
    "headers":      "headers",
    "bodySize":     "sizeBytes",
    "totalDelayMs": "responseTimeMs",
    "dns":          "dnsMs",
    "connect":      "connectMs",
    "tls":          "tlsMs",
    "ttfb":         "ttfbMs",
    "transfer":     "transferMs",
    "size":         "sizeBytes",
}


def _evaluate_scope_blocks(
    chain: dict[str, Any],
    env: _Env,
    response: dict[str, Any],
    call_index: int,
) -> tuple[bool, list[dict[str, Any]]]:
    hard_fail = False
    records: list[dict[str, Any]] = []
    for method in ("expect", "check"):
        block = chain.get(method)
        if not block:
            continue
        for field in [k for k in block if not k.startswith("__")]:
            # Spec §15.7: tls scope is skipped entirely when the call had
            # no TLS phase (plain HTTP → tlsMs == 0).
            if field == "tls" and ((env.this or {}).get("tlsMs", 0) or 0) == 0:
                continue
            sv = block[field]
            expected = _eval(sv.get("value"), env)
            op = sv.get("op") or _DEFAULT_OP.get(field, "eq")
            match_sel = sv.get("match")   # only meaningful for redirects scope
            resolved_options = _resolve_options(sv.get("options"), env)

            # Fire `on before {method}` hook.
            _fire_scope_hook(env, f"before {method}", call_index, field,
                             expected, op, resolved_options, None, None)

            mode = sv.get("mode")
            actual, outcome = _evaluate_scope(
                field, op, expected, env, response, match_sel, mode=mode
            )
            rec = {
                "method": method,
                "scope": field,
                "op": op,
                "outcome": outcome,
                "actual": _jsonable(actual),
                "expected": _jsonable(expected),
                "options": resolved_options if resolved_options else None,
            }
            if field == "redirects":
                rec["match"] = match_sel or "any"
            records.append(rec)

            # Fire `on {method}` post-hook with actual + outcome.
            _fire_scope_hook(env, method, call_index, field,
                             expected, op, resolved_options, actual, outcome)

            if outcome == "failed" and method == "expect":
                hard_fail = True
    return hard_fail, records


def _fire_scope_hook(
    env: _Env,
    hook: str,
    call_index: int,
    scope_name: str,
    expected: Any,
    op: str,
    options: dict[str, Any] | None,
    actual: Any,
    outcome: str | None,
) -> None:
    """Build the scope hook context and dispatch to the registry.

    lace-extensions.md §8.3 / §8.4 specify the context shape. `before` hooks
    have no `actual` / `outcome`; the post-hooks include both.
    """
    scope_ctx: dict[str, Any] = {
        "name": scope_name,
        "value": expected,
        "op": op,
        "options": options,
    }
    if outcome is not None:
        scope_ctx["actual"] = actual
        scope_ctx["outcome"] = outcome
    env.registry.fire_hook(hook, {
        "call": {"index": call_index},
        "scope": scope_ctx,
        "this": env.this,
        "prev": env.prev,
    })


def _resolve_options(options: Any, env: _Env) -> dict[str, Any] | None:
    """Evaluate an `options {}` block. Extension options can contain
    arbitrary expressions (e.g. `notification: text("...")`), which must be
    evaluated here rather than passed through opaquely."""
    if not options:
        return None
    out: dict[str, Any] = {}
    for k, v in options.items():
        out[k] = _eval(v, env)
    return out




def _evaluate_scope(
    field: str,
    op: str,
    expected: Any,
    env: _Env,
    response: dict[str, Any],
    match_sel: str | None,
    *,
    mode: str | None = None,
) -> tuple[Any, str]:
    """Return (actual, outcome) for one scope assertion.

    The `redirects` scope has its own semantics (§4.3): `match` selects which
    hop to compare, and comparison is always equality. All other scopes use
    the generic op-comparison path via `_apply_op`.
    """
    if field == "redirects":
        redirects: list[str] = (env.this or {}).get("redirects") or []
        match_sel = match_sel or "any"
        if match_sel == "any":
            passed = expected in redirects
            actual_repr = redirects
        elif match_sel == "first":
            actual_repr = redirects[0] if redirects else None
            passed = actual_repr == expected
        elif match_sel == "last":
            actual_repr = redirects[-1] if redirects else None
            passed = actual_repr == expected
        else:
            return redirects, "indeterminate"
        return actual_repr, ("passed" if passed else "failed")

    actual = _resolve_scope_actual(field, env, response)
    # bodySize scope accepts human size strings ("1KB", "500B"). Normalise
    # expected to bytes when actual is numeric, so ordered comparisons
    # don't degrade to indeterminate (spec §4.3, §4.4).
    if field == "bodySize" and isinstance(expected, str):
        expected = _parse_size(expected)
    # body: schema($var) — expected is a schema sentinel produced by
    # _eval_func("schema"). Validate the response body against the schema
    # (spec §4.5.1, §15.8). Null schema → hard fail.
    if field == "body" and isinstance(expected, dict) and expected.get("__lace_schema__"):
        schema_doc = expected.get("schema")
        if schema_doc is None:
            return actual, "failed"
        strict = (mode == "strict")
        return actual, _validate_schema(actual, schema_doc, strict=strict)
    return actual, _apply_op(op, actual, expected)


def _validate_schema(body: Any, schema: Any, path: str = "", *, strict: bool = False) -> str:
    """Minimal JSON Schema validator covering the common subset used by
    uptime probes: type, required, properties, enum, pattern. Good enough
    for conformance and test vectors without pulling in jsonschema as a
    runtime dep. Returns "passed" / "failed" / "indeterminate".

    When *strict* is True (spec 4.5.1 ``mode: "strict"``), extra fields
    not declared in ``properties`` cause a ``"failed"`` result at every
    nesting level, equivalent to ``additionalProperties: false``."""
    if body is None and schema is not None:
        return "failed"
    if not isinstance(schema, dict):
        return "indeterminate"
    t = schema.get("type")
    type_map = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "object": dict,
        "array": list,
        "null": type(None),
    }
    if t:
        types = [t] if isinstance(t, str) else t
        # In Python bool is a subclass of int — reject bools when the
        # schema expects integer/number but not boolean.
        if isinstance(body, bool) and "boolean" not in types:
            return "failed"
        if not any(isinstance(body, type_map[tt]) for tt in types if tt in type_map):
            return "failed"
    if schema.get("enum") is not None and body not in schema["enum"]:
        return "failed"
    if isinstance(body, dict):
        for req in schema.get("required", []) or []:
            if req not in body:
                return "failed"
        declared = schema.get("properties") or {}
        # Strict mode: reject any keys not declared in properties (spec 4.5.1).
        if strict and declared:
            extra = set(body.keys()) - set(declared.keys())
            if extra:
                return "failed"
        for k, sub in declared.items():
            if k in body:
                child_path = f"{path}.{k}" if path else f".{k}"
                out = _validate_schema(body[k], sub, child_path, strict=strict)
                if out != "passed":
                    return out
    if isinstance(body, list):
        items = schema.get("items")
        if isinstance(items, dict):
            for idx, it in enumerate(body):
                child_path = f"{path}[{idx}]"
                out = _validate_schema(it, items, child_path, strict=strict)
                if out != "passed":
                    return out
    if isinstance(body, str):
        pat = schema.get("pattern")
        if pat is not None:
            import re as _re
            if not _re.search(pat, body):
                return "failed"
    return "passed"


def _body_capture_limit(chain: dict[str, Any], env: _Env) -> int | None:
    """Look up the bodySize threshold across .expect/.check scopes and
    return it as bytes (for capture gating). None when no bodySize scope
    is declared — capture is uncapped."""
    for method in ("expect", "check"):
        block = chain.get(method) or {}
        sv = block.get("bodySize")
        if not sv:
            continue
        expected = _eval(sv.get("value"), env)
        if isinstance(expected, str):
            expected = _parse_size(expected)
        if isinstance(expected, (int, float)):
            return int(expected)
    return None


_SIZE_RE = re.compile(r"^(\d+)(k|kb|m|mb|g|gb)?$", re.IGNORECASE)

def _parse_size(s: str) -> int | str:
    """Parse a size_string per spec §4.3: ``/\\d+(k|kb|m|mb|g|gb)?/i``.

    Returns bytes as int, or the original string on parse failure (callers
    treat strings as indeterminate vs ints)."""
    if not isinstance(s, str):
        return s
    m = _SIZE_RE.match(s.strip())
    if not m:
        return s
    num = int(m.group(1))
    suf = (m.group(2) or "").upper()
    multipliers = {"": 1, "K": 1024, "KB": 1024, "M": 1024 ** 2, "MB": 1024 ** 2,
                   "G": 1024 ** 3, "GB": 1024 ** 3}
    return num * multipliers[suf]


def _resolve_scope_actual(field: str, env: _Env, response: dict[str, Any]) -> Any:
    key = _SCOPE_ACTUAL_KEY.get(field)
    if key == "body":
        return (env.this or {}).get("body")
    if key == "headers":
        return response.get("headers")
    if key is None:
        return None
    return response.get(key)


def _evaluate_assert_block(
    chain: dict[str, Any],
    env: _Env,
    call_index: int,
) -> tuple[bool, list[dict[str, Any]]]:
    hard_fail = False
    records: list[dict[str, Any]] = []
    block = chain.get("assert")
    if not block:
        return False, records
    for kind in ("expect", "check"):
        items = block.get(kind) or []
        for idx, item in enumerate(items):
            cond = item.get("condition")
            expression_src = fmt_expr(cond) if cond is not None else ""
            resolved_options = _resolve_options(item.get("options"), env)

            _fire_assert_hook(env, "before assert", call_index, idx, kind,
                              expression_src, resolved_options, None, None, None)

            lhs_node, rhs_node = _split_operands(cond)
            actual_lhs = _eval(lhs_node, env) if lhs_node is not None else None
            actual_rhs = _eval(rhs_node, env) if rhs_node is not None else None
            # Spec §5.4: null as operand of ordered comparisons or
            # arithmetic (+, -, *, /, %) is indeterminate — the assertion
            # neither passes nor fails.
            op_name = cond.get("op") if isinstance(cond, dict) else None
            _INDETERMINATE_OPS = ("lt", "lte", "gt", "gte",
                                  "add", "sub", "mul", "div", "mod")
            if op_name in _INDETERMINATE_OPS and (actual_lhs is None or actual_rhs is None):
                outcome = "indeterminate"
            else:
                result = _eval(cond, env)
                outcome = "passed" if bool(result) else "failed"
            rec = {
                "method": "assert",
                "kind": kind,
                "index": idx,
                "outcome": outcome,
                "expression": expression_src,
                "actualLhs": _jsonable(actual_lhs),
                "actualRhs": _jsonable(actual_rhs),
                "options": resolved_options if resolved_options else None,
            }
            records.append(rec)

            _fire_assert_hook(env, "assert", call_index, idx, kind,
                              expression_src, resolved_options,
                              actual_lhs, actual_rhs, outcome)

            if outcome == "failed" and kind == "expect":
                hard_fail = True
    return hard_fail, records


def _fire_assert_hook(
    env: _Env,
    hook: str,
    call_index: int,
    index: int,
    kind: str,
    expression_src: str,
    options: dict[str, Any] | None,
    actual_lhs: Any,
    actual_rhs: Any,
    outcome: str | None,
) -> None:
    cond_ctx: dict[str, Any] = {
        "index": index,
        "kind": kind,
        "expression": expression_src,
        "options": options,
    }
    if outcome is not None:
        cond_ctx["actualLhs"] = actual_lhs
        cond_ctx["actualRhs"] = actual_rhs
        cond_ctx["outcome"] = outcome
    env.registry.fire_hook(hook, {
        "call": {"index": call_index},
        "condition": cond_ctx,
        "this": env.this,
        "prev": env.prev,
    })


def _split_operands(expr: Any) -> tuple[Any, Any]:
    if isinstance(expr, dict) and expr.get("kind") == "binary":
        return expr.get("left"), expr.get("right")
    return expr, None


def _apply_op(op: str, actual: Any, expected: Any) -> str:
    if isinstance(expected, list):
        return "passed" if actual in expected else "failed"
    if actual is None or expected is None:
        if op in ("eq", "neq"):
            eq = actual == expected
            return "passed" if (eq if op == "eq" else not eq) else "failed"
        return "indeterminate"  # per spec §4.5 null ordered comparison
    try:
        if op == "eq":  return "passed" if actual == expected else "failed"
        if op == "neq": return "passed" if actual != expected else "failed"
        if op == "lt":  return "passed" if actual <  expected else "failed"
        if op == "lte": return "passed" if actual <= expected else "failed"
        if op == "gt":  return "passed" if actual >  expected else "failed"
        if op == "gte": return "passed" if actual >= expected else "failed"
    except TypeError:
        return "indeterminate"
    return "indeterminate"


# ═════════════════════════════════════════════════════════════════
# .store
# ═════════════════════════════════════════════════════════════════

def _apply_store(
    block: dict[str, Any],
    env: _Env,
    writeback: dict[str, Any],
    warnings: list[str],
) -> None:
    call_index = block.get("__call_index", 0)
    for key in [k for k in block if not k.startswith("__")]:
        entry = block[key]
        # Spec §8.7: store hook context exposes the entry (key/value/scope)
        # rather than the raw store-block key. `before store` fires before
        # the value is written; `store` fires after with `entry.written`
        # indicating whether the write actually committed (write-once
        # collisions surface as written=false).
        scope = "run" if entry["scope"] == "run" else "writeback"
        # Eval the value once so both hook contexts and the actual write
        # see the same payload.
        val = _eval(entry["value"], env)
        env.registry.fire_hook("before store", {
            "call": {"index": call_index},
            "entry": {"key": key, "value": val, "scope": scope},
            "this": env.this,
            "prev": env.prev,
        })
        # Spec §4.6: any JSON-serialisable shape is valid.
        written = True
        if entry["scope"] == "run":
            bare = key[2:] if key.startswith("$$") else key
            if bare in env.run_vars:
                warnings.append(f"run-scope var {bare!r} already assigned; write-once skip")
                written = False
            else:
                env.run_vars[bare] = val
        else:
            # Spec §4.6: $name keys have the $ stripped in actions.variables.
            wb_key = key[1:] if key.startswith("$") else key
            writeback[wb_key] = val
        env.registry.fire_hook("store", {
            "call": {"index": call_index},
            "entry": {"key": key, "value": val, "scope": scope, "written": written},
            "this": env.this,
            "prev": env.prev,
        })


# ═════════════════════════════════════════════════════════════════
# Expression evaluation
# ═════════════════════════════════════════════════════════════════

def _eval(node: Any, env: _Env) -> Any:
    if not isinstance(node, dict):
        return node
    k = node.get("kind")
    if k == "literal":
        if node.get("valueType") == "string":
            return _interp(node["value"], env)
        return node["value"]
    if k == "scriptVar":
        return _walk_var_path(env.script_vars.get(node["name"]), node.get("path"))
    if k == "runVar":
        return _walk_var_path(env.run_vars.get(node["name"]), node.get("path"))
    if k == "thisRef":
        return _walk_path(env.this, node.get("path", []))
    if k == "prevRef":
        cur: Any = env.prev
        for seg in node.get("path", []):
            if seg["type"] == "field":
                cur = cur.get(seg["name"]) if isinstance(cur, dict) else None
            else:
                i = seg["index"]
                cur = cur[i] if isinstance(cur, list) and 0 <= i < len(cur) else None
        return cur
    if k == "unary":
        op = node.get("op", "not")
        v = _eval(node["operand"], env)
        if op == "not":
            return not bool(v)
        if op == "-":
            return -v if isinstance(v, (int, float)) and not isinstance(v, bool) else None
        return None
    if k == "binary":
        return _eval_binary(node, env)
    if k == "funcCall":
        return _eval_func(node, env)
    if k == "objectLit":
        return {e["key"]: _eval(e["value"], env) for e in node.get("entries", [])}
    if k == "arrayLit":
        return [_eval(i, env) for i in node.get("items", [])]
    return None


def _eval_binary(node: dict[str, Any], env: _Env) -> Any:
    op = node["op"]
    # Spec §2.1: short-circuit evaluation — return the deciding operand.
    if op == "and":
        left = _eval(node["left"], env)
        return _eval(node["right"], env) if left else left
    if op == "or":
        left = _eval(node["left"], env)
        return left if left else _eval(node["right"], env)
    a = _eval(node["left"], env)
    b = _eval(node["right"], env)
    if a is None or b is None:
        if op == "eq":  return a == b
        if op == "neq": return a != b
        return None  # ordered comparisons with null → indeterminate upstream
    try:
        if op == "eq":  return a == b
        if op == "neq": return a != b
        if op == "lt":  return a <  b
        if op == "lte": return a <= b
        if op == "gt":  return a >  b
        if op == "gte": return a >= b
        if op == "+":  return a + b if not isinstance(a, str) or isinstance(b, str) else f"{a}{b}"
        if op == "-":  return a - b
        if op == "*":  return a * b
        if op == "/":  return a / b if b != 0 else None
        if op == "%":  return a % b if b != 0 else None
    except TypeError:
        return None
    return None


def _eval_func(node: dict[str, Any], env: _Env) -> Any:
    name = node["name"]
    args_nodes = node.get("args", [])
    if name in ("json", "form"):
        return _eval(args_nodes[0], env) if args_nodes else None
    if name == "schema":
        val = _eval(args_nodes[0], env) if args_nodes else None
        return {"__lace_schema__": True, "schema": val}
    # Extension-registered tag constructors (from .laceext [types.*] one_of).
    if name in env.tag_ctors:
        args = [_eval(a, env) for a in args_nodes]
        return env.tag_ctors[name](args)
    return None


def _walk_var_path(value: Any, path: list[dict[str, Any]] | None) -> Any:
    """Walk a typed path (fields + int indices) on a scriptVar / runVar.
    Null-propagates on every miss (spec §15.4: field access on null → null)."""
    if not path:
        return value
    cur = value
    for seg in path:
        if cur is None:
            return None
        if seg["type"] == "field":
            cur = cur.get(seg["name"]) if isinstance(cur, dict) else None
        else:
            i = seg["index"]
            cur = cur[i] if isinstance(cur, list) and 0 <= i < len(cur) else None
    return cur


def _walk_path(obj: Any, path: list[str]) -> Any:
    cur = obj
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur


# ═════════════════════════════════════════════════════════════════
# String interpolation
# ═════════════════════════════════════════════════════════════════

def _interp_header_value(v: Any, env: _Env, warnings: list[str]) -> str:
    val = _eval(v, env)
    if val is None:
        warnings.append("null value interpolated as \"null\"")
    return _stringify(val)


def _interp(s: str, env: _Env, warnings: list[str] | None = None) -> str:
    def repl(m: re.Match[str]) -> str:
        # Groups: 1=${$$run}, 2=${$script}, 3=$$run, 4=$script
        if m.group(1):
            # ${$$runvar} — strip leading $$
            varname = m.group(1)[2:]
            val = env.run_vars.get(varname)
            name = m.group(1)
        elif m.group(2):
            # ${$scriptvar} — strip leading $
            varname = m.group(2)[1:]
            val = env.script_vars.get(varname)
            name = m.group(2)
        elif m.group(3):
            val = env.run_vars.get(m.group(3))
            name = "$$" + m.group(3)
        else:
            val = env.script_vars.get(m.group(4))
            name = "$" + m.group(4)
        if val is None and warnings is not None:
            warnings.append(f"null variable {name!r} interpolated as \"null\"")
        return _stringify(val)
    return _INTERP_RE.sub(repl, s)


def _stringify(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float, str)):
        return str(v)
    return json.dumps(v)


def _to_header_name(s: str) -> str:
    return s


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _default_bodies_dir() -> str:
    return os.environ.get("LACE_BODIES_DIR") or os.path.join(tempfile.gettempdir(), "lacelang-bodies")


def _default_max_redirects_from(config: dict[str, Any] | None) -> int:
    """Extract `executor.maxRedirects` from a loaded lace.config dict.

    Falls back to the spec default (10) when the config is absent or the
    field is missing. This is used as the per-call default when a call
    omits `redirects.max`.
    """
    if not config:
        return 10
    executor_cfg = config.get("executor") or {}
    try:
        return int(executor_cfg.get("maxRedirects", 10))
    except (TypeError, ValueError):
        return 10


def _resolve_node(node: Any, env: _Env) -> Any:
    if isinstance(node, dict):
        if "kind" in node:
            # AST expression — evaluate in the current env.
            return _eval(node, env)
        out: dict[str, Any] = {}
        for k, v in node.items():
            if k.startswith("__"):
                continue
            if k == "extensions" and isinstance(v, dict):
                # Preserve extensions sub-object structure (spec §3.2).
                ext_out = {}
                for ek, ev in v.items():
                    ext_out[ek] = _resolve_node(ev, env)
                out["extensions"] = ext_out
                continue
            out[k] = _resolve_node(v, env)
        return out
    if isinstance(node, list):
        return [_resolve_node(i, env) for i in node]
    return node




def _jsonable(v: Any) -> Any:
    """Ensure a value is JSON-serialisable for AssertionRecord.actual /
    expected / actual_lhs / actual_rhs (spec allows any type)."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, list):
        return [_jsonable(i) for i in v]
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    return str(v)
