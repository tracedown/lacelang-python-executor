"""Egress guard + forced TLS verification seams.

These are host-facing security seams (spec-neutral): with no guard configured
the executor behaves exactly as before, so conformance is unaffected. The
tests here drive the seams a host (e.g. the probe agent) uses to enforce a
target-egress policy in the process that actually opens the connection.

All tests use loopback sockets — no external network.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from lacelang_validator.parser import parse

from lacelang_executor import EgressBlocked
from lacelang_executor.executor import run_script
from lacelang_executor.http_timing import (
    DnsMeta,
    HttpResponse,
    HttpResult,
    Timings,
)


# ── Loopback servers ────────────────────────────────────────────────

class _OkHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 — http.server API
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):  # silence per-request stderr noise
        pass


def _make_redirect_handler(location: str):
    class _RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_):
            pass

    return _RedirectHandler


@pytest.fixture
def ok_server():
    server = HTTPServer(("127.0.0.1", 0), _OkHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


# ── Tests ───────────────────────────────────────────────────────────

def _run(source: str, **kwargs) -> dict:
    return run_script(parse(source), **kwargs)


def test_no_guard_leaves_default_behaviour(ok_server):
    """With no guard configured the loopback call succeeds — the seam is a
    pure no-op by default (conformance-preserving)."""
    result = _run(f'get("{ok_server}").expect(status: 200)')
    assert result["outcome"] == "success"
    assert result["calls"][0]["response"]["status"] == 200


def test_blocked_initial_host_is_refused(ok_server):
    """A guard that rejects the initial target's address fails the call before
    any bytes are exchanged."""
    def guard(host: str, port: int, ip: str) -> None:
        if ip == "127.0.0.1":
            raise EgressBlocked(f"blocked initial target {ip}")

    result = _run(f'get("{ok_server}").expect(status: 200)', before_connect=guard)
    call = result["calls"][0]
    assert result["outcome"] == "failure"
    assert call["outcome"] == "failure"
    assert "blocked initial target" in (call["error"] or "")


def test_redirect_to_blocked_address_is_refused():
    """A 302 pointing at an internal address is re-vetted on the redirect hop
    and refused — the initial (allowed) host does not exempt the hop."""
    # Redirect to a different loopback address the guard blocks. Nothing needs
    # to listen there: the guard rejects before any connection is attempted.
    handler = _make_redirect_handler("http://127.0.0.2:9/internal")
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"

    def guard(host: str, port: int, ip: str) -> None:
        # Allow the initial loopback server, block the redirect target.
        if ip == "127.0.0.2" or host == "127.0.0.2":
            raise EgressBlocked(f"blocked redirect target {ip}")

    try:
        result = _run(
            f'get("{base}", {{ redirects: {{ follow: true, max: 5 }} }})'
            f'.expect(status: 200)',
            before_connect=guard,
        )
    finally:
        server.shutdown()

    call = result["calls"][0]
    assert result["outcome"] == "failure"
    assert call["outcome"] == "failure"
    assert "blocked redirect target" in (call["error"] or "")


def test_guard_does_not_fall_through_to_a_second_address(monkeypatch):
    """If any candidate address is blocked the request aborts — it must not
    silently try the next resolved address (DNS-answer padding defence)."""
    import socket as _socket

    from lacelang_executor import http_timing

    # Two resolved addresses: first blocked, second would be "allowed".
    def fake_getaddrinfo(host, port, *a, **kw):
        return [
            (_socket.AF_INET, _socket.SOCK_STREAM, 6, "", ("10.0.0.5", port)),
            (_socket.AF_INET, _socket.SOCK_STREAM, 6, "", ("203.0.113.9", port)),
        ]

    monkeypatch.setattr(http_timing.socket, "getaddrinfo", fake_getaddrinfo)

    seen: list[str] = []

    def guard(host: str, port: int, ip: str) -> None:
        seen.append(ip)
        if ip.startswith("10."):
            raise EgressBlocked(f"blocked {ip}")

    result = _run('get("http://example.test/").expect(status: 200)',
                  before_connect=guard)
    # Only the first (blocked) address was vetted; we never fell through.
    assert seen == ["10.0.0.5"]
    assert result["calls"][0]["outcome"] == "failure"


def test_force_verify_tls_overrides_script_optout(monkeypatch):
    """`force_verify_tls=True` ignores a script's rejectInvalidCerts=false and
    enforces verification, surfacing a warning — the spec feature stays but the
    host policy wins."""
    captured: dict = {}

    def fake_send_request(method, url, headers, body, timeout,
                          verify_tls=True, before_connect=None):
        captured["verify_tls"] = verify_tls
        return HttpResult(
            response=HttpResponse(
                status=200,
                status_text="OK",
                headers={"content-type": "application/json"},
                body=b"{}",
                timings=Timings(),
                final_url=url,
                dns=DnsMeta(),
                tls=None,
            ),
            timings=Timings(),
        )

    monkeypatch.setattr("lacelang_executor.executor.send_request", fake_send_request)

    result = _run(
        'get("https://example.test/", { security: { rejectInvalidCerts: false } })'
        '.expect(status: 200)',
        force_verify_tls=True,
    )

    assert captured["verify_tls"] is True
    warnings = result["calls"][0]["warnings"]
    assert any("host policy" in w for w in warnings)


def test_script_optout_honoured_without_force(monkeypatch):
    """Default (force_verify_tls=False): the script's rejectInvalidCerts=false
    still disables verification — spec behaviour intact."""
    captured: dict = {}

    def fake_send_request(method, url, headers, body, timeout,
                          verify_tls=True, before_connect=None):
        captured["verify_tls"] = verify_tls
        return HttpResult(
            response=HttpResponse(
                status=200,
                status_text="OK",
                headers={},
                body=b"{}",
                timings=Timings(),
                final_url=url,
                dns=DnsMeta(),
                tls=None,
            ),
            timings=Timings(),
        )

    monkeypatch.setattr("lacelang_executor.executor.send_request", fake_send_request)
    # Also stub the speculative cert probe so no real connection is attempted.
    monkeypatch.setattr("lacelang_executor.http_timing.probe_tls_verify",
                        lambda *a, **kw: None)

    _run(
        'get("https://example.test/", { security: { rejectInvalidCerts: false } })'
        '.expect(status: 200)',
        force_verify_tls=False,
    )
    assert captured["verify_tls"] is False
