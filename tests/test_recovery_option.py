"""laceEmitRecovery: script-declared recovery notifications.

The `recovery` option on any expect/check scope or assert condition names
the notification a recovery emits:

    options: { recovery: { notification: template("back-up") } }
    options: { recovery: { notification: "plain text" } }

Precedence: script-declared > config `notification` > config
`recovery_message`. Uses a loopback HTTP server — no external network.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from lacelang_executor import LaceExecutor


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 — http.server API
        body = json.dumps({"ok": True, "items": [1, 2]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):  # silence per-request stderr noise
        pass


@pytest.fixture(scope="module")
def base_url():
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _run(script, prev, config=None):
    executor = LaceExecutor(
        root=None, track_prev=False,
        extensions=["laceNotifications", "laceEmitRecovery"],
    )
    if config:
        executor._config.setdefault("extensions", {})["laceEmitRecovery"] = config
    result = executor.run(script=script, vars=None, prev=prev)
    return (result.get("actions") or {}).get("notifications")


def _recovered(notifications):
    assert notifications is not None
    events = [n for n in notifications if n["trigger"] == "recovered"]
    assert len(events) == 1
    return events[0]["notification"]


def test_scope_template_declaration_wins(base_url):
    script = (
        f'get("{base_url}/get")\n'
        '.expect(status: { value: 200, options: '
        '{ recovery: { notification: template("back-up") } } })'
    )
    notif = _recovered(_run(script, prev={"outcome": "failure"}))
    assert notif == {"tag": "template", "name": "back-up"}


def test_bare_string_is_text_shorthand(base_url):
    script = (
        f'get("{base_url}/get")\n'
        '.expect(status: { value: 200, options: '
        '{ recovery: { notification: "all clear" } } })'
    )
    notif = _recovered(_run(script, prev={"outcome": "timeout"}))
    assert notif == {"tag": "text", "value": "all clear"}


def test_assert_condition_declaration(base_url):
    script = (
        f'get("{base_url}/get")\n'
        '.expect(status: 200)\n'
        '.assert({ expect: [ { condition: count(this.body.items) eq 2, options: '
        '{ recovery: { notification: template("counted") } } } ] })'
    )
    notif = _recovered(_run(script, prev={"outcome": "failure"}))
    assert notif == {"tag": "template", "name": "counted"}


def test_undeclared_falls_back_to_config_message(base_url):
    script = f'get("{base_url}/get")\n.expect(status: 200)'
    notif = _recovered(_run(script, prev={"outcome": "failure"}))
    assert notif == {"tag": "text", "value": "Service recovered"}


def test_declared_beats_config_notification(base_url):
    script = (
        f'get("{base_url}/get")\n'
        '.expect(status: { value: 200, options: '
        '{ recovery: { notification: template("from-script") } } })'
    )
    notif = _recovered(_run(
        script, prev={"outcome": "failure"},
        config={
            "recovery_message": "cfg text",
            "notification": {"tag": "template", "name": "from-config"},
        },
    ))
    assert notif == {"tag": "template", "name": "from-script"}


def test_no_transition_emits_nothing(base_url):
    script = (
        f'get("{base_url}/get")\n'
        '.expect(status: { value: 200, options: '
        '{ recovery: { notification: template("back-up") } } })'
    )
    notifications = _run(script, prev={"outcome": "success"})
    assert not [n for n in (notifications or []) if n["trigger"] == "recovered"]
