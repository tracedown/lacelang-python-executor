"""Integration tests — require network access (httpbin.org).

Run with: pytest --network
"""

import pytest

from lacelang_validator.parser import parse
from lacelang_executor.executor import run_script
from lacelang_executor import LaceExecutor

pytestmark = pytest.mark.network


# ── Helpers ─────────────────────────────────────────────────────

def _run(source: str, **kwargs) -> dict:
    ast = parse(source)
    return run_script(ast, **kwargs)


# ── Basic HTTP ──────────────────────────────────────────────────

class TestBasicHTTP:
    def test_get_200(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        assert result["outcome"] == "success"
        assert len(result["calls"]) == 1
        assert result["calls"][0]["outcome"] == "success"
        assert result["calls"][0]["response"]["status"] == 200

    def test_get_404_expect_fails(self):
        result = _run('get("https://httpbin.org/status/404")\n    .expect(status: 200)')
        assert result["outcome"] == "failure"
        assert result["calls"][0]["assertions"][0]["outcome"] == "failed"

    def test_get_404_check_continues(self):
        result = _run(
            'get("https://httpbin.org/status/404")\n'
            '    .check(status: 200)\n'
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "success"
        assert result["calls"][0]["assertions"][0]["outcome"] == "failed"
        assert result["calls"][1]["outcome"] == "success"

    def test_post_with_json_body(self):
        result = _run(
            'post("https://httpbin.org/post", {\n'
            '    body: json({ key: "value" })\n'
            '})\n'
            '.expect(status: 200)'
        )
        assert result["outcome"] == "success"

    def test_put_method(self):
        result = _run('put("https://httpbin.org/put")\n    .expect(status: 200)')
        assert result["outcome"] == "success"

    def test_patch_method(self):
        result = _run('patch("https://httpbin.org/patch")\n    .expect(status: 200)')
        assert result["outcome"] == "success"

    def test_delete_method(self):
        result = _run('delete("https://httpbin.org/delete")\n    .expect(status: 200)')
        assert result["outcome"] == "success"


# ── Result structure ────────────────────────────────────────────

class TestResultStructure:
    def test_required_top_level_fields(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        for field in ("outcome", "startedAt", "endedAt", "elapsedMs", "runVars", "calls", "actions"):
            assert field in result, f"missing top-level field: {field}"

    def test_elapsed_ms_is_positive(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        assert isinstance(result["elapsedMs"], int)
        assert result["elapsedMs"] >= 0

    def test_call_record_fields(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        call = result["calls"][0]
        for field in ("index", "outcome", "startedAt", "endedAt", "request",
                       "response", "redirects", "assertions", "config", "warnings", "error"):
            assert field in call, f"missing call field: {field}"

    def test_response_timing_fields(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        resp = result["calls"][0]["response"]
        for field in ("status", "statusText", "headers", "bodyPath",
                       "responseTimeMs", "dnsMs", "connectMs", "tlsMs",
                       "ttfbMs", "transferMs", "sizeBytes", "dns", "tls"):
            assert field in resp, f"missing response field: {field}"

    def test_dns_metadata(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        dns = result["calls"][0]["response"]["dns"]
        assert "resolvedIps" in dns
        assert "resolvedIp" in dns

    def test_tls_metadata(self):
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        tls = result["calls"][0]["response"]["tls"]
        assert tls is not None
        assert "protocol" in tls
        assert "cipher" in tls
        assert "certificate" in tls
        cert = tls["certificate"]
        assert "subject" in cert
        assert "notBefore" in cert
        assert "notAfter" in cert

    def test_config_has_defaults(self):
        """Call record config must include resolved defaults (spec §9.2)."""
        result = _run('get("https://httpbin.org/status/200")\n    .expect(status: 200)')
        cfg = result["calls"][0]["config"]
        assert cfg["timeout"]["ms"] == 30000
        assert cfg["timeout"]["action"] == "fail"
        assert cfg["redirects"]["follow"] is True
        assert cfg["redirects"]["max"] == 10
        assert cfg["security"]["rejectInvalidCerts"] is True

    def test_skipped_call_record(self):
        result = _run(
            'get("https://httpbin.org/status/500")\n'
            '    .expect(status: 200)\n'
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "failure"
        assert result["calls"][1]["outcome"] == "skipped"


# ── Variables and store ─────────────────────────────────────────

class TestVariables:
    def test_script_var_interpolation(self):
        result = _run(
            'get("$base_url/status/200")\n    .expect(status: 200)',
            script_vars={"base_url": "https://httpbin.org"},
        )
        assert result["outcome"] == "success"
        assert "httpbin.org" in result["calls"][0]["request"]["url"]

    def test_store_run_var(self):
        result = _run(
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: 200)\n'
            '    .store({ $$code: this.status })'
        )
        assert result["runVars"]["code"] == 200

    def test_store_writeback(self):
        result = _run(
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: 200)\n'
            '    .store({ $code: this.status })'
        )
        assert result["actions"]["variables"]["code"] == 200

    def test_run_var_chaining(self):
        result = _run(
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: 200)\n'
            '    .store({ $$code: this.status })\n'
            'get("https://httpbin.org/headers", {\n'
            '    headers: { "X-Code": "$$code" }\n'
            '})\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "success"
        assert result["runVars"]["code"] == 200


# ── Redirects ───────────────────────────────────────────────────

class TestRedirects:
    def test_redirect_followed(self):
        result = _run(
            'get("https://httpbin.org/redirect/1")\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "success"
        assert len(result["calls"][0]["redirects"]) > 0

    def test_redirect_limit_exceeded(self):
        result = _run(
            'get("https://httpbin.org/redirect/5", {\n'
            '    redirects: { max: 2 }\n'
            '})\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "failure"
        assert "redirect limit" in (result["calls"][0]["error"] or "")


# ── Timeout ─────────────────────────────────────────────────────

class TestTimeout:
    def test_timeout_fails(self):
        result = _run(
            'get("https://httpbin.org/delay/5", {\n'
            '    timeout: { ms: 1000, action: "fail" }\n'
            '})\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "timeout"
        assert result["calls"][0]["outcome"] == "timeout"

    def test_timeout_warn_continues(self):
        result = _run(
            'get("https://httpbin.org/delay/5", {\n'
            '    timeout: { ms: 1000, action: "warn" }\n'
            '})\n'
            '    .expect(status: 200)\n'
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: 200)'
        )
        assert result["outcome"] == "success"
        assert result["calls"][0]["outcome"] == "timeout"
        assert result["calls"][1]["outcome"] == "success"


# ── Assertions ──────────────────────────────────────────────────

class TestAssertions:
    def test_status_array_match(self):
        result = _run(
            'get("https://httpbin.org/status/200")\n'
            '    .expect(status: [200, 201])'
        )
        assert result["outcome"] == "success"

    def test_body_match(self):
        result = _run(
            'get("https://httpbin.org/get")\n'
            '    .expect(status: 200)\n'
            '    .expect(body: { op: "contains", value: "httpbin.org" })'
        )
        assert result["outcome"] == "success"

    def test_custom_assert(self):
        result = _run(
            'get("https://httpbin.org/status/200")\n'
            '    .assert({\n'
            '        expect: [\n'
            '            this.status eq 200\n'
            '        ]\n'
            '    })'
        )
        assert result["outcome"] == "success"
        assert result["calls"][0]["assertions"][0]["outcome"] == "passed"

    def test_custom_assert_indeterminate_null(self):
        """Null in arithmetic → indeterminate, not failed."""
        result = _run(
            'get("https://httpbin.org/status/200")\n'
            '    .assert({\n'
            '        check: [\n'
            '            $missing gt 0\n'
            '        ]\n'
            '    })'
        )
        assert result["calls"][0]["assertions"][0]["outcome"] == "indeterminate"


# ── High-level API ──────────────────────────────────────────────

class TestLaceExecutorAPI:
    def test_one_shot_run(self):
        executor = LaceExecutor()
        result = executor.run(
            'get("https://httpbin.org/status/200")\n    .expect(status: 200)'
        )
        assert result["outcome"] == "success"

    def test_probe_auto_prev(self):
        executor = LaceExecutor()
        probe = executor.probe(
            'get("https://httpbin.org/status/200")\n    .expect(status: 200)'
        )
        r1 = probe.run()
        assert probe.prev is r1
        r2 = probe.run()
        assert probe.prev is r2

    def test_probe_no_prev_tracking(self):
        executor = LaceExecutor(track_prev=False)
        probe = executor.probe(
            'get("https://httpbin.org/status/200")\n    .expect(status: 200)'
        )
        probe.run()
        assert probe.prev is None

    def test_run_with_vars_file(self, tmp_path):
        vars_file = tmp_path / "vars.json"
        vars_file.write_text('{"url": "https://httpbin.org/status/200"}')
        executor = LaceExecutor()
        result = executor.run(
            'get("$url")\n    .expect(status: 200)',
            vars=str(vars_file),
        )
        assert result["outcome"] == "success"
