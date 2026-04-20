"""HTTP client with per-phase timing (dns / connect / tls / ttfb / transfer).

Uses stdlib http.client, patched to measure each phase separately. The
resolution is the OS clock — accurate to microseconds on modern platforms —
but reported values are milliseconds (spec §9.4).

The client returns raw response data without auto-decoding, so the caller
controls body storage. It does NOT follow redirects itself; callers that
opt into redirect following issue a new request per hop and accumulate
timings.
"""

from __future__ import annotations

import http.client
import socket
import ssl
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit


def probe_tls_verify(url: str, timeout: float) -> None:
    """Speculatively open a verified TLS connection to `url` to check whether
    the cert chain would validate. Raises ssl.SSLError / ssl.CertificateError
    / socket.error when the cert is invalid; returns silently otherwise.

    Used by the executor when rejectInvalidCerts=false: we still want to know
    whether the cert WOULD have failed so we can surface a warning.
    """
    parts = urlsplit(url)
    if parts.scheme != "https":
        return
    host = parts.hostname or ""
    port = parts.port or 443
    ctx = ssl.create_default_context()
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw:
            with ctx.wrap_socket(raw, server_hostname=host):
                pass
    except (ssl.SSLError, ssl.CertificateError):
        raise
    except socket.error:
        # Non-TLS error (connection refused etc.) — not our concern here;
        # the real request will surface the error.
        return


@dataclass
class Timings:
    dns_ms: int = 0
    connect_ms: int = 0
    tls_ms: int = 0
    ttfb_ms: int = 0
    transfer_ms: int = 0
    response_time_ms: int = 0


@dataclass
class DnsMeta:
    resolved_ips: list[str] = field(default_factory=list)
    resolved_ip: str | None = None


@dataclass
class TlsMeta:
    """Captured at handshake completion. `certificate` may be None when the
    runtime can't surface a parsed cert (typical under CERT_NONE)."""
    protocol: str = ""
    cipher: str = ""
    alpn: str | None = None
    certificate: dict[str, Any] | None = None


@dataclass
class HttpResponse:
    status: int
    status_text: str
    headers: dict[str, str | list[str]]
    body: bytes
    timings: Timings
    final_url: str
    dns: DnsMeta = field(default_factory=DnsMeta)
    tls: TlsMeta | None = None


@dataclass
class HttpResult:
    response: HttpResponse | None = None
    error: str | None = None
    timed_out: bool = False
    timings: Timings = field(default_factory=Timings)


def _ms(s: float) -> int:
    return max(0, int(round(s * 1000)))


def _unique_preserve(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def _format_certificate(cert: dict[str, Any]) -> dict[str, Any]:
    """Shape Python's getpeercert() dict to match spec §3.4.2."""
    def _rdn_to_cn(rdns: tuple) -> str | None:
        # rdns is a tuple of tuples of tuples; CN is (('commonName', 'value'),).
        for rdn in rdns or ():
            for attr in rdn:
                if attr[0] == "commonName":
                    return attr[1]
        return None

    subject = {"cn": _rdn_to_cn(cert.get("subject", ()))}
    issuer  = {"cn": _rdn_to_cn(cert.get("issuer", ()))}
    san     = [f"{k}:{v}" for (k, v) in cert.get("subjectAltName", ())]

    def _iso(date_str: str) -> str:
        # Python gives "Jan  1 00:00:00 2026 GMT". Parse best-effort.
        try:
            from datetime import datetime, timezone
            dt = datetime.strptime(date_str, "%b %d %H:%M:%S %Y %Z")
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return date_str  # leave raw if parse fails

    return {
        "subject": subject,
        "subjectAltNames": san,
        "issuer": issuer,
        "notBefore": _iso(cert.get("notBefore", "")),
        "notAfter":  _iso(cert.get("notAfter", "")),
    }


class _TimedHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection that records DNS + connect times plus DNS metadata."""

    def __init__(self, *a: Any, timings: Timings, dns: DnsMeta, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self._t = timings
        self._dns = dns

    def connect(self) -> None:
        t0 = time.perf_counter()
        infos = socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM)
        t1 = time.perf_counter()
        self._t.dns_ms = _ms(t1 - t0)
        self._dns.resolved_ips = _unique_preserve([ai[4][0] for ai in infos])

        err: Exception | None = None
        for af, st, proto, _, sa in infos:
            sock = socket.socket(af, st, proto)
            try:
                sock.settimeout(self.timeout)
                tc0 = time.perf_counter()
                sock.connect(sa)
                tc1 = time.perf_counter()
                self._t.connect_ms = _ms(tc1 - tc0)
                self._dns.resolved_ip = sa[0]
                self.sock = sock
                return
            except OSError as e:
                sock.close()
                err = e
        if err is not None:
            raise err


class _TimedHTTPSConnection(http.client.HTTPSConnection):
    """HTTPSConnection with DNS + connect + TLS timings plus DNS/TLS metadata."""

    def __init__(self, *a: Any, timings: Timings, dns: DnsMeta,
                 tls_meta_out: list[TlsMeta | None], **kw: Any) -> None:
        super().__init__(*a, **kw)
        self._t = timings
        self._dns = dns
        # List-as-out-param so the wrapping module can read it back after
        # the SSL handshake completes inside this method.
        self._tls_out = tls_meta_out

    def connect(self) -> None:
        t0 = time.perf_counter()
        infos = socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM)
        t1 = time.perf_counter()
        self._t.dns_ms = _ms(t1 - t0)
        self._dns.resolved_ips = _unique_preserve([ai[4][0] for ai in infos])

        err: Exception | None = None
        for af, st, proto, _, sa in infos:
            raw = socket.socket(af, st, proto)
            try:
                raw.settimeout(self.timeout)
                tc0 = time.perf_counter()
                raw.connect(sa)
                tc1 = time.perf_counter()
                self._dns.resolved_ip = sa[0]
                self._t.connect_ms = _ms(tc1 - tc0)

                ctx = self._context or ssl.create_default_context()
                tls0 = time.perf_counter()
                tls = ctx.wrap_socket(raw, server_hostname=self.host)
                tls1 = time.perf_counter()
                self._t.tls_ms = _ms(tls1 - tls0)
                # Capture TLS metadata per spec §3.4.2. cert dict is empty
                # when verify_mode == CERT_NONE; fall back to certificate=None.
                cert_dict = tls.getpeercert()
                certificate = _format_certificate(cert_dict) if cert_dict else None
                self._tls_out[0] = TlsMeta(
                    protocol=tls.version() or "",
                    cipher=(tls.cipher() or ("",))[0] or "",
                    alpn=tls.selected_alpn_protocol(),
                    certificate=certificate,
                )
                self.sock = tls
                return
            except OSError as e:
                raw.close()
                err = e
        if err is not None:
            raise err


def send_request(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout: float,
    verify_tls: bool = True,
) -> HttpResult:
    """Issue one request. No redirect following — caller iterates."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        return HttpResult(error=f"unsupported scheme: {parts.scheme!r}")

    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")

    timings = Timings()
    dns_meta = DnsMeta()
    tls_slot: list[TlsMeta | None] = [None]
    t_start = time.perf_counter()

    conn: http.client.HTTPConnection
    if parts.scheme == "https":
        ctx = ssl.create_default_context()
        if not verify_tls:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        # Advertise only http/1.1 — the stdlib http.client cannot actually
        # speak HTTP/2, so advertising h2 would lead to protocol mismatches
        # when the server accepts it. The ALPN we report back is whatever
        # the server agrees to.
        try:
            ctx.set_alpn_protocols(["http/1.1"])
        except (AttributeError, NotImplementedError):
            pass
        conn = _TimedHTTPSConnection(host, port, timeout=timeout, context=ctx,
                                     timings=timings, dns=dns_meta,
                                     tls_meta_out=tls_slot)
    else:
        conn = _TimedHTTPConnection(host, port, timeout=timeout,
                                    timings=timings, dns=dns_meta)

    try:
        conn.request(method, path, body=body, headers=headers)
        t_req_sent = time.perf_counter()

        resp = conn.getresponse()  # reads status + headers
        t_ttfb = time.perf_counter()
        timings.ttfb_ms = _ms(t_ttfb - t_req_sent) + timings.dns_ms + timings.connect_ms + timings.tls_ms

        raw_body = resp.read()
        t_done = time.perf_counter()
        timings.transfer_ms = _ms(t_done - t_ttfb)
        timings.response_time_ms = _ms(t_done - t_start)

        # Collect headers; preserve multi-value lists per spec §9.4.
        collected: dict[str, str | list[str]] = {}
        for k, v in resp.getheaders():
            k_lower = k.lower()
            if k_lower in collected:
                existing = collected[k_lower]
                if isinstance(existing, list):
                    existing.append(v)
                else:
                    collected[k_lower] = [existing, v]
            else:
                collected[k_lower] = v

        return HttpResult(
            response=HttpResponse(
                status=resp.status,
                status_text=resp.reason or "",
                headers=collected,
                body=raw_body,
                timings=timings,
                final_url=url,
                dns=dns_meta,
                tls=tls_slot[0],
            ),
            timings=timings,
        )
    except (socket.timeout, TimeoutError):
        return HttpResult(timed_out=True, timings=timings, error="request timed out")
    except (OSError, http.client.HTTPException) as e:
        return HttpResult(error=str(e), timings=timings)
    finally:
        try:
            conn.close()
        except Exception:
            pass
