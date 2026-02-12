from __future__ import annotations

import subprocess

import pytest

from sempervigil import http_fetch


def test_parse_curl_output_extracts_headers_and_trailer():
    output = (
        b"HTTP/2 200\r\nContent-Type: application/xml\r\nX-Test: ok\r\n\r\n"
        b"<rss><channel></channel></rss>\n"
        b"SVFINAL:https://example.com/final\nSVSTATUS:200\n"
    )
    headers, body, final_url, status = http_fetch._parse_curl_output(output)
    assert headers.get("content-type") == "application/xml"
    assert headers.get("x-test") == "ok"
    assert b"<rss" in body
    assert final_url == "https://example.com/final"
    assert status == 200


def test_fetch_prefix_uses_range_header(monkeypatch):
    captured_args = {}

    def fake_run(args, capture_output, check):
        captured_args["args"] = args
        stdout = (
            b"HTTP/2 200\r\nContent-Type: application/xml\r\n\r\n"
            b"<rss></rss>\nSVFINAL:https://example.com/\nSVSTATUS:200\n"
        )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    monkeypatch.setattr(http_fetch.subprocess, "run", fake_run)
    http_fetch.fetch_prefix(
        "https://example.com/feed",
        headers={"User-Agent": "Test"},
        timeout_seconds=10,
        max_bytes=1024,
        fetcher="curl",
    )
    args = captured_args["args"]
    assert "-H" in args
    assert "Range: bytes=0-1023" in args


def test_python_then_curl_fallback(monkeypatch):
    def fake_python(*args, **kwargs):
        raise RuntimeError("python_failed")

    def fake_curl(*args, **kwargs):
        return 200, "https://example.com", {"content-type": "application/xml"}, b"<rss></rss>", "curl"

    monkeypatch.setattr(http_fetch, "_fetch_python", fake_python)
    monkeypatch.setattr(http_fetch, "_fetch_curl", fake_curl)
    status, final_url, headers, body, fetcher_used = http_fetch.fetch_bytes(
        "https://example.com/feed",
        headers={},
        timeout_seconds=10,
        fetcher="python_then_curl",
    )
    assert status == 200
    assert fetcher_used == "curl"


def test_fetch_bytes_chunked_range(monkeypatch):
    calls = {"count": 0}

    def fake_run(args, capture_output, check):
        calls["count"] += 1
        range_header = None
        for i, arg in enumerate(args):
            if arg == "-H" and i + 1 < len(args) and args[i + 1].startswith("Range:"):
                range_header = args[i + 1]
                break
        if calls["count"] == 1:
            assert range_header == "Range: bytes=0-1048575"
            body = b"a" * 3
        else:
            assert range_header == "Range: bytes=3-1048578"
            body = b""
        stdout = (
            b"HTTP/2 200\r\nContent-Type: application/xml\r\n\r\n"
            + body
            + b"\nSVFINAL:https://example.com/\nSVSTATUS:200\n"
        )
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr=b"")

    monkeypatch.setattr(http_fetch.subprocess, "run", fake_run)
    status, final_url, headers, body, fetcher_used = http_fetch.fetch_bytes(
        "https://example.com/feed",
        headers={"User-Agent": "Test"},
        timeout_seconds=10,
        fetcher="curl",
    )
    assert status == 200
    assert fetcher_used == "curl"
    assert body == b"aaa"
