from __future__ import annotations

import subprocess
import urllib.request
from typing import Any


def fetch_bytes(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    fetcher: str = "python",
    compressed: bool = True,
) -> tuple[int | None, str | None, dict[str, str], bytes, str]:
    return _fetch(url, headers, timeout_seconds, fetcher, max_bytes=None, compressed=compressed)


def fetch_prefix(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    max_bytes: int = 8192,
    fetcher: str = "python",
    compressed: bool = True,
) -> tuple[int | None, str | None, dict[str, str], bytes, str]:
    return _fetch(
        url, headers, timeout_seconds, fetcher, max_bytes=max_bytes, compressed=compressed
    )


def _fetch(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    fetcher: str,
    max_bytes: int | None,
    compressed: bool,
) -> tuple[int | None, str | None, dict[str, str], bytes, str]:
    if fetcher == "curl":
        return _fetch_curl(url, headers, timeout_seconds, max_bytes, compressed=compressed)
    if fetcher == "python_then_curl":
        try:
            return _fetch_python(url, headers, timeout_seconds, max_bytes, "python_then_curl")
        except Exception:
            return _fetch_curl(url, headers, timeout_seconds, max_bytes, compressed=compressed)
    return _fetch_python(url, headers, timeout_seconds, max_bytes, fetcher)


def _fetch_python(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    max_bytes: int | None,
    fetcher_used: str,
) -> tuple[int | None, str | None, dict[str, str], bytes, str]:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        if max_bytes is None:
            body = response.read()
        else:
            body = response.read(max_bytes)
        body = body or b""
        status_code = getattr(response, "status", None)
        final_url = str(getattr(response, "url", None) or url)
        headers_dict = {str(k).lower(): str(v) for k, v in response.headers.items()}
    return status_code, final_url, headers_dict, body, fetcher_used


def _fetch_curl(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    max_bytes: int | None,
    *,
    compressed: bool = True,
) -> tuple[int | None, str | None, dict[str, str], bytes, str]:
    curl_headers = dict(headers or {})
    if max_bytes is not None:
        if "range" not in {k.lower() for k in curl_headers}:
            curl_headers["Range"] = f"bytes=0-{max_bytes - 1}"
        headers_dict, body, final_url, status_code = _run_curl(
            url, curl_headers, timeout_seconds, compressed=compressed
        )
        body = body[:max_bytes]
        return status_code, final_url, headers_dict, body, "curl"

    chunk_size = 1024 * 1024
    max_total = 10 * 1024 * 1024
    total = 0
    buffer = bytearray()
    final_url = None
    status_code: int | None = None
    headers_dict: dict[str, str] = {}
    while True:
        range_headers = dict(curl_headers)
        range_headers["Range"] = f"bytes={total}-{total + chunk_size - 1}"
        headers_dict, body, chunk_final_url, chunk_status = _run_curl(
            url, range_headers, timeout_seconds, compressed=compressed
        )
        if chunk_status == 416:
            break
        if chunk_final_url:
            final_url = chunk_final_url
        if chunk_status is not None:
            status_code = chunk_status
        if not body:
            break
        buffer.extend(body)
        total += len(body)
        if len(body) < chunk_size:
            break
        if total > max_total:
            raise RuntimeError("curl_max_size_exceeded")
    return status_code, final_url, headers_dict, bytes(buffer), "curl"


def _run_curl(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    *,
    compressed: bool = True,
) -> tuple[dict[str, str], bytes, str | None, int | None]:
    args = [
        "curl",
        "-4",
        "-fsSL",
        "--max-time",
        str(timeout_seconds),
        "-D",
        "-",
        "-w",
        "\nSVFINAL:%{url_effective}\nSVSTATUS:%{http_code}\n",
    ]
    if compressed:
        args.append("--compressed")
    for key, value in headers.items():
        args.extend(["-H", f"{key}: {value}"])
    args.append(url)
    result = subprocess.run(args, capture_output=True, check=False)
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="ignore").strip()
        raise RuntimeError(stderr or "curl_failed")
    return _parse_curl_output(result.stdout)


def _parse_curl_output(
    output: bytes,
) -> tuple[dict[str, str], bytes, str | None, int | None]:
    final_url = None
    status_code: int | None = None
    trailer_split = output.rsplit(b"\nSVFINAL:", 1)
    body_with_headers = output
    if len(trailer_split) == 2:
        body_with_headers, trailer = trailer_split
        trailer_lines = trailer.splitlines()
        if trailer_lines:
            final_url = trailer_lines[0].decode("utf-8", errors="ignore").strip()
        for line in trailer_lines[1:]:
            if line.startswith(b"SVSTATUS:"):
                try:
                    status_code = int(line.split(b":", 1)[1].strip())
                except ValueError:
                    status_code = None
                break

    headers_dict: dict[str, str] = {}
    pos = 0
    body = body_with_headers
    while True:
        sep = body_with_headers.find(b"\r\n\r\n", pos)
        if sep == -1:
            break
        header_block = body_with_headers[pos:sep].decode("utf-8", errors="ignore")
        lines = header_block.split("\r\n")
        if not lines or not lines[0].startswith("HTTP/"):
            break
        headers_dict = {}
        for line in lines[1:]:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            headers_dict[key.strip().lower()] = value.strip()
        pos = sep + 4
        if not body_with_headers[pos:pos + 5].startswith(b"HTTP/"):
            body = body_with_headers[pos:]
            break
    return headers_dict, body, final_url, status_code
