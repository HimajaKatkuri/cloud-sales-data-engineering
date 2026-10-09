"""A small simulated payments REST API (standard library only).

It stands in for a third-party payment provider so the project runs offline:

    GET /health
    GET /api/v1/payments?page=1&page_size=100&updated_since=2025-07-01T00:00:00Z

Responses use the ``ApiPage`` envelope. Records are served in a stable order
(updated_at, payment_id) so pagination is deterministic. ``fail_first_n`` makes the
first N payment requests return HTTP 503, which is handy for demonstrating retries.
"""

from __future__ import annotations

import json
import logging
import math
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

PAYMENTS_PATH = "/api/v1/payments"


class _State:
    def __init__(self, data_file: Path, fail_first_n: int) -> None:
        self.data_file = data_file
        self.remaining_failures = fail_first_n
        self.lock = threading.Lock()

    def should_fail(self) -> bool:
        with self.lock:
            if self.remaining_failures > 0:
                self.remaining_failures -= 1
                return True
            return False

    def load_records(self) -> list[dict[str, Any]]:
        # Re-read on every request so regenerated sample data is picked up immediately.
        records: list[dict[str, Any]] = json.loads(self.data_file.read_text(encoding="utf-8"))
        return sorted(records, key=lambda r: (str(r.get("updated_at")), str(r.get("payment_id"))))


def _parse_ts(value: object) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def build_handler(state: _State) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            logger.debug("mock-api: " + format, *args)

        def _send(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            url = urlparse(self.path)
            if url.path == "/health":
                self._send(200, {"status": "ok"})
                return
            if url.path != PAYMENTS_PATH:
                self._send(404, {"error": "not found"})
                return
            if state.should_fail():
                self._send(503, {"error": "simulated outage"})
                return
            self._serve_payments(parse_qs(url.query))

        def _serve_payments(self, query: dict[str, list[str]]) -> None:
            try:
                page = int(query.get("page", ["1"])[0])
                page_size = int(query.get("page_size", ["100"])[0])
            except ValueError:
                self._send(400, {"error": "page and page_size must be integers"})
                return
            if page < 1 or not 1 <= page_size <= 1000:
                self._send(400, {"error": "page must be >= 1 and page_size within 1..1000"})
                return

            records = state.load_records()
            since_raw = query.get("updated_since", [None])[0]
            if since_raw:
                since = _parse_ts(since_raw)
                if since is None:
                    self._send(400, {"error": "updated_since must be ISO-8601"})
                    return
                # Records with an unparseable updated_at are still returned so the
                # pipeline can quarantine them rather than have them vanish upstream.
                records = [
                    r
                    for r in records
                    if (ts := _parse_ts(r.get("updated_at"))) is None or ts > since
                ]

            total = len(records)
            total_pages = max(1, math.ceil(total / page_size))
            start = (page - 1) * page_size
            self._send(
                200,
                {
                    "data": records[start : start + page_size],
                    "page": page,
                    "page_size": page_size,
                    "total_records": total,
                    "total_pages": total_pages,
                    "next_page": page + 1 if page < total_pages else None,
                },
            )

    return Handler


def create_server(
    data_file: Path, host: str = "127.0.0.1", port: int = 8000, fail_first_n: int = 0
) -> ThreadingHTTPServer:
    """Create (but do not start) the server. ``port=0`` picks a free port (used in tests)."""
    if not data_file.is_file():
        raise FileNotFoundError(f"Mock API data file not found: {data_file}")
    return ThreadingHTTPServer((host, port), build_handler(_State(data_file, fail_first_n)))
