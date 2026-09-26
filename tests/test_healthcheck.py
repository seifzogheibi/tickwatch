import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from tickwatch import healthcheck


def serve(body: str) -> Iterator[int]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *args) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()


def check(monkeypatch: pytest.MonkeyPatch, body: str) -> int:
    gen = serve(body)
    port = next(gen)
    monkeypatch.setenv("METRICS_PORT", str(port))
    try:
        return healthcheck.main()
    finally:
        gen.close()


def test_recent_frame_is_healthy(monkeypatch: pytest.MonkeyPatch) -> None:
    body = f"tickwatch_last_frame_timestamp_seconds {time.time() - 2}\n"
    assert check(monkeypatch, body) == 0


def test_stale_frame_is_unhealthy(monkeypatch: pytest.MonkeyPatch) -> None:
    body = f"tickwatch_last_frame_timestamp_seconds {time.time() - 120}\n"
    assert check(monkeypatch, body) == 1


def test_no_frame_yet_is_unhealthy(monkeypatch: pytest.MonkeyPatch) -> None:
    # Gauge exists from import but the series only appears once set; a
    # similarly named metric must not be mistaken for it.
    body = "tickwatch_last_frame_timestamp_seconds_created 1\n"
    assert check(monkeypatch, body) == 1


def test_unreachable_endpoint_is_unhealthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METRICS_PORT", "1")  # nothing listens on port 1
    assert healthcheck.main() == 1
