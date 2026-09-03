"""Operational HTTP server for the sandbox manager."""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event
from urllib.parse import urlsplit


class Readiness:
    """Thread-safe manager readiness state."""

    def __init__(self) -> None:
        self._ready = Event()

    def set(self, ready: bool) -> None:
        if ready:
            self._ready.set()
        else:
            self._ready.clear()

    @property
    def ready(self) -> bool:
        return self._ready.is_set()


class OperationalHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


class OperationalHandler(BaseHTTPRequestHandler):
    server_version = "sandbox-manager"
    sys_version = ""
    readiness: Readiness | None = None

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/healthz":
            self._write_json(HTTPStatus.OK, b'{"status":"ok"}\n')
            return
        if path == "/readyz":
            if self.readiness is None or not self.readiness.ready:
                self._write_json(
                    HTTPStatus.SERVICE_UNAVAILABLE, b'{"status":"not_ready"}\n'
                )
                return
            self._write_json(HTTPStatus.OK, b'{"status":"ready"}\n')
            return
        self._write_not_found()

    def do_HEAD(self) -> None:
        self._reject_or_not_found()

    def do_POST(self) -> None:
        self._reject_or_not_found()

    def do_PUT(self) -> None:
        self._reject_or_not_found()

    def do_PATCH(self) -> None:
        self._reject_or_not_found()

    def do_DELETE(self) -> None:
        self._reject_or_not_found()

    def do_OPTIONS(self) -> None:
        self._reject_or_not_found()

    def do_TRACE(self) -> None:
        self._reject_or_not_found()

    def _reject_or_not_found(self) -> None:
        if urlsplit(self.path).path not in {"/healthz", "/readyz"}:
            self._write_not_found()
            return
        self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
        self.send_header("Allow", "GET")
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        body = b'{"status":"method_not_allowed"}\n'
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_json(self, status: HTTPStatus, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_not_found(self) -> None:
        body = b"404 page not found\n"
        self.send_response(HTTPStatus.NOT_FOUND)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def create_server(
    host: str, port: int, readiness: Readiness | None
) -> OperationalHTTPServer:
    readiness_state = readiness

    class BoundOperationalHandler(OperationalHandler):
        readiness = readiness_state

    return OperationalHTTPServer((host, port), BoundOperationalHandler)
