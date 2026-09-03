from __future__ import annotations

import http.client
import threading
import unittest

from sandbox_manager.server import Readiness, create_server


class ServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.readiness = Readiness()
        self.server = create_server("127.0.0.1", 0, self.readiness)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method: str, path: str) -> tuple[int, dict[str, str], bytes]:
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_address[1], timeout=2
        )
        connection.request(method, path)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def test_health_handler(self) -> None:
        status, headers, body = self.request("GET", "/healthz")

        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"status":"ok"}\n')
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_readiness_handler(self) -> None:
        status, _, body = self.request("GET", "/readyz")
        self.assertEqual(status, 503)
        self.assertEqual(body, b'{"status":"not_ready"}\n')

        self.readiness.set(True)
        status, _, body = self.request("GET", "/readyz")
        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"status":"ready"}\n')

        self.readiness.set(False)
        status, _, _ = self.request("GET", "/readyz")
        self.assertEqual(status, 503)

    def test_handler_rejects_unsupported_method(self) -> None:
        status, headers, body = self.request("POST", "/healthz")

        self.assertEqual(status, 405)
        self.assertEqual(headers["Allow"], "GET")
        self.assertEqual(body, b'{"status":"method_not_allowed"}\n')


if __name__ == "__main__":
    unittest.main()
