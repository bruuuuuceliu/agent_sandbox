from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import threading
import time
import unittest
import urllib.parse
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from unittest.mock import patch

from sandbox_manager import kubernetes_exec
from sandbox_manager.commands import AdmittedCommand
from sandbox_manager.kubernetes_exec import (
    RELAY_HELPER_PATH,
    TASK_ANNOTATION,
    KubernetesApi,
    KubernetesApiConfig,
    KubernetesCommandRelay,
    KubernetesExecError,
    PodIdentityError,
    PodSnapshot,
    PodTarget,
    PodVerification,
    build_exec_path,
    build_pod_get_path,
    classify_termination,
    encode_helper_argv,
)
from sandbox_manager.relay_protocol import (
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    encode_frame,
)

_NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)

_NAMESPACE = "sandbox-tasks"

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def verification(
    expected_uid: str = "uid-1",
    task_id: str = "task-1",
    container: str = "sandbox",
) -> PodVerification:
    return PodVerification(
        expected_uid=expected_uid,
        task_id=task_id,
        namespace=_NAMESPACE,
        expected_labels=(("app", "sandbox"),),
        container=container,
    )


def snapshot(
    uid: str = "uid-1",
    phase: str = "Running",
    namespace: str = _NAMESPACE,
    task_id: str = "task-1",
    containers: tuple[str, ...] = ("sandbox",),
) -> PodSnapshot:
    return PodSnapshot(
        name="pod-1",
        namespace=namespace,
        uid=uid,
        phase=phase,
        labels=(("app", "sandbox"),),
        annotations=((TASK_ANNOTATION, task_id),),
        containers=containers,
    )


def fake_pod_body(
    uid: str = "uid-1",
    phase: str = "Running",
    task_id: str = "task-1",
    containers: tuple[str, ...] = ("sandbox",),
) -> dict[str, object]:
    return {
        "metadata": {
            "name": "pod-1",
            "uid": uid,
            "labels": {"app": "sandbox"},
            "annotations": {TASK_ANNOTATION: task_id},
        },
        "spec": {"containers": [{"name": name} for name in containers]},
        "status": {"phase": phase},
    }


def admitted_command() -> AdmittedCommand:
    return AdmittedCommand(
        task_id="task-1",
        command_id="command-1",
        agent_session_id="agent-session-1",
        profile="default",
        fence_generation="fence-1",
        activated=True,
        admitted_at=_NOW,
    )


def relay_envelope(
    operation: RelayOperation = RelayOperation.EXEC,
) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id="command-1",
        task_id="task-1",
        operation=operation,
        correlation_id="correlation-1",
        declared_input_bytes=64,
        declared_output_bytes=1024,
    )


async def argv_stream(argv: tuple[str, ...]) -> AsyncIterator[bytes]:
    payload = json.dumps({"argv": list(argv)}).encode("utf-8")
    yield encode_frame(
        DataFrame("command-1", DataChannel.STDIN, 0, payload)
    )


async def upload_stream(
    argv: tuple[str, ...], payload: bytes
) -> AsyncIterator[bytes]:
    """One upload's relay input: the argv frame, then payload frames."""

    argv_payload = json.dumps({"argv": list(argv)}).encode("utf-8")
    yield encode_frame(DataFrame("command-1", DataChannel.STDIN, 0, argv_payload))
    sequence = 1
    offset = 0
    while offset < len(payload):
        piece = payload[offset : offset + 4096]
        yield encode_frame(
            DataFrame("command-1", DataChannel.STDIN, sequence, piece)
        )
        sequence += 1
        offset += len(piece)


class _FakeKubeHTTPServer(ThreadingHTTPServer):
    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, _FakeKubeHandler)
        self.pods: dict[str, dict[str, object]] = {}
        self.exec_frames: list[tuple[int, bytes]] = []
        self.exec_drop = False
        self.exec_status = 101
        self.get_status = 200
        self.exec_delay = 0.0
        # Fault-injection knobs for the exec stream hardening tests.
        self.exec_fragment = False
        self.exec_close_code: int | None = None
        self.exec_pong = True
        self.exec_violation: str | None = None
        self.exec_accept_header: str | None = None
        self.exec_protocol_header: str | None = None
        self.exec_drain_deadline: float | None = None
        self.ws_client_opcodes: list[int] = []
        self.records: list[dict[str, object]] = []
        self.daemon_threads = True


class _FakeKubeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    _recv_buffer: bytearray

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        server = cast(_FakeKubeHTTPServer, self.server)
        split = urllib.parse.urlsplit(self.path)
        if split.path.endswith("/exec"):
            self._handle_exec(server, split)
        else:
            self._handle_get(server, split)

    def _handle_get(
        self, server: _FakeKubeHTTPServer, split: urllib.parse.SplitResult
    ) -> None:
        name = split.path.rstrip("/").rsplit("/", 1)[-1]
        body = server.pods.get(name)
        server.records.append(
            {
                "kind": "get",
                "path": split.path,
                "authorization": self.headers.get("Authorization", ""),
            }
        )
        if body is None or server.get_status != 200:
            self._send_json(server.get_status, {"kind": "Status"})
            return
        self._send_json(200, body)

    def _send_json(self, status: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _handle_exec(
        self, server: _FakeKubeHTTPServer, split: urllib.parse.SplitResult
    ) -> None:
        key = self.headers.get("Sec-WebSocket-Key", "")
        subprotocol = self.headers.get("Sec-WebSocket-Protocol", "")
        authorization = self.headers.get("Authorization", "")
        if server.exec_status != 101:
            self._send_json(server.exec_status, {"kind": "Status"})
            return

        accept = server.exec_accept_header
        if accept is None:
            accept = base64.b64encode(
                hashlib.sha1(f"{key}{_WS_GUID}".encode("ascii")).digest()
            ).decode("ascii")
        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        if server.exec_protocol_header != "":
            protocol = subprotocol
            if server.exec_protocol_header is not None:
                protocol = server.exec_protocol_header
            self.send_header("Sec-WebSocket-Protocol", protocol)
        self.end_headers()
        self.wfile.flush()

        argv: list[str] = []
        stdin_payload = b""
        self._recv_buffer = bytearray()
        frame = self._read_ws_frame()
        if frame is not None:
            server.ws_client_opcodes.append(frame[0])
        if frame is not None and frame[1][:1] == b"\x00":
            body = frame[1][1:]
            length = int.from_bytes(body[:4], "big")
            argv = list(json.loads(body[4 : 4 + length]).get("argv", []))

        # Drain further stdin frames (an upload payload) until a short idle
        # window passes — or until the optional wall-clock drain deadline —
        # then answer like the fixed helper would.
        drain_deadline = (
            time.monotonic() + server.exec_drain_deadline
            if server.exec_drain_deadline is not None
            else None
        )
        while drain_deadline is None or time.monotonic() < drain_deadline:
            self.connection.settimeout(0.2)
            try:
                extra = self._read_ws_frame()
            except (TimeoutError, OSError):
                break
            if extra is None:
                break
            server.ws_client_opcodes.append(extra[0])
            if extra[0] == 0x9 and server.exec_pong:
                self._write_ws_frame(0xA, extra[1])
            if extra[0] == 0x2 and extra[1][:1] == b"\x00":
                stdin_payload += extra[1][1:]

        server.records.append(
            {
                "kind": "exec",
                "path": split.path,
                "query": split.query,
                "authorization": authorization,
                "subprotocol": subprotocol,
                "argv": argv,
                "stdin": stdin_payload,
            }
        )

        try:
            if server.exec_delay:
                time.sleep(server.exec_delay)
            if server.exec_violation is not None:
                self._write_violation(server.exec_violation)
            for channel, payload in server.exec_frames:
                self._write_channel_frames(server, channel, payload)
            if server.exec_drop:
                # Abrupt loss: close everything so the peer actually sees EOF
                # (rfile/wfile hold the descriptor open otherwise).
                self.wfile.flush()
                self.close_connection = True
                self.wfile.close()
                self.rfile.close()
                self.connection.close()
                return
            if server.exec_close_code is not None:
                self._write_ws_frame(
                    0x8, server.exec_close_code.to_bytes(2, "big")
                )
            else:
                self._write_ws_frame(0x8, b"")
        except OSError:
            return
        # Read (and count) any client replies — e.g. the pongs provoked by
        # interleaved pings — before the handler returns: closing a socket
        # with unread inbound data sends RST, which can destroy stream frames
        # the client has not read yet. A real API server reads its
        # connections; this drain reproduces that.
        self.connection.settimeout(0.2)
        while True:
            try:
                reply = self._read_ws_frame()
            except (TimeoutError, OSError):
                break
            if reply is None:
                break
            server.ws_client_opcodes.append(reply[0])
        self.close_connection = True

    def _write_channel_frames(
        self, server: _FakeKubeHTTPServer, channel: int, payload: bytes
    ) -> None:
        data = bytes((channel,)) + payload
        if server.exec_fragment:
            # One message as two data fragments with a control frame
            # interleaved: exercises continuation reassembly and ping
            # handling mid-message on the client.
            half = max(1, len(data) // 2)
            self._write_ws_frame(0x2, data[:half], fin=False)
            self._write_ws_frame(0x9, b"")
            self._write_ws_frame(0x0, data[half:], fin=True)
        else:
            self._write_ws_frame(0x2, data)

    def _write_violation(self, kind: str) -> None:
        if kind == "masked":
            payload = b"\x01masked-server-frame"
            mask = b"\x00\x00\x00\x00"
            masked = bytes(
                byte ^ mask[index % 4] for index, byte in enumerate(payload)
            )
            header = bytes((0x82, 0x80 | (4 + len(masked))))
            self.wfile.write(header + mask + masked)
        elif kind == "rsv":
            self.wfile.write(bytes((0xC2, 0x02)) + b"\x01x")
        elif kind == "continuation":
            self.wfile.write(bytes((0x80, 0x07)) + b"\x01orphan")
        elif kind == "oversized_control":
            self.wfile.write(
                bytes((0x89, 126)) + (130).to_bytes(2, "big") + b"x" * 130
            )
        self.wfile.flush()

    def _recv_exact(self, size: int) -> bytes:
        """Read ``size`` bytes off the raw socket into the frame buffer.

        Raw ``recv`` is used instead of the buffered ``rfile`` on purpose:
        once a ``makefile()`` reader raises a timeout it is permanently
        poisoned ("cannot read from timed out object"), and the exec
        handler's idle-window timeouts would break every later read.
        """

        while len(self._recv_buffer) < size:
            chunk = self.connection.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self._recv_buffer.extend(chunk)
        data = bytes(self._recv_buffer[:size])
        del self._recv_buffer[:size]
        return data

    def _read_ws_frame(self) -> tuple[int, bytes] | None:
        try:
            header = self._recv_exact(2)
            opcode = header[0] & 0x0F
            length = header[1] & 0x7F
            if length == 126:
                length = int.from_bytes(self._recv_exact(2), "big")
            elif length == 127:
                length = int.from_bytes(self._recv_exact(8), "big")
            if header[1] & 0x80:
                mask = self._recv_exact(4)
                data = self._recv_exact(length)
                payload = bytes(
                    byte ^ mask[index % 4] for index, byte in enumerate(data)
                )
            else:
                payload = self._recv_exact(length)
        except (TimeoutError, OSError):
            return None
        return opcode, payload

    def _write_ws_frame(
        self, opcode: int, payload: bytes, fin: bool = True
    ) -> None:
        length = len(payload)
        first = 0x80 | opcode if fin else opcode
        if length < 126:
            header = bytes((first, length))
        elif length < 65536:
            header = bytes((first, 126)) + length.to_bytes(2, "big")
        else:
            header = bytes((first, 127)) + length.to_bytes(8, "big")
        self.wfile.write(header + payload)
        self.wfile.flush()


class FakeKubeServer:
    """Minimal pods/get + pods/exec stand-in for local contract tests."""

    def __init__(self) -> None:
        self._httpd = _FakeKubeHTTPServer(("127.0.0.1", 0))
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    @property
    def pods(self) -> dict[str, dict[str, object]]:
        return self._httpd.pods

    @property
    def records(self) -> list[dict[str, object]]:
        return self._httpd.records

    @property
    def exec_frames(self) -> list[tuple[int, bytes]]:
        return self._httpd.exec_frames

    @exec_frames.setter
    def exec_frames(self, frames: list[tuple[int, bytes]]) -> None:
        self._httpd.exec_frames = frames

    @property
    def exec_drop(self) -> bool:
        return self._httpd.exec_drop

    @exec_drop.setter
    def exec_drop(self, value: bool) -> None:
        self._httpd.exec_drop = value

    @property
    def exec_status(self) -> int:
        return self._httpd.exec_status

    @exec_status.setter
    def exec_status(self, value: int) -> None:
        self._httpd.exec_status = value

    @property
    def get_status(self) -> int:
        return self._httpd.get_status

    @get_status.setter
    def get_status(self, value: int) -> None:
        self._httpd.get_status = value

    @property
    def exec_delay(self) -> float:
        return self._httpd.exec_delay

    @exec_delay.setter
    def exec_delay(self, value: float) -> None:
        self._httpd.exec_delay = value

    @property
    def exec_fragment(self) -> bool:
        return self._httpd.exec_fragment

    @exec_fragment.setter
    def exec_fragment(self, value: bool) -> None:
        self._httpd.exec_fragment = value

    @property
    def exec_close_code(self) -> int | None:
        return self._httpd.exec_close_code

    @exec_close_code.setter
    def exec_close_code(self, value: int | None) -> None:
        self._httpd.exec_close_code = value

    @property
    def exec_pong(self) -> bool:
        return self._httpd.exec_pong

    @exec_pong.setter
    def exec_pong(self, value: bool) -> None:
        self._httpd.exec_pong = value

    @property
    def exec_violation(self) -> str | None:
        return self._httpd.exec_violation

    @exec_violation.setter
    def exec_violation(self, value: str | None) -> None:
        self._httpd.exec_violation = value

    @property
    def exec_accept_header(self) -> str | None:
        return self._httpd.exec_accept_header

    @exec_accept_header.setter
    def exec_accept_header(self, value: str | None) -> None:
        self._httpd.exec_accept_header = value

    @property
    def exec_protocol_header(self) -> str | None:
        return self._httpd.exec_protocol_header

    @exec_protocol_header.setter
    def exec_protocol_header(self, value: str | None) -> None:
        self._httpd.exec_protocol_header = value

    @property
    def exec_drain_deadline(self) -> float | None:
        return self._httpd.exec_drain_deadline

    @exec_drain_deadline.setter
    def exec_drain_deadline(self, value: float | None) -> None:
        self._httpd.exec_drain_deadline = value

    @property
    def ws_client_opcodes(self) -> list[int]:
        return self._httpd.ws_client_opcodes

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2.0)


class RequestPathTests(unittest.TestCase):
    def test_pod_get_path(self) -> None:
        self.assertEqual(
            build_pod_get_path("sandbox-tasks", "pod-1"),
            "/api/v1/namespaces/sandbox-tasks/pods/pod-1",
        )

    def test_exec_path_carries_only_the_fixed_helper(self) -> None:
        path = build_exec_path("sandbox-tasks", "pod-1", "sandbox", tty=False)
        split = urllib.parse.urlsplit(path)

        self.assertEqual(split.path, "/api/v1/namespaces/sandbox-tasks/pods/pod-1/exec")
        query = urllib.parse.parse_qs(split.query)
        self.assertEqual(query["command"], [RELAY_HELPER_PATH])
        self.assertEqual(query["container"], ["sandbox"])
        self.assertEqual(query["tty"], ["false"])
        self.assertEqual(
            set(query), {"command", "container", "stdin", "stdout", "stderr", "tty"}
        )

    def test_exec_path_tty_flag(self) -> None:
        path = build_exec_path("sandbox-tasks", "pod-1", "sandbox", tty=True)
        self.assertIn("tty=true", path)

    def test_helper_argv_travels_length_prefixed(self) -> None:
        encoded = encode_helper_argv(("ls", "-l", "/workspace"))

        length = int.from_bytes(encoded[:4], "big")
        self.assertEqual(length, len(encoded) - 4)
        self.assertEqual(json.loads(encoded[4:]), {"argv": ["ls", "-l", "/workspace"]})


class ClassifyTerminationTests(unittest.TestCase):
    def test_confirmed_clean_exit_is_success(self) -> None:
        self.assertIs(classify_termination(b"", True), CommandOutcome.SUCCESS)
        self.assertIs(
            classify_termination(b"command terminated with exit code 0", True),
            CommandOutcome.SUCCESS,
        )

    def test_confirmed_nonzero_exit_is_failed(self) -> None:
        self.assertIs(
            classify_termination(b"command terminated with exit code 3", True),
            CommandOutcome.FAILED,
        )

    def test_lost_stream_is_unknown(self) -> None:
        self.assertIs(classify_termination(b"", False), CommandOutcome.UNKNOWN)
        self.assertIs(classify_termination(None, True), CommandOutcome.UNKNOWN)

    def test_unrecognised_status_is_unknown(self) -> None:
        self.assertIs(
            classify_termination(b"something unexpected", True),
            CommandOutcome.UNKNOWN,
        )


class PodVerificationTests(unittest.TestCase):
    def test_accepts_matching_snapshot(self) -> None:
        verification().verify(snapshot())

    def test_rejects_replaced_pod_uid(self) -> None:
        with self.assertRaises(PodIdentityError) as raised:
            verification().verify(snapshot(uid="uid-replacement"))

        self.assertEqual(raised.exception.reason, "uid_mismatch")
        self.assertEqual(raised.exception.observed, "uid-replacement")

    def test_rejects_wrong_namespace(self) -> None:
        with self.assertRaises(PodIdentityError) as raised:
            verification().verify(snapshot(namespace="default"))

        self.assertEqual(raised.exception.reason, "not_running")

    def test_rejects_not_running_phase(self) -> None:
        with self.assertRaises(PodIdentityError) as raised:
            verification().verify(snapshot(phase="Succeeded"))

        self.assertEqual(raised.exception.reason, "not_running")

    def test_rejects_task_annotation_mismatch(self) -> None:
        with self.assertRaises(PodIdentityError) as raised:
            verification().verify(snapshot(task_id="task-other"))

        self.assertEqual(raised.exception.reason, "task_annotation_mismatch")
        self.assertEqual(raised.exception.observed, "task-other")

    def test_rejects_label_mismatch(self) -> None:
        altered = PodSnapshot(
            name="pod-1",
            namespace=_NAMESPACE,
            uid="uid-1",
            phase="Running",
            labels=(("app", "other"),),
            annotations=((TASK_ANNOTATION, "task-1"),),
            containers=("sandbox",),
        )

        with self.assertRaises(PodIdentityError) as raised:
            verification().verify(altered)

        self.assertEqual(raised.exception.reason, "label_mismatch")
        self.assertEqual(raised.exception.observed, "app=other")

    def test_rejects_absent_container(self) -> None:
        with self.assertRaises(PodIdentityError) as raised:
            verification().verify(snapshot(containers=("sidecar",)))

        self.assertEqual(raised.exception.reason, "container_absent")


class SnapshotParsingTests(unittest.TestCase):
    def test_rejects_payload_without_uid(self) -> None:
        body = fake_pod_body()
        del body["metadata"]

        with self.assertRaises(KubernetesExecError):
            kubernetes_exec._parse_pod_snapshot(_NAMESPACE, "pod-1", body)

    def test_rejects_non_object_payload(self) -> None:
        with self.assertRaises(KubernetesExecError):
            kubernetes_exec._parse_pod_snapshot(_NAMESPACE, "pod-1", ["not", "a", "pod"])


class ServiceAccountConfigTests(unittest.TestCase):
    def test_reads_service_account_files(self) -> None:
        with patch.dict(
            os.environ,
            {"KUBERNETES_SERVICE_HOST": "10.0.0.1", "KUBERNETES_SERVICE_PORT": "443"},
        ):
            config = KubernetesApiConfig.from_service_account(
                service_account_dir=str(_write_service_account())
            )

        self.assertEqual(config.host, "10.0.0.1")
        self.assertEqual(config.port, 443)
        self.assertEqual(config.bearer_token, "token-value")

    def test_requires_service_host(self) -> None:
        with (
            patch.dict(os.environ, {"KUBERNETES_SERVICE_HOST": ""}),
            self.assertRaises(KubernetesExecError),
        ):
            KubernetesApiConfig.from_service_account(
                service_account_dir=str(_write_service_account())
            )

    def test_requires_bearer_token(self) -> None:
        with self.assertRaises(KubernetesExecError):
            KubernetesApiConfig(host="h", port=1, bearer_token="")


def _write_service_account() -> Path:
    import tempfile

    directory = Path(tempfile.mkdtemp())
    (directory / "token").write_text("token-value\n", encoding="utf-8")
    return directory


class _KubeTestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.server = FakeKubeServer()
        self.server.pods["pod-1"] = fake_pod_body()
        self.server.start()
        self.api = KubernetesApi(
            KubernetesApiConfig(
                host="127.0.0.1",
                port=self.server.port,
                bearer_token="staging-token",
                secure=False,
            )
        )
        self.target = PodTarget(
            namespace=_NAMESPACE, name="pod-1", verification=verification()
        )

    def locate(self, task_id: str) -> PodTarget | None:
        return self.target if task_id == "task-1" else None

    def tearDown(self) -> None:
        self.server.stop()


class KubernetesCommandRelayTests(_KubeTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.relay = KubernetesCommandRelay(self.api, self.locate)

    async def test_execute_success_streams_without_argv_in_request(self) -> None:
        self.server.exec_frames = [
            (1, b"output chunk"),
            (2, b"warning chunk"),
            (3, b"command terminated with exit code 0"),
        ]

        result = await asyncio.wait_for(
            self.relay.execute(
                admitted_command(),
                relay_envelope(),
                argv_stream(("sensitive-argv-token", "-l")),
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        exec_records = [r for r in self.server.records if r["kind"] == "exec"]
        self.assertEqual(len(exec_records), 1)
        record = exec_records[0]
        self.assertEqual(record["argv"], ["sensitive-argv-token", "-l"])
        self.assertEqual(
            record["authorization"], "Bearer staging-token"
        )
        self.assertIn("v4.channel.k8s.io", str(record["subprotocol"]))
        query = urllib.parse.parse_qs(str(record["query"]))
        self.assertEqual(query["command"], [RELAY_HELPER_PATH])
        self.assertNotIn("sensitive-argv-token", str(record["query"]))
        self.assertNotIn("sensitive-argv-token", str(record["path"]))

    async def test_execute_nonzero_exit_is_failed(self) -> None:
        self.server.exec_frames = [(3, b"command terminated with exit code 3")]

        result = await asyncio.wait_for(
            self.relay.execute(
                admitted_command(), relay_envelope(), argv_stream(("false",))
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.FAILED)
        self.assertIsNone(result.reason_code)

    async def test_execute_stream_loss_is_unknown(self) -> None:
        self.server.exec_frames = [(1, b"partial output")]
        self.server.exec_drop = True

        result = await asyncio.wait_for(
            self.relay.execute(
                admitted_command(), relay_envelope(), argv_stream(("sleep", "10"))
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertEqual(result.reason_code, "stream_lost")

    async def test_execute_refuses_replaced_pod_before_exec(self) -> None:
        self.server.pods["pod-1"] = fake_pod_body(uid="uid-replacement")
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]

        with self.assertRaises(PodIdentityError) as raised:
            await asyncio.wait_for(
                self.relay.execute(
                    admitted_command(), relay_envelope(), argv_stream(("ls",))
                ),
                timeout=10.0,
            )

        self.assertEqual(raised.exception.reason, "uid_mismatch")
        self.assertEqual(
            [r for r in self.server.records if r["kind"] == "exec"], []
        )

    async def test_execute_without_target_is_refused(self) -> None:
        relay = KubernetesCommandRelay(self.api, lambda task_id: None)

        with self.assertRaises(KubernetesExecError):
            await asyncio.wait_for(
                relay.execute(
                    admitted_command(), relay_envelope(), argv_stream(("ls",))
                ),
                timeout=10.0,
            )

    async def test_pod_get_failure_is_reported(self) -> None:
        self.server.get_status = 404

        with self.assertRaises(KubernetesExecError):
            await asyncio.wait_for(
                self.api.pod_snapshot(_NAMESPACE, "pod-1"), timeout=10.0
            )

    async def test_exec_upgrade_refusal_is_reported(self) -> None:
        self.server.exec_status = 403

        with self.assertRaises(KubernetesExecError):
            await asyncio.wait_for(
                self.relay.execute(
                    admitted_command(), relay_envelope(), argv_stream(("ls",))
                ),
                timeout=10.0,
            )

    async def test_pty_operation_sets_tty_flag(self) -> None:
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]

        await asyncio.wait_for(
            self.relay.execute(
                admitted_command(),
                relay_envelope(operation=RelayOperation.PTY),
                argv_stream(("bash",)),
            ),
            timeout=10.0,
        )

        exec_records = [r for r in self.server.records if r["kind"] == "exec"]
        self.assertIn("tty=true", str(exec_records[0]["query"]))

    async def test_upload_payload_reaches_stdin_and_sink_sees_output(self) -> None:
        self.server.exec_frames = [
            (1, b"chunk-a"),
            (2, b"chunk-b"),
            (3, b"command terminated with exit code 0"),
        ]
        observed: list[tuple[int, bytes]] = []

        def sink(channel: int, payload: bytes) -> None:
            observed.append((channel, payload))

        result = await asyncio.wait_for(
            self.relay.execute(
                admitted_command(),
                relay_envelope(operation=RelayOperation.UPLOAD),
                upload_stream(
                    (
                        "upload",
                        "--dest",
                        "/workspace/file.bin",
                        "--bytes",
                        "24",
                        "--sha256",
                        "0" * 64,
                    ),
                    b"sensitive-upload-payload!!",
                ),
                sink,
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertEqual(
            observed,
            [(1, b"chunk-a"), (2, b"chunk-b")],
        )
        record = next(r for r in self.server.records if r["kind"] == "exec")
        self.assertEqual(
            record["argv"],
            [
                "upload",
                "--dest",
                "/workspace/file.bin",
                "--bytes",
                "24",
                "--sha256",
                "0" * 64,
            ],
        )
        self.assertEqual(record["stdin"], b"sensitive-upload-payload!!")
        self.assertNotIn("sensitive-upload-payload", str(record["path"]))
        self.assertNotIn("sensitive-upload-payload", str(record["query"]))

    async def test_deadline_expiry_fails_closed(self) -> None:
        self.server.exec_delay = 2.0
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]
        relay = KubernetesCommandRelay(
            self.api,
            lambda task_id: self.target if task_id == "task-1" else None,
            stream_deadline_seconds=0.1,
        )

        result = await asyncio.wait_for(
            relay.execute(
                admitted_command(), relay_envelope(), argv_stream(("sleep", "5"))
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertEqual(result.reason_code, "deadline_exceeded")

    async def test_rejects_non_positive_deadline(self) -> None:
        with self.assertRaises(ValueError):
            KubernetesCommandRelay(
                self.api, lambda task_id: None, stream_deadline_seconds=0.0
            )


class ExecStreamHardeningTests(_KubeTestBase):
    async def test_fragmented_frames_are_reassembled(self) -> None:
        self.server.exec_fragment = True
        self.server.exec_frames = [
            (1, b"chunk-one"),
            (2, b"chunk-two"),
            (3, b"command terminated with exit code 0"),
        ]
        relay = KubernetesCommandRelay(self.api, self.locate)
        observed: list[tuple[int, bytes]] = []

        def sink(channel: int, payload: bytes) -> None:
            observed.append((channel, payload))

        result = await asyncio.wait_for(
            relay.execute(
                admitted_command(),
                relay_envelope(),
                argv_stream(("ls", "-l")),
                sink,
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertEqual(observed, [(1, b"chunk-one"), (2, b"chunk-two")])
        record = next(r for r in self.server.records if r["kind"] == "exec")
        self.assertEqual(record["argv"], ["ls", "-l"])

    async def test_interleaved_pings_are_answered_mid_fragmentation(self) -> None:
        self.server.exec_fragment = True
        self.server.exec_frames = [
            (1, b"chunk-one"),
            (3, b"command terminated with exit code 0"),
        ]
        stream = await asyncio.wait_for(
            self.api.exec_stream(_NAMESPACE, "pod-1", "sandbox", tty=False),
            timeout=10.0,
        )
        frames: list[tuple[int, bytes]] = []
        try:
            await stream.send_stdin(encode_helper_argv(("ls",)))
            while True:
                frame = await asyncio.wait_for(stream.receive(), timeout=5.0)
                if frame is None:
                    break
                frames.append(frame)
        finally:
            await stream.close()

        self.assertEqual(
            frames,
            [
                (1, b"chunk-one"),
                (3, b"command terminated with exit code 0"),
            ],
        )
        self.assertGreaterEqual(stream.pings_received, 2)

    async def test_close_code_is_captured_and_fails_closed(self) -> None:
        self.server.exec_frames = [(1, b"partial")]
        self.server.exec_close_code = 1011

        stream = await asyncio.wait_for(
            self.api.exec_stream(_NAMESPACE, "pod-1", "sandbox", tty=False),
            timeout=10.0,
        )
        try:
            await stream.send_stdin(encode_helper_argv(("ls",)))
            self.assertEqual(
                await asyncio.wait_for(stream.receive(), timeout=5.0),
                (1, b"partial"),
            )
            self.assertIsNone(
                await asyncio.wait_for(stream.receive(), timeout=5.0)
            )
        finally:
            await stream.close()

        self.assertEqual(stream.close_code, 1011)

    async def test_close_code_without_status_channel_is_unknown(self) -> None:
        self.server.exec_frames = [(1, b"partial")]
        self.server.exec_close_code = 1000
        relay = KubernetesCommandRelay(self.api, self.locate)

        result = await asyncio.wait_for(
            relay.execute(
                admitted_command(), relay_envelope(), argv_stream(("sleep", "5"))
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertEqual(result.reason_code, "stream_lost")

    async def test_protocol_violations_fail_closed(self) -> None:
        relay = KubernetesCommandRelay(self.api, self.locate)
        for kind in ("masked", "rsv", "continuation", "oversized_control"):
            with self.subTest(kind=kind):
                self.server.exec_violation = kind
                self.server.exec_frames = [
                    (3, b"command terminated with exit code 0")
                ]
                with self.assertRaises(KubernetesExecError) as raised:
                    await asyncio.wait_for(
                        relay.execute(
                            admitted_command(),
                            relay_envelope(),
                            argv_stream(("ls",)),
                        ),
                        timeout=10.0,
                    )
                self.assertIn("protocol violation", str(raised.exception))

    async def test_keepalive_detects_a_silent_peer(self) -> None:
        self.server.exec_pong = False
        self.server.exec_drain_deadline = 1.0
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]
        relay = KubernetesCommandRelay(
            self.api,
            self.locate,
            keepalive_interval=0.05,
            pong_timeout=0.15,
        )

        started = time.monotonic()
        result = await asyncio.wait_for(
            relay.execute(
                admitted_command(), relay_envelope(), argv_stream(("sleep", "30"))
            ),
            timeout=10.0,
        )
        elapsed = time.monotonic() - started

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertEqual(result.reason_code, "stream_lost")
        self.assertLess(elapsed, 0.9)
        self.assertIn(0x9, self.server.ws_client_opcodes)

    async def test_keepalive_heartbeat_keeps_a_responsive_stream(self) -> None:
        self.server.exec_drain_deadline = 0.5
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]
        relay = KubernetesCommandRelay(
            self.api,
            self.locate,
            keepalive_interval=0.05,
            pong_timeout=5.0,
        )

        result = await asyncio.wait_for(
            relay.execute(
                admitted_command(), relay_envelope(), argv_stream(("ls",))
            ),
            timeout=10.0,
        )

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertGreaterEqual(self.server.ws_client_opcodes.count(0x9), 3)

    async def test_upgrade_accept_mismatch_is_refused(self) -> None:
        self.server.exec_accept_header = "invalid-accept-value"
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]
        relay = KubernetesCommandRelay(self.api, self.locate)

        with self.assertRaises(KubernetesExecError) as raised:
            await asyncio.wait_for(
                relay.execute(
                    admitted_command(), relay_envelope(), argv_stream(("ls",))
                ),
                timeout=10.0,
            )

        self.assertIn("WebSocket validation", str(raised.exception))

    async def test_upgrade_without_subprotocol_echo_is_refused(self) -> None:
        self.server.exec_protocol_header = ""
        self.server.exec_frames = [(3, b"command terminated with exit code 0")]
        relay = KubernetesCommandRelay(self.api, self.locate)

        with self.assertRaises(KubernetesExecError) as raised:
            await asyncio.wait_for(
                relay.execute(
                    admitted_command(), relay_envelope(), argv_stream(("ls",))
                ),
                timeout=10.0,
            )

        self.assertIn("subprotocol", str(raised.exception))

    async def test_rejects_invalid_keepalive_configuration(self) -> None:
        with self.assertRaises(ValueError):
            KubernetesCommandRelay(
                self.api, self.locate, keepalive_interval=-1.0
            )
        with self.assertRaises(ValueError):
            KubernetesCommandRelay(
                self.api, self.locate, keepalive_interval=1.0, pong_timeout=0.0
            )


if __name__ == "__main__":
    unittest.main()
