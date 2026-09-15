from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import socket
import ssl
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
from sandbox_manager.kubernetes import (
    TASK_ANNOTATION,
    SandboxWorkload,
    build_sandbox_resources,
)
from sandbox_manager.kubernetes_exec import (
    RELAY_HELPER_PATH,
    ExecStream,
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
    CancelSignal,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayFrame,
    RelayOperation,
    RelayProtocolError,
    TerminalFrame,
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
    *,
    declared_input_bytes: int = 64,
    declared_output_bytes: int = 1024,
) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id="command-1",
        task_id="task-1",
        operation=operation,
        correlation_id="correlation-1",
        declared_input_bytes=declared_input_bytes,
        declared_output_bytes=declared_output_bytes,
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


async def frame_stream(*frames: RelayFrame) -> AsyncIterator[bytes]:
    for frame in frames:
        yield encode_frame(frame)


def argv_frame(
    *,
    command_id: str = "command-1",
    channel: DataChannel = DataChannel.STDIN,
    sequence: int = 0,
    payload: bytes | None = None,
) -> DataFrame:
    if payload is None:
        payload = json.dumps({"argv": ["ls"]}).encode("utf-8")
    return DataFrame(command_id, channel, sequence, payload)


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
                self.wfile.flush()
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
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
    def test_accepts_annotation_from_resource_builder(self) -> None:
        resources = build_sandbox_resources(
            SandboxWorkload(
                namespace=_NAMESPACE,
                task_id="task-1",
                image="registry.example/sandbox/python@sha256:" + "a" * 64,
                command=("python", "-m", "sandbox_agent"),
                runtime_class_name="runsc",
                storage_class_name="encrypted-rwo",
                result_storage="1Gi",
                results_mount_path="/results",
            )
        )
        pod = resources.pod.copy()
        metadata = cast(dict[str, object], pod["metadata"]).copy()
        metadata["uid"] = "uid-1"
        pod["metadata"] = metadata
        pod["status"] = {"phase": "Running"}

        built = kubernetes_exec._parse_pod_snapshot(
            _NAMESPACE, cast(str, metadata["name"]), pod
        )

        PodVerification(
            expected_uid="uid-1", task_id="task-1", namespace=_NAMESPACE
        ).verify(built)

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

    def test_requires_a_positive_finite_request_timeout(self) -> None:
        for timeout in (0.0, -1.0, float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(
                KubernetesExecError
            ):
                KubernetesApiConfig(
                    host="h",
                    port=1,
                    bearer_token="token",
                    request_timeout_seconds=timeout,
                )

    def test_request_timeout_is_passed_to_http_connections(self) -> None:
        insecure = KubernetesApiConfig(
            host="kubernetes.internal",
            port=80,
            bearer_token="token",
            secure=False,
            request_timeout_seconds=2.5,
        )
        with patch.object(kubernetes_exec, "HTTPConnection") as connection_type:
            response = connection_type.return_value.getresponse.return_value
            response.status = 200
            response.read.return_value = b"{}"

            KubernetesApi(insecure)._get_json("/api/v1/pods")

        self.assertEqual(connection_type.call_args.kwargs["timeout"], 2.5)

        secure = KubernetesApiConfig(
            host="kubernetes.internal",
            port=443,
            bearer_token="token",
            request_timeout_seconds=3.5,
        )
        with patch.object(kubernetes_exec, "HTTPSConnection") as connection_type:
            response = connection_type.return_value.getresponse.return_value
            response.status = 200
            response.read.return_value = b"{}"

            KubernetesApi(secure)._get_json("/api/v1/pods")

        self.assertEqual(connection_type.call_args.kwargs["timeout"], 3.5)

    def test_bearer_token_is_redacted_from_repr(self) -> None:
        config = KubernetesApiConfig(
            host="kubernetes.internal",
            port=443,
            bearer_token="do-not-expose-this-token",
        )

        self.assertNotIn("do-not-expose-this-token", repr(config))

    def test_tls_without_service_account_ca_uses_verified_system_roots(self) -> None:
        config = KubernetesApiConfig(
            host="kubernetes.internal", port=443, bearer_token="token"
        )

        context = config.ssl_context()

        self.assertIsNotNone(context)
        assert context is not None
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_unreadable_ca_fails_instead_of_disabling_verification(self) -> None:
        config = KubernetesApiConfig(
            host="kubernetes.internal",
            port=443,
            bearer_token="token",
            ca_cert_path="/path/that/does/not/exist/ca.crt",
        )

        with self.assertRaises(FileNotFoundError):
            config.ssl_context()


def _write_service_account() -> Path:
    import tempfile

    directory = Path(tempfile.mkdtemp())
    (directory / "token").write_text("token-value\n", encoding="utf-8")
    return directory


class _RecordingExecStream:
    def __init__(self, frames: list[tuple[int, bytes]] | None = None) -> None:
        self.closed = False
        self.close_calls = 0
        self.sent: list[bytes] = []
        self.frames = [] if frames is None else list(frames)
        self._closed = asyncio.Event()

    async def send_stdin(self, payload: bytes) -> None:
        self.sent.append(payload)

    def start_keepalive(self, interval: float, pong_timeout: float) -> None:
        return

    async def receive(self) -> tuple[int, bytes] | None:
        if self.frames:
            return self.frames.pop(0)
        await self._closed.wait()
        return None

    async def close(self) -> None:
        self.closed = True
        self.close_calls += 1
        self._closed.set()


class _ControlledRelayApi:
    def __init__(
        self,
        stream: _RecordingExecStream,
        *,
        block_pod_lookup: bool = False,
        block_exec_setup: bool = False,
    ) -> None:
        self.stream = stream
        self.block_pod_lookup = block_pod_lookup
        self.block_exec_setup = block_exec_setup
        self.pod_calls = 0
        self.exec_calls = 0

    async def pod_snapshot(self, namespace: str, name: str) -> PodSnapshot:
        self.pod_calls += 1
        if self.block_pod_lookup:
            await asyncio.Event().wait()
        return snapshot(namespace=namespace)

    async def exec_stream(
        self, namespace: str, name: str, container: str, tty: bool
    ) -> ExecStream:
        self.exec_calls += 1
        if self.block_exec_setup:
            await asyncio.Event().wait()
        return cast(ExecStream, self.stream)


class _RecordingWriter:
    def __init__(self) -> None:
        self.closed = False
        self.drain_started = asyncio.Event()

    def write(self, data: bytes) -> None:
        return

    async def drain(self) -> None:
        self.drain_started.set()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return


class KubernetesRelayIntegrityTests(unittest.IsolatedAsyncioTestCase):
    def relay(
        self, stream: _RecordingExecStream, **api_options: bool
    ) -> KubernetesCommandRelay:
        api = _ControlledRelayApi(stream, **api_options)
        return KubernetesCommandRelay(api, lambda task_id: PodTarget(
            namespace=_NAMESPACE,
            name="pod-1",
            verification=verification(),
        ))

    async def test_deadline_includes_pod_lookup_and_exec_setup(self) -> None:
        for blocked_stage in ("pod", "exec"):
            with self.subTest(blocked_stage=blocked_stage):
                stream = _RecordingExecStream()
                api = _ControlledRelayApi(
                    stream,
                    block_pod_lookup=blocked_stage == "pod",
                    block_exec_setup=blocked_stage == "exec",
                )
                relay = KubernetesCommandRelay(
                    api,
                    lambda task_id: PodTarget(
                        namespace=_NAMESPACE,
                        name="pod-1",
                        verification=verification(),
                    ),
                    stream_deadline_seconds=0.01,
                    keepalive_interval=0,
                )

                result = await relay.execute(
                    admitted_command(), relay_envelope(), None
                )

                self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
                self.assertEqual(result.reason_code, "deadline_exceeded")
                self.assertEqual(api.pod_calls, 1)
                self.assertEqual(api.exec_calls, blocked_stage == "exec")

    async def test_malformed_argv_closes_opened_stream(self) -> None:
        stream = _RecordingExecStream()

        with self.assertRaises(RelayProtocolError):
            await self.relay(stream).execute(
                admitted_command(),
                relay_envelope(),
                frame_stream(argv_frame(payload=b"{not-json")),
            )

        self.assertTrue(stream.closed)

    async def test_cancellation_while_awaiting_argv_closes_opened_stream(self) -> None:
        stream = _RecordingExecStream()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def waiting_input() -> AsyncIterator[bytes]:
            entered.set()
            await release.wait()
            yield encode_frame(argv_frame())

        task = asyncio.create_task(
            self.relay(stream).execute(
                admitted_command(), relay_envelope(), waiting_input()
            )
        )
        await entered.wait()
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(stream.closed)

    async def test_rejects_wrong_command_id_and_non_stdin_argv(self) -> None:
        cases = (
            argv_frame(command_id="command-2"),
            argv_frame(channel=DataChannel.STDOUT),
        )
        for frame in cases:
            with self.subTest(frame=frame):
                stream = _RecordingExecStream()
                with self.assertRaises(RelayProtocolError):
                    await self.relay(stream).execute(
                        admitted_command(), relay_envelope(), frame_stream(frame)
                    )
                self.assertTrue(stream.closed)

    async def test_rejects_stdin_sequence_gaps_and_duplicates(self) -> None:
        for invalid_sequence in (0, 2):
            with self.subTest(invalid_sequence=invalid_sequence):
                stream = _RecordingExecStream()
                with self.assertRaises(RelayProtocolError):
                    await self.relay(stream).execute(
                        admitted_command(),
                        relay_envelope(),
                        frame_stream(
                            argv_frame(),
                            DataFrame(
                                "command-1",
                                DataChannel.STDIN,
                                invalid_sequence,
                                b"payload",
                            ),
                        ),
                    )
                self.assertTrue(stream.closed)

    async def test_rejects_wrong_cancel_command_and_terminal_input(self) -> None:
        invalid_frames: tuple[RelayFrame, ...] = (
            CancelSignal(RELAY_SCHEMA_VERSION, "command-2"),
            TerminalFrame(
                RELAY_SCHEMA_VERSION, "command-1", CommandOutcome.SUCCESS
            ),
        )
        for invalid_frame in invalid_frames:
            with self.subTest(frame=invalid_frame):
                stream = _RecordingExecStream()
                with self.assertRaises(RelayProtocolError):
                    await self.relay(stream).execute(
                        admitted_command(),
                        relay_envelope(),
                        frame_stream(argv_frame(), invalid_frame),
                    )
                self.assertTrue(stream.closed)

    async def test_declared_input_budget_excludes_argv_and_rejects_overflow(
        self,
    ) -> None:
        stream = _RecordingExecStream()

        with self.assertRaisesRegex(RelayProtocolError, "declared input"):
            await self.relay(stream).execute(
                admitted_command(),
                relay_envelope(declared_input_bytes=2),
                frame_stream(
                    argv_frame(payload=b'{"argv":["a very long argument"]}'),
                    DataFrame("command-1", DataChannel.STDIN, 1, b"abc"),
                ),
            )

        self.assertTrue(stream.closed)
        self.assertEqual(len(stream.sent), 1)

    async def test_declared_output_budget_accepts_the_exact_bound(self) -> None:
        stream = _RecordingExecStream(
            [
                (kubernetes_exec.CHANNEL_STDOUT, b"abc"),
                (kubernetes_exec.CHANNEL_STDERR, b"de"),
                (
                    kubernetes_exec.CHANNEL_ERROR,
                    b"command terminated with exit code 0",
                ),
            ]
        )
        observed: list[tuple[int, bytes]] = []

        async def sink(channel: int, payload: bytes) -> None:
            observed.append((channel, payload))

        result = await self.relay(stream).execute(
            admitted_command(),
            relay_envelope(declared_output_bytes=5),
            frame_stream(argv_frame()),
            sink,
        )

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertEqual(
            observed,
            [
                (kubernetes_exec.CHANNEL_STDOUT, b"abc"),
                (kubernetes_exec.CHANNEL_STDERR, b"de"),
            ],
        )

    async def test_declared_output_overflow_closes_before_sink_delivery(self) -> None:
        stream = _RecordingExecStream(
            [
                (kubernetes_exec.CHANNEL_STDOUT, b"abc"),
                (kubernetes_exec.CHANNEL_STDERR, b"def"),
                (
                    kubernetes_exec.CHANNEL_ERROR,
                    b"command terminated with exit code 0",
                ),
            ]
        )
        observed: list[tuple[int, bytes]] = []

        async def sink(channel: int, payload: bytes) -> None:
            observed.append((channel, payload))

        with self.assertRaisesRegex(RelayProtocolError, "declared output"):
            await self.relay(stream).execute(
                admitted_command(),
                relay_envelope(declared_output_bytes=5),
                frame_stream(argv_frame()),
                sink,
            )

        self.assertTrue(stream.closed)
        self.assertEqual(observed, [(kubernetes_exec.CHANNEL_STDOUT, b"abc")])


class ExecUpgradeCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_upgrade_timeout_closes_the_connected_writer(self) -> None:
        reader = asyncio.StreamReader()
        writer = _RecordingWriter()

        async def open_connection(
            *args: object, **kwargs: object
        ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            return reader, cast(asyncio.StreamWriter, writer)

        api = KubernetesApi(
            KubernetesApiConfig(
                host="kubernetes.internal",
                port=80,
                bearer_token="token",
                secure=False,
                request_timeout_seconds=0.01,
            )
        )
        with (
            patch.object(asyncio, "open_connection", open_connection),
            self.assertRaisesRegex(KubernetesExecError, "timed out"),
        ):
            await api.exec_stream(_NAMESPACE, "pod-1", "sandbox", False)

        self.assertTrue(writer.closed)

    async def test_upgrade_cancellation_closes_the_connected_writer(self) -> None:
        reader = asyncio.StreamReader()
        writer = _RecordingWriter()

        async def open_connection(
            *args: object, **kwargs: object
        ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            return reader, cast(asyncio.StreamWriter, writer)

        config = KubernetesApiConfig(
            host="kubernetes.internal",
            port=80,
            bearer_token="token",
            secure=False,
        )
        with patch.object(asyncio, "open_connection", open_connection):
            task = asyncio.create_task(
                kubernetes_exec._open_exec_stream(config, "/exec")
            )
            await writer.drain_started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(writer.closed)


class WebSocketFrameHeaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_headers_fail_without_waiting_for_payload(self) -> None:
        oversized = kubernetes_exec._WS_MAX_MESSAGE_BYTES + 1
        cases = {
            "masked": b"\x82\x80",
            "reserved bit": b"\xc2\x00",
            "reserved opcode": b"\x83\x00",
            "fragmented control": b"\x09\x00",
            "extended control": b"\x89\x7e",
            "oversized data": b"\x82\x7f" + oversized.to_bytes(8, "big"),
            "invalid 64-bit length": b"\x82\x7f\x80" + b"\x00" * 7,
            "non-canonical 16-bit": b"\x82\x7e\x00\x7d",
            "non-canonical 64-bit": b"\x82\x7f" + (65535).to_bytes(8, "big"),
        }

        for name, header in cases.items():
            with self.subTest(name=name):
                reader = asyncio.StreamReader()
                reader.feed_data(header)
                with self.assertRaises(KubernetesExecError):
                    await asyncio.wait_for(
                        kubernetes_exec._ws_read_frame(reader), timeout=0.1
                    )


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

        async def sink(channel: int, payload: bytes) -> None:
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

        async def sink(channel: int, payload: bytes) -> None:
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
