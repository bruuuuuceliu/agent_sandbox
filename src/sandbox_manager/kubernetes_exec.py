"""Thin Kubernetes ``pods/get`` + ``pods/exec`` client for the manager relay.

Rules this client exists to prove (plan Decisions 17, 20, 22; KR-008, KR-009):

- The Pod is read and verified (UID, task annotation, labels, namespace,
  container, phase) immediately before every ``pods/exec``; a replaced Pod
  (UID mismatch) is never exec'd into.
- ``pods/exec`` invokes one fixed, qualified helper path. User argv travels on
  the exec stdin channel inside a length-prefixed frame and never appears in
  the request URL, headers, or Kubernetes audit metadata.
- A lost exec stream is classified as ``UNKNOWN``; only a terminal error
  channel payload (or its confirmed absence) yields ``SUCCESS`` or ``FAILED``.

In-cluster service-account style configuration (environment host/port, token
file, CA bundle) is used instead of a kubeconfig. The client is deliberately
dependency-free: a minimal RFC 6455 WebSocket implementation over an asyncio
socket with the ``v4.channel.k8s.io`` subprotocol. The framing layer is
production-grade: fragmented messages are reassembled with control frames
honoured mid-message, server pings are answered, a proactive keepalive turns
a silent peer into a prompt stream loss, close codes are surfaced for
diagnosis, and every protocol violation fails closed. Live streaming
verification requires a staging cluster and is gated outside this module.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import ssl
import urllib.parse
from collections.abc import AsyncIterator
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPResponse, HTTPSConnection
from typing import Protocol

from .commands import (
    AdmittedCommand,
    CommandResult,
    InputStream,
    OutputSink,
)
from .relay_protocol import (
    CancelSignal,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    decode_frame,
)

#: The only command ever passed to ``pods/exec`` (Decision 17 / KR-009).
RELAY_HELPER_PATH = "/usr/local/bin/sandbox-relay-helper"

#: Pod annotation carrying the owning task ID (KR-008).
TASK_ANNOTATION = "sandbox-manager.io/task"

CHANNEL_STDIN = 0
CHANNEL_STDOUT = 1
CHANNEL_STDERR = 2
CHANNEL_ERROR = 3

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_WS_SUBPROTOCOL = "v4.channel.k8s.io"
_EXIT_CODE_PATTERN = re.compile(rb"exit code (\d+)")

#: Fixed maximum bytes relayed per command direction (the declared bound).
_MAX_STREAM_BYTES = 256 * 1024 * 1024

_WS_OP_CONTINUATION = 0x0
_WS_OP_TEXT = 0x1
_WS_OP_BINARY = 0x2
_WS_OP_CLOSE = 0x8
_WS_OP_PING = 0x9
_WS_OP_PONG = 0xA
_WS_RESERVED_OPCODES = frozenset((3, 4, 5, 6, 7, 11, 12, 13, 14, 15))
_WS_MAX_CONTROL_PAYLOAD = 125
_WS_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
_WS_CLOSE_NORMAL = 1000


class KubernetesExecError(RuntimeError):
    """Raised when the Kubernetes API interaction fails."""


class PodIdentityError(KubernetesExecError):
    """Raised when the observed Pod does not match the durable task record."""

    def __init__(self, reason: str, expected: str, observed: str) -> None:
        self.reason = reason
        self.expected = expected
        self.observed = observed
        super().__init__(f"pod verification failed ({reason})")


@dataclass(frozen=True, slots=True)
class PodSnapshot:
    """The verified subset of one Pod returned by ``pods/get``."""

    name: str
    namespace: str
    uid: str
    phase: str
    labels: tuple[tuple[str, str], ...]
    annotations: tuple[tuple[str, str], ...]
    containers: tuple[str, ...]

    def annotation(self, name: str) -> str | None:
        for key, value in self.annotations:
            if key == name:
                return value
        return None

    def label(self, name: str) -> str | None:
        for key, value in self.labels:
            if key == name:
                return value
        return None


@dataclass(frozen=True, slots=True)
class PodVerification:
    """The durable task record's expectation of its Pod (KR-008)."""

    expected_uid: str
    task_id: str
    namespace: str
    expected_labels: tuple[tuple[str, str], ...] = ()
    container: str = "sandbox"

    def verify(self, snapshot: PodSnapshot) -> None:
        """Confirm the snapshot is still the task's own, running Pod."""

        if snapshot.uid != self.expected_uid:
            raise PodIdentityError(
                "uid_mismatch", expected=self.expected_uid, observed=snapshot.uid
            )
        if snapshot.namespace != self.namespace or snapshot.phase != "Running":
            raise PodIdentityError(
                "not_running",
                expected=f"Running in {self.namespace}",
                observed=f"{snapshot.phase} in {snapshot.namespace}",
            )
        annotation = snapshot.annotation(TASK_ANNOTATION)
        if annotation != self.task_id:
            raise PodIdentityError(
                "task_annotation_mismatch",
                expected=self.task_id,
                observed=annotation or "<absent>",
            )
        for label, value in self.expected_labels:
            if snapshot.label(label) != value:
                raise PodIdentityError(
                    "label_mismatch",
                    expected=f"{label}={value}",
                    observed=f"{label}={snapshot.label(label) or '<absent>'}",
                )
        if self.container not in snapshot.containers:
            raise PodIdentityError(
                "container_absent",
                expected=self.container,
                observed=",".join(snapshot.containers) or "<none>",
            )


@dataclass(frozen=True, slots=True)
class PodTarget:
    """Where a task's Pod lives, from the durable task record."""

    namespace: str
    name: str
    verification: PodVerification


@dataclass
class KubernetesApiConfig:
    """API server access, service-account style; never a kubeconfig."""

    host: str
    port: int
    bearer_token: str
    ca_cert_path: str | None = None
    secure: bool = True

    def __post_init__(self) -> None:
        if not self.host or not self.port:
            raise KubernetesExecError("API host and port are required")
        if not self.bearer_token:
            raise KubernetesExecError("bearer token is required")

    @classmethod
    def from_service_account(
        cls, service_account_dir: str = "/var/run/secrets/kubernetes.io/serviceaccount"
    ) -> KubernetesApiConfig:
        """Build the in-cluster configuration from the mounted service account."""

        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port_text = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        try:
            port = int(port_text)
        except ValueError as error:
            raise KubernetesExecError("invalid KUBERNETES_SERVICE_PORT") from error
        if not host:
            raise KubernetesExecError("KUBERNETES_SERVICE_HOST is not set")
        token_path = os.path.join(service_account_dir, "token")
        try:
            with open(token_path, encoding="utf-8") as handle:
                token = handle.read().strip()
        except OSError as error:
            raise KubernetesExecError("service account token unreadable") from error
        ca_path = os.path.join(service_account_dir, "ca.crt")
        return cls(
            host=host,
            port=port,
            bearer_token=token,
            ca_cert_path=ca_path if os.path.exists(ca_path) else None,
            secure=True,
        )

    def ssl_context(self) -> ssl.SSLContext | None:
        if not self.secure:
            return None
        if self.ca_cert_path:
            return ssl.create_default_context(cafile=self.ca_cert_path)
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context


def build_pod_get_path(namespace: str, name: str) -> str:
    """Return the ``pods/get`` request path for one Pod."""

    return f"/api/v1/namespaces/{namespace}/pods/{name}"


def build_exec_path(namespace: str, name: str, container: str, tty: bool) -> str:
    """Return the ``pods/exec`` request path.

    The only command in the query is the fixed qualified helper (KR-009): the
    signature accepts no caller argv, so user data cannot reach the URL, the
    request line, or the Kubernetes API audit log.
    """

    tty_flag = "true" if tty else "false"
    return (
        f"/api/v1/namespaces/{namespace}/pods/{name}/exec"
        f"?command={urllib.parse.quote(RELAY_HELPER_PATH, safe='')}"
        f"&container={urllib.parse.quote(container, safe='')}"
        f"&stdin=true&stdout=true&stderr=true"
        f"&tty={tty_flag}"
    )


def encode_helper_argv(argv: tuple[str, ...]) -> bytes:
    """Encode one length-prefixed argv frame for the fixed helper's stdin.

    The helper receives argv on its stdin channel only (KR-009): a four-byte
    big-endian length followed by the UTF-8 JSON argument vector.
    """

    payload = json.dumps({"argv": list(argv)}, separators=(",", ":")).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def classify_termination(
    error_payload: bytes | None, stream_completed: bool
) -> CommandOutcome:
    """Map one terminated exec stream onto a command outcome.

    Fail-closed: anything other than a confirmed clean helper exit is
    ``UNKNOWN`` (Decision 20). A non-zero exit code is a confirmed ``FAILED``.
    """

    if not stream_completed or error_payload is None:
        return CommandOutcome.UNKNOWN
    if not error_payload.strip():
        return CommandOutcome.SUCCESS
    match = _EXIT_CODE_PATTERN.search(error_payload)
    if match is not None:
        code = int(match.group(1))
        return CommandOutcome.SUCCESS if code == 0 else CommandOutcome.FAILED
    return CommandOutcome.UNKNOWN


class KubernetesClient(Protocol):
    async def pod_snapshot(self, namespace: str, name: str) -> PodSnapshot: ...

    async def exec_stream(
        self, namespace: str, name: str, container: str, tty: bool
    ) -> ExecStream: ...


class ExecStream:
    """One framed ``pods/exec`` WebSocket stream (``v4.channel.k8s.io``).

    The framing implements the RFC 6455 rules a Kubernetes API server
    exercises: fragmented messages are reassembled while control frames may
    interleave mid-message, server PINGs are answered, a proactive keepalive
    turns a silent peer into a stream loss, and a received CLOSE completes
    the closing handshake. Protocol violations fail closed as
    ``KubernetesExecError``; a lost or closed stream surfaces as ``None``
    from ``receive`` so termination stays fail-closed. ``close_code`` and the
    ping/pong counters are diagnostics only and carry no payload.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_message_bytes: int = _WS_MAX_MESSAGE_BYTES,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._max_message_bytes = max_message_bytes
        self._fragments: bytearray | None = None
        self._pong_event = asyncio.Event()
        self._keepalive_task: asyncio.Task[None] | None = None
        self._closed = False
        self.close_code: int | None = None
        self.close_reason: str = ""
        self.pings_received = 0
        self.pongs_received = 0

    async def send_stdin(self, payload: bytes) -> None:
        await _ws_send_frame(
            self._writer, _WS_OP_BINARY, bytes((CHANNEL_STDIN,)) + payload
        )

    async def send_ping(self) -> None:
        await _ws_send_frame(self._writer, _WS_OP_PING, b"")

    def start_keepalive(self, interval: float, pong_timeout: float) -> None:
        """Probe the peer periodically; a silent peer tears the stream down."""

        if self._keepalive_task is not None or self._closed:
            return
        self._keepalive_task = asyncio.create_task(
            self._keepalive_loop(interval, pong_timeout),
            name="exec-keepalive",
        )

    async def receive(self) -> tuple[int, bytes] | None:
        """Return the next (channel, payload) frame, or None when closed."""

        while True:
            frame = await _ws_read_frame(self._reader)
            if frame is None:
                return None
            opcode, payload = frame.opcode, frame.payload
            if opcode == _WS_OP_CLOSE:
                await self._handle_close(payload)
                return None
            if opcode == _WS_OP_PING:
                self.pings_received += 1
                try:
                    await _ws_send_frame(self._writer, _WS_OP_PONG, payload)
                except (OSError, ConnectionError):
                    return None
                continue
            if opcode == _WS_OP_PONG:
                self.pongs_received += 1
                self._pong_event.set()
                continue
            if opcode not in (
                _WS_OP_CONTINUATION,
                _WS_OP_TEXT,
                _WS_OP_BINARY,
            ):  # pragma: no cover - reserved opcodes fail in _ws_read_frame
                continue
            if opcode == _WS_OP_CONTINUATION:
                if self._fragments is None:
                    raise KubernetesExecError(
                        "websocket protocol violation "
                        "(continuation frame without an initial frame)"
                    )
            elif self._fragments is not None:
                raise KubernetesExecError(
                    "websocket protocol violation "
                    "(new data frame before the previous message finished)"
                )
            else:
                self._fragments = bytearray()
            self._fragments.extend(payload)
            if len(self._fragments) > self._max_message_bytes:
                raise KubernetesExecError(
                    "websocket message exceeds bound "
                    f"({len(self._fragments)} bytes)"
                )
            if frame.fin:
                message = bytes(self._fragments)
                self._fragments = None
                if not message:
                    continue
                return message[0], message[1:]

    async def close(self) -> None:
        """Complete a best-effort closing handshake and drop the socket."""

        if self._closed:
            return
        self._closed = True
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            await asyncio.gather(self._keepalive_task, return_exceptions=True)
            self._keepalive_task = None
        try:
            self._writer.write(
                _ws_client_frame(
                    _WS_OP_CLOSE, _WS_CLOSE_NORMAL.to_bytes(2, "big")
                )
            )
            await self._writer.drain()
        except (OSError, ConnectionError):
            pass
        await _close_writer(self._writer)

    async def _keepalive_loop(self, interval: float, pong_timeout: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                self._pong_event.clear()
                await _ws_send_frame(self._writer, _WS_OP_PING, b"")
                try:
                    await asyncio.wait_for(
                        self._pong_event.wait(), timeout=pong_timeout
                    )
                except TimeoutError:
                    # The peer stopped answering. Tearing the socket down
                    # unblocks receive() with None so the command fails
                    # closed as UNKNOWN instead of waiting out its deadline.
                    await self._abort()
                    return
        except (OSError, ConnectionError):
            return  # the read loop observes the loss

    async def _abort(self) -> None:
        """Drop the socket without a handshake (the keepalive failure path)."""

        self._closed = True
        await _close_writer(self._writer)

    async def _handle_close(self, payload: bytes) -> None:
        if len(payload) >= 2:
            self.close_code = int.from_bytes(payload[:2], "big")
            self.close_reason = payload[2:].decode("utf-8", "replace")
            echo = payload[:2]
        else:
            echo = b""
        try:
            self._writer.write(_ws_client_frame(_WS_OP_CLOSE, echo))
            await self._writer.drain()
        except (OSError, ConnectionError):
            pass
        self._closed = True
        await _close_writer(self._writer)


class KubernetesApi:
    """Client for ``pods/get`` and ``pods/exec`` over one config."""

    def __init__(self, config: KubernetesApiConfig) -> None:
        self._config = config

    async def pod_snapshot(self, namespace: str, name: str) -> PodSnapshot:
        path = build_pod_get_path(namespace, name)
        body = await asyncio.to_thread(self._get_json, path)
        return _parse_pod_snapshot(namespace, name, body)

    async def exec_stream(
        self, namespace: str, name: str, container: str, tty: bool
    ) -> ExecStream:
        path = build_exec_path(namespace, name, container, tty)
        return await _open_exec_stream(self._config, path)

    def _get_json(self, path: str) -> object:
        headers = {
            "Authorization": f"Bearer {self._config.bearer_token}",
            "Accept": "application/json",
        }
        if self._config.secure:
            connection: HTTPConnection = HTTPSConnection(
                self._config.host, self._config.port, context=self._config.ssl_context()
            )
        else:
            connection = HTTPConnection(self._config.host, self._config.port)
        try:
            connection.request("GET", path, headers=headers)
            response: HTTPResponse = connection.getresponse()
            payload = response.read()
            if response.status != 200:
                raise KubernetesExecError(f"pods/get returned {response.status}")
        finally:
            connection.close()
        try:
            return json.loads(payload)
        except json.JSONDecodeError as error:
            raise KubernetesExecError("pods/get returned invalid JSON") from error


def _parse_pod_snapshot(
    namespace: str, name: str, body: object
) -> PodSnapshot:
    if not isinstance(body, dict):
        raise KubernetesExecError("pod payload must be an object")
    metadata = body.get("metadata")
    spec = body.get("spec")
    status = body.get("status")
    if not isinstance(metadata, dict) or not isinstance(spec, dict) or not isinstance(
        status, dict
    ):
        raise KubernetesExecError("pod payload is missing metadata/spec/status")
    uid = metadata.get("uid")
    if not isinstance(uid, str) or not uid:
        raise KubernetesExecError("pod payload is missing a UID")
    phase = status.get("phase")
    if not isinstance(phase, str):
        raise KubernetesExecError("pod payload is missing a phase")
    labels = _string_pairs(metadata.get("labels"))
    annotations = _string_pairs(metadata.get("annotations"))
    names: list[str] = []
    for container in spec.get("containers", ()):
        if isinstance(container, dict):
            container_name = container.get("name")
            if isinstance(container_name, str):
                names.append(container_name)
    containers = tuple(names)
    return PodSnapshot(
        name=name,
        namespace=namespace,
        uid=uid,
        phase=phase,
        labels=labels,
        annotations=annotations,
        containers=containers,
    )


def _string_pairs(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, dict):
        return ()
    return tuple(
        (key, item)
        for key, item in value.items()
        if isinstance(key, str) and isinstance(item, str)
    )


class PodLocator(Protocol):
    def __call__(self, task_id: str) -> PodTarget | None: ...


class KubernetesCommandRelay:
    """The Stage 4b relay shape, proven by the Stage 4a spike.

    Verifies the Pod, execs only the fixed helper, sends argv over the stdin
    channel, and classifies termination fail-closed. The ``input_stream``
    carries relay ``DataFrame`` frames; STDIN sequence 0 holds the argv and the
    remaining STDIN frames stream onward to the Pod (upload payloads). The
    optional ``output_sink`` observes each output frame as it arrives so
    downloads can stream instead of buffering. ``stream_deadline_seconds``
    bounds the whole exchange; expiry fails closed to ``UNKNOWN``. A positive
    ``keepalive_interval`` probes the stream with WebSocket pings so a silent
    peer is detected in ``pong_timeout`` rather than at the deadline.
    """

    def __init__(
        self,
        api: KubernetesApi,
        locate: PodLocator,
        stream_deadline_seconds: float = 900.0,
        keepalive_interval: float = 30.0,
        pong_timeout: float = 30.0,
    ) -> None:
        if stream_deadline_seconds <= 0:
            raise ValueError("stream deadline must be positive")
        if keepalive_interval < 0:
            raise ValueError("keepalive interval must not be negative")
        if keepalive_interval > 0 and pong_timeout <= 0:
            raise ValueError("pong timeout must be positive")
        self._api = api
        self._locate = locate
        self._deadline = stream_deadline_seconds
        self._keepalive_interval = keepalive_interval
        self._pong_timeout = pong_timeout

    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        target = self._locate(command.task_id)
        if target is None:
            raise KubernetesExecError(f"no pod target for task {command.task_id}")

        snapshot = await self._api.pod_snapshot(
            target.namespace, target.name
        )
        target.verification.verify(snapshot)

        stream = await self._api.exec_stream(
            target.namespace,
            target.name,
            target.verification.container,
            tty=envelope.operation is RelayOperation.PTY,
        )

        argv = await _read_argv(input_stream)
        await stream.send_stdin(encode_helper_argv(argv))
        if self._keepalive_interval > 0:
            stream.start_keepalive(self._keepalive_interval, self._pong_timeout)

        stdout_bytes = 0
        stderr_bytes = 0
        error_payload: bytes | None = None
        completed = False
        reason_code: str | None = None
        forwarder = asyncio.create_task(_forward_stdin(input_stream, stream))
        try:
            async with asyncio.timeout(self._deadline):
                async for channel, payload in _frames(stream):
                    if channel == CHANNEL_STDOUT:
                        stdout_bytes += len(payload)
                        if output_sink is not None:
                            output_sink(CHANNEL_STDOUT, payload)
                    elif channel == CHANNEL_STDERR:
                        stderr_bytes += len(payload)
                        if output_sink is not None:
                            output_sink(CHANNEL_STDERR, payload)
                    elif channel == CHANNEL_ERROR:
                        error_payload = payload
                        completed = True
                        break
                    if stdout_bytes + stderr_bytes > _MAX_STREAM_BYTES:
                        error_payload = b"spike stream budget exceeded"
                        completed = True
                        break
        except TimeoutError:
            reason_code = "deadline_exceeded"
        finally:
            forwarder.cancel()
            forwarded = await asyncio.gather(forwarder, return_exceptions=True)
            await stream.close()

        failure = forwarded[0]
        if isinstance(failure, BaseException) and not isinstance(
            failure, asyncio.CancelledError
        ):
            # The input stream failed on its own (a truncated upload, for
            # example); its typed error outranks the fail-closed result the
            # closed stream would otherwise produce.
            raise failure

        outcome = classify_termination(error_payload, completed)
        if outcome is CommandOutcome.UNKNOWN and reason_code is None:
            reason_code = "stream_lost"
        return CommandResult(
            command_id=command.command_id,
            outcome=outcome,
            reason_code=reason_code if outcome is CommandOutcome.UNKNOWN else None,
        )


async def _forward_stdin(
    input_stream: InputStream | None, stream: ExecStream
) -> None:
    """Stream remaining STDIN frames to the Pod until input ends or cancels.

    A ``CancelSignal`` stops forwarding (best-effort cancellation); the read
    loop keeps waiting for the error-channel status, and only a received
    status can confirm termination — an unconfirmed cancel stays ``UNKNOWN``.
    An input stream that fails closes the Pod stream so the read loop cannot
    wait on a helper that will never see more stdin, then re-raises.
    """

    if input_stream is None:
        return
    try:
        async for frame_bytes in input_stream:
            frame = decode_frame(frame_bytes)
            if isinstance(frame, DataFrame) and frame.channel is DataChannel.STDIN:
                await stream.send_stdin(frame.payload)
            elif isinstance(frame, CancelSignal):
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        await stream.close()
        raise


async def _read_argv(input_stream: InputStream | None) -> tuple[str, ...]:
    """Read the argv frame (STDIN sequence 0) from the relay input stream."""

    if input_stream is None:
        return ()
    async for frame_bytes in input_stream:
        frame = decode_frame(frame_bytes)
        if isinstance(frame, DataFrame) and frame.channel is DataChannel.STDIN:
            if frame.sequence != 0:
                raise KubernetesExecError("argv must arrive on STDIN sequence 0")
            payload = json.loads(frame.payload.decode("utf-8"))
            argv = payload.get("argv", ())
            if not isinstance(argv, list) or not all(
                isinstance(item, str) for item in argv
            ):
                raise KubernetesExecError("argv frame must carry a string vector")
            return tuple(argv)
        raise KubernetesExecError("first input frame must be STDIN data")
    return ()


async def _frames(stream: ExecStream) -> AsyncIterator[tuple[int, bytes]]:
    while True:
        frame = await stream.receive()
        if frame is None:
            return
        yield frame


@dataclass(frozen=True, slots=True)
class _WsFrame:
    fin: bool
    opcode: int
    payload: bytes


async def _open_exec_stream(
    config: KubernetesApiConfig, path: str
) -> ExecStream:
    key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {config.host}:{config.port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        f"Sec-WebSocket-Protocol: {_WS_SUBPROTOCOL}\r\n"
        f"Authorization: Bearer {config.bearer_token}\r\n"
        "\r\n"
    )
    ssl_context = config.ssl_context()
    try:
        if ssl_context is not None:
            reader, writer = await asyncio.open_connection(
                config.host, config.port, ssl=ssl_context
            )
        else:
            reader, writer = await asyncio.open_connection(config.host, config.port)
    except OSError as error:
        raise KubernetesExecError("could not reach the API server") from error

    writer.write(request.encode("ascii"))
    await writer.drain()

    status_line = await reader.readline()
    try:
        status = int(status_line.split()[1])
    except (IndexError, ValueError) as error:
        await _close_writer(writer)
        raise KubernetesExecError("malformed upgrade response") from error
    if status != 101:
        await _close_writer(writer)
        raise KubernetesExecError(f"exec upgrade refused with {status}")

    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, _, value = line.partition(b":")
        headers[name.decode("latin-1").strip().lower()] = value.decode(
            "latin-1"
        ).strip()

    expected_accept = base64.b64encode(
        hashlib.sha1(f"{key}{_WS_GUID}".encode("ascii")).digest()
    ).decode("ascii")
    if (
        headers.get("upgrade", "").lower() != "websocket"
        or headers.get("sec-websocket-accept") != expected_accept
    ):
        await _close_writer(writer)
        raise KubernetesExecError("exec upgrade failed WebSocket validation")
    if headers.get("sec-websocket-protocol") != _WS_SUBPROTOCOL:
        await _close_writer(writer)
        raise KubernetesExecError(
            "exec upgrade did not confirm the channel subprotocol"
        )
    return ExecStream(reader=reader, writer=writer)


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass


async def _ws_send_frame(
    writer: asyncio.StreamWriter, opcode: int, payload: bytes
) -> None:
    writer.write(_ws_client_frame(opcode, payload))
    await writer.drain()


def _ws_client_frame(opcode: int, payload: bytes) -> bytes:
    """Encode one masked client frame (RFC 6455 clients must mask)."""

    mask = secrets.token_bytes(4)
    masked = _apply_mask(payload, mask)
    length = len(payload)
    if length < 126:
        header = bytes((0x80 | opcode, 0x80 | length))
    elif length < 65536:
        header = bytes((0x80 | opcode, 0x80 | 126)) + length.to_bytes(2, "big")
    else:
        header = bytes((0x80 | opcode, 0x80 | 127)) + length.to_bytes(8, "big")
    return header + mask + masked


def _apply_mask(payload: bytes, mask: bytes) -> bytes:
    return bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))


async def _ws_read_frame(reader: asyncio.StreamReader) -> _WsFrame | None:
    """Read one server frame; return None on a clean or broken close.

    Protocol violations — reserved bits, masked server frames, reserved
    opcodes, fragmented or oversized control frames — fail closed rather
    than being tolerated: an untrusted framing layer must not be
    interpreted further.
    """

    try:
        header = await reader.readexactly(2)
        opcode = header[0] & 0x0F
        length = header[1] & 0x7F
        if length == 126:
            length = int.from_bytes(await reader.readexactly(2), "big")
        elif length == 127:
            length = int.from_bytes(await reader.readexactly(8), "big")
        masked = bool(header[1] & 0x80)
        if masked:
            mask = await reader.readexactly(4)
            payload = _apply_mask(await reader.readexactly(length), mask)
        else:
            payload = await reader.readexactly(length)
    except (asyncio.IncompleteReadError, ConnectionError, OSError):
        return None
    if masked:
        raise KubernetesExecError(
            "websocket protocol violation (masked server frame)"
        )
    if header[0] & 0x70:
        raise KubernetesExecError(
            "websocket protocol violation (reserved bits set)"
        )
    if opcode >= 0x8 and (
        not header[0] & 0x80 or length > _WS_MAX_CONTROL_PAYLOAD
    ):
        raise KubernetesExecError(
            "websocket protocol violation "
            "(fragmented or oversized control frame)"
        )
    if opcode in _WS_RESERVED_OPCODES:
        raise KubernetesExecError(
            f"websocket protocol violation (reserved opcode 0x{opcode:x})"
        )
    return _WsFrame(fin=bool(header[0] & 0x80), opcode=opcode, payload=payload)
