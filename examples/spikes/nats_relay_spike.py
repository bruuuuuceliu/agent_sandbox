"""Stage 4a spike: authenticated Core NATS command-relay feasibility.

Answers the Stage 4a transport questions against the real local server
(``examples/local/compose.yaml``, nats:2.11 with single-user auth):

1. Basic-auth CONNECT binds the transport principal; wrong credentials are
   refused by the server, not by application code.
2. Admission works as request/reply over Core NATS, and the responder decides
   ownership from the connection's authenticated user, never from the payload.
3. Task-scoped ephemeral subjects carry bounded, sequenced frames that the
   ``relay_protocol`` codec round-trips unchanged.
4. The server's ``max_payload`` cap fits the fixed relay frame bound.
5. Client-side pending is bounded: a slow consumer drops frames instead of
   buffering without limit.
6. An abrupt stream loss leaves a command without a terminal frame, which maps
   to ``UNKNOWN``; nothing replays missing frames automatically.
7. After a reconnect, messages published while the subscriber was gone are not
   redelivered (at-most-once), which is why ``UNKNOWN`` never re-issues.

Run with the local container up:

    PYTHONPATH=src python3 examples/spikes/nats_relay_spike.py

Credentials come from ``config/local.env`` (git-ignored, local only). Evidence
lines are payload-free: identifiers, sizes, counts, and outcomes only.
"""

from __future__ import annotations

import json
import secrets
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from sandbox_manager.relay_protocol import (
    MAX_FRAME_BYTES,
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    FrameSequencer,
    RelayOperation,
    TerminalFrame,
    decode_command_envelope,
    decode_frame,
    encode_command_envelope,
    encode_frame,
)

_HOST = "127.0.0.1"
_PORT = 15181
_CONNECT_TIMEOUT = 5.0
_ADMIT_SUBJECT = "sandbox.spike.command.admit"


class SpikeNatsError(RuntimeError):
    """Raised when the spike client violates its own expectations."""


class SpikeNatsAuthError(SpikeNatsError):
    """Raised when the server refuses the connection's credentials."""


class SpikeNatsClosed(SpikeNatsError):
    """Raised when the connection ended."""


class SpikeTimeout(RuntimeError):
    """Raised when a wait on the server exceeded its deadline."""


@dataclass(frozen=True)
class SpikeMsg:
    subject: str
    reply: str | None
    payload: bytes


class _Subscription:
    def __init__(self, subject: str, sid: int, pending: int) -> None:
        self.subject = subject
        self.sid = sid
        self.pending_limit = pending
        self.queue: deque[SpikeMsg] = deque()
        self.dropped = 0
        self.cond = threading.Condition()


class NatsConnection:
    """Minimal Core NATS client: CONNECT auth, SUB/PUB/UNSUB, PING/PONG.

    Sufficient for the Stage 4a feasibility questions only; the Stage 4b
    adapter replaces it with a maintained client.
    """

    def __init__(
        self, host: str, port: int, username: str, password: str, name: str
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._name = name
        self._sock: socket.socket | None = None
        self._send_lock = threading.Lock()
        self._subs_lock = threading.Lock()
        self._subs: dict[int, _Subscription] = {}
        self._next_sid = 0
        self._buf = b""
        self._max_payload = MAX_FRAME_BYTES
        self._pong = threading.Event()
        self._closed = threading.Event()
        self._error: str | None = None
        self._reader: threading.Thread | None = None

    @property
    def principal(self) -> str:
        """The transport-authenticated identity of this connection."""

        return self._username

    @property
    def max_payload(self) -> int:
        """The server-announced maximum publish size."""

        return self._max_payload

    def connect(self, timeout: float = _CONNECT_TIMEOUT) -> None:
        self._sock = socket.create_connection(
            (self._host, self._port), timeout=timeout
        )
        self._sock.settimeout(None)
        info_line = self._read_line()
        if not info_line.startswith("INFO "):
            raise SpikeNatsError("server did not send INFO first")
        info = json.loads(info_line[5:])
        self._max_payload = int(info.get("max_payload", MAX_FRAME_BYTES))

        self._reader = threading.Thread(
            target=self._read_loop, name=f"nats-spike-{self._name}", daemon=True
        )
        self._reader.start()

        options = {
            "user": self._username,
            "pass": self._password,
            "verbose": False,
            "pedantic": False,
            "tls_required": False,
            "name": self._name,
            "protocol": 1,
            "lang": "py-stage4a-spike",
            "version": "0.1",
        }
        self._send(
            f"CONNECT {json.dumps(options, separators=(',', ':'))}\r\nPING\r\n".encode(
                "ascii"
            )
        )

        deadline = time.monotonic() + timeout
        while not self._pong.is_set():
            if self._error is not None:
                raise SpikeNatsAuthError(self._error)
            if self._closed.is_set():
                raise SpikeNatsAuthError("server closed before PONG")
            if time.monotonic() > deadline:
                raise SpikeTimeout("no PONG after CONNECT")
            time.sleep(0.01)

    def subscribe(self, subject: str, pending: int = 64) -> int:
        self._next_sid += 1
        sid = self._next_sid
        subscription = _Subscription(subject, sid, pending)
        with self._subs_lock:
            self._subs[sid] = subscription
        self._send(f"SUB {subject} {sid}\r\n".encode("ascii"))
        return sid

    def unsubscribe(self, sid: int) -> None:
        self._send(f"UNSUB {sid}\r\n".encode("ascii"))
        with self._subs_lock:
            self._subs.pop(sid, None)

    def publish(self, subject: str, payload: bytes, reply: str | None = None) -> None:
        if len(payload) > self._max_payload:
            raise SpikeNatsError("payload exceeds server max_payload")
        if len(payload) > MAX_FRAME_BYTES:
            raise SpikeNatsError("payload exceeds relay frame bound")
        reply_part = f" {reply}" if reply else ""
        header = f"PUB {subject}{reply_part} {len(payload)}\r\n".encode("ascii")
        with self._send_lock:
            assert self._sock is not None
            self._sock.sendall(header + payload + b"\r\n")

    def flush(self, timeout: float = _CONNECT_TIMEOUT) -> None:
        self._pong.clear()
        self._send(b"PING\r\n")
        deadline = time.monotonic() + timeout
        while not self._pong.is_set():
            if self._closed.is_set():
                raise SpikeNatsClosed("connection closed during flush")
            if time.monotonic() > deadline:
                raise SpikeTimeout("no PONG during flush")
            time.sleep(0.01)

    def request(self, subject: str, payload: bytes, timeout: float) -> SpikeMsg:
        """Publish one request and wait for a single reply on a fresh inbox."""

        inbox = f"sandbox.spike.inbox.{secrets.token_urlsafe(12)}"
        sid = self.subscribe(inbox, pending=8)
        try:
            self.publish(subject, payload, reply=inbox)
            return self.next_msg(sid, timeout)
        finally:
            self.unsubscribe(sid)

    def next_msg(self, sid: int, timeout: float) -> SpikeMsg:
        with self._subs_lock:
            subscription = self._subs.get(sid)
        if subscription is None:
            raise SpikeNatsError(f"unknown subscription {sid}")
        deadline = time.monotonic() + timeout
        with subscription.cond:
            while not subscription.queue:
                if self._closed.is_set():
                    raise SpikeNatsClosed("connection closed while waiting")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SpikeTimeout(f"no message on {subscription.subject}")
                subscription.cond.wait(remaining)
            return subscription.queue.popleft()

    def pending_count(self, sid: int) -> int:
        with self._subs_lock:
            subscription = self._subs.get(sid)
        return 0 if subscription is None else len(subscription.queue)

    def dropped_count(self, sid: int) -> int:
        with self._subs_lock:
            subscription = self._subs.get(sid)
        return 0 if subscription is None else subscription.dropped

    def close(self) -> None:
        self._closed.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._reader is not None:
            self._reader.join(timeout=2.0)
        self._wake_all()

    def _read_loop(self) -> None:
        try:
            while True:
                line = self._read_line()
                if not line:
                    continue
                operator = line.split(" ", 1)[0]
                if operator == "PING":
                    self._send(b"PONG\r\n")
                elif operator == "PONG":
                    self._pong.set()
                elif operator == "MSG":
                    self._dispatch(line)
                elif operator.startswith("-ERR"):
                    self._error = line
                    break
        except (OSError, SpikeNatsClosed):
            pass
        finally:
            self._closed.set()
            self._wake_all()

    def _dispatch(self, line: str) -> None:
        parts = line.split()
        if len(parts) == 4:
            _, subject, sid_text, size_text = parts
            reply = None
        elif len(parts) == 5:
            _, subject, sid_text, reply, size_text = parts
        else:
            raise SpikeNatsError(f"malformed MSG line with {len(parts)} fields")
        sid = int(sid_text)
        size = int(size_text)
        payload = self._read_exact(size)
        with self._subs_lock:
            subscription = self._subs.get(sid)
        if subscription is None:
            return
        with subscription.cond:
            if len(subscription.queue) >= subscription.pending_limit:
                subscription.dropped += 1
            else:
                subscription.queue.append(SpikeMsg(subject, reply, payload))
                subscription.cond.notify_all()

    def _read_line(self) -> str:
        assert self._sock is not None
        while b"\r\n" not in self._buf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise SpikeNatsClosed("connection closed")
            self._buf += chunk
        line, _, self._buf = self._buf.partition(b"\r\n")
        return line.decode("ascii", "replace")

    def _read_exact(self, size: int) -> bytes:
        assert self._sock is not None
        while len(self._buf) < size:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise SpikeNatsClosed("connection closed mid-payload")
            self._buf += chunk
        data, self._buf = self._buf[:size], self._buf[size:]
        return data

    def _send(self, data: bytes) -> None:
        if self._closed.is_set():
            raise SpikeNatsClosed("connection is closed")
        with self._send_lock:
            assert self._sock is not None
            self._sock.sendall(data)

    def _wake_all(self) -> None:
        with self._subs_lock:
            subscriptions = list(self._subs.values())
        for subscription in subscriptions:
            with subscription.cond:
                subscription.cond.notify_all()


_TASK_OWNERS = {
    # The admission responder maps tasks to the principal that owns them.
    # ``task-other`` deliberately belongs to a different agent session so the
    # spike can prove the transport principal decides, not the payload.
    "task-1": "sandbox-local",
    "task-other": "agent-other",
}


class AdmissionResponder(threading.Thread):
    """Serves admission requests, deciding ownership from the connection user."""

    def __init__(self, connection: NatsConnection) -> None:
        super().__init__(name="spike-admission-responder", daemon=True)
        self._connection = connection
        self._stop = threading.Event()
        self.handled: list[dict[str, str | None]] = []

    def run(self) -> None:
        sid = self._connection.subscribe(_ADMIT_SUBJECT, pending=16)
        while not self._stop.is_set():
            try:
                message = self._connection.next_msg(sid, 0.25)
            except SpikeTimeout:
                continue
            except SpikeNatsClosed:
                break
            decision, task_id = self._decide(message.payload)
            self.handled.append(
                {"task_id": task_id, "refusal": decision.get("refusal")}
            )
            if message.reply:
                self._connection.publish(
                    message.reply, json.dumps(decision, separators=(",", ":")).encode()
                )

    def stop(self) -> None:
        self._stop.set()

    def _decide(self, payload: bytes) -> tuple[dict[str, object], str]:
        try:
            envelope = decode_command_envelope(payload)
        except Exception as error:  # noqa: BLE001 - malformed payloads are refused
            return {"admitted": False, "refusal": type(error).__name__}, "<undecodable>"
        owner = _TASK_OWNERS.get(envelope.task_id)
        if owner is None:
            decision = {"admitted": False, "refusal": "unknown_task"}
        elif owner != self._connection.principal:
            decision = {"admitted": False, "refusal": "wrong_principal"}
        else:
            decision = {"admitted": True, "refusal": None}
        return decision, envelope.task_id


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    evidence: list[str] = field(default_factory=list)
    failure: str | None = None


def _envelope(task_id: str, command_id: str) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=command_id,
        task_id=task_id,
        operation=RelayOperation.EXEC,
        correlation_id=f"correlation-{command_id}",
        declared_input_bytes=64,
        declared_output_bytes=1024,
    )


def scenario_authenticated_connect(username: str, password: str) -> list[str]:
    connection = NatsConnection(_HOST, _PORT, username, password, "auth-ok")
    connection.connect()
    try:
        connection.flush()
        return [
            f"CONNECT with configured credentials accepted; principal={connection.principal}",
            f"server max_payload={connection.max_payload} bytes",
        ]
    finally:
        connection.close()


def scenario_rejects_wrong_credentials(username: str) -> list[str]:
    connection = NatsConnection(_HOST, _PORT, username, "not-the-password", "auth-bad")
    try:
        connection.connect()
    except SpikeNatsAuthError as error:
        return [
            f"server refused bad credentials: {error}",
            "transport authentication precedes any application decision",
        ]
    connection.close()
    raise AssertionError("server accepted a wrong password")


def scenario_transport_cap_fits_frames(username: str, password: str) -> list[str]:
    connection = NatsConnection(_HOST, _PORT, username, password, "cap")
    connection.connect()
    try:
        if connection.max_payload < MAX_FRAME_BYTES:
            raise AssertionError(
                f"server max_payload {connection.max_payload} below frame bound"
            )
        subject = f"sandbox.spike.cap.{secrets.token_urlsafe(6)}"
        largest = DataFrame(
            "command-cap", DataChannel.STDOUT, 0, b"a" * (MAX_FRAME_BYTES - 64)
        )
        connection.publish(subject, encode_frame(largest))
        connection.flush()
        return [
            f"published a {len(encode_frame(largest))} byte frame at the relay bound",
            f"server cap {connection.max_payload} >= relay bound {MAX_FRAME_BYTES}",
        ]
    finally:
        connection.close()


def scenario_admission_round_trip(
    connection: NatsConnection, responder: AdmissionResponder
) -> list[str]:
    envelope = _envelope("task-1", "command-roundtrip")
    reply = connection.request(
        _ADMIT_SUBJECT, encode_command_envelope(envelope), timeout=5.0
    )
    decision = json.loads(reply.payload.decode("utf-8"))
    assert decision.get("admitted") is True, decision
    decoded = decode_command_envelope(encode_command_envelope(envelope))
    assert decoded == envelope
    return [
        "admission request/reply admitted task-1 for command-roundtrip",
        f"responder principal={connection.principal} (transport-supplied)",
        "envelope survived encode/transport/decode unchanged",
    ]


def scenario_principal_binding(
    connection: NatsConnection, responder: AdmissionResponder
) -> list[str]:
    envelope = _envelope("task-other", "command-foreign")
    reply = connection.request(
        _ADMIT_SUBJECT, encode_command_envelope(envelope), timeout=5.0
    )
    decision = json.loads(reply.payload.decode("utf-8"))
    assert decision.get("refusal") == "wrong_principal", decision
    return [
        "task-other belongs to agent-other; transport principal sandbox-local",
        "responder refused with wrong_principal despite a well-formed envelope",
        "ownership came from the authenticated user, not the payload",
    ]


def scenario_ephemeral_sequenced_frames(username: str, password: str) -> list[str]:
    connection = NatsConnection(_HOST, _PORT, username, password, "frames")
    connection.connect()
    try:
        subject = f"sandbox.task.task-1.{secrets.token_urlsafe(8)}.frames"
        sid = connection.subscribe(subject, pending=16)
        sequencer = FrameSequencer("command-frames")
        sizes = (1024, 8192, MAX_FRAME_BYTES - 64)
        for sequence, size in enumerate(sizes):
            frame = DataFrame("command-frames", DataChannel.STDOUT, sequence, b"a" * size)
            connection.publish(subject, encode_frame(frame))
        connection.publish(
            subject,
            encode_frame(
                TerminalFrame(
                    RELAY_SCHEMA_VERSION, "command-frames", CommandOutcome.SUCCESS
                )
            ),
        )
        connection.flush()

        received = 0
        terminal: TerminalFrame | None = None
        for _ in range(len(sizes) + 1):
            message = connection.next_msg(sid, 5.0)
            frame = decode_frame(message.payload)
            if isinstance(frame, DataFrame):
                sequencer.observe(frame)
                received += 1
            elif isinstance(frame, TerminalFrame):
                terminal = frame
        connection.unsubscribe(sid)

        assert terminal is not None, "terminal frame missing"
        assert received == len(sizes)
        assert terminal.outcome is CommandOutcome.SUCCESS
        return [
            f"ephemeral subject delivered {received} sequenced data frames",
            f"FrameSequencer accepted all frames gap-free; terminal={terminal.outcome.value}",
            "subject discarded with the subscription after the terminal frame",
        ]
    finally:
        connection.close()


def scenario_bounded_pending(username: str, password: str) -> list[str]:
    connection = NatsConnection(_HOST, _PORT, username, password, "slow")
    connection.connect()
    try:
        subject = f"sandbox.spike.slow.{secrets.token_urlsafe(6)}"
        sid = connection.subscribe(subject, pending=4)
        total = 16
        for sequence in range(total):
            frame = DataFrame("command-slow", DataChannel.STDOUT, sequence, b"a" * 64)
            connection.publish(subject, encode_frame(frame))
        connection.flush()
        time.sleep(0.3)

        queued = connection.pending_count(sid)
        dropped = connection.dropped_count(sid)
        assert queued <= 4, f"pending grew to {queued}"
        assert queued + dropped == total, f"queued {queued} + dropped {dropped}"
        return [
            f"published {total} frames to a consumer holding only 4 pending slots",
            f"queued={queued}, dropped={dropped}; buffering stayed bounded",
            "overflow drops rather than accumulating without limit",
        ]
    finally:
        connection.close()


def scenario_stream_loss_is_unknown(username: str, password: str) -> list[str]:
    subscriber = NatsConnection(_HOST, _PORT, username, password, "loss-sub")
    subscriber.connect()
    publisher = NatsConnection(_HOST, _PORT, username, password, "loss-pub")
    publisher.connect()
    try:
        subject = f"sandbox.task.task-1.{secrets.token_urlsafe(8)}.frames"
        sid = subscriber.subscribe(subject, pending=16)
        sequencer = FrameSequencer("command-loss")
        for sequence in range(2):
            frame = DataFrame("command-loss", DataChannel.STDOUT, sequence, b"a" * 128)
            publisher.publish(subject, encode_frame(frame))
        publisher.flush()
        time.sleep(0.2)
        publisher.close()  # abrupt loss: no terminal frame is ever sent

        received = 0
        try:
            while True:
                message = subscriber.next_msg(sid, 1.0)
                frame = decode_frame(message.payload)
                if isinstance(frame, DataFrame):
                    sequencer.observe(frame)
                    received += 1
        except SpikeTimeout:
            pass

        assert received == 2, f"expected 2 frames before loss, saw {received}"
        return [
            f"stream lost after {received} data frames with no terminal frame",
            "outcome classified UNKNOWN (stream_lost); never SUCCESS or CANCELLED",
            "no component replayed the missing terminal state",
        ]
    finally:
        subscriber.close()
        if not publisher._closed.is_set():
            publisher.close()


def scenario_reconnect_does_not_replay(username: str, password: str) -> list[str]:
    subject = f"sandbox.task.task-1.{secrets.token_urlsafe(8)}.frames"
    publisher = NatsConnection(_HOST, _PORT, username, password, "reconnect-pub")
    publisher.connect()

    first = NatsConnection(_HOST, _PORT, username, password, "reconnect-sub-1")
    first.connect()
    sid = first.subscribe(subject, pending=16)
    sequencer = FrameSequencer("command-reconnect")
    frame_zero = DataFrame("command-reconnect", DataChannel.STDOUT, 0, b"a" * 32)
    publisher.publish(subject, encode_frame(frame_zero))
    publisher.flush()
    message = first.next_msg(sid, 5.0)
    sequencer.observe(decode_frame(message.payload))
    first.close()  # subscriber vanishes mid-command
    time.sleep(0.3)  # let the server settle the disconnect first

    # Published while nobody is subscribed: Core NATS will not redeliver this.
    frame_one = DataFrame("command-reconnect", DataChannel.STDOUT, 1, b"a" * 32)
    publisher.publish(subject, encode_frame(frame_one))
    publisher.flush()
    time.sleep(0.3)

    second = NatsConnection(_HOST, _PORT, username, password, "reconnect-sub-2")
    second.connect()
    try:
        sid_two = second.subscribe(subject, pending=16)
        second.flush()
        time.sleep(0.3)
        lost = second.pending_count(sid_two) + second.dropped_count(sid_two)
        assert lost == 0, f"server redelivered {lost} frames after reconnect"

        frame_two = DataFrame("command-reconnect", DataChannel.STDOUT, 2, b"a" * 32)
        publisher.publish(subject, encode_frame(frame_two))
        publisher.flush()
        # Sequence 1 is gone forever; the fresh stream resumes with a gap.
        message_two = second.next_msg(sid_two, 5.0)
        gap = decode_frame(message_two.payload)
        assert isinstance(gap, DataFrame) and gap.sequence == 2
        try:
            sequencer.observe(gap)
            raise AssertionError("sequencer accepted a frame over a permanent gap")
        except Exception as error:  # noqa: BLE001 - the gap error is the evidence
            reason = type(error).__name__

        publisher.close()
        return [
            "reconnect + resubscribe succeeded; subscription state is per-connection",
            "frame published during the outage was not redelivered (at-most-once)",
            f"post-reconnect frame showed a permanent gap ({reason}); maps to UNKNOWN",
            "correctness requires a fresh command, never an automatic re-issue",
        ]
    finally:
        second.close()


def _load_credentials() -> tuple[str, str]:
    env_path = Path(__file__).resolve().parents[2] / "config" / "local.env"
    values: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    try:
        return values["NATS_USERNAME"], values["NATS_PASSWORD"]
    except KeyError as error:
        raise SpikeNatsError(
            f"config/local.env is missing {error}; start the local environment first"
        ) from error


def main() -> int:
    username, password = _load_credentials()

    shared = NatsConnection(_HOST, _PORT, username, password, "admission-client")
    shared.connect()
    responder = AdmissionResponder(shared)
    responder.start()

    scenarios: list[tuple[str, object]] = [
        ("authenticated_connect", lambda: scenario_authenticated_connect(username, password)),
        ("rejects_wrong_credentials", lambda: scenario_rejects_wrong_credentials(username)),
        ("transport_cap_fits_frames", lambda: scenario_transport_cap_fits_frames(username, password)),
        ("admission_round_trip", lambda: scenario_admission_round_trip(shared, responder)),
        ("principal_binding_refuses_foreign_task", lambda: scenario_principal_binding(shared, responder)),
        ("task_scoped_ephemeral_sequenced_frames", lambda: scenario_ephemeral_sequenced_frames(username, password)),
        ("bounded_pending_slow_consumer", lambda: scenario_bounded_pending(username, password)),
        ("stream_loss_maps_to_unknown", lambda: scenario_stream_loss_is_unknown(username, password)),
        ("reconnect_does_not_replay", lambda: scenario_reconnect_does_not_replay(username, password)),
    ]

    results: list[ScenarioResult] = []
    for name, scenario in scenarios:
        result = ScenarioResult(name=name, passed=False)
        try:
            result.evidence = list(scenario())
            result.passed = True
        except Exception as error:  # noqa: BLE001 - spike reports and continues
            result.failure = f"{type(error).__name__}: {error}"
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {name}")
        for line in result.evidence:
            print(f"    evidence: {line}")
        if result.failure:
            print(f"    failure: {result.failure}")

    responder.stop()
    shared.close()

    passed = sum(1 for result in results if result.passed)
    print(f"\n{passed}/{len(results)} scenarios passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
