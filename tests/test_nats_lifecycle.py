"""In-process JetStream fake and tests for the lifecycle transports.

``FakeJetStreamServer`` extends the Core NATS wire fake with the JetStream
behaviour the lifecycle transports rely on: the ``$JS.API`` JSON
request/reply endpoints, stream storage with message-ID deduplication,
durable explicit-ack pull consumers with ``$JS.ACK`` handling, publish
acknowledgments on reply inboxes, and 408 status messages when a pull
window expires empty. Everything runs over real sockets and real wire
framing, so header encoding (``HPUB``/``HMSG``) is exercised end to end.
"""

from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import Callable, Coroutine, Mapping
from datetime import UTC, datetime
from typing import Any, cast

from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    AdmissionRefusal,
    AdmissionRequest,
    CommandEvent,
    CommandEventKind,
    InMemoryCommandRepository,
    TaskAdmissionState,
)
from sandbox_manager.intake import QueuedTask, StoreResult
from sandbox_manager.nats_client import (
    AsyncNatsConnection,
    NatsMessage,
    NatsTransportError,
    parse_headers,
)
from sandbox_manager.nats_lifecycle import (
    COMMAND_EVENT_SCHEMA_VERSION,
    COMMAND_EVENT_SUBJECT,
    READY_SCHEMA_VERSION,
    JetStream,
    JetStreamApiError,
    JetStreamCommandEventJournal,
    JetStreamError,
    JetStreamMessage,
    JetStreamReadyPublisher,
    JetStreamSpawnIntake,
    NatsLifecycleError,
    decode_command_event,
    decode_ready_response,
    encode_command_event,
    encode_ready_response,
    principal_from_ready_subject,
    principal_from_spawn_subject,
    ready_subject,
    spawn_subject,
)
from sandbox_manager.nats_relay import admission_subject
from sandbox_manager.profiles import (
    PROFILE_SCHEMA_VERSION,
    ProfileMount,
    SandboxProfile,
)
from sandbox_manager.provisioning import (
    ProvisionedSandbox,
    ProvisioningTask,
    ReadyTask,
    RelayReference,
)
from sandbox_manager.relay_protocol import RELAY_SCHEMA_VERSION, CommandOutcome
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest
from tests.test_nats_relay import FakeNatsServer, _FakeNatsSession, _subject_matches

_MANAGER_USER = "sandbox-manager"

_AGENT_USER = "agent-session-1"


class _ApiError(Exception):
    def __init__(self, code: int, description: str, err_code: int = 0) -> None:
        self.code = code
        self.description = description
        self.err_code = err_code
        super().__init__(description)


class _StoredMessage:
    def __init__(
        self,
        subject: str,
        payload: bytes,
        headers: dict[str, str] | None,
        sequence: int,
    ) -> None:
        self.subject = subject
        self.payload = payload
        self.headers = headers
        self.sequence = sequence
        self.acked = False


class _FakeStream:
    def __init__(self, name: str, subjects: list[str]) -> None:
        self.name = name
        self.subjects = subjects
        self.messages: list[_StoredMessage] = []
        self.msg_ids: dict[str, int] = {}
        self.consumers: dict[str, _FakeConsumer] = {}
        self.next_sequence = 1


class _FakeConsumer:
    def __init__(
        self, durable: str, filter_subject: str, ack_policy: str = "explicit"
    ) -> None:
        self.durable = durable
        self.filter_subject = filter_subject
        self.ack_policy = ack_policy
        self.delivered = 0
        # Acknowledged sequences are a per-consumer cursor: one consumer
        # advancing never hides messages from another consumer of the
        # same stream, exactly as the server treats independent cursors.
        self.acked_sequences: set[int] = set()
        # Every generated ack subject keeps its message; a NAK leaves the
        # sequence unacked so a later pull redelivers it, exactly as a real
        # server treats a negative acknowledgment.
        self.delivered_acks: dict[str, _StoredMessage] = {}

    def next_message(self, stream: _FakeStream) -> _StoredMessage | None:
        for message in stream.messages:
            if message.sequence in self.acked_sequences:
                continue
            if not _js_subject_matches(self.filter_subject, message.subject):
                continue
            return message
        return None


def _js_subject_matches(pattern: str, subject: str) -> bool:
    """Subject matching with ``*`` tokens and a trailing ``>`` tail."""

    pattern_tokens = pattern.split(".")
    if pattern_tokens[-1] == ">":
        head = pattern_tokens[:-1]
        subject_tokens = subject.split(".")
        if len(subject_tokens) <= len(head):
            return False
        return _subject_matches(".".join(head), ".".join(subject_tokens[: len(head)]))
    return _subject_matches(pattern, subject)


class _JetStreamSession(_FakeNatsSession):
    async def _handle(self, line: str) -> bool:
        operator = line.split(" ", 1)[0]
        if operator == "HPUB":
            parts = line.split()
            subject = parts[1]
            reply = parts[2] if len(parts) == 5 else None
            header_size = int(parts[-2])
            total = int(parts[-1])
            blob = await self._read_exact(total)
            headers = parse_headers(blob[:header_size]) if header_size else None
            server = cast(FakeJetStreamServer, self._server)
            server.route(subject, reply, blob[header_size:], headers)
            return True
        return await super()._handle(line)

    def deliver_with_headers(
        self,
        subject: str,
        sid: int,
        reply: str | None,
        payload: bytes,
        header_block: bytes,
    ) -> None:
        reply_part = f" {reply}" if reply else ""
        frame = (
            f"HMSG {subject} {sid}{reply_part} "
            f"{len(header_block)} {len(header_block) + len(payload)}\r\n"
        ).encode("ascii") + header_block + payload + b"\r\n"
        try:
            self._writer.write(frame)
        except (ConnectionError, OSError):
            return


class FakeJetStreamServer(FakeNatsServer):
    """Core NATS fake plus JetStream storage, consumers, and acks."""

    def __init__(self) -> None:
        super().__init__()
        self.streams: dict[str, _FakeStream] = {}
        self.acks: list[tuple[str, bytes]] = []
        self._next_ephemeral = 1
        self._background: set[asyncio.Task[None]] = set()

    async def _on_connect(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session = _JetStreamSession(self, reader, writer)
        await session.serve()

    async def stop(self) -> None:
        for task in list(self._background):
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        await super().stop()

    def route(
        self,
        subject: str,
        reply: str | None,
        payload: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        if subject.startswith("$JS.API."):
            self._spawn(self._handle_api(subject, reply, payload))
            return
        if subject.startswith("$JS.ACK."):
            self.acks.append((subject, payload))
            self._apply_ack(subject, payload)
            return
        stream = self._capturing(subject)
        if stream is not None:
            self._store(stream, subject, reply, payload, headers)
            return
        if reply is not None and not subject.startswith("$JS"):
            # A request-style publish no stream captures gets the real
            # server's "no responders" status reply, not silence.
            self._send_status(subject, reply, "503", "no responders")
            return
        super().route(subject, reply, payload)

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coroutine)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    # -- stream storage -------------------------------------------------

    def _capturing(self, subject: str) -> _FakeStream | None:
        for stream in self.streams.values():
            if any(_js_subject_matches(pattern, subject) for pattern in stream.subjects):
                return stream
        return None

    def _store(
        self,
        stream: _FakeStream,
        subject: str,
        reply: str | None,
        payload: bytes,
        headers: dict[str, str] | None,
    ) -> None:
        msg_id = (headers or {}).get("Nats-Msg-Id")
        if msg_id is not None and msg_id in stream.msg_ids:
            sequence = stream.msg_ids[msg_id]
        else:
            sequence = stream.next_sequence
            stream.next_sequence += 1
            message = _StoredMessage(subject, payload, headers, sequence)
            stream.messages.append(message)
            if msg_id is not None:
                stream.msg_ids[msg_id] = sequence
        if reply is None:
            return
        document: dict[str, object] = {
            "type": "io.nats.jetstream.api.v1.pub_ack_response",
            "stream": stream.name,
            "seq": sequence,
        }
        self._reply_on(
            subject, reply, json.dumps(document).encode("utf-8"), None, None
        )

    def _apply_ack(self, ack_subject: str, payload: bytes) -> None:
        tokens = ack_subject.split(".")
        if len(tokens) < 4:
            return
        stream = self.streams.get(tokens[2])
        consumer = stream.consumers.get(tokens[3]) if stream is not None else None
        if consumer is None:
            return
        message = consumer.delivered_acks.get(ack_subject)
        if message is None or message.sequence in consumer.acked_sequences:
            return
        if payload in (b"+ACK", b"", b"+TERM"):
            consumer.acked_sequences.add(message.sequence)
            message.acked = True
        # -NAK and anything else leave the cursor back for redelivery.

    # -- JetStream API --------------------------------------------------

    async def _handle_api(self, subject: str, reply: str | None, payload: bytes) -> None:
        tokens = subject.split(".")
        try:
            if tokens[2] == "STREAM" and tokens[3] == "CREATE":
                document = self._stream_create(tokens[4], payload)
            elif tokens[2] == "STREAM" and tokens[3] == "INFO":
                document = self._stream_info(tokens[4])
            elif (
                tokens[2] == "CONSUMER"
                and tokens[3] == "CREATE"
                and len(tokens) == 5
            ):
                document = self._ephemeral_consumer_create(tokens[4], payload)
            elif tokens[2] == "CONSUMER" and tokens[3] == "CREATE":
                document = self._consumer_create(tokens[4], tokens[5], payload)
            elif tokens[2] == "CONSUMER" and tokens[3] == "INFO":
                document = self._consumer_info(tokens[4], tokens[5])
            elif tokens[2] == "CONSUMER" and tokens[3] == "DELETE":
                document = self._consumer_delete(tokens[4], tokens[5])
            elif tokens[2] == "CONSUMER" and tokens[3] == "MSG" and tokens[4] == "NEXT":
                await self._pull(subject, tokens[5], tokens[6], reply, payload)
                return
            else:
                raise _ApiError(400, "unknown JetStream API endpoint")
        except _ApiError as failure:
            document = {
                "error": {
                    "code": failure.code,
                    "err_code": failure.err_code,
                    "description": failure.description,
                }
            }
        if reply is not None:
            self._reply_on(
                subject, reply, json.dumps(document).encode("utf-8"), None, None
            )

    def _stream_create(self, name: str, payload: bytes) -> dict[str, object]:
        request = json.loads(payload.decode("utf-8")) if payload else {}
        subjects = [str(item) for item in request.get("subjects", [])]
        existing = self.streams.get(name)
        if existing is not None:
            raise _ApiError(400, "stream name already in use", 10058)
        stream = _FakeStream(name, subjects)
        self.streams[name] = stream
        return {
            "type": "io.nats.jetstream.api.v1.stream_create_response",
            "config": {"name": name, "subjects": subjects},
        }

    def _stream_info(self, name: str) -> dict[str, object]:
        stream = self.streams.get(name)
        if stream is None:
            raise _ApiError(404, "stream not found", 10059)
        return {
            "type": "io.nats.jetstream.api.v1.stream_info_response",
            "config": {"name": stream.name, "subjects": stream.subjects},
        }

    def _consumer_create(
        self, stream_name: str, durable: str, payload: bytes
    ) -> dict[str, object]:
        stream = self.streams.get(stream_name)
        if stream is None:
            raise _ApiError(404, "stream not found", 10059)
        request = json.loads(payload.decode("utf-8")) if payload else {}
        config = request.get("config")
        if not isinstance(config, dict):
            raise _ApiError(400, "consumer create requires a config object")
        filter_subject = str(config.get("filter_subject", ""))
        ack_policy = str(config.get("ack_policy", ""))
        existing = stream.consumers.get(durable)
        if existing is not None:
            if existing.filter_subject != filter_subject or ack_policy != "explicit":
                raise _ApiError(400, "consumer already exists", 10148)
        elif ack_policy != "explicit":
            raise _ApiError(400, "only explicit ack consumers are supported")
        else:
            stream.consumers[durable] = _FakeConsumer(durable, filter_subject)
        # Like server 2.11, the durable is reported as "name".
        return {
            "type": "io.nats.jetstream.api.v1.consumer_create_response",
            "config": {
                "name": durable,
                "filter_subject": filter_subject,
                "ack_policy": ack_policy,
            },
        }

    def _ephemeral_consumer_create(
        self, stream_name: str, payload: bytes
    ) -> dict[str, object]:
        stream = self.streams.get(stream_name)
        if stream is None:
            raise _ApiError(404, "stream not found", 10059)
        request = json.loads(payload.decode("utf-8")) if payload else {}
        config = request.get("config")
        if not isinstance(config, dict):
            raise _ApiError(400, "ephemeral consumer create requires a config object")
        if str(config.get("ack_policy", "")) != "none":
            raise _ApiError(400, "ephemeral replay consumers must not track acks")
        name = f"EPHEMERAL{self._next_ephemeral}"
        self._next_ephemeral += 1
        stream.consumers[name] = _FakeConsumer(
            name, str(config.get("filter_subject", "")), ack_policy="none"
        )
        return {
            "type": "io.nats.jetstream.api.v1.consumer_create_response",
            "name": name,
            "config": {"name": name, "ack_policy": "none"},
        }

    def _consumer_delete(self, stream_name: str, name: str) -> dict[str, object]:
        stream = self.streams.get(stream_name)
        if stream is None or stream.consumers.pop(name, None) is None:
            raise _ApiError(404, "consumer not found", 10114)
        return {
            "type": "io.nats.jetstream.api.v1.consumer_delete_response",
            "success": True,
        }

    def _consumer_info(self, stream_name: str, durable: str) -> dict[str, object]:
        stream = self.streams.get(stream_name)
        if stream is None:
            raise _ApiError(404, "stream not found", 10059)
        consumer = stream.consumers.get(durable)
        if consumer is None:
            raise _ApiError(404, "consumer not found", 10114)
        pending = sum(
            1
            for message in stream.messages
            if message.sequence not in consumer.acked_sequences
            and _js_subject_matches(consumer.filter_subject, message.subject)
        )
        return {
            "type": "io.nats.jetstream.api.v1.consumer_info_response",
            "num_pending": pending,
            "config": {
                "name": consumer.durable,
                "filter_subject": consumer.filter_subject,
                "ack_policy": consumer.ack_policy,
            },
        }

    async def _pull(
        self,
        subject: str,
        stream_name: str,
        durable: str,
        reply: str | None,
        payload: bytes,
    ) -> None:
        if reply is None:
            return
        stream = self.streams.get(stream_name)
        if stream is None:
            self._send_status(subject, reply, "503", "Stream not found")
            return
        consumer = stream.consumers.get(durable)
        if consumer is None:
            self._send_status(subject, reply, "503", "Consumer not found")
            return
        request = json.loads(payload.decode("utf-8")) if payload else {}
        expires = int(request.get("expires", 0) or 0) / 1_000_000_000
        message = consumer.next_message(stream)
        if message is None:
            if expires > 0:
                self._spawn(self._expire_pull(subject, reply, expires))
            else:
                self._send_status(subject, reply, "204", "No Messages")
            return
        consumer.delivered += 1
        ack_subject = (
            f"$JS.ACK.{stream_name}.{durable}.{consumer.delivered}"
            f".{message.sequence}.{message.sequence}.0.1"
        )
        consumer.delivered_acks[ack_subject] = message
        if consumer.ack_policy == "none":
            # A no-ack consumer treats every delivery as acknowledged at
            # once: its own cursor advances whether or not the client
            # ever acknowledges, and other consumers never notice.
            consumer.acked_sequences.add(message.sequence)
        self._reply_on(message.subject, reply, message.payload, ack_subject, None)

    async def _expire_pull(
        self, subject: str, reply: str, expires: float
    ) -> None:
        await asyncio.sleep(expires)
        self._send_status(subject, reply, "408", "Request Timeout")

    # -- reply routing --------------------------------------------------

    def _reply_on(
        self,
        subject: str,
        inbox: str,
        payload: bytes,
        reply: str | None,
        headers: dict[str, str] | None,
    ) -> None:
        """Deliver to the inbox's subscriber, keeping the original subject.

        A NATS ``MSG`` frame carries the subject a message was published to;
        the subscription ID selects the receiver. Replies therefore frame
        with the request subject, not the inbox.
        """

        for session in list(self.sessions):
            if not isinstance(session, _JetStreamSession):
                continue
            for sid, pattern in session.subscriptions.items():
                if _subject_matches(pattern, inbox):
                    if headers is None:
                        session.deliver(subject, sid, reply, payload)
                    else:
                        session.deliver_with_headers(
                            subject, sid, reply, payload, _header_block(headers)
                        )

    def _send_status(
        self, subject: str, reply: str, status: str, description: str
    ) -> None:
        self._reply_on(
            subject,
            reply,
            b"",
            None,
            {"Status-Line": f"{status} {description}"},
        )


def _header_block(headers: dict[str, str]) -> bytes:
    status_line = headers.get("Status-Line")
    if status_line is not None:
        return f"NATS/1.0 {status_line}\r\n\r\n".encode("ascii")
    lines = ["NATS/1.0"]
    lines.extend(f"{key}: {value}" for key, value in headers.items())
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")


def spawn_payload(
    agent_session_id: str = _AGENT_USER,
    task_id: str = "task-1",
    request_id: str = "request-1",
    sandbox_type: str = "python",
) -> bytes:
    document = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "task_id": task_id,
        "agent_session_id": agent_session_id,
        "sandbox_type": sandbox_type,
    }
    return json.dumps(document).encode("utf-8")


def profile() -> SandboxProfile:
    return SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )


def ready_task(
    task_id: str = "task-9", agent_session_id: str = _AGENT_USER
) -> ReadyTask:
    request = SpawnRequest(
        schema_version=SCHEMA_VERSION,
        request_id=f"request-{task_id}",
        task_id=task_id,
        agent_session_id=agent_session_id,
        sandbox_type="python",
    )
    return ReadyTask(
        provisioning_task=ProvisioningTask(QueuedTask.from_request(request), profile()),
        resources=ProvisionedSandbox(
            f"pods/{task_id}",
            f"uid-{task_id}",
            f"pvc/{task_id}-results",
            f"uid-{task_id}-results",
        ),
        relay=RelayReference(
            RELAY_SCHEMA_VERSION, admission_subject(agent_session_id)
        ),
    )


class IntakeRepository:
    """``TaskRepository`` double with injection points."""

    def __init__(self) -> None:
        self.tasks: dict[str, QueuedTask] = {}
        self.failures_remaining = 0
        self.calls = 0

    async def accept_spawn(self, task: QueuedTask) -> StoreResult:
        self.calls += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("simulated durable-store outage")
        existing = self.tasks.get(task.request.task_id)
        if existing is not None:
            if existing.request_fingerprint != task.request_fingerprint:
                return StoreResult.CONFLICT
            return StoreResult.DUPLICATE
        self.tasks[task.request.task_id] = task
        return StoreResult.CREATED


class RecordingObserver:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def poisoned(self, subject: str, reason: str) -> None:
        self.events.append(("poisoned", reason))

    def deferred(self, subject: str, reason: str) -> None:
        self.events.append(("deferred", reason))


class AckFaultConnection(AsyncNatsConnection):
    def __init__(self, failures: Mapping[bytes, int]) -> None:
        super().__init__("127.0.0.1", 4222, _MANAGER_USER, "unused", "ack-fault")
        self.failures = dict(failures)
        self.published: list[tuple[str, bytes]] = []

    async def publish(
        self,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.published.append((subject, payload))
        remaining = self.failures.get(payload, 0)
        if remaining > 0:
            self.failures[payload] = remaining - 1
            raise NatsTransportError("injected acknowledgment failure")


class ScriptedIntakeJetStream(JetStream):
    def __init__(
        self, connection: AsyncNatsConnection, deliveries: list[NatsMessage]
    ) -> None:
        super().__init__(connection)
        self.deliveries = deliveries
        self.empty = asyncio.Event()

    async def ensure_stream(
        self, name: str, subjects: tuple[str, ...]
    ) -> dict[str, object]:
        return {}

    async def ensure_consumer(
        self,
        stream: str,
        durable: str,
        filter_subject: str,
        *,
        ack_wait_seconds: float,
        max_deliver: int,
        max_ack_pending: int = 1,
    ) -> dict[str, object]:
        return {}

    async def fetch(
        self, stream: str, durable: str, *, wait_seconds: float
    ) -> NatsMessage | None:
        if self.deliveries:
            return self.deliveries.pop(0)
        await self.empty.wait()
        return None


class TransientEmptyJetStream(JetStream):
    def __init__(self, connection: AsyncNatsConnection) -> None:
        super().__init__(connection)
        self.fetch_calls = 0

    async def fetch(
        self, stream: str, durable: str, *, wait_seconds: float
    ) -> NatsMessage | None:
        self.fetch_calls += 1
        if self.fetch_calls == 1:
            return None
        return await super().fetch(stream, durable, wait_seconds=wait_seconds)


class LifecycleEnvironment:
    """One fake JetStream server and one authenticated manager connection."""

    def __init__(self) -> None:
        self.server = FakeJetStreamServer()
        self.connection: AsyncNatsConnection | None = None
        self.jetstream: JetStream | None = None
        self.agent: AsyncNatsConnection | None = None

    async def start(self) -> None:
        await self.server.start()
        self.connection = AsyncNatsConnection(
            "127.0.0.1", self.server.port, _MANAGER_USER, "unused", "manager"
        )
        await self.connection.connect()
        self.jetstream = JetStream(self.connection)
        self.agent = AsyncNatsConnection(
            "127.0.0.1", self.server.port, _AGENT_USER, "unused", "agent"
        )
        await self.agent.connect()

    async def stop(self) -> None:
        for connection in (self.agent, self.connection):
            if connection is not None:
                await connection.close()
        await self.server.stop()


async def _until(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not reached in time")
        await asyncio.sleep(0.01)


class SubjectLayoutTests(unittest.TestCase):
    def test_subjects_are_deterministic_and_token_safe(self) -> None:
        self.assertEqual(spawn_subject("agent-1"), "sandbox.agent.agent-1.spawn")
        self.assertEqual(
            ready_subject("agent-1", "task-1"),
            "sandbox.manager.agent-1.task.task-1.ready",
        )

    def test_subject_tokens_are_validated(self) -> None:
        with self.assertRaises(NatsLifecycleError):
            spawn_subject("not a token")
        with self.assertRaises(NatsLifecycleError):
            ready_subject("agent-1", "bad/task")

    def test_principals_are_recovered_from_both_subjects(self) -> None:
        self.assertEqual(
            principal_from_spawn_subject("sandbox.agent.agent-1.spawn"), "agent-1"
        )
        self.assertEqual(
            principal_from_ready_subject("sandbox.manager.agent-1.task.t.ready"),
            "agent-1",
        )
        with self.assertRaises(NatsLifecycleError):
            principal_from_ready_subject("sandbox.manager.agent-1.task.bad/value.ready")

    def test_foreign_subjects_are_rejected(self) -> None:
        for subject in (
            "sandbox.agent.agent-1",
            "sandbox.agent.agent-1.admit",
            "sandbox.manager.agent-1.ready",
        ):
            with self.subTest(subject=subject):
                with self.assertRaises(NatsLifecycleError):
                    principal_from_spawn_subject(subject)
                with self.assertRaises(NatsLifecycleError):
                    principal_from_ready_subject(subject)


class ReadyResponseTests(unittest.TestCase):
    def test_round_trip_preserves_identifiers_and_routing(self) -> None:
        task = ready_task()
        decoded = decode_ready_response(encode_ready_response(task))
        self.assertEqual(decoded.schema_version, READY_SCHEMA_VERSION)
        self.assertEqual(decoded.task_id, "task-9")
        self.assertEqual(decoded.request_id, "request-task-9")
        self.assertEqual(decoded.agent_session_id, _AGENT_USER)
        self.assertEqual(decoded.sandbox_type, "python")
        self.assertEqual(decoded.relay_protocol_version, RELAY_SCHEMA_VERSION)
        self.assertEqual(decoded.relay_route, admission_subject(_AGENT_USER))
        self.assertEqual(decoded.sandbox_reference, "pods/task-9")
        self.assertEqual(decoded.sandbox_uid, "uid-task-9")
        self.assertEqual(decoded.result_volume_reference, "pvc/task-9-results")
        self.assertEqual(decoded.result_volume_uid, "uid-task-9-results")

    def test_document_is_payload_free(self) -> None:
        document = json.loads(encode_ready_response(ready_task()))
        self.assertEqual(
            set(document),
            {
                "schema_version",
                "task_id",
                "request_id",
                "agent_session_id",
                "sandbox_type",
                "relay_protocol_version",
                "relay_route",
                "sandbox_reference",
                "sandbox_uid",
                "result_volume_reference",
                "result_volume_uid",
            },
        )

    def test_rejections_fail_closed(self) -> None:
        valid = json.loads(encode_ready_response(ready_task()))
        cases: list[tuple[str, object]] = [
            ("not json", b"\x00"),
            ("array", b"[]"),
            ("boolean version", _mutate(valid, schema_version=True)),
            ("unknown version", _mutate(valid, schema_version=99)),
            ("missing field", _drop(valid, "task_id")),
            ("extra field", _mutate(valid, extra="x")),
            ("empty text", _mutate(valid, task_id="")),
            ("unsafe identifier", _mutate(valid, task_id="task.bad")),
            ("non-text", _mutate(valid, sandbox_type=7)),
            ("non-integer version", _mutate(valid, relay_protocol_version="1")),
            ("boolean relay version", _mutate(valid, relay_protocol_version=True)),
            ("unknown relay version", _mutate(valid, relay_protocol_version=99)),
            (
                "owner route mismatch",
                _mutate(valid, relay_route=admission_subject("different-agent")),
            ),
            (
                "duplicate field",
                encode_ready_response(ready_task()).replace(
                    b'"schema_version":1',
                    b'"schema_version":1,"schema_version":1',
                ),
            ),
        ]
        for name, payload in cases:
            with self.subTest(case=name):
                encoded = (
                    payload
                    if isinstance(payload, bytes)
                    else json.dumps(payload).encode("utf-8")
                )
                with self.assertRaises(NatsLifecycleError):
                    decode_ready_response(encoded)


def _mutate(document: dict[str, object], **changes: object) -> dict[str, object]:
    updated = dict(document)
    for key, value in changes.items():
        updated[key] = value
    return updated


def _drop(document: dict[str, object], key: str) -> dict[str, object]:
    updated = dict(document)
    del updated[key]
    return updated


class JetStreamTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_stream_is_idempotent_and_detects_conflicts(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            jetstream = environment.jetstream
            assert jetstream is not None
            await jetstream.ensure_stream("SANDBOX_SPAWN", ("sandbox.agent.*.spawn",))
            await jetstream.ensure_stream("SANDBOX_SPAWN", ("sandbox.agent.*.spawn",))
            await jetstream.ensure_stream(
                "SANDBOX_OTHER", ("other.subject",)
            )
            with self.assertRaises(JetStreamError):
                await jetstream.ensure_stream(
                    "SANDBOX_SPAWN", ("sandbox.agent.*.spawn", "more.subject")
                )
        finally:
            await environment.stop()

    async def test_ensure_consumer_verifies_compatibility(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            jetstream = environment.jetstream
            assert jetstream is not None
            await jetstream.ensure_stream("SANDBOX_SPAWN", ("sandbox.agent.*.spawn",))
            await jetstream.ensure_consumer(
                "SANDBOX_SPAWN",
                "intake",
                "sandbox.agent.*.spawn",
                ack_wait_seconds=30,
                max_deliver=5,
            )
            await jetstream.ensure_consumer(
                "SANDBOX_SPAWN",
                "intake",
                "sandbox.agent.*.spawn",
                ack_wait_seconds=60,
                max_deliver=9,
            )
            with self.assertRaises(JetStreamError):
                await jetstream.ensure_consumer(
                    "SANDBOX_SPAWN",
                    "intake",
                    "sandbox.agent.other.spawn",
                    ack_wait_seconds=30,
                    max_deliver=5,
                )
            with self.assertRaises(JetStreamApiError):
                await jetstream.ensure_consumer(
                    "NOSUCH_STREAM",
                    "intake",
                    "sandbox.agent.*.spawn",
                    ack_wait_seconds=30,
                    max_deliver=5,
                )
        finally:
            await environment.stop()

    async def test_publish_acks_and_deduplicates_by_message_id(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            jetstream = environment.jetstream
            agent = environment.agent
            assert jetstream is not None and agent is not None
            await jetstream.ensure_stream("SANDBOX_SPAWN", ("sandbox.agent.*.spawn",))
            first = await jetstream.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(),
                msg_id="request-1",
            )
            self.assertEqual(first.stream, "SANDBOX_SPAWN")
            self.assertEqual(first.sequence, 1)
            duplicate = await jetstream.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(),
                msg_id="request-1",
            )
            self.assertEqual(duplicate.sequence, first.sequence)
            second = await jetstream.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(task_id="task-2", request_id="request-2"),
                msg_id="request-2",
            )
            self.assertEqual(second.sequence, 2)
            with self.assertRaises(JetStreamError):
                await jetstream.publish("uncaptured.subject", b"x")
        finally:
            await environment.stop()

    async def test_fetch_delivers_and_reports_empty_windows(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            jetstream = environment.jetstream
            agent = environment.agent
            assert jetstream is not None and agent is not None
            await jetstream.ensure_stream("SANDBOX_SPAWN", ("sandbox.agent.*.spawn",))
            await jetstream.ensure_consumer(
                "SANDBOX_SPAWN",
                "intake",
                "sandbox.agent.*.spawn",
                ack_wait_seconds=30,
                max_deliver=5,
            )
            empty = await jetstream.fetch(
                "SANDBOX_SPAWN", "intake", wait_seconds=0.05
            )
            self.assertIsNone(empty)
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            message = await jetstream.fetch(
                "SANDBOX_SPAWN", "intake", wait_seconds=0.05
            )
            self.assertIsNotNone(message)
            assert message is not None
            self.assertEqual(message.subject, spawn_subject(_AGENT_USER))
            self.assertTrue(message.reply and message.reply.startswith("$JS.ACK."))
            connection = environment.connection
            assert connection is not None
            delivered = JetStreamMessage(message, connection)
            await delivered.acknowledge()
            await _until(
                lambda: environment.server.streams["SANDBOX_SPAWN"]
                .messages[0]
                .acked
            )
        finally:
            await environment.stop()

    async def test_ack_operations_use_the_wire_protocol(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            jetstream = environment.jetstream
            agent = environment.agent
            assert jetstream is not None and agent is not None
            await jetstream.ensure_stream("SANDBOX_SPAWN", ("sandbox.agent.*.spawn",))
            await jetstream.ensure_consumer(
                "SANDBOX_SPAWN",
                "intake",
                "sandbox.agent.*.spawn",
                ack_wait_seconds=30,
                max_deliver=5,
            )
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            message = await jetstream.fetch(
                "SANDBOX_SPAWN", "intake", wait_seconds=0.05
            )
            assert message is not None
            connection = environment.connection
            assert connection is not None
            delivered = JetStreamMessage(message, connection)
            await delivered.nak(delay_seconds=0.25)
            await delivered.terminate()
            await _until(lambda: len(environment.server.acks) >= 2)
            self.assertEqual(
                environment.server.acks[-2:],
                [
                    (
                        message.reply,
                        b'-NAK {"delay": 250000000}',
                    ),
                    (message.reply, b"+TERM"),
                ],
            )
            redelivered = await jetstream.fetch(
                "SANDBOX_SPAWN", "intake", wait_seconds=0.05
            )
            self.assertIsNone(redelivered)
        finally:
            await environment.stop()


class SpawnIntakeTests(unittest.IsolatedAsyncioTestCase):
    def _intake(
        self,
        environment: LifecycleEnvironment,
        repository: IntakeRepository,
        observer: RecordingObserver,
    ) -> JetStreamSpawnIntake:
        assert environment.jetstream is not None
        return JetStreamSpawnIntake(
            environment.jetstream,
            repository,
            pull_wait_seconds=0.05,
            redelivery_delay_seconds=0.01,
            retry_backoff_seconds=0.02,
            observer=observer,
        )

    async def test_valid_spawn_is_durable_and_acked(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = self._intake(environment, repository, observer)
        try:
            await intake.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            await _until(lambda: "task-1" in repository.tasks)
            self.assertEqual(observer.events, [])
            stream = environment.server.streams["SANDBOX_SPAWN"]
            await _until(lambda: stream.messages[0].acked)
            self.assertEqual(repository.tasks["task-1"].request.task_id, "task-1")
        finally:
            await intake.stop()
            await environment.stop()

    async def test_duplicate_start_is_rejected_without_leaking_a_worker(self) -> None:
        connection = AckFaultConnection({})
        jetstream = ScriptedIntakeJetStream(connection, [])
        intake = JetStreamSpawnIntake(jetstream, IntakeRepository())
        await intake.start()
        first_worker = intake._task
        try:
            with self.assertRaisesRegex(NatsLifecycleError, "already started"):
                await intake.start()
            self.assertIs(intake._task, first_worker)
        finally:
            await intake.stop()

        assert first_worker is not None
        self.assertTrue(first_worker.done())

    async def test_principal_mismatch_is_terminated_not_redelivered(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = self._intake(environment, repository, observer)
        try:
            await intake.start()
            agent = environment.agent
            assert agent is not None
            # The payload claims a different principal than the subject.
            await agent.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(agent_session_id="someone-else"),
            )
            await _until(
                lambda: ("poisoned", "principal_mismatch") in observer.events
            )
            self.assertEqual(repository.tasks, {})
            await _until(
                lambda: environment.server.streams["SANDBOX_SPAWN"]
                .messages[0]
                .acked
            )
        finally:
            await intake.stop()
            await environment.stop()

    async def test_undecodable_payload_is_terminated(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = self._intake(environment, repository, observer)
        try:
            await intake.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), b"\x00not-json")
            await _until(lambda: ("poisoned", "invalid_request") in observer.events)
            self.assertEqual(repository.tasks, {})
        finally:
            await intake.stop()
            await environment.stop()

    async def test_identifier_conflict_is_terminated(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = self._intake(environment, repository, observer)
        try:
            await intake.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            await _until(lambda: "task-1" in repository.tasks)
            # Same task_id with a different request fingerprint conflicts.
            await agent.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(request_id="request-conflicting"),
            )
            await _until(
                lambda: ("poisoned", "identifier_conflict") in observer.events
            )
        finally:
            await intake.stop()
            await environment.stop()

    async def test_duplicate_publication_acks_without_conflict(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = self._intake(environment, repository, observer)
        try:
            await intake.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            await _until(lambda: repository.calls >= 2)
            stream = environment.server.streams["SANDBOX_SPAWN"]
            await _until(
                lambda: len(stream.messages) == 2
                and all(message.acked for message in stream.messages)
            )
            self.assertEqual(observer.events, [])
            self.assertEqual(len(repository.tasks), 1)
        finally:
            await intake.stop()
            await environment.stop()

    async def test_transient_failure_is_nak_and_redelivered(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        repository = IntakeRepository()
        repository.failures_remaining = 1
        observer = RecordingObserver()
        intake = self._intake(environment, repository, observer)
        try:
            await intake.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            await _until(lambda: "task-1" in repository.tasks)
            self.assertEqual(repository.calls, 2)
            self.assertEqual(
                observer.events, [("deferred", "RuntimeError")]
            )
            stream = environment.server.streams["SANDBOX_SPAWN"]
            await _until(lambda: stream.messages[0].acked)
        finally:
            await intake.stop()
            await environment.stop()

    async def test_failed_term_is_deferred_and_worker_continues(self) -> None:
        connection = AckFaultConnection({b"+TERM": 1})
        subject = spawn_subject(_AGENT_USER)
        jetstream = ScriptedIntakeJetStream(
            connection,
            [
                NatsMessage(subject, "$JS.ACK.S.C.1", b"not-json"),
                NatsMessage(subject, "$JS.ACK.S.C.2", b"not-json"),
                NatsMessage(
                    subject,
                    "$JS.ACK.S.C.3",
                    spawn_payload(task_id="task-2", request_id="request-2"),
                ),
            ],
        )
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = JetStreamSpawnIntake(
            jetstream,
            repository,
            redelivery_delay_seconds=0.01,
            observer=observer,
        )
        try:
            await intake.start()
            await _until(lambda: "task-2" in repository.tasks)

            self.assertEqual(
                observer.events,
                [
                    ("deferred", "invalid_request:term_lost"),
                    ("poisoned", "invalid_request"),
                ],
            )
        finally:
            await intake.stop()

    async def test_failed_ack_is_redelivered_and_worker_continues(self) -> None:
        connection = AckFaultConnection({b"+ACK": 1})
        subject = spawn_subject(_AGENT_USER)
        first = spawn_payload()
        jetstream = ScriptedIntakeJetStream(
            connection,
            [
                NatsMessage(subject, "$JS.ACK.S.C.1", first),
                NatsMessage(subject, "$JS.ACK.S.C.2", first),
                NatsMessage(
                    subject,
                    "$JS.ACK.S.C.3",
                    spawn_payload(task_id="task-2", request_id="request-2"),
                ),
            ],
        )
        repository = IntakeRepository()
        observer = RecordingObserver()
        intake = JetStreamSpawnIntake(
            jetstream,
            repository,
            redelivery_delay_seconds=0.01,
            observer=observer,
        )
        try:
            await intake.start()
            await _until(lambda: "task-2" in repository.tasks)

            self.assertEqual(repository.calls, 3)
            self.assertEqual(
                observer.events, [("deferred", "NatsTransportError")]
            )
        finally:
            await intake.stop()


class ReadyPublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_response_lands_on_the_deterministic_subject(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        publisher = None
        try:
            assert environment.jetstream is not None
            publisher = JetStreamReadyPublisher(environment.jetstream)
            await publisher.start()
            task = ready_task()
            await publisher.publish_ready(task)
            stream = environment.server.streams["SANDBOX_READY"]
            self.assertEqual(len(stream.messages), 1)
            message = stream.messages[0]
            self.assertEqual(
                message.subject, ready_subject(_AGENT_USER, "task-9")
            )
            self.assertEqual(
                message.headers, {"Nats-Msg-Id": "ready-task-9"}
            )
            decoded = decode_ready_response(message.payload)
            self.assertEqual(decoded.task_id, "task-9")
            self.assertEqual(decoded.relay_route, admission_subject(_AGENT_USER))
        finally:
            await environment.stop()

    async def test_republication_deduplicates_by_task(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            publisher = JetStreamReadyPublisher(environment.jetstream)
            await publisher.start()
            task = ready_task()
            await publisher.publish_ready(task)
            await publisher.publish_ready(task)
            stream = environment.server.streams["SANDBOX_READY"]
            self.assertEqual(len(stream.messages), 1)
        finally:
            await environment.stop()


class CommandEventCodecTests(unittest.TestCase):
    def test_events_round_trip_per_kind(self) -> None:
        samples = (
            CommandEvent(
                kind=CommandEventKind.SEED_READY,
                task_id="task-1",
                agent_session_id=_AGENT_USER,
                profile="default",
                fence_generation="fence-a",
            ),
            CommandEvent(
                kind=CommandEventKind.ADMITTED,
                task_id="task-1",
                command_id="command-1",
                authenticated_principal=_AGENT_USER,
                fence_generation="fence-a",
                admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
            ),
            CommandEvent(
                kind=CommandEventKind.FINISHED,
                task_id="task-1",
                command_id="command-1",
                outcome=CommandOutcome.SUCCESS,
                completed_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
            ),
            CommandEvent(
                kind=CommandEventKind.TERMINATION_BEGUN, task_id="task-1"
            ),
            CommandEvent(
                kind=CommandEventKind.FENCE_ADOPTED, fence_generation="fence-b"
            ),
        )

        for event in samples:
            decoded = decode_command_event(encode_command_event(event))
            self.assertEqual(decoded, event)

    def test_wire_documents_carry_identifiers_only(self) -> None:
        allowed = {
            "schema_version",
            "kind",
            "task_id",
            "agent_session_id",
            "profile",
            "fence_generation",
            "command_id",
            "authenticated_principal",
            "outcome",
            "admitted_at",
            "completed_at",
        }

        event = CommandEvent(
            kind=CommandEventKind.ADMITTED,
            task_id="task-1",
            command_id="command-1",
            authenticated_principal=_AGENT_USER,
            fence_generation="fence-a",
            admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
        )
        document = json.loads(encode_command_event(event).decode("utf-8"))

        self.assertEqual(
            document["schema_version"], COMMAND_EVENT_SCHEMA_VERSION
        )
        self.assertTrue(set(document) <= allowed)

    def test_decode_rejects_drift(self) -> None:
        good = {
            "schema_version": COMMAND_EVENT_SCHEMA_VERSION,
            "kind": "termination_begun",
            "task_id": "task-1",
        }

        def mutate(**changes: object) -> bytes:
            return json.dumps({**good, **changes}).encode("utf-8")

        def drop(field: str) -> bytes:
            return json.dumps(
                {key: value for key, value in good.items() if key != field}
            ).encode("utf-8")

        for payload in (
            b"not-json",
            b"[]",
            mutate(schema_version=True),
            mutate(schema_version=2),
            mutate(kind="demolished"),
            mutate(kind="admitted"),
            drop("task_id"),
            mutate(task_id=""),
            mutate(task_id="task.bad"),
            mutate(command_id="command-1"),
            b'{"schema_version":1,"schema_version":1,"kind":"termination_begun","task_id":"task-1"}',
        ):
            with self.subTest(payload=payload), self.assertRaises(
                NatsLifecycleError
            ):
                decode_command_event(payload)

    def test_decode_rejects_bad_timestamps_and_outcomes(self) -> None:
        base = {
            "schema_version": COMMAND_EVENT_SCHEMA_VERSION,
            "kind": "finished",
            "task_id": "task-1",
            "command_id": "command-1",
        }

        for extra, label in (
            ({"outcome": "SUCCESS", "completed_at": "2026-09-02T12:00:00"}, "naive"),
            ({"outcome": "SUCCESS", "completed_at": "yesterday"}, "not a date"),
            ({"outcome": "EXPLODED", "completed_at": "2026-09-02T12:00:00+00:00"}, "outcome"),
            ({"outcome": 7, "completed_at": "2026-09-02T12:00:00+00:00"}, "outcome type"),
        ):
            with self.subTest(label=label), self.assertRaises(
                NatsLifecycleError
            ):
                decode_command_event(json.dumps({**base, **extra}).encode("utf-8"))


class CommandEventJournalTests(unittest.IsolatedAsyncioTestCase):
    def _journal(self, jetstream: JetStream) -> JetStreamCommandEventJournal:
        return JetStreamCommandEventJournal(jetstream, pull_wait_seconds=0.05)

    async def test_replay_rebuilds_the_repository_and_its_fences(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            journal = self._journal(environment.jetstream)
            await journal.start()

            fence = AdmissionFence("fence-a")
            live = InMemoryCommandRepository(journal=journal)
            await live.seed_ready(
                TaskAdmissionState(
                    task_id="task-1",
                    agent_session_id=_AGENT_USER,
                    profile="default",
                    lifecycle=AdmissionLifecycle.READY,
                    fence_generation=fence.generation,
                )
            )
            decision = await live.admit(
                AdmissionRequest(
                    task_id="task-1",
                    command_id="command-1",
                    authenticated_principal=_AGENT_USER,
                    fence_generation=fence.generation,
                    admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
                )
            )
            assert decision.command is not None
            await live.finish(
                decision.command, CommandOutcome.SUCCESS, datetime(2026, 9, 2, 12, tzinfo=UTC)
            )
            await live.begin_termination("task-1")
            # task-2 crashes the "process" mid-command: in-flight, unfinished.
            await live.seed_ready(
                TaskAdmissionState(
                    task_id="task-2",
                    agent_session_id=_AGENT_USER,
                    profile="default",
                    lifecycle=AdmissionLifecycle.READY,
                    fence_generation=fence.generation,
                )
            )
            in_flight = await live.admit(
                AdmissionRequest(
                    task_id="task-2",
                    command_id="command-2",
                    authenticated_principal=_AGENT_USER,
                    fence_generation=fence.generation,
                    admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
                )
            )
            assert in_flight.command is not None

            replayed = InMemoryCommandRepository()
            count = await journal.replay_into(replayed)
            self.assertEqual(count, 6)

            terminated = await replayed.admission_state("task-1")
            assert terminated is not None
            self.assertEqual(terminated.lifecycle, AdmissionLifecycle.TERMINATING)
            self.assertIsNone(terminated.in_flight_command_id)
            self.assertEqual(
                await replayed.outcome("command-1"), CommandOutcome.SUCCESS
            )
            interrupted = await replayed.admission_state("task-2")
            assert interrupted is not None
            self.assertEqual(interrupted.lifecycle, AdmissionLifecycle.ACTIVE)
            self.assertEqual(interrupted.in_flight_command_id, "command-2")

            recovered = await journal.load_repository(AdmissionFence("fence-b"))
            after = await recovered.admission_state("task-2")
            assert after is not None
            self.assertEqual(after.fence_generation, "fence-b")
            self.assertIsNone(after.in_flight_command_id)
            self.assertEqual(
                await recovered.outcome("command-2"), CommandOutcome.UNKNOWN
            )
            refusal = await recovered.admit(
                AdmissionRequest(
                    task_id="task-1",
                    command_id="command-9",
                    authenticated_principal=_AGENT_USER,
                    fence_generation="fence-b",
                    admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
                )
            )
            self.assertEqual(refusal.refusal, AdmissionRefusal.NOT_ADMISSIBLE)
            stale = await recovered.admit(
                AdmissionRequest(
                    task_id="task-2",
                    command_id="command-9",
                    authenticated_principal=_AGENT_USER,
                    fence_generation="fence-a",
                    admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
                )
            )
            self.assertEqual(stale.refusal, AdmissionRefusal.STALE_FENCE)

            # load_repository re-journaled the new fence adoption.
            events = environment.server.streams[journal.stream].messages
            kinds = [
                decode_command_event(message.payload).kind
                for message in events
            ]
            self.assertEqual(kinds[-1], CommandEventKind.FENCE_ADOPTED)
            self.assertEqual(events[-1].subject, COMMAND_EVENT_SUBJECT)
        finally:
            await environment.stop()

    async def test_replay_fails_closed_on_an_undecodable_entry(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            journal = self._journal(environment.jetstream)
            await journal.start()
            await environment.jetstream.publish(
                COMMAND_EVENT_SUBJECT, b"definitely not a journal entry"
            )

            with self.assertRaises(JetStreamError):
                await journal.replay_into(InMemoryCommandRepository())
        finally:
            await environment.stop()

    async def test_duplicate_events_fold_idempotently(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            journal = self._journal(environment.jetstream)
            await journal.start()
            seed = CommandEvent(
                kind=CommandEventKind.SEED_READY,
                task_id="task-1",
                agent_session_id=_AGENT_USER,
                profile="default",
                fence_generation="fence-a",
            )
            admitted = CommandEvent(
                kind=CommandEventKind.ADMITTED,
                task_id="task-1",
                command_id="command-1",
                authenticated_principal=_AGENT_USER,
                fence_generation="fence-a",
                admitted_at=datetime(2026, 9, 2, 12, tzinfo=UTC),
            )
            # A lost publish acknowledgment can store one event twice.
            await journal.append(seed)
            await journal.append(seed)
            await journal.append(admitted)
            await journal.append(admitted)

            replayed = InMemoryCommandRepository()
            count = await journal.replay_into(replayed)

            self.assertEqual(count, 4)
            state = await replayed.admission_state("task-1")
            assert state is not None
            self.assertEqual(state.fence_generation, "fence-a")
            self.assertEqual(state.in_flight_command_id, "command-1")
        finally:
            await environment.stop()

    async def test_replay_of_an_empty_journal(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            journal = self._journal(environment.jetstream)
            await journal.start()

            replayed = InMemoryCommandRepository()
            self.assertEqual(await journal.replay_into(replayed), 0)
            self.assertIsNone(await replayed.admission_state("task-1"))
        finally:
            await environment.stop()

    async def test_replay_continues_after_transient_empty_pull(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            jetstream = TransientEmptyJetStream(environment.jetstream.connection)
            journal = self._journal(jetstream)
            await journal.start()
            await journal.append(
                CommandEvent(
                    kind=CommandEventKind.SEED_READY,
                    task_id="task-after-empty",
                    agent_session_id=_AGENT_USER,
                    profile="default",
                    fence_generation="fence-a",
                )
            )

            replayed = InMemoryCommandRepository()
            self.assertEqual(await journal.replay_into(replayed), 1)
            self.assertIsNotNone(
                await replayed.admission_state("task-after-empty")
            )
            self.assertGreaterEqual(jetstream.fetch_calls, 2)
        finally:
            await environment.stop()


if __name__ == "__main__":
    unittest.main()
