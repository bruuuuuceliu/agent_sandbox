from __future__ import annotations

import asyncio
import contextlib
import json
import unittest
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from typing import cast

from sandbox_manager.command_policy import (
    PolicyDecision,
    PolicyEvaluation,
    PolicyResult,
)
from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    AdmittedCommand,
    CommandAdmission,
    CommandAdmissionGate,
    CommandAudit,
    CommandAuditEntry,
    CommandResult,
    InMemoryCommandRepository,
    InputStream,
    OutputSink,
    TaskAdmissionState,
)
from sandbox_manager.nats_client import (
    AsyncNatsConnection,
    NatsAuthError,
    NatsMessage,
    NatsSubscription,
    NatsTransportError,
)
from sandbox_manager.nats_relay import (
    AdmissionReply,
    NatsCommandClient,
    NatsCommandServer,
    NatsRelayError,
    admission_subject,
    agent_base_subject,
    principal_from_admission_subject,
    task_subjects,
)
from sandbox_manager.relay_protocol import (
    MAX_FRAME_BYTES,
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    TerminalFrame,
    decode_frame,
    encode_command_envelope,
    encode_frame,
)

_NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)

_MANAGER_USER = "sandbox-manager"

_AGENT_USER = "agent-session-1"

_FENCE = "fence-generation-1"


class FixedClock:
    def now(self) -> datetime:
        return _NOW


class AllowPolicy:
    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        return PolicyResult(
            decision=PolicyDecision.ALLOW,
            policy_version=3,
            decision_reference="allow-v3",
        )


class RecordingAudit:
    def __init__(self) -> None:
        self.entries: list[CommandAuditEntry] = []

    async def record(self, entry: CommandAuditEntry) -> None:
        self.entries.append(entry)


class FailingAudit:
    async def record(self, entry: CommandAuditEntry) -> None:
        raise RuntimeError("audit transport unavailable")


class BridgeRelay:
    """Stands in for the Kubernetes relay on the manager side."""

    def __init__(
        self,
        outputs: tuple[tuple[int, bytes], ...] = ((1, b"output-chunk"),),
        hang: bool = False,
    ) -> None:
        self._outputs = outputs
        self._hang = hang
        self.calls: list[AdmittedCommand] = []
        self.frames: list[bytes] = []

    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        self.calls.append(command)
        if input_stream is not None:
            async for frame in input_stream:
                self.frames.append(frame)
        if output_sink is not None:
            for channel, payload in self._outputs:
                await output_sink(channel, payload)
        if self._hang:
            await asyncio.Event().wait()
        return CommandResult(command.command_id, CommandOutcome.SUCCESS)


class AdmissionSetupConnection(AsyncNatsConnection):
    def __init__(
        self,
        *,
        fail_subscribe_at: int | None = None,
        block_subscribe_at: int | None = None,
        fail_publish: bool = False,
        fail_flush: bool = False,
    ) -> None:
        super().__init__("127.0.0.1", 4222, _MANAGER_USER, "unused", "setup")
        self.fail_subscribe_at = fail_subscribe_at
        self.block_subscribe_at = block_subscribe_at
        self.fail_publish = fail_publish
        self.fail_flush = fail_flush
        self.subscribe_calls = 0
        self.subscribe_blocked = asyncio.Event()
        self.published_event = asyncio.Event()
        self.subscriptions: list[NatsSubscription] = []
        self.unsubscribed: list[NatsSubscription] = []
        self.published: list[tuple[str, bytes]] = []
        self.flush_calls = 0

    async def subscribe(
        self, subject: str, pending_limit: int = 64
    ) -> NatsSubscription:
        self.subscribe_calls += 1
        if self.subscribe_calls == self.block_subscribe_at:
            self.subscribe_blocked.set()
            await asyncio.Event().wait()
        if self.subscribe_calls == self.fail_subscribe_at:
            raise NatsTransportError("injected subscription failure")
        subscription = NatsSubscription(subject, pending_limit)
        self.subscriptions.append(subscription)
        return subscription

    async def unsubscribe(self, subscription: NatsSubscription) -> None:
        self.unsubscribed.append(subscription)

    async def flush(self, timeout: float = 5.0) -> None:
        self.flush_calls += 1
        if self.fail_flush:
            raise NatsTransportError("injected flush failure")

    async def publish(
        self,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.published.append((subject, payload))
        self.published_event.set()
        if self.fail_publish:
            raise NatsTransportError("injected publication failure")


class FailFirstAdmissionGate(CommandAdmissionGate):
    def __init__(
        self, repository: InMemoryCommandRepository, relay: BridgeRelay
    ) -> None:
        super().__init__(
            repository=repository,
            policy=AllowPolicy(),
            audit=RecordingAudit(),
            relay=relay,
            fence=AdmissionFence(_FENCE),
            clock=FixedClock(),
        )
        self.calls = 0

    async def admit_detailed(
        self, principal: str, envelope: CommandEnvelope
    ) -> CommandAdmission:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("injected request fault")
        return await super().admit_detailed(principal, envelope)


class FailFirstOutputConnection(AdmissionSetupConnection):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def publish(
        self,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise NatsTransportError("injected output failure")
        await super().publish(subject, payload, reply, headers)


class FakeNatsServer:
    """In-process Core NATS stand-in: routing, wildcards, basic auth."""

    def __init__(self, credentials: dict[str, str] | None = None) -> None:
        self.credentials = credentials
        self.sessions: list[_FakeNatsSession] = []
        self.published: list[tuple[str, bytes]] = []
        self._server: asyncio.Server | None = None

    @staticmethod
    def info_payload() -> bytes:
        info = {"max_payload": 1048576, "server_id": "fake"}
        return f"INFO {json.dumps(info)}\r\n".encode("ascii")

    @property
    def port(self) -> int:
        assert self._server is not None
        return cast(int, self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_connect, "127.0.0.1", 0)

    async def stop(self) -> None:
        assert self._server is not None
        for session in list(self.sessions):
            session.close()
        self._server.close()
        await self._server.wait_closed()

    async def _on_connect(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session = _FakeNatsSession(self, reader, writer)
        await session.serve()

    def register(self, session: _FakeNatsSession) -> None:
        self.sessions.append(session)

    def drop_session(self, session: _FakeNatsSession) -> None:
        if session in self.sessions:
            self.sessions.remove(session)

    def route(self, subject: str, reply: str | None, payload: bytes) -> None:
        self.published.append((subject, payload))
        for session in list(self.sessions):
            for sid, pattern in session.subscriptions.items():
                if _subject_matches(pattern, subject):
                    session.deliver(subject, sid, reply, payload)

    def kill_connection(self, name: str) -> None:
        for session in list(self.sessions):
            if session.name == name:
                session.close()
                self.drop_session(session)
                return
        raise AssertionError(f"no connection named {name}")


class _FakeNatsSession:
    def __init__(
        self,
        server: FakeNatsServer,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._server = server
        self._reader = reader
        self._writer = writer
        self._buf = b""
        self.name = ""
        self.user = ""
        self.subscriptions: dict[int, str] = {}

    async def serve(self) -> None:
        self._writer.write(self._server.info_payload())
        await self._writer.drain()
        try:
            while True:
                line = await self._read_line()
                if line and not await self._handle(line):
                    return
        except (ConnectionError, OSError, asyncio.CancelledError):
            return
        finally:
            self._server.drop_session(self)
            # EOF alone keeps a plain-TCP server transport open
            # (StreamReaderProtocol.eof_received returns True), which would
            # pin Server.wait_closed() forever; close the accepted socket.
            self.close()

    async def _handle(self, line: str) -> bool:
        operator = line.split(" ", 1)[0]
        if operator == "CONNECT":
            options = json.loads(line[8:])
            self.user = str(options.get("user", ""))
            self.name = str(options.get("name", ""))
            credentials = self._server.credentials
            if credentials is not None and self.user not in credentials:
                self._writer.write(b"-ERR 'Authorization Violation'\r\n")
                await self._writer.drain()
                self._writer.close()
                return False
            self._server.register(self)
            return True
        if operator == "PING":
            await self._send(b"PONG\r\n")
            return True
        if operator == "PUB":
            parts = line.split()
            subject = parts[1]
            reply = parts[2] if len(parts) == 4 else None
            size = int(parts[-1])
            payload = await self._read_exact(size)
            self._server.route(subject, reply, payload)
            return True
        if operator == "SUB":
            parts = line.split()
            self.subscriptions[int(parts[2])] = parts[1]
            return True
        if operator == "UNSUB":
            parts = line.split()
            if len(parts) >= 2:
                self.subscriptions.pop(int(parts[1]), None)
            return True
        return True

    def deliver(
        self, subject: str, sid: int, reply: str | None, payload: bytes
    ) -> None:
        reply_part = f" {reply}" if reply else ""
        frame = (
            f"MSG {subject} {sid}{reply_part} {len(payload)}\r\n".encode("ascii")
            + payload
            + b"\r\n"
        )
        try:
            self._writer.write(frame)
        except (ConnectionError, OSError):
            return

    def close(self) -> None:
        with contextlib.suppress(ConnectionError, OSError):
            self._writer.close()

    async def _send(self, data: bytes) -> None:
        try:
            self._writer.write(data)
            await self._writer.drain()
        except (ConnectionError, OSError):
            pass

    async def _read_line(self) -> str:
        while b"\r\n" not in self._buf:
            chunk = await self._reader.read(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self._buf += chunk
        line, _, self._buf = self._buf.partition(b"\r\n")
        return line.decode("ascii", "replace")

    async def _read_exact(self, size: int) -> bytes:
        while len(self._buf) < size:
            chunk = await self._reader.read(65536)
            if not chunk:
                raise ConnectionError("connection closed mid-payload")
            self._buf += chunk
        data, self._buf = self._buf[:size], self._buf[size:]
        if self._buf.startswith(b"\r\n"):
            self._buf = self._buf[2:]
        return data


def _subject_matches(pattern: str, subject: str) -> bool:
    pattern_tokens = pattern.split(".")
    subject_tokens = subject.split(".")
    if len(pattern_tokens) != len(subject_tokens):
        return False
    return all(
        expected == "*" or expected == actual
        for expected, actual in zip(pattern_tokens, subject_tokens, strict=True)
    )


def admission_state(
    agent_session_id: str = _AGENT_USER, task_id: str = "task-1"
) -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id=task_id,
        agent_session_id=agent_session_id,
        profile="default",
        lifecycle=AdmissionLifecycle.READY,
        fence_generation=_FENCE,
    )


def command_gate(
    repository: InMemoryCommandRepository,
    relay: BridgeRelay,
    audit: CommandAudit | None = None,
) -> CommandAdmissionGate:
    return CommandAdmissionGate(
        repository=repository,
        policy=AllowPolicy(),
        audit=audit or RecordingAudit(),
        relay=relay,
        fence=AdmissionFence(_FENCE),
        clock=FixedClock(),
    )


def envelope(
    command_id: str = "command-1", task_id: str = "task-1"
) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=command_id,
        task_id=task_id,
        operation=RelayOperation.EXEC,
        correlation_id=f"correlation-{command_id}",
        declared_input_bytes=128,
        declared_output_bytes=256,
    )


async def argv_frames(command_id: str) -> AsyncIterator[bytes]:
    payload = json.dumps({"argv": ["run-job", "--quiet"]}).encode("utf-8")
    yield encode_frame(DataFrame(command_id, DataChannel.STDIN, 0, payload))


class NatsRelayEnvironment:
    """One fake server, one manager server, and one agent connection."""

    def __init__(
        self,
        relay: BridgeRelay,
        state: TaskAdmissionState,
        audit: CommandAudit | None = None,
    ) -> None:
        self.server = FakeNatsServer()
        self.relay = relay
        self.repository = InMemoryCommandRepository({state.task_id: state})
        self.audit = audit
        self.manager_connection: AsyncNatsConnection | None = None
        self.agent_connection: AsyncNatsConnection | None = None
        self.command_server: NatsCommandServer | None = None

    async def start(self) -> None:
        await self.server.start()
        self.manager_connection = AsyncNatsConnection(
            "127.0.0.1", self.server.port, _MANAGER_USER, "unused", "manager"
        )
        await self.manager_connection.connect()
        self.command_server = NatsCommandServer(
            self.manager_connection,
            command_gate(self.repository, self.relay, self.audit),
        )
        await self.command_server.start()
        self.agent_connection = AsyncNatsConnection(
            "127.0.0.1", self.server.port, _AGENT_USER, "unused", "agent"
        )
        await self.agent_connection.connect()

    async def stop(self) -> None:
        if self.command_server is not None:
            await self.command_server.stop()
        for connection in (self.agent_connection, self.manager_connection):
            if connection is not None:
                await connection.close()
        await self.server.stop()

    def client(self) -> NatsCommandClient:
        assert self.agent_connection is not None
        return NatsCommandClient(self.agent_connection)


class SubjectLayoutTests(unittest.TestCase):
    def test_subjects_are_deterministic_and_token_safe(self) -> None:
        input_subject, closed_subject, output_subject = task_subjects(
            "agent-a", "task-9", "command-4"
        )
        self.assertEqual(
            input_subject, "sandbox.agent.agent-a.task.task-9.command-4.in"
        )
        self.assertEqual(
            closed_subject, "sandbox.agent.agent-a.task.task-9.command-4.closed"
        )
        self.assertEqual(
            output_subject, "sandbox.agent.agent-a.task.task-9.command-4.out"
        )
        self.assertEqual(admission_subject("agent-a"), "sandbox.agent.agent-a.admit")
        self.assertEqual(agent_base_subject("agent-a"), "sandbox.agent.agent-a")

    def test_identifiers_must_be_single_tokens(self) -> None:
        for bad in ("a.b", "a b", "", "../escape", "x" * 65):
            with self.assertRaises(NatsRelayError):
                task_subjects(bad, "task-1", "command-1")
            with self.assertRaises(NatsRelayError):
                admission_subject(bad)

    def test_principal_is_recovered_from_admission_subject(self) -> None:
        self.assertEqual(
            principal_from_admission_subject("sandbox.agent.agent-a.admit"),
            "agent-a",
        )
        for bad in (
            "sandbox.agent.agent-a.task.t.c.admit",
            "other.agent.a.admit",
            "sandbox.agent.*.admit",
            "sandbox.agent.agent-a.in",
            "sandbox.agent.agent-a",
        ):
            with self.assertRaises(NatsRelayError):
                principal_from_admission_subject(bad)


class AdmissionReplyTests(unittest.TestCase):
    def test_grant_round_trip(self) -> None:
        reply = AdmissionReply(
            True, input_subject="i", closed_subject="c", output_subject="o"
        )
        self.assertEqual(AdmissionReply.decode(reply.encode()), reply)

    def test_refusal_round_trip(self) -> None:
        reply = AdmissionReply(False, refusal="wrong_principal")
        self.assertEqual(AdmissionReply.decode(reply.encode()), reply)

    def test_refused_reply_requires_a_refusal(self) -> None:
        with self.assertRaises(NatsRelayError):
            AdmissionReply.decode(b'{"admitted": false}')

    def test_granted_reply_names_every_subject(self) -> None:
        with self.assertRaises(NatsRelayError):
            AdmissionReply.decode(b'{"admitted": true, "input_subject": "i"}')

    def test_reply_must_be_a_json_object_with_boolean_admission(self) -> None:
        for bad in (b"not json", b"[1]", b'{"admitted": "yes", "refusal": "x"}'):
            with self.assertRaises(NatsRelayError):
                AdmissionReply.decode(bad)


class AdmissionHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_surfaces_admission_subscription_failure(self) -> None:
        connection = AdmissionSetupConnection(fail_subscribe_at=1)
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        server = NatsCommandServer(
            connection, command_gate(repo, BridgeRelay(outputs=()))
        )

        with self.assertRaisesRegex(NatsTransportError, "subscription failure"):
            await server.start()

        self.assertIsNone(server._admit_task)

    async def test_duplicate_start_is_rejected_without_another_subscription(
        self,
    ) -> None:
        connection = AdmissionSetupConnection()
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        server = NatsCommandServer(
            connection, command_gate(repo, BridgeRelay(outputs=()))
        )
        await server.start()
        try:
            with self.assertRaisesRegex(NatsRelayError, "already started"):
                await server.start()
            self.assertEqual(connection.subscribe_calls, 1)
        finally:
            await server.stop()

    async def test_start_waits_for_subscription_and_cleans_up_flush_failure(
        self,
    ) -> None:
        connection = AdmissionSetupConnection(fail_flush=True)
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        server = NatsCommandServer(
            connection, command_gate(repo, BridgeRelay(outputs=()))
        )

        with self.assertRaisesRegex(NatsTransportError, "flush failure"):
            await server.start()

        self.assertEqual(connection.flush_calls, 1)
        self.assertEqual(connection.unsubscribed, connection.subscriptions)
        self.assertIsNone(server._admit_task)

    async def test_output_publish_failure_settles_command_as_unknown(self) -> None:
        connection = FailFirstOutputConnection()
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        gate = command_gate(repo, BridgeRelay())
        command_envelope = envelope()
        admission = await gate.admit_detailed(_AGENT_USER, command_envelope)
        input_sub = NatsSubscription("input.subject", 1)
        closed_sub = NatsSubscription("closed.subject", 1)
        closed_sub.offer(NatsMessage("closed.subject", None, b""))
        server = NatsCommandServer(connection, gate)

        await server._run_session(
            admission,
            command_envelope,
            _AGENT_USER,
            input_sub,
            closed_sub,
            "output.subject",
        )

        self.assertIs(await repo.outcome("command-1"), CommandOutcome.UNKNOWN)
        terminal = decode_frame(connection.published[-1][1])
        self.assertIsInstance(terminal, TerminalFrame)
        assert isinstance(terminal, TerminalFrame)
        self.assertIs(terminal.outcome, CommandOutcome.UNKNOWN)

    async def test_admission_loop_continues_after_unexpected_request_fault(
        self,
    ) -> None:
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        connection = AdmissionSetupConnection()
        gate = FailFirstAdmissionGate(repo, BridgeRelay(outputs=()))
        server = NatsCommandServer(connection, gate)
        await server.start()
        try:
            admissions = connection.subscriptions[0]
            admissions.offer(
                NatsMessage(
                    admission_subject(_AGENT_USER),
                    "reply.first",
                    encode_command_envelope(envelope("command-1")),
                )
            )
            admissions.offer(
                NatsMessage(
                    admission_subject(_AGENT_USER),
                    "reply.second",
                    encode_command_envelope(envelope("command-2")),
                )
            )

            await asyncio.wait_for(connection.published_event.wait(), timeout=1.0)
            reply_subject, payload = connection.published[-1]
            self.assertEqual(reply_subject, "reply.second")
            self.assertTrue(AdmissionReply.decode(payload).admitted)
            self.assertEqual(gate.calls, 2)
        finally:
            await server.stop()

    async def test_input_subscription_failure_abandons_durable_admission(self) -> None:
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        connection = AdmissionSetupConnection(fail_subscribe_at=1)
        server = NatsCommandServer(
            connection, command_gate(repo, BridgeRelay(outputs=()))
        )

        await server._handle_admission(
            NatsMessage(
                admission_subject(_AGENT_USER),
                "reply.subject",
                encode_command_envelope(envelope()),
            )
        )

        self.assertIs(
            await repo.outcome("command-1"), CommandOutcome.UNKNOWN
        )
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)
        self.assertEqual(connection.subscriptions, [])

    async def test_subscription_failure_abandons_and_cleans_partial_setup(
        self,
    ) -> None:
        for fail_at, cleaned in ((1, 0), (2, 1)):
            with self.subTest(fail_at=fail_at):
                repo = InMemoryCommandRepository(
                    {"task-1": admission_state()}
                )
                connection = AdmissionSetupConnection(
                    fail_subscribe_at=fail_at
                )
                server = NatsCommandServer(
                    connection, command_gate(repo, BridgeRelay(outputs=()))
                )

                await server._handle_admission(
                    NatsMessage(
                        admission_subject(_AGENT_USER),
                        "reply.subject",
                        encode_command_envelope(envelope()),
                    )
                )

                self.assertIs(
                    await repo.outcome("command-1"), CommandOutcome.UNKNOWN
                )
                state = await repo.admission_state("task-1")
                assert state is not None
                self.assertIsNone(state.in_flight_command_id)
                self.assertEqual(len(connection.unsubscribed), cleaned)

    async def test_grant_failure_abandons_and_cleans_both_subscriptions(self) -> None:
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        connection = AdmissionSetupConnection(fail_publish=True)
        server = NatsCommandServer(
            connection, command_gate(repo, BridgeRelay(outputs=()))
        )

        await server._handle_admission(
            NatsMessage(
                admission_subject(_AGENT_USER),
                "reply.subject",
                encode_command_envelope(envelope()),
            )
        )

        self.assertIs(
            await repo.outcome("command-1"), CommandOutcome.UNKNOWN
        )
        self.assertEqual(len(connection.unsubscribed), 2)

    async def test_setup_cancellation_abandons_and_cleans_first_subscription(
        self,
    ) -> None:
        repo = InMemoryCommandRepository({"task-1": admission_state()})
        connection = AdmissionSetupConnection(block_subscribe_at=2)
        server = NatsCommandServer(
            connection, command_gate(repo, BridgeRelay(outputs=()))
        )
        handling = asyncio.create_task(
            server._handle_admission(
                NatsMessage(
                    admission_subject(_AGENT_USER),
                    "reply.subject",
                    encode_command_envelope(envelope()),
                )
            )
        )
        await asyncio.wait_for(connection.subscribe_blocked.wait(), timeout=1.0)

        handling.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await handling

        self.assertIs(
            await repo.outcome("command-1"), CommandOutcome.UNKNOWN
        )
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)
        self.assertEqual(len(connection.unsubscribed), 1)


class NatsRelayFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_round_trip_streams_and_terminates(self) -> None:
        relay = BridgeRelay(outputs=((1, b"out-1"), (2, b"err-1")))
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        try:
            observed: list[tuple[int, bytes]] = []

            async def record(channel: int, payload: bytes) -> None:
                observed.append((channel, payload))

            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope(), argv_frames("command-1"), record
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            self.assertIsNone(result.reason_code)
            self.assertEqual(observed, [(1, b"out-1"), (2, b"err-1")])
            self.assertEqual(len(relay.calls), 1)
            self.assertEqual(
                await environment.repository.outcome("command-1"),
                CommandOutcome.SUCCESS,
            )
            self.assertEqual(len(relay.frames), 1)
            frame = decode_frame(relay.frames[0])
            assert isinstance(frame, DataFrame)
            self.assertIs(frame.channel, DataChannel.STDIN)
            self.assertEqual(frame.sequence, 0)
            self.assertEqual(
                json.loads(frame.payload), {"argv": ["run-job", "--quiet"]}
            )
        finally:
            await environment.stop()

    async def test_frames_queued_ahead_of_close_are_not_dropped(self) -> None:
        # The relay only starts consuming input once released, so the argv
        # frame and the close signal are both queued before the merged input
        # stream wakes; ordering must still deliver the frame.
        consume = asyncio.Event()

        class GatedInputRelay(BridgeRelay):
            async def execute(
                self,
                command: AdmittedCommand,
                envelope: CommandEnvelope,
                input_stream: InputStream | None,
                output_sink: OutputSink | None = None,
            ) -> CommandResult:
                self.calls.append(command)
                await consume.wait()
                return await super().execute(
                    command, envelope, input_stream, output_sink
                )

        relay = GatedInputRelay(outputs=())
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        try:
            executing = asyncio.create_task(
                environment.client().execute(envelope(), argv_frames("command-1"))
            )
            await asyncio.sleep(0.3)  # both messages queued behind the gate
            consume.set()
            result = await asyncio.wait_for(executing, timeout=10.0)

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            self.assertEqual(len(relay.frames), 1)
        finally:
            consume.set()
            await environment.stop()

    async def test_wrong_principal_is_refused_without_relay(self) -> None:
        relay = BridgeRelay()
        environment = NatsRelayEnvironment(relay, admission_state("someone-else"))
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(envelope(), None), timeout=10.0
            )

            self.assertIs(result.outcome, CommandOutcome.REJECTED)
            self.assertEqual(result.reason_code, "wrong_principal")
            self.assertEqual(relay.calls, [])
            self.assertIsNone(await environment.repository.outcome("command-1"))
        finally:
            await environment.stop()

    async def test_second_command_while_in_flight_is_refused(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        class GatedRelay(BridgeRelay):
            async def execute(
                self,
                command: AdmittedCommand,
                envelope: CommandEnvelope,
                input_stream: InputStream | None,
                output_sink: OutputSink | None = None,
            ) -> CommandResult:
                self.calls.append(command)
                started.set()
                await release.wait()
                return CommandResult(command.command_id, CommandOutcome.SUCCESS)

        relay = GatedRelay(outputs=())
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        try:
            first = asyncio.create_task(
                environment.client().execute(envelope(), None)
            )
            await asyncio.wait_for(started.wait(), timeout=10.0)
            second = await asyncio.wait_for(
                environment.client().execute(envelope("command-2"), None),
                timeout=10.0,
            )

            self.assertIs(second.outcome, CommandOutcome.REJECTED)
            self.assertEqual(second.reason_code, "command_in_flight")
            self.assertEqual(len(relay.calls), 1)
        finally:
            release.set()
            first_result = await asyncio.wait_for(first, timeout=10.0)
            self.assertIs(first_result.outcome, CommandOutcome.SUCCESS)
            await environment.stop()

    async def test_manager_stream_loss_is_unknown_to_the_agent(self) -> None:
        relay = BridgeRelay(outputs=(), hang=True)
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(envelope(), None, terminal_timeout=0.3),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(result.reason_code, "no_terminal_frame")
        finally:
            environment.server.kill_connection("manager")
            await environment.stop()

        # The manager lost its transport, not its outcome duty: stopping the
        # server abandons the in-flight command as UNKNOWN durably.
        self.assertIs(
            await environment.repository.outcome("command-1"),
            CommandOutcome.UNKNOWN,
        )

    async def test_stop_abandons_in_flight_commands_fail_closed(self) -> None:
        relay = BridgeRelay(hang=True)
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        client = environment.client()
        command_server = environment.command_server
        assert command_server is not None

        executing = asyncio.create_task(client.execute(envelope(), None))
        await asyncio.sleep(0.3)  # admit and enter the hanging relay
        await command_server.stop()

        self.assertIs(
            await environment.repository.outcome("command-1"),
            CommandOutcome.UNKNOWN,
        )
        state = await environment.repository.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)

        executing.cancel()
        await asyncio.gather(executing, return_exceptions=True)
        await environment.stop()

    async def test_invalid_admission_reply_fails_closed(self) -> None:
        # No real manager: a rogue connection answers the admission request
        # with junk, and the agent must classify the command as UNKNOWN.
        server = FakeNatsServer()
        await server.start()
        try:
            rogue = AsyncNatsConnection(
                "127.0.0.1", server.port, "rogue-manager", "unused", "rogue"
            )
            await rogue.connect()
            admission_sub = await rogue.subscribe("sandbox.agent.*.admit")

            agent = AsyncNatsConnection(
                "127.0.0.1", server.port, _AGENT_USER, "unused", "agent"
            )
            await agent.connect()
            executing = asyncio.create_task(
                NatsCommandClient(agent).execute(envelope(), None)
            )

            request = await asyncio.wait_for(
                admission_sub.queue.get(), timeout=10.0
            )
            assert request.reply is not None
            await rogue.publish(request.reply, b"{not json")
            result = await asyncio.wait_for(executing, timeout=10.0)

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(result.reason_code, "invalid_admission_reply")
            await agent.close()
            await rogue.close()
        finally:
            await server.stop()

    async def test_granted_reply_with_foreign_subjects_fails_closed(self) -> None:
        # A rogue grant naming subjects other than the deterministic task
        # subjects is not the approved contract: the agent must classify the
        # command UNKNOWN and publish no input to the foreign subjects.
        server = FakeNatsServer()
        await server.start()
        try:
            rogue = AsyncNatsConnection(
                "127.0.0.1", server.port, "rogue-manager", "unused", "rogue"
            )
            await rogue.connect()
            admission_sub = await rogue.subscribe("sandbox.agent.*.admit")
            foreign_input = await rogue.subscribe("rogue.input.subject")

            agent = AsyncNatsConnection(
                "127.0.0.1", server.port, _AGENT_USER, "unused", "agent"
            )
            await agent.connect()
            executing = asyncio.create_task(
                NatsCommandClient(agent).execute(envelope(), argv_frames("command-1"))
            )

            request = await asyncio.wait_for(
                admission_sub.queue.get(), timeout=10.0
            )
            assert request.reply is not None
            await rogue.publish(
                request.reply,
                AdmissionReply(
                    True,
                    input_subject="rogue.input.subject",
                    closed_subject="rogue.closed.subject",
                    output_subject="rogue.output.subject",
                ).encode(),
            )
            result = await asyncio.wait_for(executing, timeout=10.0)

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(result.reason_code, "subject_mismatch")
            self.assertTrue(foreign_input.queue.empty())
            await agent.close()
            await rogue.close()
        finally:
            await server.stop()

    async def test_fast_relay_failure_delivers_its_terminal_frame(self) -> None:
        # A relay that fails immediately publishes its terminal right after
        # the grant; the agent's pre-request output subscription must receive
        # it instead of falling into the terminal timeout.
        class FailingRelay(BridgeRelay):
            async def execute(
                self,
                command: AdmittedCommand,
                envelope: CommandEnvelope,
                input_stream: InputStream | None,
                output_sink: OutputSink | None = None,
            ) -> CommandResult:
                self.calls.append(command)
                raise RuntimeError("relay exploded before any stream")

        relay = FailingRelay(outputs=())
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope(), None, terminal_timeout=1.0
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(
                result.reason_code, "relay_stream_lost:RuntimeError"
            )
            self.assertEqual(
                await environment.repository.outcome("command-1"),
                CommandOutcome.UNKNOWN,
            )
        finally:
            await environment.stop()

    async def test_audit_failure_still_delivers_the_terminal_frame(self) -> None:
        environment = NatsRelayEnvironment(
            BridgeRelay(outputs=()), admission_state(), audit=FailingAudit()
        )
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope(), None, terminal_timeout=1.0
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            self.assertIsNone(result.reason_code)
            self.assertIs(
                await environment.repository.outcome("command-1"),
                CommandOutcome.SUCCESS,
            )
        finally:
            await environment.stop()


class NatsClientTests(unittest.IsolatedAsyncioTestCase):
    def test_username_must_be_safe_for_an_inbox_subject(self) -> None:
        with self.assertRaisesRegex(NatsTransportError, "single subject token"):
            AsyncNatsConnection(
                "127.0.0.1", 4222, "agent.unscoped", "unused", "client"
            )

    async def test_subjects_are_validated_before_transport_use(self) -> None:
        connection = AsyncNatsConnection(
            "127.0.0.1", 4222, "sandbox-manager", "unused", "client"
        )
        for subject in ("", "bad subject", "bad\r\nPING", "bad..token", "caf\N{LATIN SMALL LETTER E WITH ACUTE}"):
            with self.subTest(subject=subject):
                with self.assertRaises(NatsTransportError):
                    await connection.publish(subject, b"")
                with self.assertRaises(NatsTransportError):
                    await connection.subscribe(subject)
        with self.assertRaises(NatsTransportError):
            await connection.publish("sandbox.agent.*", b"")
        with self.assertRaises(NatsTransportError):
            await connection.subscribe("sandbox.>.tail")

    async def test_inbound_lengths_are_rejected_before_payload_reads(self) -> None:
        connection = AsyncNatsConnection(
            "127.0.0.1", 4222, "sandbox-manager", "unused", "client"
        )
        connection._reader = asyncio.StreamReader()
        for line in (
            f"MSG probe.subject 1 {MAX_FRAME_BYTES + 1}",
            "MSG probe.subject 0 1",
            "HMSG probe.subject 1 8 4",
        ):
            with self.subTest(line=line), self.assertRaises(NatsTransportError):
                await connection._dispatch(line)

    async def test_control_lines_and_header_messages_are_bounded(self) -> None:
        connection = AsyncNatsConnection(
            "127.0.0.1", 4222, "sandbox-manager", "unused", "client"
        )
        reader = asyncio.StreamReader()
        reader.feed_data(b"x" * (MAX_FRAME_BYTES + 1))
        connection._reader = reader
        with self.assertRaisesRegex(NatsTransportError, "control line"):
            await connection._read_line()

        with self.assertRaisesRegex(NatsTransportError, "message exceeds"):
            await connection.publish(
                "probe.subject", b"x" * MAX_FRAME_BYTES, headers={"A": "b"}
            )

    def test_subscription_requires_a_positive_pending_limit(self) -> None:
        for value in (True, 0, -1):
            with self.subTest(value=value), self.assertRaises(NatsTransportError):
                NatsSubscription("probe.subject", value)

    async def test_request_inbox_is_namespaced_by_authenticated_principal(self) -> None:
        server = FakeNatsServer()
        await server.start()
        requester = AsyncNatsConnection(
            "127.0.0.1", server.port, "agent-a", "unused", "requester"
        )
        responder = AsyncNatsConnection(
            "127.0.0.1", server.port, "sandbox-manager", "unused", "responder"
        )
        try:
            await requester.connect()
            await responder.connect()
            requests = await responder.subscribe("probe.request")
            await responder.flush()

            pending = asyncio.create_task(
                requester.request("probe.request", b"request", timeout=1.0)
            )
            request = await responder.next_message(requests, timeout=1.0)

            self.assertIsNotNone(request.reply)
            assert request.reply is not None
            self.assertRegex(
                request.reply, r"^sandbox\.inbox\.agent-a\.[A-Za-z0-9_-]+$"
            )
            await responder.publish(request.reply, b"response")
            response = await pending
            self.assertEqual(response.payload, b"response")
        finally:
            await requester.close()
            await responder.close()
            await server.stop()

    async def test_wrong_credentials_are_refused_by_the_server(self) -> None:
        server = FakeNatsServer(credentials={"sandbox-manager": "secret"})
        await server.start()
        connection = AsyncNatsConnection(
            "127.0.0.1", server.port, "intruder", "wrong", "intruder"
        )
        try:
            with self.assertRaises(NatsAuthError):
                await connection.connect()
            self.assertTrue(connection.closed)
            self.assertIsNone(connection._writer)
            self.assertIsNone(connection._reader_task)
            await connection.close()
            await connection.close()
        finally:
            await connection.close()
            await server.stop()

    async def test_invalid_info_closes_the_peer_and_releases_streams(self) -> None:
        peer_closed = asyncio.Event()

        async def serve_invalid_info(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            writer.write(b"INFO not-json\r\n")
            await writer.drain()
            await reader.read()
            peer_closed.set()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(serve_invalid_info, "127.0.0.1", 0)
        port = cast(int, server.sockets[0].getsockname()[1])
        connection = AsyncNatsConnection(
            "127.0.0.1", port, "sandbox-manager", "unused", "invalid-info"
        )
        try:
            with self.assertRaisesRegex(NatsTransportError, "invalid INFO"):
                await connection.connect()
            await asyncio.wait_for(peer_closed.wait(), timeout=1.0)
            self.assertIsNone(connection._writer)
            self.assertIsNone(connection._reader_task)
        finally:
            await connection.close()
            server.close()
            await server.wait_closed()

    async def test_connect_timeout_releases_reader_and_writer(self) -> None:
        peer_closed = asyncio.Event()

        async def withhold_pong(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            writer.write(FakeNatsServer.info_payload())
            await writer.drain()
            await reader.read()
            peer_closed.set()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(withhold_pong, "127.0.0.1", 0)
        port = cast(int, server.sockets[0].getsockname()[1])
        connection = AsyncNatsConnection(
            "127.0.0.1", port, "sandbox-manager", "unused", "no-pong"
        )
        try:
            with self.assertRaisesRegex(NatsAuthError, "no PONG"):
                await connection.connect(timeout=0.05)
            await asyncio.wait_for(peer_closed.wait(), timeout=1.0)
            self.assertIsNone(connection._writer)
            self.assertIsNone(connection._reader_task)
        finally:
            await connection.close()
            server.close()
            await server.wait_closed()

    async def test_remote_close_disposes_streams_before_explicit_close(self) -> None:
        server = FakeNatsServer()
        await server.start()
        connection = AsyncNatsConnection(
            "127.0.0.1", server.port, "sandbox-manager", "unused", "remote-close"
        )
        try:
            await connection.connect()
            server.kill_connection("remote-close")
            for _ in range(100):
                if connection.closed and connection._writer is None:
                    break
                await asyncio.sleep(0.01)

            self.assertTrue(connection.closed)
            self.assertIsNone(connection._writer)
            self.assertIsNone(connection._reader_task)
            await connection.close()
            await connection.close()
        finally:
            await connection.close()
            await server.stop()

    async def test_slow_consumer_drops_instead_of_growing(self) -> None:
        server = FakeNatsServer()
        await server.start()
        try:
            connection = AsyncNatsConnection(
                "127.0.0.1", server.port, "sandbox-manager", "unused", "client"
            )
            await connection.connect()
            subscription = await connection.subscribe(
                "probe.subject", pending_limit=4
            )
            for index in range(16):
                await connection.publish(
                    "probe.subject", str(index).encode("ascii")
                )
            await connection.flush()

            self.assertEqual(subscription.queue.qsize(), 4)
            self.assertEqual(subscription.dropped, 12)
            await connection.close()
        finally:
            await server.stop()

    async def test_publish_over_frame_bound_is_refused(self) -> None:
        server = FakeNatsServer()
        await server.start()
        try:
            connection = AsyncNatsConnection(
                "127.0.0.1", server.port, "sandbox-manager", "unused", "client"
            )
            await connection.connect()
            with self.assertRaises(NatsTransportError):
                await connection.publish("probe.subject", b"x" * (64 * 1024 + 1))
            await connection.close()
        finally:
            await server.stop()


if __name__ == "__main__":
    unittest.main()
