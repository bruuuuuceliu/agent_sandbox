from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import AsyncIterator
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
    CommandAdmissionGate,
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
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    decode_frame,
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
                output_sink(channel, payload)
        if self._hang:
            await asyncio.Event().wait()
        return CommandResult(command.command_id, CommandOutcome.SUCCESS)


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
        try:
            self._writer.close()
        except (ConnectionError, OSError):
            pass

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
        for expected, actual in zip(pattern_tokens, subject_tokens)
    )


def admission_state(agent_session_id: str = _AGENT_USER) -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id="task-1",
        agent_session_id=agent_session_id,
        profile="default",
        lifecycle=AdmissionLifecycle.READY,
        fence_generation=_FENCE,
    )


def command_gate(
    repository: InMemoryCommandRepository, relay: BridgeRelay
) -> CommandAdmissionGate:
    return CommandAdmissionGate(
        repository=repository,
        policy=AllowPolicy(),
        audit=RecordingAudit(),
        relay=relay,
        fence=AdmissionFence(_FENCE),
        clock=FixedClock(),
    )


def envelope(command_id: str = "command-1") -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=command_id,
        task_id="task-1",
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

    def __init__(self, relay: BridgeRelay, state: TaskAdmissionState) -> None:
        self.server = FakeNatsServer()
        self.relay = relay
        self.repository = InMemoryCommandRepository({state.task_id: state})
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
            self.manager_connection, command_gate(self.repository, self.relay)
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


class NatsRelayFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_round_trip_streams_and_terminates(self) -> None:
        relay = BridgeRelay(outputs=((1, b"out-1"), (2, b"err-1")))
        environment = NatsRelayEnvironment(relay, admission_state())
        await environment.start()
        try:
            observed: list[tuple[int, bytes]] = []

            def record(channel: int, payload: bytes) -> None:
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


class NatsClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_wrong_credentials_are_refused_by_the_server(self) -> None:
        server = FakeNatsServer(credentials={"sandbox-manager": "secret"})
        await server.start()
        try:
            connection = AsyncNatsConnection(
                "127.0.0.1", server.port, "intruder", "wrong", "intruder"
            )
            with self.assertRaises(NatsAuthError):
                await connection.connect()
        finally:
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
