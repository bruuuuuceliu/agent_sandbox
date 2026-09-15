"""End-to-end relay flow verification for the Stage 4b cutover (plan 5.5).

Composes the shipped chain against in-process protocol fakes instead of a
``BridgeRelay`` stand-in: the agent drives ``NatsCommandClient`` over the real
NATS wire protocol into ``FakeNatsServer``; the manager serves admissions with
``NatsCommandServer``, admits through the durable ``CommandAdmissionGate``, and
relays through ``KubernetesCommandRelay`` into ``FakeKubeServer`` over real
HTTP ``pods/get`` and WebSocket ``pods/exec``.

Each flow maps to a Stage 4b exit criterion: normal exec and transfer use,
policy denial before any side effect, transport-principal ownership, the
termination admission race, Pod replacement, restart recovery, stream loss,
and transport-bound abuse caps. Audit and request records stay payload-free:
identifiers, sizes, counts, and outcomes only.
"""

from __future__ import annotations

import asyncio
import json
import unittest
import urllib.parse
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime

from sandbox_manager.command_policy import (
    PolicyDecision,
    PolicyEvaluation,
    PolicyResult,
)
from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    AdmissionRecovery,
    AdmissionRecoveryEntry,
    CommandAdmissionGate,
    CommandAuditEntry,
    InMemoryCommandRepository,
    TaskAdmissionState,
)
from sandbox_manager.kubernetes_exec import (
    RELAY_HELPER_PATH,
    KubernetesApi,
    KubernetesApiConfig,
    KubernetesCommandRelay,
    PodTarget,
    PodVerification,
)
from sandbox_manager.nats_client import AsyncNatsConnection
from sandbox_manager.nats_relay import (
    AdmissionReply,
    NatsCommandClient,
    NatsCommandServer,
    admission_subject,
)
from sandbox_manager.relay_protocol import (
    MAX_DECLARED_BYTES,
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    encode_frame,
)
from sandbox_manager.relay_workspace import RelayWorkspaceService
from sandbox_manager.workspace import (
    WorkspaceLayout,
    checksum_bytes,
)
from tests.test_kubernetes_exec import FakeKubeServer, fake_pod_body
from tests.test_nats_relay import FakeNatsServer

_NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)

_OWNER = "agent-session-1"

_OTHER_OWNER = "agent-session-2"

_NAMESPACE = "sandbox-tasks"

_FENCE = "fence-flow-1"


class FixedClock:
    def now(self) -> datetime:
        return _NOW


class OperationPolicy:
    """Deterministic per-operation stand-in pending Decision 19's method."""

    def __init__(self, allowed: tuple[RelayOperation, ...]) -> None:
        self._allowed = frozenset(allowed)
        self.evaluations: list[PolicyEvaluation] = []

    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        self.evaluations.append(request)
        allowed = request.operation in self._allowed
        return PolicyResult(
            decision=PolicyDecision.ALLOW if allowed else PolicyDecision.DENY,
            policy_version=2,
            decision_reference="flow-allow-v2" if allowed else "flow-deny-v2",
        )


class RecordingAudit:
    def __init__(self) -> None:
        self.entries: list[CommandAuditEntry] = []

    async def record(self, entry: CommandAuditEntry) -> None:
        self.entries.append(entry)


class OpenInspector:
    """PathInspector double: the fake helper's filesystem has no traps."""

    def is_symlink(self, path: str) -> bool:
        return False

    def is_special_file(self, path: str) -> bool:
        return False


def admission_state(
    task_id: str = "task-1",
    owner: str = _OWNER,
    fence: str = _FENCE,
    lifecycle: AdmissionLifecycle = AdmissionLifecycle.READY,
    in_flight: str | None = None,
) -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id=task_id,
        agent_session_id=owner,
        profile="default",
        lifecycle=lifecycle,
        fence_generation=fence,
        in_flight_command_id=in_flight,
    )


def envelope(
    command_id: str,
    task_id: str = "task-1",
    operation: RelayOperation = RelayOperation.EXEC,
    declared_input: int = 64,
    declared_output: int = 1024,
) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=command_id,
        task_id=task_id,
        operation=operation,
        correlation_id=f"correlation-{command_id}",
        declared_input_bytes=declared_input,
        declared_output_bytes=declared_output,
    )


async def argv_frames(
    command_id: str, argv: tuple[str, ...]
) -> AsyncIterator[bytes]:
    payload = json.dumps({"argv": list(argv)}).encode("utf-8")
    yield encode_frame(DataFrame(command_id, DataChannel.STDIN, 0, payload))


async def payload_frames(
    command_id: str, payload: bytes, first_sequence: int = 1
) -> AsyncIterator[bytes]:
    sequence = first_sequence
    for offset in range(0, len(payload), 4096):
        yield encode_frame(
            DataFrame(
                command_id, DataChannel.STDIN, sequence, payload[offset : offset + 4096]
            )
        )
        sequence += 1


async def _wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached in time")
        await asyncio.sleep(0.02)


class RelayFlowEnvironment:
    """The full Stage 4b chain: agent, NATS, gate, and pods/exec relay.

    ``restart_recovery`` models a manager that reconciles prior-fence
    admission state before it starts serving commands.
    """

    def __init__(
        self,
        states: Mapping[str, TaskAdmissionState],
        *,
        fence: str = _FENCE,
        allowed_operations: tuple[RelayOperation, ...] | None = None,
        restart_recovery: bool = False,
    ) -> None:
        self.nats = FakeNatsServer()
        self.kube = FakeKubeServer()
        self.kube.start()
        self.repository = InMemoryCommandRepository(states)
        self.audit = RecordingAudit()
        self.fence = AdmissionFence(fence)
        self.pod_targets: dict[str, PodTarget] = {}
        allowed = (
            allowed_operations
            if allowed_operations is not None
            else tuple(RelayOperation)
        )
        self.policy = OperationPolicy(allowed)
        self._restart_recovery = restart_recovery

        def locate(task_id: str) -> PodTarget | None:
            return self.pod_targets.get(task_id)

        self.gate = CommandAdmissionGate(
            repository=self.repository,
            policy=self.policy,
            audit=self.audit,
            relay=KubernetesCommandRelay(
                KubernetesApi(
                    KubernetesApiConfig(
                        host="127.0.0.1",
                        port=self.kube.port,
                        bearer_token="relay-token",
                        secure=False,
                    )
                ),
                locate,
            ),
            fence=self.fence,
            clock=FixedClock(),
        )
        self.recovery: AdmissionRecovery | None = None
        self.command_server: NatsCommandServer | None = None
        self.manager: AsyncNatsConnection | None = None
        self.agent: AsyncNatsConnection | None = None

    def seed_pod(self, task_id: str = "task-1", uid: str = "uid-1") -> None:
        self.kube.pods[f"pod-{task_id}"] = fake_pod_body(uid=uid, task_id=task_id)
        self.pod_targets[task_id] = PodTarget(
            namespace=_NAMESPACE,
            name=f"pod-{task_id}",
            verification=PodVerification(
                expected_uid=uid,
                task_id=task_id,
                namespace=_NAMESPACE,
                expected_labels=(("app", "sandbox"),),
            ),
        )

    async def start(self) -> None:
        if self._restart_recovery:
            self.recovery = await self.repository.recover(self.fence.generation)
        await self.nats.start()
        self.manager = AsyncNatsConnection(
            "127.0.0.1", self.nats.port, "sandbox-manager", "unused", "manager"
        )
        await self.manager.connect()
        self.command_server = NatsCommandServer(self.manager, self.gate)
        await self.command_server.start()
        self.agent = AsyncNatsConnection(
            "127.0.0.1", self.nats.port, _OWNER, "unused", "agent"
        )
        await self.agent.connect()

    async def stop(self) -> None:
        if self.command_server is not None:
            await self.command_server.stop()
        for connection in (self.agent, self.manager):
            if connection is not None:
                await connection.close()
        await self.nats.stop()
        self.kube.stop()

    def client(self) -> NatsCommandClient:
        assert self.agent is not None
        return NatsCommandClient(self.agent)

    def records_of(self, kind: str) -> list[dict[str, object]]:
        return [record for record in self.kube.records if record["kind"] == kind]


class RelayFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_exec_round_trip_executes_only_the_fixed_helper(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [
            (1, b"out-chunk"),
            (2, b"err-chunk"),
            (3, b""),
        ]
        await environment.start()
        try:
            observed: list[tuple[int, bytes]] = []

            async def record(channel: int, payload: bytes) -> None:
                observed.append((channel, payload))

            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-1"),
                    argv_frames("command-1", ("run-job", "--quiet")),
                    record,
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            self.assertEqual(
                observed, [(1, b"out-chunk"), (2, b"err-chunk")]
            )
            self.assertIs(
                await environment.repository.outcome("command-1"),
                CommandOutcome.SUCCESS,
            )
            self.assertIs(
                await environment.repository.lifecycle("task-1"),
                AdmissionLifecycle.ACTIVE,
            )

            gets = environment.records_of("get")
            execs = environment.records_of("exec")
            self.assertEqual(len(gets), 1)  # the Pod was verified first
            self.assertEqual(len(execs), 1)
            record_exec = execs[0]
            query = urllib.parse.parse_qs(str(record_exec["query"]))
            self.assertEqual(query["command"], [RELAY_HELPER_PATH])
            self.assertEqual(query["tty"], ["false"])
            # User argv traveled on the helper stdin channel, never in the
            # request the API server and its audit would record.
            self.assertEqual(record_exec["argv"], ["run-job", "--quiet"])
            self.assertNotIn("run-job", str(record_exec["path"]))
            self.assertNotIn("run-job", str(record_exec["query"]))
            # Manager audit stays payload-free (AC-025).
            self.assertEqual(len(environment.audit.entries), 1)
            audited = environment.audit.entries[0]
            self.assertIs(audited.outcome, CommandOutcome.SUCCESS)
            self.assertEqual(audited.authenticated_principal, _OWNER)
            self.assertEqual(audited.decision_reference, "flow-allow-v2")
            self.assertNotIn("run-job", repr(audited))
            self.assertNotIn("out-chunk", repr(audited))
        finally:
            await environment.stop()

    async def test_upload_and_download_travel_the_relay(self) -> None:
        payload = b"transfer-payload" * 512  # spans several bounded frames
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:
            upload_argv = (
                "upload",
                "--dest",
                "/workspace/input.bin",
                "--bytes",
                str(len(payload)),
                "--sha256",
                checksum_bytes(payload),
            )
            async def upload_input() -> AsyncIterator[bytes]:
                async for frame in argv_frames("command-upload", upload_argv):
                    yield frame
                async for frame in payload_frames("command-upload", payload):
                    yield frame

            uploaded = await asyncio.wait_for(
                environment.client().execute(
                    envelope(
                        "command-upload",
                        operation=RelayOperation.UPLOAD,
                        declared_input=len(payload),
                        declared_output=0,
                    ),
                    upload_input(),
                ),
                timeout=10.0,
            )
            self.assertIs(uploaded.outcome, CommandOutcome.SUCCESS)

            environment.kube.exec_frames = [
                (1, b"part-one,"),
                (1, b"part-two"),
                (3, b""),
            ]
            observed: list[bytes] = []

            async def record(channel: int, chunk: bytes) -> None:
                if channel == 1:
                    observed.append(chunk)

            downloaded = await asyncio.wait_for(
                environment.client().execute(
                    envelope(
                        "command-download",
                        operation=RelayOperation.DOWNLOAD,
                        declared_input=0,
                        declared_output=len(b"part-one,part-two"),
                    ),
                    None,
                    record,
                ),
                timeout=10.0,
            )

            self.assertIs(downloaded.outcome, CommandOutcome.SUCCESS)
            self.assertEqual(b"".join(observed), b"part-one,part-two")
            execs = environment.records_of("exec")
            self.assertEqual(len(execs), 2)
            upload_record = execs[0]
            self.assertEqual(upload_record["argv"], list(upload_argv))
            self.assertEqual(upload_record["stdin"], payload)
            for command_id in ("command-upload", "command-download"):
                self.assertIs(
                    await environment.repository.outcome(command_id),
                    CommandOutcome.SUCCESS,
                )
        finally:
            await environment.stop()

    async def test_workspace_service_uses_the_verified_relay(self) -> None:
        payload = b"workspace-service-payload"
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:
            service = RelayWorkspaceService(
                environment.gate,
                WorkspaceLayout("/workspace", readable_roots=("/workspace",)),
                OpenInspector(),
                _OWNER,
            )

            async def frames() -> AsyncIterator[bytes]:
                yield payload

            uploaded = await asyncio.wait_for(
                service.upload(
                    "task-1",
                    "command-upload",
                    "notes/input.bin",
                    len(payload),
                    checksum_bytes(payload),
                    frames(),
                ),
                timeout=10.0,
            )
            self.assertIs(uploaded.outcome, CommandOutcome.SUCCESS)

            environment.kube.exec_frames = [(1, b"body"), (3, b"")]
            chunks = [
                chunk
                async for chunk in service.download(
                    "task-1", "command-download", "/workspace/notes/input.bin", 64
                )
            ]

            self.assertEqual(chunks, [b"body"])
            execs = environment.records_of("exec")
            self.assertEqual(len(execs), 2)
            self.assertEqual(
                execs[0]["argv"],
                [
                    "upload",
                    "--dest",
                    "/workspace/notes/input.bin",
                    "--bytes",
                    str(len(payload)),
                    "--sha256",
                    checksum_bytes(payload),
                ],
            )
            self.assertEqual(execs[0]["stdin"], payload)
        finally:
            await environment.stop()

    async def test_policy_denial_precedes_any_kubernetes_side_effect(self) -> None:
        environment = RelayFlowEnvironment(
            {"task-1": admission_state()},
            allowed_operations=(RelayOperation.EXEC,),
        )
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-1", operation=RelayOperation.DOWNLOAD), None
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.REJECTED)
            self.assertEqual(result.reason_code, "policy_denied")
            self.assertEqual(environment.kube.records, [])
            self.assertIsNone(await environment.repository.outcome("command-1"))
            self.assertIs(
                await environment.repository.lifecycle("task-1"),
                AdmissionLifecycle.READY,  # denial left the task un-activated
            )
            self.assertEqual(len(environment.audit.entries), 1)
            denied = environment.audit.entries[0]
            self.assertIs(denied.outcome, CommandOutcome.REJECTED)
            self.assertEqual(denied.decision_reference, "flow-deny-v2")
        finally:
            await environment.stop()

    async def test_foreign_principal_and_cross_task_commands_are_refused(
        self,
    ) -> None:
        environment = RelayFlowEnvironment(
            {
                "task-1": admission_state("task-1", owner=_OWNER),
                "task-2": admission_state("task-2", owner=_OTHER_OWNER),
            }
        )
        environment.seed_pod("task-1")
        environment.seed_pod("task-2")
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:
            foreign = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-x", task_id="task-2"), None
                ),
                timeout=10.0,
            )

            self.assertIs(foreign.outcome, CommandOutcome.REJECTED)
            self.assertEqual(foreign.reason_code, "wrong_principal")
            self.assertEqual(environment.kube.records, [])

            owned = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-1", task_id="task-1"),
                    argv_frames("command-1", ("run-job",)),
                ),
                timeout=10.0,
            )
            self.assertIs(owned.outcome, CommandOutcome.SUCCESS)
            execs = environment.records_of("exec")
            self.assertEqual(len(execs), 1)
            self.assertIn("/pods/pod-task-1/", str(execs[0]["path"]))
        finally:
            await environment.stop()

    async def test_termination_closes_admission_while_in_flight_finishes(
        self,
    ) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        environment.kube.exec_delay = 0.4
        await environment.start()
        try:
            first = asyncio.create_task(
                environment.client().execute(
                    envelope("command-1"), argv_frames("command-1", ("run-job",))
                )
            )
            await _wait_until(lambda: len(environment.records_of("exec")) == 1)

            # Termination wins the durable admission race exactly as the
            # lifecycle manager closes it: before any cleanup side effect.
            await environment.repository.begin_termination("task-1")

            second = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-2"), argv_frames("command-2", ("run-job",))
                ),
                timeout=10.0,
            )
            self.assertIs(second.outcome, CommandOutcome.REJECTED)
            self.assertEqual(second.reason_code, "not_admissible")

            first_result = await asyncio.wait_for(first, timeout=10.0)
            self.assertIs(first_result.outcome, CommandOutcome.SUCCESS)
            self.assertIs(
                await environment.repository.outcome("command-1"),
                CommandOutcome.SUCCESS,
            )
            self.assertIs(
                await environment.repository.lifecycle("task-1"),
                AdmissionLifecycle.TERMINATING,
            )
            self.assertEqual(len(environment.records_of("exec")), 1)
        finally:
            await environment.stop()

    async def test_replaced_pod_is_refused_before_exec(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod(uid="uid-original")
        # The deterministic name now resolves to a replacement Pod.
        environment.kube.pods["pod-task-1"] = fake_pod_body(
            uid="uid-replacement", task_id="task-1"
        )
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-1"), argv_frames("command-1", ("run-job",))
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(result.reason_code, "relay_stream_lost:PodIdentityError")
            # The replacement was detected by the verification read; the
            # wrong Pod was never exec'd into.
            self.assertEqual(len(environment.records_of("get")), 1)
            self.assertEqual(environment.records_of("exec"), [])
            self.assertIs(
                await environment.repository.outcome("command-1"),
                CommandOutcome.UNKNOWN,
            )
        finally:
            await environment.stop()

    async def test_exec_stream_loss_is_unknown_on_both_sides(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_drop = True
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-1"), argv_frames("command-1", ("run-job",))
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(result.reason_code, "stream_lost")
            self.assertIs(
                await environment.repository.outcome("command-1"),
                CommandOutcome.UNKNOWN,
            )
        finally:
            await environment.stop()

    async def test_restart_classifies_prior_in_flight_as_unknown(self) -> None:
        crashed = admission_state(
            lifecycle=AdmissionLifecycle.ACTIVE,
            fence="fence-crashed",
            in_flight="command-crashed",
        )
        environment = RelayFlowEnvironment(
            {"task-1": crashed}, fence="fence-restarted", restart_recovery=True
        )
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:
            assert environment.recovery is not None
            self.assertEqual(
                environment.recovery.entries,
                (AdmissionRecoveryEntry("task-1", "command-crashed"),),
            )
            self.assertIs(
                await environment.repository.outcome("command-crashed"),
                CommandOutcome.UNKNOWN,
            )

            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-after"), argv_frames("command-after", ("next",))
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            # The crashed command is never re-issued; only the new one ran.
            self.assertEqual(len(environment.records_of("exec")), 1)
            self.assertEqual(
                environment.records_of("exec")[0]["argv"], ["next"]
            )
            state = await environment.repository.admission_state("task-1")
            assert state is not None
            self.assertIsNone(state.in_flight_command_id)
        finally:
            await environment.stop()

    async def test_pty_operation_runs_with_tty_enabled(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [(1, b"tty-out"), (3, b"")]
        await environment.start()
        try:
            observed: list[tuple[int, bytes]] = []

            async def record(channel: int, payload: bytes) -> None:
                observed.append((channel, payload))

            result = await asyncio.wait_for(
                environment.client().execute(
                    envelope("command-pty", operation=RelayOperation.PTY),
                    None,
                    record,
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            self.assertEqual(observed, [(1, b"tty-out")])
            query = urllib.parse.parse_qs(
                str(environment.records_of("exec")[0]["query"])
            )
            self.assertEqual(query["tty"], ["true"])
        finally:
            await environment.stop()

    async def test_oversized_admission_payload_is_refused_undecoded(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        await environment.start()
        try:
            assert environment.agent is not None
            abusive = json.dumps(
                {
                    "schema_version": RELAY_SCHEMA_VERSION,
                    "command_id": "command-1",
                    "task_id": "task-1",
                    "operation": "EXEC",
                    "correlation_id": "correlation-command-1",
                    "declared_input_bytes": MAX_DECLARED_BYTES + 1,
                    "declared_output_bytes": 0,
                }
            ).encode("utf-8")

            reply = await asyncio.wait_for(
                environment.agent.request(
                    admission_subject(_OWNER), abusive, 5.0
                ),
                timeout=10.0,
            )
            decoded = AdmissionReply.decode(reply.payload)

            self.assertFalse(decoded.admitted)
            self.assertEqual(decoded.refusal, "invalid_envelope")
            self.assertEqual(environment.kube.records, [])
        finally:
            await environment.stop()

    async def test_malformed_input_frame_fails_closed_as_unknown(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()

        async def garbage() -> AsyncIterator[bytes]:
            yield b"\xff" * 16  # not a relay frame

        try:
            result = await asyncio.wait_for(
                environment.client().execute(envelope("command-1"), garbage()),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertIs(
                await environment.repository.outcome("command-1"),
                CommandOutcome.UNKNOWN,
            )
            execs = environment.records_of("exec")
            self.assertEqual(len(execs), 1)
            # The helper connection opened, but no user argv was ever decoded
            # or delivered to it.
            self.assertEqual(execs[0]["argv"], [])
        finally:
            await environment.stop()

    async def test_oversized_input_fails_closed_at_the_transport(self) -> None:
        environment = RelayFlowEnvironment({"task-1": admission_state()})
        environment.seed_pod()
        await environment.start()

        async def oversized() -> AsyncIterator[bytes]:
            yield b"x" * (64 * 1024 + 1)  # over the NATS frame bound

        executing = asyncio.create_task(
            environment.client().execute(envelope("command-1"), oversized())
        )
        try:
            result = await asyncio.wait_for(executing, timeout=10.0)

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            self.assertEqual(result.reason_code, "input_unpublished")
            self.assertEqual(environment.records_of("exec"), [])

            # The manager never saw the command end; stopping it abandons the
            # session fail-closed and releases the in-flight slot.
            await environment.stop()
        except BaseException:
            executing.cancel()
            await asyncio.gather(executing, return_exceptions=True)
            raise
        self.assertIs(
            await environment.repository.outcome("command-1"),
            CommandOutcome.UNKNOWN,
        )
        state = await environment.repository.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)


if __name__ == "__main__":
    unittest.main()
