"""End-to-end sandbox flows across the domain components."""

from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from sandbox_manager.health import (
    AuthenticatedHealthResponse,
    HealthChallenge,
    HealthCheckConfig,
    HealthCheckEngine,
    SystemHealthClock,
)
from sandbox_manager.intake import (
    IntakeResult,
    QueuedTask,
    SpawnIntakeProcessor,
    StoreResult,
    TaskState,
)
from sandbox_manager.lifecycle import (
    ActiveRecord,
    CleanupStep,
    LifecycleManager,
    LifecyclePersistenceError,
    ReadyRecord,
    RecoveryRecord,
    RunningRecord,
    TerminalReason,
    TerminalRecord,
    TerminalState,
    TerminatingRecord,
    TerminationDecision,
    TerminationTrigger,
)
from sandbox_manager.profiles import (
    PROFILE_SCHEMA_VERSION,
    ProfileMount,
    SandboxProfile,
    SandboxProfileRegistry,
)
from sandbox_manager.provisioning import (
    CapacityLimiter,
    FailureStage,
    ProvisionedSandbox,
    ProvisioningOrchestrator,
    ProvisioningResult,
    ProvisioningState,
    ProvisioningStatus,
    ProvisioningTask,
    ReadyTask,
    RelayReference,
)
from sandbox_manager.spawn import SCHEMA_VERSION
from sandbox_manager.supervision import (
    HealthLifecycleSupervisor,
    HealthSuccess,
)

_READY_AT = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def python_profile() -> SandboxProfile:
    return SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )


def spawn_payload(task_id: str, agent_session_id: str = "session-1") -> bytes:
    values: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "request_id": f"request-{task_id}",
        "task_id": task_id,
        "agent_session_id": agent_session_id,
        "sandbox_type": "python",
    }
    return json.dumps(values).encode()


class QueueDelivery:
    """One durable queue delivery that records its acknowledgement."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.acknowledgements = 0

    @property
    def payload(self) -> bytes:
        return self._payload

    async def acknowledge(self) -> None:
        self.acknowledgements += 1


class InMemoryStateStore:
    """Durable-state double covering intake, provisioning, and lifecycle."""

    def __init__(self) -> None:
        self.now = _READY_AT
        self.tasks: dict[str, QueuedTask] = {}
        self.requests: dict[str, str] = {}
        self.provisioning: dict[str, ProvisioningTask] = {}
        self.ready_tasks: dict[str, ReadyTask] = {}
        self.ready_records: dict[str, ReadyRecord] = {}
        self.active_records: dict[str, ActiveRecord] = {}
        self.terminating: dict[str, TerminatingRecord] = {}
        self.terminal: dict[str, TerminalRecord] = {}
        self.failures: list[tuple[str, FailureStage]] = []
        self.rollbacks: list[str] = []

    async def accept_spawn(self, task: QueuedTask) -> StoreResult:
        request = task.request
        by_task = self.tasks.get(request.task_id)
        by_request = self.requests.get(request.request_id)
        if by_task is None and by_request is None:
            self.tasks[request.task_id] = task
            self.requests[request.request_id] = request.task_id
            return StoreResult.CREATED
        if (
            by_task is not None
            and by_request == request.task_id
            and by_task.request_fingerprint == task.request_fingerprint
        ):
            return StoreResult.DUPLICATE
        return StoreResult.CONFLICT

    async def mark_provisioning(
        self, task: QueuedTask, profile: SandboxProfile
    ) -> ProvisioningTask:
        record = ProvisioningTask(task, profile)
        self.provisioning[record.task_id] = record
        return record

    async def record_resources(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        return None

    async def mark_ready(
        self,
        task: ProvisioningTask,
        resources: ProvisionedSandbox,
        relay: RelayReference,
    ) -> ReadyTask:
        ready = ReadyTask(task, resources, relay)
        self.ready_tasks[ready.task_id] = ready
        self.ready_records[ready.task_id] = ReadyRecord(ready, self.now)
        return ready

    async def record_failure(self, task_id: str, stage: FailureStage) -> None:
        self.failures.append((task_id, stage))

    async def record_rollback(self, task_id: str) -> None:
        self.rollbacks.append(task_id)

    async def mark_active(
        self, task: ReadyRecord, activated_at: datetime
    ) -> ActiveRecord:
        record = ActiveRecord(task, activated_at)
        self.active_records[record.task_id] = record
        return record

    async def begin_termination(
        self,
        task: ReadyRecord | ActiveRecord,
        decision: TerminationDecision,
        started_at: datetime,
    ) -> TerminatingRecord:
        if task.task_id in self.terminating or task.task_id in self.terminal:
            raise RuntimeError("task already has a terminal decision")
        record = TerminatingRecord(task, decision, started_at)
        self.terminating[record.task_id] = record
        return record

    async def record_cleanup_step(
        self, task: TerminatingRecord, step: CleanupStep
    ) -> TerminatingRecord:
        if self.terminating.get(task.task_id) != task:
            raise RuntimeError("terminating record is stale")
        updated = replace(task, completed_steps=task.completed_steps + (step,))
        self.terminating[updated.task_id] = updated
        return updated

    async def mark_terminal(
        self, task: TerminatingRecord, completed_at: datetime
    ) -> TerminalRecord:
        existing = self.terminal.get(task.task_id)
        if existing is not None:
            return existing
        if self.terminating.get(task.task_id) != task:
            raise RuntimeError("terminating record is stale")
        record = TerminalRecord(
            task.task_id,
            task.decision.terminal_state,
            task.decision.reason,
            task.ready_task.resources.result_volume_reference,
            completed_at,
        )
        self.terminal[record.task_id] = record
        self.terminating.pop(record.task_id)
        return record

    async def recovery_records(self) -> tuple[RecoveryRecord, ...]:
        running: list[RecoveryRecord] = [
            record
            for task_id, record in self.ready_records.items()
            if task_id not in self.active_records
            and task_id not in self.terminating
            and task_id not in self.terminal
        ]
        running.extend(
            record
            for task_id, record in self.active_records.items()
            if task_id not in self.terminating and task_id not in self.terminal
        )
        running.extend(self.terminating.values())
        return tuple(running)


class FakeProvisioner:
    """Kubernetes provisioner double that observes created sandboxes."""

    def __init__(self) -> None:
        self.provisioned: list[str] = []
        self.deleted: list[str] = []
        self.active = 0
        self.maximum_active = 0

    async def provision(self, task: ProvisioningTask) -> ProvisionedSandbox:
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        await asyncio.sleep(0)
        self.provisioned.append(task.task_id)
        return ProvisionedSandbox(
            f"pod/default/{task.task_id}",
            f"uid-pod-{task.task_id}",
            f"pvc/default/{task.task_id}",
            f"uid-pvc-{task.task_id}",
        )

    async def wait_ready(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        await asyncio.sleep(0)

    async def rollback(self, task_id: str) -> None:
        self.deleted.append(task_id)


class FakeCleaner:
    """Ephemeral-resource cleaner double recording each cleanup step."""

    def __init__(self, provisioner: FakeProvisioner) -> None:
        self.provisioner = provisioner
        self.events: list[str] = []
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def stop_workload(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> None:
        self.events.append("stop_workload")
        self.started.set()
        if self.release is not None:
            await self.release.wait()

    async def delete_ephemeral_resources(
        self,
        task_id: str,
        resource_reference: str,
        resource_uid: str,
        result_volume_uid: str,
    ) -> None:
        self.events.append("delete_ephemeral_resources")
        self.provisioner.deleted.append(task_id)


class FakeAdmission:
    """Command-admission closer double recording closed task IDs."""

    def __init__(self) -> None:
        self.closed: list[str] = []

    async def begin_termination(self, task_id: str) -> None:
        self.closed.append(task_id)


class FakePublisher:
    """Ready-response publisher double collecting published endpoints."""

    def __init__(self) -> None:
        self.published: list[ReadyTask] = []

    async def publish_ready(self, task: ReadyTask) -> None:
        self.published.append(task)


class SequentialNonces:
    """Nonce provider double issuing distinct values per challenge."""

    def __init__(self) -> None:
        self._next = 0

    def next_nonce(self) -> str:
        self._next += 1
        return f"nonce-{self._next}"


class SilentAgentTransport:
    """Health transport double for an agent that never answers."""

    def __init__(self) -> None:
        self.challenges: list[HealthChallenge] = []

    async def exchange(
        self, challenge: HealthChallenge
    ) -> AuthenticatedHealthResponse | None:
        self.challenges.append(challenge)
        return None


class RecordingHealthRepository:
    """Health persistence double echoing each recorded success."""

    def __init__(self) -> None:
        self.successes: list[HealthSuccess] = []

    async def record_health_success(
        self, task: RunningRecord, success: HealthSuccess
    ) -> HealthSuccess:
        self.successes.append(success)
        return success


class SandboxFlow:
    """Wires the production orchestrators over the in-memory ports."""

    def __init__(self, maximum: int = 10) -> None:
        self.store = InMemoryStateStore()
        self.capacity = CapacityLimiter(maximum)
        self.provisioner = FakeProvisioner()
        self.publisher = FakePublisher()
        self.cleaner = FakeCleaner(self.provisioner)
        self.intake = SpawnIntakeProcessor(self.store)
        self.orchestrator = ProvisioningOrchestrator(
            SandboxProfileRegistry([python_profile()]),
            self.capacity,
            self.store,
            self.provisioner,
            self.publisher,
        )
        self.lifecycle = LifecycleManager(
            self.store, self.cleaner, self.capacity, FakeAdmission()
        )

    async def deliver(self, payload: bytes) -> QueueDelivery:
        """Deliver one spawn request from the durable queue and process it."""

        delivery = QueueDelivery(payload)
        await self.intake.process(delivery)
        return delivery

    async def submit(self, task_id: str) -> IntakeResult:
        """Deliver one spawn request and return its intake classification."""

        return await self.intake.process(QueueDelivery(spawn_payload(task_id)))

    async def dispatch(self, task_id: str) -> ProvisioningResult:
        """Run the worker step that drives one durably queued task."""

        return await self.orchestrator.provision(self.store.tasks[task_id])

    async def activate(self, task_id: str) -> ActiveRecord:
        """Mark a ready task as in use by its owning agent session."""

        self.store.now = self.store.now + timedelta(seconds=1)
        return await self.lifecycle.activate(
            self.store.ready_records[task_id], self.store.now
        )

    async def close(
        self,
        task_id: str,
        trigger: TerminationTrigger = TerminationTrigger.AGENT_UNRESPONSIVE,
        agent_session_id: str | None = None,
    ) -> TerminalRecord:
        """Terminate one running sandbox through the full cleanup path."""

        self.store.now = self.store.now + timedelta(seconds=1)
        running: ReadyRecord | ActiveRecord = self.store.active_records.get(
            task_id, self.store.ready_records[task_id]
        )
        return await self.lifecycle.terminate(
            running, trigger, self.store.now, agent_session_id=agent_session_id
        )


class SandboxStartTests(unittest.IsolatedAsyncioTestCase):
    """A submitted request starts exactly one sandbox with one connection."""

    async def test_request_produces_one_ready_sandbox_and_connection(self) -> None:
        flow = SandboxFlow()

        delivery = await flow.deliver(spawn_payload("task-1"))
        result = await flow.dispatch("task-1")

        self.assertEqual(delivery.acknowledgements, 1)
        self.assertEqual(result.status, ProvisioningStatus.READY)
        ready = result.ready_task
        assert ready is not None
        self.assertEqual(ready.state, ProvisioningState.READY)
        self.assertEqual(flow.provisioner.provisioned, ["task-1"])
        self.assertEqual(flow.publisher.published, [ready])
        self.assertEqual(ready.relay.route, "sandbox.agent.session-1.admit")
        self.assertEqual(ready.relay.protocol_version, 1)
        self.assertEqual(ready.resources.result_volume_reference, "pvc/default/task-1")

    async def test_redelivered_request_creates_no_second_sandbox(self) -> None:
        flow = SandboxFlow()
        await flow.submit("task-1")
        await flow.dispatch("task-1")

        duplicate = await flow.submit("task-1")

        self.assertEqual(duplicate, IntakeResult.DUPLICATE)
        self.assertEqual(flow.provisioner.provisioned, ["task-1"])


class SandboxCloseTests(unittest.IsolatedAsyncioTestCase):
    """Closing a sandbox cleans up, retains results, and frees its slot."""

    async def test_connection_close_is_not_a_recognized_trigger(self) -> None:
        self.assertEqual(
            {trigger.name for trigger in TerminationTrigger},
            {
                "COMPLETION",
                "CANCELLATION",
                "DEADLINE",
                "POD_LOSS",
                "PLATFORM_FAILURE",
                "AGENT_UNRESPONSIVE",
            },
        )

    async def test_unresponsive_agent_closes_sandbox_and_frees_capacity(
        self,
    ) -> None:
        flow = SandboxFlow()
        await flow.submit("task-1")
        await flow.dispatch("task-1")
        await flow.activate("task-1")

        terminal = await flow.close("task-1", TerminationTrigger.AGENT_UNRESPONSIVE)

        self.assertEqual(
            (terminal.state, terminal.reason),
            (TerminalState.TIMED_OUT, TerminalReason.AGENT_UNRESPONSIVE),
        )
        self.assertEqual(
            flow.cleaner.events,
            ["stop_workload", "delete_ephemeral_resources"],
        )
        self.assertEqual(flow.provisioner.deleted, ["task-1"])
        self.assertEqual(terminal.result_volume_reference, "pvc/default/task-1")

        self.assertTrue(await flow.capacity.try_reserve("task-replacement"))
        await flow.capacity.release("task-replacement")

    async def test_agent_that_stops_answering_is_closed_by_supervision(
        self,
    ) -> None:
        flow = SandboxFlow()
        flow.store.now = datetime.now(UTC) - timedelta(minutes=1)
        transport = SilentAgentTransport()
        health_repository = RecordingHealthRepository()
        engine = HealthCheckEngine(
            HealthCheckConfig(
                interval=timedelta(seconds=15), timeout=timedelta(seconds=5)
            ),
            SystemHealthClock(),
            SequentialNonces(),
            transport,
            "epoch-1",
        )
        supervisor = HealthLifecycleSupervisor(
            engine, health_repository, flow.lifecycle
        )
        await flow.submit("task-1")
        await flow.dispatch("task-1")
        active = await flow.activate("task-1")

        terminal = await supervisor.run(active)

        self.assertEqual(
            (terminal.state, terminal.reason),
            (TerminalState.TIMED_OUT, TerminalReason.AGENT_UNRESPONSIVE),
        )
        self.assertEqual(len(transport.challenges), 1)
        self.assertEqual(health_repository.successes, [])
        self.assertEqual(
            flow.cleaner.events,
            ["stop_workload", "delete_ephemeral_resources"],
        )
        self.assertTrue(await flow.capacity.try_reserve("task-replacement"))
        await flow.capacity.release("task-replacement")

    async def test_competing_managers_preserve_the_first_terminal_decision(
        self,
    ) -> None:
        flow = SandboxFlow()
        flow.cleaner.release = asyncio.Event()
        await flow.submit("task-1")
        await flow.dispatch("task-1")
        active = await flow.activate("task-1")
        competing = LifecycleManager(
            flow.store, flow.cleaner, flow.capacity, FakeAdmission()
        )
        first = asyncio.create_task(
            flow.lifecycle.terminate(
                active,
                TerminationTrigger.COMPLETION,
                flow.store.now + timedelta(seconds=1),
                agent_session_id="session-1",
            )
        )
        await flow.cleaner.started.wait()

        with self.assertRaises(LifecyclePersistenceError):
            await competing.terminate(
                active,
                TerminationTrigger.POD_LOSS,
                flow.store.now + timedelta(seconds=1),
            )

        flow.cleaner.release.set()
        terminal = await first
        self.assertEqual(terminal.state, TerminalState.COMPLETED)
        self.assertEqual(flow.store.terminal["task-1"], terminal)
        self.assertEqual(flow.cleaner.events.count("stop_workload"), 1)


class SandboxCapacityTests(unittest.IsolatedAsyncioTestCase):
    """The eleventh request stays queued until a sandbox closes."""

    async def test_eleventh_request_waits_until_capacity_is_released(self) -> None:
        flow = SandboxFlow(maximum=10)
        for number in range(10):
            await flow.submit(f"task-{number}")

        results = await asyncio.gather(
            *(flow.dispatch(f"task-{number}") for number in range(10))
        )

        self.assertEqual(
            {result.status for result in results}, {ProvisioningStatus.READY}
        )
        self.assertEqual(flow.provisioner.maximum_active, 10)

        await flow.submit("task-10")
        pending = await flow.dispatch("task-10")

        self.assertEqual(pending.status, ProvisioningStatus.AT_CAPACITY)
        self.assertNotIn("task-10", flow.provisioner.provisioned)
        self.assertEqual(flow.store.tasks["task-10"].state, TaskState.QUEUED)

        await flow.close(
            "task-0",
            TerminationTrigger.COMPLETION,
            agent_session_id="session-1",
        )
        admitted = await flow.dispatch("task-10")

        self.assertEqual(admitted.status, ProvisioningStatus.READY)
        self.assertEqual(len(flow.provisioner.provisioned), 11)
        self.assertEqual(flow.provisioner.provisioned[-1], "task-10")


if __name__ == "__main__":
    unittest.main()
