from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from sandbox_manager.health import (
    AuthenticatedHealthResponse,
    HealthChallenge,
    HealthCheckConfig,
    HealthCheckEngine,
    HealthResponse,
)
from sandbox_manager.intake import QueuedTask
from sandbox_manager.lifecycle import (
    ActiveRecord,
    CleanupStep,
    LifecycleManager,
    ReadyRecord,
    RecoveryRecord,
    TerminalReason,
    TerminalRecord,
    TerminalState,
    TerminatingRecord,
    TerminationDecision,
)
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
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest
from sandbox_manager.supervision import (
    HealthLifecycleSupervisor,
    HealthPersistenceError,
    HealthRepositoryError,
    HealthSuccess,
)

_NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.current = _NOW
        self.monotonic_value = 100.0

    def wall_time(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return self.monotonic_value

    async def sleep(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)
        self.monotonic_value += seconds
        await asyncio.sleep(0)

    def now(self) -> datetime:
        return self.current


class SequentialNonces:
    def __init__(self) -> None:
        self.value = 0

    def next_nonce(self) -> str:
        self.value += 1
        return f"nonce-{self.value}"


class SequenceTransport:
    def __init__(self, responses: list[bool], *, identity: str = "session-1") -> None:
        self.responses = responses
        self.identity = identity

    async def exchange(
        self, challenge: HealthChallenge
    ) -> AuthenticatedHealthResponse | None:
        if not self.responses.pop(0):
            return None
        response = HealthResponse(
            challenge.nonce,
            challenge.task_id,
            challenge.agent_session_id,
            challenge.process_epoch,
        )
        return AuthenticatedHealthResponse(self.identity, response)


class RecordingHealthRepository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.error: Exception | None = None
        self.persisted: HealthSuccess | None = None
        self.successes: list[HealthSuccess] = []

    async def record_health_success(
        self, task: ReadyRecord | ActiveRecord, success: HealthSuccess
    ) -> HealthSuccess:
        self.events.append("record_health_success")
        if self.error is not None:
            raise self.error
        self.successes.append(success)
        return self.persisted or success


class RecordingLifecycleRepository:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def mark_active(
        self, task: ReadyRecord, activated_at: datetime
    ) -> ActiveRecord:
        return ActiveRecord(task, activated_at)

    async def begin_termination(
        self,
        task: ReadyRecord | ActiveRecord,
        decision: TerminationDecision,
        started_at: datetime,
    ) -> TerminatingRecord:
        self.events.append("begin_termination")
        return TerminatingRecord(task, decision, started_at)

    async def record_cleanup_step(
        self, task: TerminatingRecord, step: CleanupStep
    ) -> TerminatingRecord:
        self.events.append(f"record_{step.value}")
        return replace(task, completed_steps=(*task.completed_steps, step))

    async def mark_terminal(
        self, task: TerminatingRecord, completed_at: datetime
    ) -> TerminalRecord:
        self.events.append("mark_terminal")
        return TerminalRecord(
            task.task_id,
            task.decision.terminal_state,
            task.decision.reason,
            task.ready_task.resources.result_volume_reference,
            completed_at,
        )

    async def recovery_records(self) -> tuple[RecoveryRecord, ...]:
        return ()


class RecordingCleaner:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def stop_workload(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> None:
        self.events.append("stop_workload")

    async def delete_ephemeral_resources(
        self,
        task_id: str,
        resource_reference: str,
        resource_uid: str,
        result_volume_uid: str,
    ) -> None:
        self.events.append("delete_ephemeral_resources")


class RecordingCapacity:
    async def try_reserve(self, task_id: str) -> bool:
        return True

    async def release(self, task_id: str) -> None:
        return None


class RecordingAdmission:
    """Command-admission closer double recording closed task IDs."""

    def __init__(self) -> None:
        self.closed: list[str] = []

    async def begin_termination(self, task_id: str) -> None:
        self.closed.append(task_id)


def ready_record() -> ReadyRecord:
    request = SpawnRequest(
        SCHEMA_VERSION,
        "request-1",
        "task-1",
        "session-1",
        "python",
    )
    profile = SandboxProfile(
        PROFILE_SCHEMA_VERSION,
        1,
        "python",
        "registry.example/python@sha256:" + "a" * 64,
        ("python",),
        (ProfileMount("results", "/results"),),
    )
    ready = ReadyTask(
        ProvisioningTask(QueuedTask.from_request(request), profile),
        ProvisionedSandbox(
            "pod/default/task-1",
            "uid-pod-task-1",
            "pvc/default/task-1",
            "uid-pvc-task-1",
        ),
        RelayReference(1, "sandbox.agent.session-1.admit"),
    )
    return ReadyRecord(ready, _NOW)


def supervisor(
    responses: list[bool],
    health_repository: RecordingHealthRepository,
    events: list[str],
    *,
    identity: str = "session-1",
) -> HealthLifecycleSupervisor:
    clock = FakeClock()
    engine = HealthCheckEngine(
        HealthCheckConfig(timedelta(seconds=15), timedelta(seconds=5)),
        clock,
        SequentialNonces(),
        SequenceTransport(responses, identity=identity),
        process_epoch="manager-epoch-1",
    )
    lifecycle = LifecycleManager(
        RecordingLifecycleRepository(events),
        RecordingCleaner(events),
        RecordingCapacity(),
        RecordingAdmission(),
        clock,
    )
    return HealthLifecycleSupervisor(engine, health_repository, lifecycle)


class HealthLifecycleSupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_then_timeout_completes_the_lifecycle(self) -> None:
        events: list[str] = []
        health_repository = RecordingHealthRepository(events)

        terminal = await supervisor([True, False], health_repository, events).run(
            ready_record()
        )

        self.assertEqual(terminal.state, TerminalState.TIMED_OUT)
        self.assertEqual(terminal.reason, TerminalReason.AGENT_UNRESPONSIVE)
        self.assertEqual(terminal.result_volume_reference, "pvc/default/task-1")
        self.assertEqual(len(health_repository.successes), 1)
        self.assertEqual(
            events[:3],
            [
                "record_health_success",
                "begin_termination",
                "record_revoke_access",
            ],
        )

    async def test_health_persistence_ambiguity_prevents_cleanup(self) -> None:
        events: list[str] = []
        health_repository = RecordingHealthRepository(events)
        health_repository.error = TimeoutError("write outcome unknown")

        with self.assertRaises(HealthPersistenceError):
            await supervisor([True], health_repository, events).run(ready_record())

        self.assertEqual(events, ["record_health_success"])

    async def test_transport_identity_mismatch_terminates_without_success(self) -> None:
        events: list[str] = []
        health_repository = RecordingHealthRepository(events)

        terminal = await supervisor(
            [True], health_repository, events, identity="another-session"
        ).run(ready_record())

        self.assertEqual(terminal.reason, TerminalReason.AGENT_UNRESPONSIVE)
        self.assertEqual(health_repository.successes, [])
        self.assertEqual(events[0], "begin_termination")

    async def test_inconsistent_health_record_prevents_cleanup(self) -> None:
        events: list[str] = []
        health_repository = RecordingHealthRepository(events)
        health_repository.persisted = HealthSuccess(
            "another-task",
            "session-1",
            "manager-epoch-1",
            "a" * 64,
            _NOW,
        )

        with self.assertRaises(HealthRepositoryError):
            await supervisor([True], health_repository, events).run(ready_record())

        self.assertEqual(events, ["record_health_success"])


if __name__ == "__main__":
    unittest.main()
