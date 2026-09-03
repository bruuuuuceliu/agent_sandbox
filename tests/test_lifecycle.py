from __future__ import annotations

import asyncio
import unittest
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta

from sandbox_manager.intake import QueuedTask
from sandbox_manager.lifecycle import (
    ActiveRecord,
    CleanupStep,
    LifecycleManager,
    LifecycleOwnershipError,
    LifecyclePersistenceError,
    LifecycleReconciler,
    LifecycleRepositoryError,
    LifecycleState,
    ReadyRecord,
    RecoveryRecord,
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
)
from sandbox_manager.provisioning import (
    ProvisionedSandbox,
    ProvisioningTask,
    ReadyTask,
    RelayReference,
)
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest

_READY_AT = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
_LATER = _READY_AT + timedelta(minutes=1)


def ready_record(task_id: str = "task-1") -> ReadyRecord:
    request = SpawnRequest(
        schema_version=SCHEMA_VERSION,
        request_id=f"request-{task_id}",
        task_id=task_id,
        agent_session_id="session-1",
        sandbox_type="python",
    )
    profile = SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )
    provisioning = ProvisioningTask(QueuedTask.from_request(request), profile)
    ready = ReadyTask(
        provisioning,
        ProvisionedSandbox(
            f"pod/default/{task_id}",
            f"uid-pod-{task_id}",
            f"pvc/default/{task_id}",
            f"uid-pvc-{task_id}",
        ),
        RelayReference(1, "sandbox.agent.session-1.admit"),
    )
    return ReadyRecord(ready, _READY_AT)


class RecordingRepository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.begin_error: Exception | None = None
        self.step_error: CleanupStep | None = None
        self.decision_override: TerminationDecision | None = None
        self.records: tuple[RecoveryRecord, ...] = ()

    async def mark_active(
        self, task: ReadyRecord, activated_at: datetime
    ) -> ActiveRecord:
        self.events.append("mark_active")
        return ActiveRecord(task, activated_at)

    async def begin_termination(
        self,
        task: ReadyRecord | ActiveRecord,
        decision: TerminationDecision,
        started_at: datetime,
    ) -> TerminatingRecord:
        self.events.append("begin_termination")
        if self.begin_error is not None:
            raise self.begin_error
        return TerminatingRecord(task, self.decision_override or decision, started_at)

    async def record_cleanup_step(
        self, task: TerminatingRecord, step: CleanupStep
    ) -> TerminatingRecord:
        self.events.append(f"record_{step.value}")
        if self.step_error is step:
            raise OSError("state unavailable")
        return replace(task, completed_steps=task.completed_steps + (step,))

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
        self.events.append("recovery_records")
        return self.records


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
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def try_reserve(self, task_id: str) -> bool:
        return True

    async def release(self, task_id: str) -> None:
        self.events.append("release_capacity")


class RecordingAdmission:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def begin_termination(self, task_id: str) -> None:
        self.events.append("close_admission")


class FailingAdmission:
    """Admission closer double whose durable close fails."""

    async def begin_termination(self, task_id: str) -> None:
        raise OSError("admission store unavailable")


class RecordingObserver:
    def __init__(self, existing: set[str]) -> None:
        self.existing = existing
        self.observed: list[tuple[str, str]] = []

    async def workload_exists(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> bool:
        self.observed.append((task_id, resource_uid))
        return task_id in self.existing


class FixedClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


class BlockingCleaner(RecordingCleaner):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stop_workload(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> None:
        await super().stop_workload(task_id, resource_reference, resource_uid)
        self.started.set()
        await self.release.wait()


def components() -> tuple[
    list[str], RecordingRepository, LifecycleManager, RecordingObserver
]:
    events: list[str] = []
    repository = RecordingRepository(events)
    manager = LifecycleManager(
        repository,
        RecordingCleaner(events),
        RecordingCapacity(events),
        RecordingAdmission(events),
        FixedClock(_LATER + timedelta(seconds=1)),
    )
    return events, repository, manager, RecordingObserver(set())


class LifecycleManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_records_are_immutable_and_activation_is_durable(self) -> None:
        events, _, manager, _ = components()
        ready = ready_record()

        active = await manager.activate(ready, _LATER)

        self.assertEqual(active.state, LifecycleState.ACTIVE)
        self.assertEqual(events, ["mark_active"])
        with self.assertRaises(FrozenInstanceError):
            active.activated_at = _LATER + timedelta(minutes=1)  # type: ignore[misc]

    async def test_each_trigger_selects_the_expected_terminal_outcome(self) -> None:
        cases = {
            TerminationTrigger.COMPLETION: (
                TerminalState.COMPLETED,
                TerminalReason.AGENT_COMPLETED,
            ),
            TerminationTrigger.CANCELLATION: (
                TerminalState.CANCELLED,
                TerminalReason.AGENT_CANCELLED,
            ),
            TerminationTrigger.DEADLINE: (
                TerminalState.TIMED_OUT,
                TerminalReason.DEADLINE_EXCEEDED,
            ),
            TerminationTrigger.POD_LOSS: (
                TerminalState.FAILED,
                TerminalReason.POD_LOST,
            ),
            TerminationTrigger.PLATFORM_FAILURE: (
                TerminalState.FAILED,
                TerminalReason.PLATFORM_FAILURE,
            ),
            TerminationTrigger.AGENT_UNRESPONSIVE: (
                TerminalState.TIMED_OUT,
                TerminalReason.AGENT_UNRESPONSIVE,
            ),
        }

        for trigger, (state, reason) in cases.items():
            with self.subTest(trigger=trigger):
                events, _, manager, _ = components()
                owner = (
                    "session-1"
                    if trigger
                    in {
                        TerminationTrigger.COMPLETION,
                        TerminationTrigger.CANCELLATION,
                    }
                    else None
                )
                terminal = await manager.terminate(
                    ready_record(), trigger, _LATER, agent_session_id=owner
                )

                self.assertEqual((terminal.state, terminal.reason), (state, reason))
                self.assertEqual(
                    events,
                    [
                        "begin_termination",
                        "close_admission",
                        "record_revoke_access",
                        "stop_workload",
                        "record_stop_workload",
                        "delete_ephemeral_resources",
                        "record_delete_ephemeral_resources",
                        "release_capacity",
                        "record_release_capacity",
                        "mark_terminal",
                    ],
                )

    async def test_cleanup_preserves_the_result_volume_reference(self) -> None:
        events, _, manager, _ = components()

        terminal = await manager.terminate(
            ready_record(),
            TerminationTrigger.COMPLETION,
            _LATER,
            agent_session_id="session-1",
        )

        self.assertEqual(terminal.result_volume_reference, "pvc/default/task-1")
        self.assertFalse(any("pvc" in event for event in events))

    async def test_ambiguous_termination_persistence_prevents_cleanup(self) -> None:
        events, repository, manager, _ = components()
        repository.begin_error = TimeoutError("write outcome unknown")

        with self.assertRaises(LifecyclePersistenceError) as raised:
            await manager.terminate(
                ready_record(),
                TerminationTrigger.CANCELLATION,
                _LATER,
                agent_session_id="session-1",
            )

        self.assertEqual(raised.exception.operation, "begin_termination")
        self.assertEqual(events, ["begin_termination"])

    async def test_repository_cannot_substitute_a_termination_decision(self) -> None:
        events, repository, manager, _ = components()
        repository.decision_override = TerminationDecision(
            TerminationTrigger.CANCELLATION,
            TerminalState.CANCELLED,
            TerminalReason.AGENT_CANCELLED,
        )

        with self.assertRaises(LifecycleRepositoryError):
            await manager.terminate(
                ready_record(),
                TerminationTrigger.COMPLETION,
                _LATER,
                agent_session_id="session-1",
            )

        self.assertEqual(events, ["begin_termination"])

    async def test_same_task_lifecycle_operations_are_serialized(self) -> None:
        events: list[str] = []
        repository = RecordingRepository(events)
        cleaner = BlockingCleaner(events)
        manager = LifecycleManager(
            repository,
            cleaner,
            RecordingCapacity(events),
            RecordingAdmission(events),
            FixedClock(_LATER + timedelta(seconds=1)),
        )
        task = ready_record()
        first = asyncio.create_task(
            manager.terminate(task, TerminationTrigger.PLATFORM_FAILURE, _LATER)
        )
        await cleaner.started.wait()
        second = asyncio.create_task(
            manager.terminate(task, TerminationTrigger.POD_LOSS, _LATER)
        )
        await asyncio.sleep(0)

        self.assertEqual(events.count("begin_termination"), 1)

        second.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await second
        cleaner.release.set()
        await first

    async def test_terminal_timestamp_follows_completed_cleanup(self) -> None:
        events: list[str] = []
        repository = RecordingRepository(events)
        cleaner = BlockingCleaner(events)
        clock = FixedClock(_LATER)
        manager = LifecycleManager(
            repository,
            cleaner,
            RecordingCapacity(events),
            RecordingAdmission(events),
            clock,
        )
        terminating = asyncio.create_task(
            manager.terminate(
                ready_record(), TerminationTrigger.PLATFORM_FAILURE, _LATER
            )
        )
        await cleaner.started.wait()

        completed_at = _LATER + timedelta(minutes=2)
        clock.current = completed_at
        cleaner.release.set()

        terminal = await terminating
        self.assertEqual(terminal.completed_at, completed_at)

    async def test_ambiguous_step_persistence_stops_later_cleanup(self) -> None:
        events, repository, manager, _ = components()
        repository.step_error = CleanupStep.REVOKE_ACCESS

        with self.assertRaises(LifecyclePersistenceError):
            await manager.terminate(
                ready_record(), TerminationTrigger.PLATFORM_FAILURE, _LATER
            )

        self.assertEqual(
            events,
            [
                "begin_termination",
                "close_admission",
                "record_revoke_access",
            ],
        )

    async def test_admission_close_failure_prevents_cleanup(self) -> None:
        events: list[str] = []
        repository = RecordingRepository(events)
        manager = LifecycleManager(
            repository,
            RecordingCleaner(events),
            RecordingCapacity(events),
            FailingAdmission(),
            FixedClock(_LATER),
        )

        with self.assertRaises(LifecyclePersistenceError) as raised:
            await manager.terminate(
                ready_record(), TerminationTrigger.PLATFORM_FAILURE, _LATER
            )

        self.assertEqual(raised.exception.operation, "close_command_admission")
        self.assertEqual(events, ["begin_termination"])

    async def test_agent_trigger_requires_the_owning_session(self) -> None:
        events, _, manager, _ = components()

        with self.assertRaises(LifecycleOwnershipError):
            await manager.terminate(
                ready_record(),
                TerminationTrigger.COMPLETION,
                _LATER,
                agent_session_id="another-session",
            )

        self.assertEqual(events, [])

    async def test_restart_resumes_after_the_last_durable_cleanup_step(self) -> None:
        events, repository, manager, observer = components()
        decision = TerminationDecision(
            TerminationTrigger.CANCELLATION,
            TerminalState.CANCELLED,
            TerminalReason.AGENT_CANCELLED,
        )
        terminating = TerminatingRecord(
            ready_record(),
            decision,
            _LATER,
            (CleanupStep.REVOKE_ACCESS, CleanupStep.STOP_WORKLOAD),
        )
        repository.records = (terminating,)

        report = await LifecycleReconciler(repository, manager, observer).reconcile(
            _LATER + timedelta(minutes=1)
        )

        self.assertEqual(len(report.terminal_records), 1)
        self.assertEqual(report.failures, ())
        self.assertNotIn("stop_workload", events)
        self.assertIn("close_admission", events)
        self.assertLess(
            events.index("close_admission"),
            events.index("delete_ephemeral_resources"),
        )
        self.assertIn("delete_ephemeral_resources", events)

    async def test_restart_terminates_missing_ready_and_active_workloads(self) -> None:
        events, repository, manager, _ = components()
        healthy = ready_record("task-healthy")
        missing = ready_record("task-missing")
        active_missing = ActiveRecord(ready_record("task-active"), _LATER)
        repository.records = (healthy, missing, active_missing)
        observer = RecordingObserver({"task-healthy"})

        report = await LifecycleReconciler(repository, manager, observer).reconcile(
            _LATER + timedelta(minutes=1)
        )

        self.assertEqual(report.healthy_task_ids, ("task-healthy",))
        self.assertEqual(
            {record.task_id for record in report.terminal_records},
            {"task-missing", "task-active"},
        )
        self.assertTrue(
            all(
                record.reason is TerminalReason.POD_LOST
                for record in report.terminal_records
            )
        )
        self.assertEqual(report.failures, ())
        self.assertEqual(events.count("begin_termination"), 2)
        self.assertEqual(
            observer.observed,
            [
                ("task-healthy", "uid-pod-task-healthy"),
                ("task-missing", "uid-pod-task-missing"),
                ("task-active", "uid-pod-task-active"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
