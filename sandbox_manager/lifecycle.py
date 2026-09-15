"""Durable sandbox lifecycle and termination orchestration."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from .commands import AdmissionCloser
from .provisioning import CapacityPort, ReadyTask


class LifecycleState(StrEnum):
    READY = "READY"
    ACTIVE = "ACTIVE"
    TERMINATING = "TERMINATING"


class TerminalState(StrEnum):
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"


class TerminationTrigger(StrEnum):
    COMPLETION = "completion"
    CANCELLATION = "cancellation"
    DEADLINE = "deadline"
    POD_LOSS = "pod_loss"
    PLATFORM_FAILURE = "platform_failure"
    AGENT_UNRESPONSIVE = "agent_unresponsive"


class TerminalReason(StrEnum):
    AGENT_COMPLETED = "AGENT_COMPLETED"
    AGENT_CANCELLED = "AGENT_CANCELLED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    POD_LOST = "POD_LOST"
    PLATFORM_FAILURE = "PLATFORM_FAILURE"
    AGENT_UNRESPONSIVE = "AGENT_UNRESPONSIVE"


class CleanupStep(StrEnum):
    REVOKE_ACCESS = "revoke_access"
    STOP_WORKLOAD = "stop_workload"
    DELETE_EPHEMERAL_RESOURCES = "delete_ephemeral_resources"
    RELEASE_CAPACITY = "release_capacity"


_CLEANUP_ORDER = tuple(CleanupStep)


@dataclass(frozen=True, slots=True)
class ReadyRecord:
    ready_task: ReadyTask
    ready_at: datetime
    state: LifecycleState = field(default=LifecycleState.READY, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ready_at", _timestamp(self.ready_at, "ready time"))

    @property
    def task_id(self) -> str:
        return self.ready_task.task_id

    @property
    def agent_session_id(self) -> str:
        return self.ready_task.provisioning_task.queued_task.request.agent_session_id


@dataclass(frozen=True, slots=True)
class ActiveRecord:
    ready_record: ReadyRecord
    activated_at: datetime
    state: LifecycleState = field(default=LifecycleState.ACTIVE, init=False)

    def __post_init__(self) -> None:
        activated_at = _timestamp(self.activated_at, "activation time")
        if activated_at < self.ready_record.ready_at:
            raise ValueError("activation time precedes ready time")
        object.__setattr__(self, "activated_at", activated_at)

    @property
    def task_id(self) -> str:
        return self.ready_record.task_id

    @property
    def agent_session_id(self) -> str:
        return self.ready_record.agent_session_id


RunningRecord = ReadyRecord | ActiveRecord


@dataclass(frozen=True, slots=True)
class TerminationDecision:
    trigger: TerminationTrigger
    terminal_state: TerminalState
    reason: TerminalReason


@dataclass(frozen=True, slots=True)
class TerminatingRecord:
    running_record: RunningRecord
    decision: TerminationDecision
    started_at: datetime
    completed_steps: tuple[CleanupStep, ...] = ()
    state: LifecycleState = field(default=LifecycleState.TERMINATING, init=False)

    def __post_init__(self) -> None:
        started_at = _timestamp(self.started_at, "termination start time")
        if started_at < _ready_record(self.running_record).ready_at:
            raise ValueError("termination start time precedes ready time")
        object.__setattr__(self, "started_at", started_at)

        steps = tuple(self.completed_steps)
        if steps != _CLEANUP_ORDER[: len(steps)]:
            raise ValueError("cleanup steps must form an ordered prefix")
        object.__setattr__(self, "completed_steps", steps)

    @property
    def task_id(self) -> str:
        return self.running_record.task_id

    @property
    def ready_task(self) -> ReadyTask:
        return _ready_record(self.running_record).ready_task


@dataclass(frozen=True, slots=True)
class TerminalRecord:
    task_id: str
    state: TerminalState
    reason: TerminalReason
    result_volume_reference: str
    completed_at: datetime

    def __post_init__(self) -> None:
        _reference(self.task_id, "task ID")
        _reference(self.result_volume_reference, "result volume reference")
        object.__setattr__(
            self, "completed_at", _timestamp(self.completed_at, "completion time")
        )


RecoveryRecord = ReadyRecord | ActiveRecord | TerminatingRecord


class LifecyclePersistenceError(RuntimeError):
    """Raised when a lifecycle mutation has no confirmed durable outcome."""

    def __init__(self, task_id: str, operation: str, cause: Exception) -> None:
        self.task_id = task_id
        self.operation = operation
        self.cause = cause
        super().__init__(f"durable lifecycle operation failed: {operation}")


class CleanupError(RuntimeError):
    """Raised when an idempotent cleanup step fails."""

    def __init__(
        self, task: TerminatingRecord, step: CleanupStep, cause: Exception
    ) -> None:
        self.task = task
        self.step = step
        self.cause = cause
        super().__init__(f"cleanup failed during {step.value}")


class LifecycleRepositoryError(RuntimeError):
    """Raised when a repository returns an inconsistent lifecycle record."""


class LifecycleOwnershipError(PermissionError):
    """Raised when an agent lifecycle command does not own the task."""


class LifecycleRepository(Protocol):
    async def mark_active(
        self, task: ReadyRecord, activated_at: datetime
    ) -> ActiveRecord:
        """Durably transition a ready task to active."""

        ...

    async def begin_termination(
        self,
        task: RunningRecord,
        decision: TerminationDecision,
        started_at: datetime,
    ) -> TerminatingRecord:
        """Atomically select the requested terminal outcome for the source record."""

        ...

    async def record_cleanup_step(
        self, task: TerminatingRecord, step: CleanupStep
    ) -> TerminatingRecord:
        """Durably record one completed cleanup step."""

        ...

    async def mark_terminal(
        self, task: TerminatingRecord, completed_at: datetime
    ) -> TerminalRecord:
        """Durably finish a completely cleaned terminating task."""

        ...

    async def recovery_records(self) -> Sequence[RecoveryRecord]:
        """Load running and incompletely terminating tasks after restart."""

        ...


class EphemeralSandboxCleaner(Protocol):
    async def stop_workload(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> None:
        """Stop the sandbox workload idempotently."""

        ...

    async def delete_ephemeral_resources(
        self,
        task_id: str,
        resource_reference: str,
        resource_uid: str,
        result_volume_uid: str,
    ) -> None:
        """Delete ephemeral task resources without deleting result storage."""

        ...


class WorkloadObserver(Protocol):
    async def workload_exists(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> bool: ...


class LifecycleClock(Protocol):
    def now(self) -> datetime: ...


class SystemLifecycleClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class LifecycleManager:
    def __init__(
        self,
        repository: LifecycleRepository,
        cleaner: EphemeralSandboxCleaner,
        capacity: CapacityPort,
        admission: AdmissionCloser,
        clock: LifecycleClock | None = None,
    ) -> None:
        self._repository = repository
        self._cleaner = cleaner
        self._capacity = capacity
        self._admission = admission
        self._clock = clock or SystemLifecycleClock()
        self._task_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._task_locks_guard = asyncio.Lock()

    async def activate(self, task: ReadyRecord, activated_at: datetime) -> ActiveRecord:
        lock = await self._task_lock(task.task_id)
        async with lock:
            return await self._activate(task, activated_at)

    async def _activate(
        self, task: ReadyRecord, activated_at: datetime
    ) -> ActiveRecord:
        activated_at = _timestamp(activated_at, "activation time")
        try:
            active = await self._repository.mark_active(task, activated_at)
        except Exception as error:
            raise LifecyclePersistenceError(
                task.task_id, "mark_active", error
            ) from error
        if active.ready_record != task or active.activated_at != activated_at:
            raise LifecycleRepositoryError("active record does not match ready task")
        return active

    async def terminate(
        self,
        task: RunningRecord,
        trigger: TerminationTrigger,
        started_at: datetime,
        *,
        agent_session_id: str | None = None,
    ) -> TerminalRecord:
        _validate_trigger_owner(task, trigger, agent_session_id)
        started_at = _timestamp(started_at, "termination start time")
        lock = await self._task_lock(task.task_id)
        async with lock:
            return await self._terminate(task, trigger, started_at)

    async def _terminate(
        self,
        task: RunningRecord,
        trigger: TerminationTrigger,
        started_at: datetime,
    ) -> TerminalRecord:
        decision = _decision(trigger)
        try:
            terminating = await self._repository.begin_termination(
                task, decision, started_at
            )
        except Exception as error:
            raise LifecyclePersistenceError(
                task.task_id, "begin_termination", error
            ) from error
        _validate_termination_source(task, decision, started_at, terminating)
        return await self._resume_termination(terminating, started_at)

    async def resume_termination(
        self, task: TerminatingRecord, completed_at: datetime
    ) -> TerminalRecord:
        completed_at = _timestamp(completed_at, "completion time")
        lock = await self._task_lock(task.task_id)
        async with lock:
            return await self._resume_termination(task, completed_at)

    async def _resume_termination(
        self, task: TerminatingRecord, completed_at: datetime
    ) -> TerminalRecord:
        # Admission closes before the first cleanup step: once termination
        # has won the durable lifecycle race, no new command may start. The
        # closer is idempotent, so restart resumes and cleanup retries
        # re-run this safely.
        try:
            await self._admission.begin_termination(task.task_id)
        except Exception as error:
            raise LifecyclePersistenceError(
                task.task_id, "close_command_admission", error
            ) from error
        current = task
        for step in _CLEANUP_ORDER[len(current.completed_steps) :]:
            try:
                await self._run_cleanup_step(current, step)
            except Exception as error:
                raise CleanupError(current, step, error) from error

            try:
                updated = await self._repository.record_cleanup_step(current, step)
            except Exception as error:
                raise LifecyclePersistenceError(
                    current.task_id, f"record_{step.value}", error
                ) from error
            _validate_cleanup_progress(current, updated, step)
            current = updated

        terminal_at = max(
            completed_at,
            _timestamp(self._clock.now(), "completion time"),
        )
        try:
            terminal = await self._repository.mark_terminal(current, terminal_at)
        except Exception as error:
            raise LifecyclePersistenceError(
                current.task_id, "mark_terminal", error
            ) from error
        _validate_terminal(current, terminal)
        return terminal

    async def _task_lock(self, task_id: str) -> asyncio.Lock:
        async with self._task_locks_guard:
            lock = self._task_locks.get(task_id)
            if lock is None:
                lock = asyncio.Lock()
                self._task_locks[task_id] = lock
            return lock

    async def _run_cleanup_step(
        self, task: TerminatingRecord, step: CleanupStep
    ) -> None:
        task_id = task.task_id
        resource_reference = task.ready_task.resources.resource_reference
        if step is CleanupStep.REVOKE_ACCESS:
            # Relay admission was closed before the first cleanup step; the
            # sandbox has no inbound network path to revoke, so this step
            # only makes the revocation durable in the termination record.
            return
        elif step is CleanupStep.STOP_WORKLOAD:
            await self._cleaner.stop_workload(
                task_id, resource_reference, task.ready_task.resources.resource_uid
            )
        elif step is CleanupStep.DELETE_EPHEMERAL_RESOURCES:
            resources = task.ready_task.resources
            await self._cleaner.delete_ephemeral_resources(
                task_id,
                resource_reference,
                resources.resource_uid,
                resources.result_volume_uid,
            )
        elif step is CleanupStep.RELEASE_CAPACITY:
            await self._capacity.release(task_id)


@dataclass(frozen=True, slots=True)
class ReconciliationFailure:
    task_id: str
    cause: Exception = field(repr=False)


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    healthy_task_ids: tuple[str, ...]
    terminal_records: tuple[TerminalRecord, ...]
    failures: tuple[ReconciliationFailure, ...]


class LifecycleReconciler:
    def __init__(
        self,
        repository: LifecycleRepository,
        manager: LifecycleManager,
        observer: WorkloadObserver,
    ) -> None:
        self._repository = repository
        self._manager = manager
        self._observer = observer

    async def reconcile(self, observed_at: datetime) -> ReconciliationReport:
        observed_at = _timestamp(observed_at, "observation time")
        records = await self._repository.recovery_records()
        healthy: list[str] = []
        terminal: list[TerminalRecord] = []
        failures: list[ReconciliationFailure] = []

        for record in records:
            try:
                if isinstance(record, TerminatingRecord):
                    completed = await self._manager.resume_termination(
                        record, observed_at
                    )
                else:
                    ready_task = _ready_record(record).ready_task
                    exists = await self._observer.workload_exists(
                        record.task_id,
                        ready_task.resources.resource_reference,
                        ready_task.resources.resource_uid,
                    )
                    if exists:
                        healthy.append(record.task_id)
                        continue
                    completed = await self._manager.terminate(
                        record, TerminationTrigger.POD_LOSS, observed_at
                    )
                terminal.append(completed)
            except Exception as error:  # noqa: BLE001
                failures.append(ReconciliationFailure(record.task_id, error))

        return ReconciliationReport(tuple(healthy), tuple(terminal), tuple(failures))


def _decision(trigger: TerminationTrigger) -> TerminationDecision:
    if trigger is TerminationTrigger.COMPLETION:
        return TerminationDecision(
            trigger, TerminalState.COMPLETED, TerminalReason.AGENT_COMPLETED
        )
    if trigger is TerminationTrigger.CANCELLATION:
        return TerminationDecision(
            trigger, TerminalState.CANCELLED, TerminalReason.AGENT_CANCELLED
        )
    if trigger is TerminationTrigger.DEADLINE:
        return TerminationDecision(
            trigger, TerminalState.TIMED_OUT, TerminalReason.DEADLINE_EXCEEDED
        )
    if trigger is TerminationTrigger.POD_LOSS:
        return TerminationDecision(
            trigger, TerminalState.FAILED, TerminalReason.POD_LOST
        )
    if trigger is TerminationTrigger.PLATFORM_FAILURE:
        return TerminationDecision(
            trigger, TerminalState.FAILED, TerminalReason.PLATFORM_FAILURE
        )
    if trigger is TerminationTrigger.AGENT_UNRESPONSIVE:
        return TerminationDecision(
            trigger, TerminalState.TIMED_OUT, TerminalReason.AGENT_UNRESPONSIVE
        )
    raise ValueError(f"unsupported termination trigger: {trigger}")


def _validate_trigger_owner(
    task: RunningRecord,
    trigger: TerminationTrigger,
    agent_session_id: str | None,
) -> None:
    if trigger not in {
        TerminationTrigger.COMPLETION,
        TerminationTrigger.CANCELLATION,
    }:
        return
    owner = _ready_record(
        task
    ).ready_task.provisioning_task.queued_task.request.agent_session_id
    if agent_session_id != owner:
        raise LifecycleOwnershipError("agent session does not own the task")


def _ready_record(task: RunningRecord) -> ReadyRecord:
    if isinstance(task, ActiveRecord):
        return task.ready_record
    return task


def _validate_termination_source(
    source: RunningRecord,
    decision: TerminationDecision,
    started_at: datetime,
    terminating: TerminatingRecord,
) -> None:
    if (
        terminating.task_id != source.task_id
        or terminating.running_record != source
        or terminating.decision != decision
        or terminating.started_at != started_at
    ):
        raise LifecycleRepositoryError(
            "terminating record does not match the requested transition"
        )


def _validate_cleanup_progress(
    previous: TerminatingRecord,
    updated: TerminatingRecord,
    step: CleanupStep,
) -> None:
    expected = (*previous.completed_steps, step)
    if (
        updated.task_id != previous.task_id
        or updated.running_record != previous.running_record
        or updated.ready_task.resources != previous.ready_task.resources
        or updated.decision != previous.decision
        or updated.started_at != previous.started_at
        or updated.completed_steps != expected
    ):
        raise LifecycleRepositoryError("cleanup progress record is inconsistent")


def _validate_terminal(
    terminating: TerminatingRecord, terminal: TerminalRecord
) -> None:
    decision = terminating.decision
    if (
        terminal.task_id != terminating.task_id
        or terminal.state is not decision.terminal_state
        or terminal.reason is not decision.reason
        or terminal.result_volume_reference
        != terminating.ready_task.resources.result_volume_reference
        or terminal.completed_at < terminating.started_at
    ):
        raise LifecycleRepositoryError("terminal record is inconsistent")


def _timestamp(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)


def _reference(value: str, field_name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise ValueError(f"{field_name} contains invalid text")
