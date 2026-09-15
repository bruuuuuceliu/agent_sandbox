"""Asynchronous sandbox provisioning orchestration."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, TypeVar

from .intake import QueuedTask
from .nats_relay import admission_subject
from .profiles import (
    SandboxProfile,
    SandboxProfileRegistry,
    UnknownSandboxTypeError,
)
from .relay_protocol import RELAY_SCHEMA_VERSION


class ProvisioningState(StrEnum):
    PROVISIONING = "PROVISIONING"
    READY = "READY"


class ProvisioningStatus(StrEnum):
    AT_CAPACITY = "at_capacity"
    READY = "ready"


class FailureStage(StrEnum):
    PROFILE_RESOLUTION = "profile_resolution"
    PROVISIONING_PERSISTENCE = "provisioning_persistence"
    RESOURCE_CREATION = "resource_creation"
    RESOURCE_PERSISTENCE = "resource_persistence"
    READINESS = "readiness"
    READY_PERSISTENCE = "ready_persistence"


@dataclass(frozen=True, slots=True)
class ProvisioningTask:
    queued_task: QueuedTask
    profile: SandboxProfile
    state: ProvisioningState = field(default=ProvisioningState.PROVISIONING, init=False)

    @property
    def task_id(self) -> str:
        return self.queued_task.request.task_id


@dataclass(frozen=True, slots=True)
class ProvisionedSandbox:
    resource_reference: str
    resource_uid: str
    result_volume_reference: str
    result_volume_uid: str

    def __post_init__(self) -> None:
        _require_reference(self.resource_reference, "resource reference")
        _require_reference(self.resource_uid, "resource UID")
        _require_reference(self.result_volume_reference, "result volume reference")
        _require_reference(self.result_volume_uid, "result volume UID")


@dataclass(frozen=True, slots=True)
class RelayReference:
    """Where the owning agent session sends commands for one task.

    Carries the relay protocol version and admission route without exposing a
    sandbox endpoint or credential.
    """

    protocol_version: int
    route: str

    def __post_init__(self) -> None:
        if type(self.protocol_version) is not int or self.protocol_version < 1:
            raise ValueError("relay protocol version must be a positive integer")
        _require_reference(self.route, "relay route")


@dataclass(frozen=True, slots=True)
class ReadyTask:
    provisioning_task: ProvisioningTask
    resources: ProvisionedSandbox
    relay: RelayReference
    state: ProvisioningState = field(default=ProvisioningState.READY, init=False)

    @property
    def task_id(self) -> str:
        return self.provisioning_task.task_id


@dataclass(frozen=True, slots=True)
class ProvisioningResult:
    status: ProvisioningStatus
    ready_task: ReadyTask | None = None

    def __post_init__(self) -> None:
        has_ready_task = self.ready_task is not None
        if has_ready_task != (self.status is ProvisioningStatus.READY):
            raise ValueError("ready result must contain a ready task")


class ProvisioningError(RuntimeError):
    """Reports a provisioning failure and any incomplete recovery work."""

    def __init__(
        self,
        task_id: str,
        stage: FailureStage,
        cause: BaseException,
        *,
        failure_record_error: Exception | None = None,
        rollback_error: Exception | None = None,
        rollback_record_error: Exception | None = None,
        capacity_release_error: Exception | None = None,
    ) -> None:
        self.task_id = task_id
        self.stage = stage
        self.cause = cause
        self.failure_record_error = failure_record_error
        self.rollback_error = rollback_error
        self.rollback_record_error = rollback_record_error
        self.capacity_release_error = capacity_release_error

        incomplete: list[str] = []
        if failure_record_error is not None:
            incomplete.append("failure record")
        if rollback_error is not None:
            incomplete.append("rollback")
        if rollback_record_error is not None:
            incomplete.append("rollback record")
        if capacity_release_error is not None:
            incomplete.append("capacity release")
        suffix = "" if not incomplete else f"; incomplete: {', '.join(incomplete)}"
        super().__init__(f"provisioning failed during {stage.value}{suffix}")

    @property
    def recovery_incomplete(self) -> bool:
        return any(
            error is not None
            for error in (
                self.failure_record_error,
                self.rollback_error,
                self.rollback_record_error,
                self.capacity_release_error,
            )
        )


class ReadyPublicationError(RuntimeError):
    """Raised when a durable ready task could not be published."""

    def __init__(self, ready_task: ReadyTask, cause: Exception) -> None:
        self.ready_task = ready_task
        self.cause = cause
        super().__init__("durable ready task could not be published")


class CapacityPort(Protocol):
    async def try_reserve(self, task_id: str) -> bool:
        """Reserve capacity for a task without waiting for another slot."""

        ...

    async def release(self, task_id: str) -> None:
        """Release a task reservation idempotently."""

        ...


class ProvisioningRepository(Protocol):
    async def mark_provisioning(
        self, task: QueuedTask, profile: SandboxProfile
    ) -> ProvisioningTask:
        """Durably transition a queued task to provisioning."""

        ...

    async def mark_ready(
        self,
        task: ProvisioningTask,
        resources: ProvisionedSandbox,
        relay: RelayReference,
    ) -> ReadyTask:
        """Durably transition a provisioning task to ready."""

        ...

    async def record_resources(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        """Durably associate the resource identities with a provisioning task."""

        ...

    async def record_failure(self, task_id: str, stage: FailureStage) -> None:
        """Durably record a failure before any required resource rollback."""

        ...

    async def record_rollback(self, task_id: str) -> None:
        """Durably record successful ephemeral-resource rollback."""

        ...


class KubernetesProvisioner(Protocol):
    async def provision(self, task: ProvisioningTask) -> ProvisionedSandbox:
        """Create or reconcile the resources for a provisioning task."""

        ...

    async def wait_ready(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        """Wait until the sandbox Pod can accept relayed commands."""

        ...

    async def rollback(self, task_id: str) -> None:
        """Remove ephemeral task resources idempotently and preserve results."""

        ...


class ReadyPublisher(Protocol):
    async def publish_ready(self, task: ReadyTask) -> None:
        """Publish a ready task idempotently."""

        ...


class CapacityLimiter:
    def __init__(self, maximum: int, active_task_ids: set[str] | None = None) -> None:
        if type(maximum) is not int or maximum < 1:
            raise ValueError("capacity maximum must be a positive integer")
        active = set() if active_task_ids is None else set(active_task_ids)
        if len(active) > maximum:
            raise ValueError("active tasks exceed capacity maximum")
        for task_id in active:
            _require_reference(task_id, "task ID")
        self._maximum = maximum
        self._active = active
        self._lock = asyncio.Lock()

    async def try_reserve(self, task_id: str) -> bool:
        _require_reference(task_id, "task ID")
        async with self._lock:
            if task_id in self._active:
                return True
            if len(self._active) >= self._maximum:
                return False
            self._active.add(task_id)
            return True

    async def release(self, task_id: str) -> None:
        async with self._lock:
            self._active.discard(task_id)


@dataclass(slots=True)
class _TaskLockEntry:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    users: int = 0


class _TaskCoordinator:
    def __init__(self) -> None:
        self._entries: dict[str, _TaskLockEntry] = {}
        self._guard = asyncio.Lock()

    @asynccontextmanager
    async def hold(self, task_id: str) -> AsyncIterator[None]:
        async with self._guard:
            entry = self._entries.setdefault(task_id, _TaskLockEntry())
            entry.users += 1
        try:
            await entry.lock.acquire()
        except BaseException:
            await self._discard(task_id, entry)
            raise
        try:
            yield
        finally:
            entry.lock.release()
            await self._discard(task_id, entry)

    async def _discard(self, task_id: str, entry: _TaskLockEntry) -> None:
        async with self._guard:
            entry.users -= 1
            if entry.users == 0:
                self._entries.pop(task_id, None)


class ProvisioningOrchestrator:
    def __init__(
        self,
        profiles: SandboxProfileRegistry,
        capacity: CapacityPort,
        repository: ProvisioningRepository,
        provisioner: KubernetesProvisioner,
        publisher: ReadyPublisher,
    ) -> None:
        self._profiles = profiles
        self._capacity = capacity
        self._repository = repository
        self._provisioner = provisioner
        self._publisher = publisher
        self._tasks = _TaskCoordinator()

    async def provision(self, task: QueuedTask) -> ProvisioningResult:
        task_id = task.request.task_id
        async with self._tasks.hold(task_id):
            return await self._provision(task)

    async def _provision(self, task: QueuedTask) -> ProvisioningResult:
        task_id = task.request.task_id
        try:
            profile = self._profiles.resolve(task.request.sandbox_type)
        except UnknownSandboxTypeError as error:
            try:
                await self._repository.record_failure(
                    task_id, FailureStage.PROFILE_RESOLUTION
                )
            except Exception as record_error:  # noqa: BLE001
                raise ProvisioningError(
                    task_id,
                    FailureStage.PROFILE_RESOLUTION,
                    error,
                    failure_record_error=record_error,
                ) from error
            raise

        if not await self._capacity.try_reserve(task_id):
            return ProvisioningResult(ProvisioningStatus.AT_CAPACITY)

        try:
            provisioning_task = await self._repository.mark_provisioning(task, profile)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise ProvisioningError(
                task_id, FailureStage.PROVISIONING_PERSISTENCE, error
            ) from error

        try:
            resources = await self._provisioner.provision(provisioning_task)
        except asyncio.CancelledError as error:
            recovery = await _complete_during_cancellation(
                self._recover(task_id, FailureStage.RESOURCE_CREATION, error)
            )
            if recovery.recovery_incomplete:
                raise recovery from error
            raise
        except Exception as error:
            raise await self._recover(
                task_id, FailureStage.RESOURCE_CREATION, error
            ) from error

        try:
            await self._repository.record_resources(provisioning_task, resources)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise ProvisioningError(
                task_id, FailureStage.RESOURCE_PERSISTENCE, error
            ) from error

        try:
            await self._provisioner.wait_ready(provisioning_task, resources)
            relay = RelayReference(
                protocol_version=RELAY_SCHEMA_VERSION,
                route=admission_subject(task.request.agent_session_id),
            )
        except asyncio.CancelledError as error:
            recovery = await _complete_during_cancellation(
                self._recover(task_id, FailureStage.READINESS, error)
            )
            if recovery.recovery_incomplete:
                raise recovery from error
            raise
        except Exception as error:
            raise await self._recover(task_id, FailureStage.READINESS, error) from error

        try:
            ready_task = await self._repository.mark_ready(
                provisioning_task, resources, relay
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise ProvisioningError(
                task_id, FailureStage.READY_PERSISTENCE, error
            ) from error

        try:
            await self._publisher.publish_ready(ready_task)
        except Exception as error:
            raise ReadyPublicationError(ready_task, error) from error
        return ProvisioningResult(ProvisioningStatus.READY, ready_task)

    async def _recover(
        self, task_id: str, stage: FailureStage, cause: BaseException
    ) -> ProvisioningError:
        failure_record_error: Exception | None = None
        rollback_error: Exception | None = None
        rollback_record_error: Exception | None = None
        capacity_release_error: Exception | None = None

        try:
            await self._repository.record_failure(task_id, stage)
        except Exception as error:  # noqa: BLE001
            failure_record_error = error

        if failure_record_error is not None:
            return ProvisioningError(
                task_id,
                stage,
                cause,
                failure_record_error=failure_record_error,
            )

        try:
            await self._provisioner.rollback(task_id)
        except Exception as error:  # noqa: BLE001
            rollback_error = error

        if rollback_error is None:
            try:
                await self._repository.record_rollback(task_id)
            except Exception as error:  # noqa: BLE001
                rollback_record_error = error
            try:
                await self._capacity.release(task_id)
            except Exception as error:  # noqa: BLE001
                capacity_release_error = error

        return ProvisioningError(
            task_id,
            stage,
            cause,
            failure_record_error=failure_record_error,
            rollback_error=rollback_error,
            rollback_record_error=rollback_record_error,
            capacity_release_error=capacity_release_error,
        )


def _require_reference(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field} contains invalid text")


_T = TypeVar("_T")


async def _complete_during_cancellation(operation: Awaitable[_T]) -> _T:
    task = asyncio.ensure_future(operation)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()
