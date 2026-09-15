"""Asynchronous spawn request intake."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from .spawn import SpawnRequest, decode_spawn_request


class TaskState(StrEnum):
    QUEUED = "QUEUED"


class StoreResult(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class IntakeResult(StrEnum):
    CREATED = "created"
    DUPLICATE = "duplicate"


class IdentifierConflictError(RuntimeError):
    """Raised when a request or task identifier refers to different input."""


@dataclass(frozen=True, slots=True)
class QueuedTask:
    request: SpawnRequest
    request_fingerprint: str
    state: TaskState = field(default=TaskState.QUEUED, init=False)

    @classmethod
    def from_request(cls, request: SpawnRequest) -> QueuedTask:
        return cls(request=request, request_fingerprint=request.fingerprint())

    def __post_init__(self) -> None:
        if self.request_fingerprint != self.request.fingerprint():
            raise ValueError("request fingerprint does not match request")


class TaskRepository(Protocol):
    async def accept_spawn(self, task: QueuedTask) -> StoreResult:
        """Durably create a queued task or atomically classify its identity."""

        ...


class SpawnDelivery(Protocol):
    @property
    def payload(self) -> bytes: ...

    async def acknowledge(self) -> None: ...


class SpawnIntakeProcessor:
    def __init__(self, repository: TaskRepository) -> None:
        self._repository = repository

    async def process(self, delivery: SpawnDelivery) -> IntakeResult:
        request = decode_spawn_request(delivery.payload)
        result = await self._repository.accept_spawn(QueuedTask.from_request(request))

        if result is StoreResult.CONFLICT:
            raise IdentifierConflictError(
                "request_id or task_id is already associated with different input"
            )
        if result is StoreResult.CREATED:
            intake_result = IntakeResult.CREATED
        elif result is StoreResult.DUPLICATE:
            intake_result = IntakeResult.DUPLICATE
        else:
            raise RuntimeError("task repository returned an unknown result")

        await delivery.acknowledge()
        return intake_result
