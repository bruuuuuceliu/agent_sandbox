"""Durable health supervision for running sandboxes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from .health import HealthCheckEngine, HealthCheckResult, HealthStatus
from .lifecycle import (
    RunningRecord,
    TerminalRecord,
    TerminationTrigger,
)


class HealthPersistenceError(RuntimeError):
    """Raised when a health mutation has no confirmed durable outcome."""

    def __init__(self, task_id: str, cause: Exception) -> None:
        self.task_id = task_id
        self.cause = cause
        super().__init__("durable health success could not be recorded")


class HealthRepositoryError(RuntimeError):
    """Raised when a repository returns inconsistent health state."""


@dataclass(frozen=True, slots=True)
class HealthSuccess:
    task_id: str
    agent_session_id: str
    process_epoch: str
    challenge_digest: str
    checked_at: datetime


class HealthLifecycleRepository(Protocol):
    async def record_health_success(
        self, task: RunningRecord, success: HealthSuccess
    ) -> HealthSuccess:
        """Persist success only while the exact running record remains current."""

        ...


class LifecycleTerminator(Protocol):
    async def terminate(
        self,
        task: RunningRecord,
        trigger: TerminationTrigger,
        started_at: datetime,
        *,
        agent_session_id: str | None = None,
    ) -> TerminalRecord: ...


class HealthLifecycleSupervisor:
    def __init__(
        self,
        engine: HealthCheckEngine,
        repository: HealthLifecycleRepository,
        lifecycle: LifecycleTerminator,
    ) -> None:
        self._engine = engine
        self._repository = repository
        self._lifecycle = lifecycle

    async def run(self, task: RunningRecord) -> TerminalRecord:
        async for result in self._engine.monitor(task.task_id, task.agent_session_id):
            if result.status is HealthStatus.RESPONSIVE:
                await self._persist_success(task, result)
                continue
            return await self._lifecycle.terminate(
                task,
                TerminationTrigger.AGENT_UNRESPONSIVE,
                result.completed_at,
            )
        raise RuntimeError("health monitor stopped without a terminal result")

    async def _persist_success(
        self, task: RunningRecord, result: HealthCheckResult
    ) -> None:
        challenge = result.challenge
        success = HealthSuccess(
            task_id=task.task_id,
            agent_session_id=task.agent_session_id,
            process_epoch=challenge.process_epoch,
            challenge_digest=_challenge_digest(
                challenge.process_epoch, challenge.nonce
            ),
            checked_at=result.completed_at,
        )
        try:
            persisted = await self._repository.record_health_success(task, success)
        except Exception as error:
            raise HealthPersistenceError(task.task_id, error) from error
        if persisted != success:
            raise HealthRepositoryError(
                "persisted health success does not match the observation"
            )


def _challenge_digest(process_epoch: str, nonce: str) -> str:
    value = f"{process_epoch}\0{nonce}".encode()
    return hashlib.sha256(value).hexdigest()
