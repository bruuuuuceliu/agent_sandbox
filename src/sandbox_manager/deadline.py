"""Asynchronous task deadline scheduling."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from .lifecycle import ActiveRecord, RunningRecord, TerminalRecord, TerminationTrigger


class DeadlineClock(Protocol):
    def wall_time(self) -> datetime: ...

    def monotonic(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class DeadlineTerminator(Protocol):
    async def terminate(
        self,
        task: RunningRecord,
        trigger: TerminationTrigger,
        started_at: datetime,
        *,
        agent_session_id: str | None = None,
    ) -> TerminalRecord: ...


class DeadlineSchedulerClosedError(RuntimeError):
    """Raised when work is scheduled after the scheduler closes."""


class DeadlineTerminationInProgressError(RuntimeError):
    """Raised when a deadline is already terminating the task."""


@dataclass(frozen=True, slots=True)
class DeadlineFailure:
    task_id: str
    cause: Exception = field(repr=False)


class SystemDeadlineClock:
    def wall_time(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class DeadlineScheduler:
    def __init__(self, clock: DeadlineClock, terminator: DeadlineTerminator) -> None:
        self._clock = clock
        self._terminator = terminator
        self._timers: dict[str, asyncio.Task[None]] = {}
        self._inflight: dict[str, asyncio.Task[None]] = {}
        self._failures: asyncio.Queue[DeadlineFailure] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def pending_task_ids(self) -> frozenset[str]:
        return frozenset(self._timers.keys() | self._inflight.keys())

    async def next_failure(self) -> DeadlineFailure:
        return await self._failures.get()

    async def schedule(self, task: RunningRecord) -> bool:
        deadline = _requested_deadline(task)
        async with self._lock:
            if self._closed:
                raise DeadlineSchedulerClosedError("deadline scheduler is closed")
            if task.task_id in self._inflight:
                raise DeadlineTerminationInProgressError(
                    f"deadline termination is in progress: {task.task_id}"
                )
            await self._cancel_locked(task.task_id)
            if deadline is None:
                return False
            timer = asyncio.create_task(
                self._wait_for_deadline(task, deadline),
                name=f"sandbox-deadline-{task.task_id}",
            )
            self._timers[task.task_id] = timer
            return True

    async def cancel(self, task_id: str) -> bool:
        async with self._lock:
            return await self._cancel_locked(task_id)

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            timers = tuple(self._timers.values())
            inflight = tuple(self._inflight.values())
            self._timers.clear()
            for timer in timers:
                timer.cancel()
        jobs = timers + inflight
        if jobs:
            await asyncio.gather(*jobs, return_exceptions=True)

    async def _cancel_locked(self, task_id: str) -> bool:
        timer = self._timers.pop(task_id, None)
        if timer is None:
            return False
        timer.cancel()
        await asyncio.gather(timer, return_exceptions=True)
        return True

    async def _wait_for_deadline(self, task: RunningRecord, deadline: datetime) -> None:
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError("deadline timer has no owning task")
        claimed = False
        failure: Exception | None = None
        try:
            wall_now = _aware_utc(self._clock.wall_time(), "clock wall time")
            target = self._clock.monotonic() + max(
                0.0, (deadline - wall_now).total_seconds()
            )
            while (remaining := target - self._clock.monotonic()) > 0:
                await self._clock.sleep(remaining)

            if not await self._claim(task.task_id):
                return
            claimed = True
            await self._terminator.terminate(
                task,
                TerminationTrigger.DEADLINE,
                _aware_utc(self._clock.wall_time(), "clock wall time"),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            failure = error
        finally:
            await asyncio.shield(self._release(task.task_id, claimed, owner))
        if failure is not None:
            await self._failures.put(DeadlineFailure(task.task_id, failure))

    async def _claim(self, task_id: str) -> bool:
        timer = asyncio.current_task()
        async with self._lock:
            if self._timers.get(task_id) is not timer:
                return False
            del self._timers[task_id]
            if timer is not None:
                self._inflight[task_id] = timer
            return True

    async def _release(
        self, task_id: str, claimed: bool, owner: asyncio.Task[None]
    ) -> None:
        jobs = self._inflight if claimed else self._timers
        if jobs.get(task_id) is not owner:
            return
        async with self._lock:
            if jobs.get(task_id) is owner:
                del jobs[task_id]


def _requested_deadline(task: RunningRecord) -> datetime | None:
    ready_task = (
        task.ready_record.ready_task
        if isinstance(task, ActiveRecord)
        else task.ready_task
    )
    deadline = ready_task.provisioning_task.queued_task.request.requested_deadline
    if deadline is None:
        return None
    return _aware_utc(deadline, "requested deadline")


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)
