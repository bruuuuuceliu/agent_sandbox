from __future__ import annotations

import asyncio
import unittest
from datetime import UTC, datetime, timedelta

from sandbox_manager.deadline import DeadlineScheduler, DeadlineSchedulerClosedError
from sandbox_manager.intake import QueuedTask
from sandbox_manager.lifecycle import (
    ActiveRecord,
    ReadyRecord,
    RunningRecord,
    TerminalReason,
    TerminalRecord,
    TerminalState,
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

_START = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.wall = _START
        self.elapsed = 100.0
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []
        self.maximum_sleepers = 0

    def wall_time(self) -> datetime:
        return self.wall

    def monotonic(self) -> float:
        return self.elapsed

    async def sleep(self, seconds: float) -> None:
        target = self.elapsed + seconds
        future = asyncio.get_running_loop().create_future()
        sleeper = (target, future)
        self._sleepers.append(sleeper)
        self.maximum_sleepers = max(self.maximum_sleepers, self.sleepers)
        try:
            await future
        finally:
            self._sleepers.remove(sleeper)

    @property
    def sleepers(self) -> int:
        return sum(not future.done() for _, future in self._sleepers)

    async def advance(self, seconds: float) -> None:
        self.elapsed += seconds
        self.wall += timedelta(seconds=seconds)
        for target, future in tuple(self._sleepers):
            if target <= self.elapsed and not future.done():
                future.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)


class RecordingTerminator:
    def __init__(self) -> None:
        self.calls: list[tuple[RunningRecord, TerminationTrigger, datetime]] = []
        self.called = asyncio.Event()
        self.release: asyncio.Event | None = None
        self.error: Exception | None = None

    async def terminate(
        self,
        task: RunningRecord,
        trigger: TerminationTrigger,
        started_at: datetime,
        *,
        agent_session_id: str | None = None,
    ) -> TerminalRecord:
        self.calls.append((task, trigger, started_at))
        self.called.set()
        if self.release is not None:
            await self.release.wait()
        if self.error is not None:
            raise self.error
        return TerminalRecord(
            task.task_id,
            TerminalState.TIMED_OUT,
            TerminalReason.DEADLINE_EXCEEDED,
            _ready_task(task).resources.result_volume_reference,
            started_at,
        )


def running_record(deadline: datetime | None, *, active: bool = False) -> RunningRecord:
    request = SpawnRequest(
        schema_version=SCHEMA_VERSION,
        request_id="request-1",
        task_id="task-1",
        agent_session_id="session-1",
        sandbox_type="python",
        requested_deadline=deadline,
    )
    profile = SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )
    ready_task = ReadyTask(
        ProvisioningTask(QueuedTask.from_request(request), profile),
        ProvisionedSandbox(
            "pod/default/task-1",
            "uid-pod-task-1",
            "pvc/default/task-1",
            "uid-pvc-task-1",
        ),
        RelayReference(1, "sandbox.agent.session-1.admit"),
    )
    ready = ReadyRecord(ready_task, _START)
    return ActiveRecord(ready, _START) if active else ready


def _ready_task(task: RunningRecord) -> ReadyTask:
    return (
        task.ready_record.ready_task
        if isinstance(task, ActiveRecord)
        else task.ready_task
    )


class DeadlineSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clock = FakeClock()
        self.terminator = RecordingTerminator()
        self.scheduler = DeadlineScheduler(self.clock, self.terminator)

    async def asyncTearDown(self) -> None:
        await self.scheduler.close()

    async def test_terminates_at_the_requested_deadline(self) -> None:
        record = running_record(_START + timedelta(seconds=10))

        scheduled = await self.scheduler.schedule(record)
        await asyncio.sleep(0)
        await self.clock.advance(9)

        self.assertTrue(scheduled)
        self.assertFalse(self.terminator.called.is_set())
        self.assertEqual(self.scheduler.pending_task_ids, {"task-1"})

        await self.clock.advance(1)
        await self.terminator.called.wait()

        task, trigger, started_at = self.terminator.calls[0]
        self.assertIs(task, record)
        self.assertIs(trigger, TerminationTrigger.DEADLINE)
        self.assertEqual(started_at, _START + timedelta(seconds=10))
        self.assertEqual(self.scheduler.pending_task_ids, set())

    async def test_cancellation_prevents_deadline_termination(self) -> None:
        await self.scheduler.schedule(running_record(_START + timedelta(seconds=10)))
        await asyncio.sleep(0)

        cancelled = await self.scheduler.cancel("task-1")
        await self.clock.advance(10)

        self.assertTrue(cancelled)
        self.assertFalse(await self.scheduler.cancel("task-1"))
        self.assertEqual(self.terminator.calls, [])

    async def test_expired_deadline_terminates_without_sleeping(self) -> None:
        await self.scheduler.schedule(running_record(_START - timedelta(seconds=1)))

        await self.terminator.called.wait()

        self.assertEqual(len(self.terminator.calls), 1)
        self.assertEqual(self.clock.maximum_sleepers, 0)

    async def test_replacement_has_only_one_timer_and_uses_latest_record(self) -> None:
        ready = running_record(_START + timedelta(seconds=10))
        active = running_record(_START + timedelta(seconds=10), active=True)

        await self.scheduler.schedule(ready)
        await asyncio.sleep(0)
        await self.scheduler.schedule(active)
        await asyncio.sleep(0)

        self.assertEqual(self.clock.sleepers, 1)
        self.assertEqual(self.clock.maximum_sleepers, 1)

        await self.clock.advance(10)
        await self.terminator.called.wait()

        self.assertEqual(len(self.terminator.calls), 1)
        self.assertIs(self.terminator.calls[0][0], active)

    async def test_missing_deadline_cancels_an_existing_timer(self) -> None:
        await self.scheduler.schedule(running_record(_START + timedelta(seconds=10)))
        await asyncio.sleep(0)

        scheduled = await self.scheduler.schedule(running_record(None))
        await self.clock.advance(10)

        self.assertFalse(scheduled)
        self.assertEqual(self.terminator.calls, [])
        self.assertEqual(self.scheduler.pending_task_ids, set())

    async def test_close_cancels_timers_and_rejects_new_work(self) -> None:
        await self.scheduler.schedule(running_record(_START + timedelta(seconds=10)))
        await asyncio.sleep(0)

        await self.scheduler.close()

        self.assertEqual(self.scheduler.pending_task_ids, set())
        with self.assertRaises(DeadlineSchedulerClosedError):
            await self.scheduler.schedule(
                running_record(_START + timedelta(seconds=20))
            )

    async def test_reports_deadline_termination_failure(self) -> None:
        self.terminator.error = TimeoutError("state unavailable")
        await self.scheduler.schedule(running_record(_START + timedelta(seconds=1)))
        await asyncio.sleep(0)

        await self.clock.advance(1)
        failure = await self.scheduler.next_failure()

        self.assertEqual(failure.task_id, "task-1")
        self.assertIs(failure.cause, self.terminator.error)
        self.assertEqual(self.scheduler.pending_task_ids, set())

    async def test_reports_timer_setup_and_sleep_failures(self) -> None:
        class FailingClock(FakeClock):
            async def sleep(self, seconds: float) -> None:
                raise OSError("clock interrupted")

        scheduler = DeadlineScheduler(FailingClock(), self.terminator)
        self.addAsyncCleanup(scheduler.close)

        await scheduler.schedule(running_record(_START + timedelta(seconds=1)))
        failure = await scheduler.next_failure()

        self.assertEqual(failure.task_id, "task-1")
        self.assertIsInstance(failure.cause, OSError)
        self.assertEqual(scheduler.pending_task_ids, set())

    async def test_close_waits_for_inflight_termination(self) -> None:
        self.terminator.release = asyncio.Event()
        await self.scheduler.schedule(running_record(_START))
        await self.terminator.called.wait()

        closing = asyncio.create_task(self.scheduler.close())
        await asyncio.sleep(0)

        self.assertFalse(closing.done())
        self.assertEqual(self.scheduler.pending_task_ids, {"task-1"})

        self.terminator.release.set()
        await closing
        self.assertEqual(self.scheduler.pending_task_ids, set())


if __name__ == "__main__":
    unittest.main()
