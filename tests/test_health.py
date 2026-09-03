from __future__ import annotations

import asyncio
import unittest
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

from sandbox_manager.health import (
    AuthenticatedHealthResponse,
    HealthChallenge,
    HealthCheckConfig,
    HealthCheckEngine,
    HealthCheckInProgressError,
    HealthFailure,
    HealthModelError,
    HealthResponse,
    HealthStatus,
)


class FakeClock:
    def __init__(self) -> None:
        self.current_wall = datetime(2026, 8, 28, 12, tzinfo=UTC)
        self.current_monotonic = 100.0
        self.sleeps: list[float] = []

    def wall_time(self) -> datetime:
        return self.current_wall

    def monotonic(self) -> float:
        return self.current_monotonic

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)
        await asyncio.sleep(0)

    def advance(self, seconds: float) -> None:
        self.current_monotonic += seconds
        self.current_wall += timedelta(seconds=seconds)


class SequentialNonces:
    def __init__(self) -> None:
        self.value = 0

    def next_nonce(self) -> str:
        self.value += 1
        return f"nonce-{self.value}"


ResponseFactory = Callable[[HealthChallenge], object | None]


class FunctionTransport:
    def __init__(self, response: ResponseFactory) -> None:
        self._response = response
        self.challenges: list[HealthChallenge] = []

    async def exchange(
        self, challenge: HealthChallenge
    ) -> AuthenticatedHealthResponse | None:
        self.challenges.append(challenge)
        response = self._response(challenge)
        if response is None:
            return None
        return AuthenticatedHealthResponse(challenge.agent_session_id, response)


class BlockingTransport:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def exchange(
        self, challenge: HealthChallenge
    ) -> AuthenticatedHealthResponse | None:
        self.started.set()
        await self.release.wait()
        return AuthenticatedHealthResponse(
            challenge.agent_session_id, matching_response(challenge)
        )


class NeverRespondingTransport:
    async def exchange(
        self, challenge: HealthChallenge
    ) -> AuthenticatedHealthResponse | None:
        await asyncio.Event().wait()
        return None


class RepeatingNonces:
    def next_nonce(self) -> str:
        return "repeated-nonce"


def matching_response(challenge: HealthChallenge) -> HealthResponse:
    return HealthResponse(
        nonce=challenge.nonce,
        task_id=challenge.task_id,
        agent_session_id=challenge.agent_session_id,
        process_epoch=challenge.process_epoch,
    )


def engine(
    clock: FakeClock,
    transport: FunctionTransport | BlockingTransport | NeverRespondingTransport,
    *,
    interval: timedelta = timedelta(seconds=15),
    timeout: timedelta = timedelta(seconds=5),
) -> HealthCheckEngine:
    return HealthCheckEngine(
        HealthCheckConfig(interval=interval, timeout=timeout),
        clock,
        SequentialNonces(),
        transport,
        process_epoch="manager-epoch-1",
    )


class HealthCheckEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_matching_response_is_responsive(self) -> None:
        clock = FakeClock()
        transport = FunctionTransport(matching_response)

        result = await engine(clock, transport).check("task-1", "session-1")

        self.assertEqual(result.status, HealthStatus.RESPONSIVE)
        self.assertIsNone(result.failure)
        self.assertEqual(result.challenge.issued_at, clock.current_wall)
        self.assertEqual(
            result.challenge.expires_at - result.challenge.issued_at,
            timedelta(seconds=5),
        )
        self.assertEqual(result.challenge.process_epoch, "manager-epoch-1")
        with self.assertRaises(FrozenInstanceError):
            result.challenge.nonce = "changed"  # type: ignore[misc]

    async def test_classifies_unresponsive_responses(self) -> None:
        cases: dict[str, tuple[ResponseFactory, HealthFailure]] = {
            "missing": (lambda _challenge: None, HealthFailure.MISSING),
            "malformed": (
                lambda _challenge: {"nonce": "value"},
                HealthFailure.MALFORMED,
            ),
            "empty field": (
                lambda challenge: HealthResponse(
                    "",
                    challenge.task_id,
                    challenge.agent_session_id,
                    challenge.process_epoch,
                ),
                HealthFailure.MALFORMED,
            ),
            "nonce": (
                lambda challenge: HealthResponse(
                    "other",
                    challenge.task_id,
                    challenge.agent_session_id,
                    challenge.process_epoch,
                ),
                HealthFailure.NONCE_MISMATCH,
            ),
            "task": (
                lambda challenge: HealthResponse(
                    challenge.nonce,
                    "other",
                    challenge.agent_session_id,
                    challenge.process_epoch,
                ),
                HealthFailure.TASK_MISMATCH,
            ),
            "session": (
                lambda challenge: HealthResponse(
                    challenge.nonce,
                    challenge.task_id,
                    "other",
                    challenge.process_epoch,
                ),
                HealthFailure.SESSION_MISMATCH,
            ),
            "epoch": (
                lambda challenge: HealthResponse(
                    challenge.nonce,
                    challenge.task_id,
                    challenge.agent_session_id,
                    "previous-manager-epoch",
                ),
                HealthFailure.EPOCH_MISMATCH,
            ),
        }

        for name, (response, failure) in cases.items():
            with self.subTest(name=name):
                result = await engine(FakeClock(), FunctionTransport(response)).check(
                    "task-1", "session-1"
                )
                self.assertEqual(result.status, HealthStatus.UNRESPONSIVE)
                self.assertEqual(result.failure, failure)

    async def test_late_response_is_unresponsive(self) -> None:
        clock = FakeClock()

        def respond_late(challenge: HealthChallenge) -> HealthResponse:
            clock.advance(5.001)
            return matching_response(challenge)

        result = await engine(clock, FunctionTransport(respond_late)).check(
            "task-1", "session-1"
        )

        self.assertEqual(result.status, HealthStatus.UNRESPONSIVE)
        self.assertEqual(result.failure, HealthFailure.LATE)
        self.assertGreater(result.latency, timedelta(seconds=5))

    async def test_rejects_transport_identity_mismatch(self) -> None:
        class MismatchedTransport:
            async def exchange(
                self, challenge: HealthChallenge
            ) -> AuthenticatedHealthResponse:
                return AuthenticatedHealthResponse(
                    "another-session", matching_response(challenge)
                )

        checker = HealthCheckEngine(
            HealthCheckConfig(timedelta(seconds=15), timedelta(seconds=5)),
            FakeClock(),
            SequentialNonces(),
            MismatchedTransport(),
            process_epoch="manager-epoch-1",
        )

        result = await checker.check("task-1", "session-1")

        self.assertEqual(result.status, HealthStatus.UNRESPONSIVE)
        self.assertEqual(result.failure, HealthFailure.AUTHENTICATION_MISMATCH)

    async def test_sequence_keeps_repeated_provider_values_unique(self) -> None:
        clock = FakeClock()
        transport = FunctionTransport(matching_response)
        checker = HealthCheckEngine(
            HealthCheckConfig(timedelta(seconds=15), timedelta(seconds=5)),
            clock,
            RepeatingNonces(),
            transport,
            process_epoch="manager-epoch-1",
        )
        results = [await checker.check("task-1", "session-1") for _ in range(1_000)]

        self.assertTrue(
            all(result.status is HealthStatus.RESPONSIVE for result in results)
        )
        self.assertEqual(
            len({challenge.nonce for challenge in transport.challenges}), 1_000
        )

    async def test_hanging_exchange_is_bounded_by_timeout(self) -> None:
        clock = FakeClock()
        checker = engine(
            clock,
            NeverRespondingTransport(),
            timeout=timedelta(milliseconds=1),
        )

        result = await checker.check("task-1", "session-1")

        self.assertEqual(result.status, HealthStatus.UNRESPONSIVE)
        self.assertEqual(result.failure, HealthFailure.MISSING)
        self.assertGreaterEqual(result.latency, timedelta(milliseconds=1))

    async def test_rejects_overlapping_check_for_same_task(self) -> None:
        clock = FakeClock()
        transport = BlockingTransport()
        checker = engine(clock, transport)
        first = asyncio.create_task(checker.check("task-1", "session-1"))
        await transport.started.wait()

        with self.assertRaises(HealthCheckInProgressError):
            await checker.check("task-1", "session-1")

        transport.release.set()
        self.assertEqual((await first).status, HealthStatus.RESPONSIVE)

    async def test_cancelled_check_releases_the_task_guard(self) -> None:
        transport = BlockingTransport()
        checker = engine(FakeClock(), transport)
        first = asyncio.create_task(checker.check("task-1", "session-1"))
        await transport.started.wait()

        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        transport.release.set()

        self.assertEqual(
            (await checker.check("task-1", "session-1")).status,
            HealthStatus.RESPONSIVE,
        )

    async def test_monitor_starts_checks_on_configured_cadence(self) -> None:
        clock = FakeClock()
        transport = FunctionTransport(matching_response)
        monitor = engine(clock, transport).monitor("task-1", "session-1")

        first = await anext(monitor)
        second = await anext(monitor)
        await monitor.aclose()

        self.assertEqual(first.status, HealthStatus.RESPONSIVE)
        self.assertEqual(second.status, HealthStatus.RESPONSIVE)
        self.assertEqual(clock.sleeps, [15.0])
        self.assertEqual(
            transport.challenges[1].issued_at - transport.challenges[0].issued_at,
            timedelta(seconds=15),
        )

    async def test_monitor_stops_after_unresponsive_outcome(self) -> None:
        monitor = engine(
            FakeClock(), FunctionTransport(lambda _challenge: None)
        ).monitor("task-1", "session-1")

        result = await anext(monitor)
        self.assertEqual(result.status, HealthStatus.UNRESPONSIVE)
        with self.assertRaises(StopAsyncIteration):
            await anext(monitor)


class HealthModelTests(unittest.TestCase):
    def test_requires_explicit_valid_timing(self) -> None:
        invalid = (
            (timedelta(0), timedelta(seconds=5)),
            (timedelta(seconds=15), timedelta(0)),
            (timedelta(seconds=5), timedelta(seconds=5)),
        )

        for interval, timeout in invalid:
            with (
                self.subTest(interval=interval, timeout=timeout),
                self.assertRaises(HealthModelError),
            ):
                HealthCheckConfig(interval, timeout)

    def test_rejects_an_explicit_empty_process_epoch(self) -> None:
        with self.assertRaises(HealthModelError):
            HealthCheckEngine(
                HealthCheckConfig(timedelta(seconds=15), timedelta(seconds=5)),
                FakeClock(),
                SequentialNonces(),
                FunctionTransport(matching_response),
                process_epoch="",
            )


if __name__ == "__main__":
    unittest.main()
