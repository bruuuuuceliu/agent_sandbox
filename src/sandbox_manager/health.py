"""Transport-neutral agent health checking."""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol


class HealthModelError(ValueError):
    """Raised when health-check domain data is invalid."""


class HealthCheckInProgressError(RuntimeError):
    """Raised when a task already has an active health check."""


class HealthStatus(StrEnum):
    RESPONSIVE = "responsive"
    UNRESPONSIVE = "unresponsive"


class HealthFailure(StrEnum):
    MISSING = "missing"
    LATE = "late"
    MALFORMED = "malformed"
    NONCE_MISMATCH = "nonce_mismatch"
    TASK_MISMATCH = "task_mismatch"
    SESSION_MISMATCH = "session_mismatch"
    EPOCH_MISMATCH = "epoch_mismatch"
    AUTHENTICATION_MISMATCH = "authentication_mismatch"


@dataclass(frozen=True, slots=True)
class HealthCheckConfig:
    interval: timedelta
    timeout: timedelta

    def __post_init__(self) -> None:
        if self.interval <= timedelta(0):
            raise HealthModelError("health-check interval must be positive")
        if self.timeout <= timedelta(0):
            raise HealthModelError("health-check timeout must be positive")
        if self.timeout >= self.interval:
            raise HealthModelError("health-check timeout must be shorter than interval")


@dataclass(frozen=True, slots=True)
class HealthChallenge:
    nonce: str = field(repr=False)
    task_id: str
    agent_session_id: str
    process_epoch: str
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        _require_text(self.nonce, "nonce")
        _require_text(self.task_id, "task ID")
        _require_text(self.agent_session_id, "agent session ID")
        _require_text(self.process_epoch, "process epoch")
        _require_aware(self.issued_at, "issued_at")
        _require_aware(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise HealthModelError("challenge expiry must follow issuance")


@dataclass(frozen=True, slots=True)
class HealthResponse:
    nonce: str = field(repr=False)
    task_id: str
    agent_session_id: str
    process_epoch: str


@dataclass(frozen=True, slots=True)
class AuthenticatedHealthResponse:
    authenticated_session_id: str
    response: object = field(repr=False)

    def __post_init__(self) -> None:
        _require_text(self.authenticated_session_id, "authenticated session ID")


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    challenge: HealthChallenge
    status: HealthStatus
    completed_at: datetime
    latency: timedelta
    failure: HealthFailure | None = None

    def __post_init__(self) -> None:
        _require_aware(self.completed_at, "completed_at")
        if self.latency < timedelta(0):
            raise HealthModelError("health-check latency must not be negative")
        responsive = self.status is HealthStatus.RESPONSIVE
        if responsive != (self.failure is None):
            raise HealthModelError("health-check status and failure do not agree")


class HealthClock(Protocol):
    def wall_time(self) -> datetime: ...

    def monotonic(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class NonceProvider(Protocol):
    def next_nonce(self) -> str: ...


class HealthTransport(Protocol):
    async def exchange(
        self, challenge: HealthChallenge
    ) -> AuthenticatedHealthResponse | None:
        """Return a response bound to its transport-authenticated session."""

        ...


class SystemHealthClock:
    def wall_time(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class SecureNonceProvider:
    def next_nonce(self) -> str:
        return secrets.token_urlsafe(32)


class HealthCheckEngine:
    def __init__(
        self,
        config: HealthCheckConfig,
        clock: HealthClock,
        nonces: NonceProvider,
        transport: HealthTransport,
        process_epoch: str | None = None,
    ) -> None:
        if process_epoch is None:
            process_epoch = secrets.token_urlsafe(32)
        _require_text(process_epoch, "process epoch")
        self._config = config
        self._clock = clock
        self._nonces = nonces
        self._transport = transport
        self._process_epoch = process_epoch
        self._nonce_sequence = 0
        self._nonce_lock = asyncio.Lock()
        self._active_tasks: set[str] = set()
        self._active_lock = asyncio.Lock()

    async def check(self, task_id: str, agent_session_id: str) -> HealthCheckResult:
        _require_text(task_id, "task ID")
        _require_text(agent_session_id, "agent session ID")
        await self._start(task_id)
        try:
            return await self._exchange(task_id, agent_session_id)
        finally:
            await asyncio.shield(self._finish(task_id))

    async def monitor(
        self, task_id: str, agent_session_id: str
    ) -> AsyncGenerator[HealthCheckResult, None]:
        while True:
            cycle_started = self._clock.monotonic()
            result = await self.check(task_id, agent_session_id)
            yield result
            if result.status is HealthStatus.UNRESPONSIVE:
                return

            elapsed = max(0.0, self._clock.monotonic() - cycle_started)
            delay = self._config.interval.total_seconds() - elapsed
            if delay > 0:
                await self._clock.sleep(delay)

    async def _exchange(self, task_id: str, agent_session_id: str) -> HealthCheckResult:
        issued_at = self._clock.wall_time()
        nonce = await self._next_unique_nonce()
        challenge = HealthChallenge(
            nonce=nonce,
            task_id=task_id,
            agent_session_id=agent_session_id,
            process_epoch=self._process_epoch,
            issued_at=issued_at,
            expires_at=issued_at + self._config.timeout,
        )
        started = self._clock.monotonic()
        try:
            async with asyncio.timeout(self._config.timeout.total_seconds()):
                exchange = await self._transport.exchange(challenge)
        except TimeoutError:
            return self._result(
                challenge,
                started,
                HealthFailure.MISSING,
                minimum_latency=self._config.timeout,
            )

        elapsed = max(0.0, self._clock.monotonic() - started)
        if elapsed > self._config.timeout.total_seconds():
            return self._result(challenge, started, HealthFailure.LATE)
        failure = self._validate_response(challenge, exchange)
        return self._result(challenge, started, failure)

    def _validate_response(
        self, challenge: HealthChallenge, exchange: object | None
    ) -> HealthFailure | None:
        if exchange is None:
            return HealthFailure.MISSING
        if not isinstance(exchange, AuthenticatedHealthResponse):
            return HealthFailure.MALFORMED
        if exchange.authenticated_session_id != challenge.agent_session_id:
            return HealthFailure.AUTHENTICATION_MISMATCH
        response = exchange.response
        if not isinstance(response, HealthResponse) or not all(
            _valid_text(value)
            for value in (
                response.nonce,
                response.task_id,
                response.agent_session_id,
                response.process_epoch,
            )
        ):
            return HealthFailure.MALFORMED
        if response.nonce != challenge.nonce:
            return HealthFailure.NONCE_MISMATCH
        if response.task_id != challenge.task_id:
            return HealthFailure.TASK_MISMATCH
        if response.agent_session_id != challenge.agent_session_id:
            return HealthFailure.SESSION_MISMATCH
        if response.process_epoch != challenge.process_epoch:
            return HealthFailure.EPOCH_MISMATCH
        return None

    async def _next_unique_nonce(self) -> str:
        random_value = self._nonces.next_nonce()
        _require_text(random_value, "nonce")
        async with self._nonce_lock:
            self._nonce_sequence += 1
            sequence = self._nonce_sequence
        return f"{sequence}.{random_value}"

    def _result(
        self,
        challenge: HealthChallenge,
        started: float,
        failure: HealthFailure | None,
        *,
        minimum_latency: timedelta = timedelta(0),
    ) -> HealthCheckResult:
        elapsed = timedelta(seconds=max(0.0, self._clock.monotonic() - started))
        return HealthCheckResult(
            challenge=challenge,
            status=(
                HealthStatus.RESPONSIVE
                if failure is None
                else HealthStatus.UNRESPONSIVE
            ),
            completed_at=self._clock.wall_time(),
            latency=max(elapsed, minimum_latency),
            failure=failure,
        )

    async def _start(self, task_id: str) -> None:
        async with self._active_lock:
            if task_id in self._active_tasks:
                raise HealthCheckInProgressError(
                    f"health check already active for task: {task_id}"
                )
            self._active_tasks.add(task_id)

    async def _finish(self, task_id: str) -> None:
        async with self._active_lock:
            self._active_tasks.discard(task_id)


def _require_text(value: object, field: str) -> None:
    if not _valid_text(value):
        raise HealthModelError(f"{field} must be a non-empty string")


def _valid_text(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _require_aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise HealthModelError(f"{field} must include a timezone")
