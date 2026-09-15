"""Transport-neutral command admission for the manager-mediated relay.

Admission ordering:

1. The transport supplies an authenticated principal separately from the
   envelope; a session ID inside the payload is never evidence of ownership.
2. The durable repository is authoritative: it atomically observes the task,
   refuses wrong principals and non-admissible states, records activation when
   a ``READY`` task receives its first command, and enforces one in-flight
   command per task under the current admission fence.
3. Policy evaluation happens before every side effect, including the durable
   admission write. A denial is audited and never executed, and it leaves a
   ``READY`` task un-activated.
4. A durable transition to ``TERMINATING`` closes admission before cleanup.

Command outcomes are terminal (``SUCCESS``, ``FAILED``, ``REJECTED``,
``CANCELLED``) or ``UNKNOWN``: stream loss, unconfirmed cancellation, or a
manager restart never imply that remote execution stopped, and the manager
never automatically re-issues an unknown command.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from .command_policy import (
    CommandPolicy,
    PolicyDecision,
    PolicyEvaluation,
    PolicyResult,
)
from .identifiers import is_identifier
from .relay_protocol import CommandEnvelope, CommandOutcome, RelayOperation


class AdmissionLifecycle(StrEnum):
    """Lifecycle states relevant to command admission."""

    READY = "READY"
    ACTIVE = "ACTIVE"
    TERMINATING = "TERMINATING"


class AdmissionRefusal(StrEnum):
    """Reasons admission is refused.

    The first five are produced by the durable repository; ``POLICY_DENIED``
    is produced by the gate before any side effect.
    """

    UNKNOWN_TASK = "unknown_task"
    WRONG_PRINCIPAL = "wrong_principal"
    NOT_ADMISSIBLE = "not_admissible"
    COMMAND_IN_FLIGHT = "command_in_flight"
    STALE_FENCE = "stale_fence"
    POLICY_DENIED = "policy_denied"


class CommandAdmissionError(RuntimeError):
    """Raised when a command cannot be admitted."""

    def __init__(self, refusal: AdmissionRefusal, message: str) -> None:
        self.refusal = refusal
        super().__init__(message)


class CommandOwnershipError(PermissionError):
    """Raised when the authenticated principal does not own the task."""

    def __init__(self, task_id: str, principal: str) -> None:
        self.task_id = task_id
        self.principal = principal
        super().__init__("authenticated principal does not own the task")


class TaskNotAdmissibleError(CommandAdmissionError):
    """Raised when the task state or fence rejects new commands."""

    def __init__(self, refusal: AdmissionRefusal, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(
            refusal, f"task {task_id} does not accept commands ({refusal.value})"
        )


class CommandInFlightError(CommandAdmissionError):
    """Raised when the task already runs its one allowed in-flight command."""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(
            AdmissionRefusal.COMMAND_IN_FLIGHT,
            f"task {task_id} already has an in-flight command",
        )


class PolicyDeniedError(CommandAdmissionError):
    """Raised when the command policy denies admission before side effects."""

    def __init__(self, task_id: str, decision_reference: str) -> None:
        self.task_id = task_id
        self.decision_reference = decision_reference
        super().__init__(
            AdmissionRefusal.POLICY_DENIED,
            f"command policy denied admission for task {task_id}",
        )


class CommandPersistenceError(RuntimeError):
    """Raised when a durable command mutation has no confirmed outcome."""

    def __init__(self, task_id: str, operation: str, cause: Exception) -> None:
        self.task_id = task_id
        self.operation = operation
        self.cause = cause
        super().__init__(f"durable command operation failed: {operation}")


@dataclass(frozen=True, slots=True)
class TaskAdmissionState:
    """The durable view of one task consulted during admission."""

    task_id: str
    agent_session_id: str
    profile: str
    lifecycle: AdmissionLifecycle
    fence_generation: str
    in_flight_command_id: str | None = None

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.task_id, "task ID"),
            (self.agent_session_id, "agent session ID"),
            (self.fence_generation, "fence generation"),
        ):
            _require_identifier(value, field_name)
        _require_text(self.profile, "profile")
        if self.in_flight_command_id is not None:
            _require_identifier(self.in_flight_command_id, "in-flight command ID")
        if not isinstance(self.lifecycle, AdmissionLifecycle):
            raise TypeError("lifecycle must be an admission lifecycle")


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    """One command asking the durable repository to begin execution."""

    task_id: str
    command_id: str
    authenticated_principal: str
    fence_generation: str
    admitted_at: datetime

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.task_id, "task ID"),
            (self.command_id, "command ID"),
            (self.authenticated_principal, "authenticated principal"),
            (self.fence_generation, "fence generation"),
        ):
            _require_identifier(value, field_name)
        object.__setattr__(self, "admitted_at", _timestamp(self.admitted_at))


@dataclass(frozen=True, slots=True)
class AdmittedCommand:
    """A command the repository durably began for one task."""

    task_id: str
    command_id: str
    agent_session_id: str
    profile: str
    fence_generation: str
    activated: bool
    admitted_at: datetime

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.task_id, "task ID"),
            (self.command_id, "command ID"),
            (self.agent_session_id, "agent session ID"),
            (self.fence_generation, "fence generation"),
        ):
            _require_identifier(value, field_name)
        _require_text(self.profile, "profile")
        if type(self.activated) is not bool:
            raise TypeError("activated must be a boolean")
        object.__setattr__(self, "admitted_at", _timestamp(self.admitted_at))


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    """The repository's atomic answer to one admission request."""

    command: AdmittedCommand | None = None
    refusal: AdmissionRefusal | None = None

    def __post_init__(self) -> None:
        if (self.command is None) == (self.refusal is None):
            raise ValueError("admission decision must be exactly one outcome")


@dataclass(frozen=True, slots=True)
class AdmissionRecoveryEntry:
    """One prior-fence in-flight command a restart classified ``UNKNOWN``."""

    task_id: str
    command_id: str

    def __post_init__(self) -> None:
        _require_identifier(self.task_id, "task ID")
        _require_identifier(self.command_id, "command ID")


@dataclass(frozen=True, slots=True)
class AdmissionRecovery:
    """Payload-free report of admission recovery after a manager restart."""

    entries: tuple[AdmissionRecoveryEntry, ...] = ()


class CommandEventKind(StrEnum):
    """Kinds of durable command-admission transitions in the event log."""

    SEED_READY = "seed_ready"
    ADMITTED = "admitted"
    FINISHED = "finished"
    TERMINATION_BEGUN = "termination_begun"
    FENCE_ADOPTED = "fence_adopted"


#: The fields each event kind carries; the union doubles as the wire shape.
COMMAND_EVENT_FIELDS: Mapping[CommandEventKind, tuple[str, ...]] = {
    CommandEventKind.SEED_READY: (
        "task_id",
        "agent_session_id",
        "profile",
        "fence_generation",
    ),
    CommandEventKind.ADMITTED: (
        "task_id",
        "command_id",
        "authenticated_principal",
        "fence_generation",
        "admitted_at",
    ),
    CommandEventKind.FINISHED: (
        "task_id",
        "command_id",
        "outcome",
        "completed_at",
    ),
    CommandEventKind.TERMINATION_BEGUN: ("task_id",),
    CommandEventKind.FENCE_ADOPTED: ("fence_generation",),
}

_EVENT_FIELDS_ALL: tuple[str, ...] = (
    "task_id",
    "agent_session_id",
    "profile",
    "fence_generation",
    "command_id",
    "authenticated_principal",
    "outcome",
    "admitted_at",
    "completed_at",
)


@dataclass(frozen=True, slots=True)
class CommandEvent:
    """One payload-free journal record of a durable admission transition.

    The journal writes each event ahead of the in-memory mutation it
    describes, so a restarted manager can fold the log back into a fresh
    repository. Records carry identifiers, lifecycle facts, and outcomes
    only: never command arguments, environment values, streams, or file
    contents. Exactly the fields of ``COMMAND_EVENT_FIELDS`` for the kind
    may be set.
    """

    kind: CommandEventKind
    task_id: str | None = None
    agent_session_id: str | None = None
    profile: str | None = None
    fence_generation: str | None = None
    command_id: str | None = None
    authenticated_principal: str | None = None
    outcome: CommandOutcome | None = None
    admitted_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, CommandEventKind):
            raise TypeError("kind must be a command event kind")
        expected = COMMAND_EVENT_FIELDS[self.kind]
        for field in _EVENT_FIELDS_ALL:
            value = getattr(self, field)
            if field in expected:
                if value is None:
                    raise ValueError(
                        f"{self.kind.value} event requires {field}"
                    )
            elif value is not None:
                raise ValueError(
                    f"{self.kind.value} event cannot carry {field}"
                )
        for field in (
            "task_id",
            "agent_session_id",
            "fence_generation",
            "command_id",
            "authenticated_principal",
        ):
            value = getattr(self, field)
            if value is not None:
                _require_identifier(value, field)
        if self.profile is not None:
            _require_text(self.profile, "profile")
        if self.outcome is not None and not isinstance(self.outcome, CommandOutcome):
            raise ValueError("outcome must be a command outcome")
        for value, _field_name in (
            (self.admitted_at, "admitted_at"),
            (self.completed_at, "completed_at"),
        ):
            if value is not None:
                _timestamp(value)

    def seed_state(self) -> TaskAdmissionState:
        """The ``READY`` admission state this ``SEED_READY`` event records."""

        if self.kind is not CommandEventKind.SEED_READY:
            raise ValueError("event is not a seed event")
        assert self.task_id is not None
        assert self.agent_session_id is not None
        assert self.profile is not None
        assert self.fence_generation is not None
        return TaskAdmissionState(
            task_id=self.task_id,
            agent_session_id=self.agent_session_id,
            profile=self.profile,
            lifecycle=AdmissionLifecycle.READY,
            fence_generation=self.fence_generation,
        )

    def admission_request(self) -> AdmissionRequest:
        """The admission request this ``ADMITTED`` event records."""

        if self.kind is not CommandEventKind.ADMITTED:
            raise ValueError("event is not an admission event")
        assert self.task_id is not None
        assert self.command_id is not None
        assert self.authenticated_principal is not None
        assert self.fence_generation is not None
        assert self.admitted_at is not None
        return AdmissionRequest(
            task_id=self.task_id,
            command_id=self.command_id,
            authenticated_principal=self.authenticated_principal,
            fence_generation=self.fence_generation,
            admitted_at=self.admitted_at,
        )


class CommandRepository(Protocol):
    async def admission_state(self, task_id: str) -> TaskAdmissionState | None:
        """Return the current admission view, or None when unknown."""

        ...

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        """Atomically begin one command or refuse it with a durable reason.

        The implementation must observe the task, verify the authenticated
        principal against the owning agent session, activate a ``READY`` task,
        enforce the single in-flight command, and validate the fence
        generation as one durable transition.
        """

        ...

    async def finish(
        self, command: AdmittedCommand, outcome: CommandOutcome, completed_at: datetime
    ) -> None:
        """Release the in-flight slot and durably record the outcome."""

        ...


class AdmissionCloser(Protocol):
    async def begin_termination(self, task_id: str) -> None:
        """Close durable command admission for one task, refusing new commands.

        Lifecycle termination calls this before any cleanup step runs, so no
        command can start after termination has won the durable race. The
        operation must be idempotent: termination resumes re-run it after a
        restart or a cleanup retry.
        """

        ...


class CommandEventJournal(Protocol):
    async def append(self, event: CommandEvent) -> None:
        """Durably record one admission transition before it is applied.

        A failed append must raise: the caller abandons the mutation, so the
        in-memory view never runs ahead of the durable log.
        """

        ...


@dataclass(frozen=True, slots=True)
class CommandAuditEntry:
    """Payload-free audit record for one admission or outcome.

    By construction this record cannot carry command arguments, environment
    values, stream payloads, or file contents.
    """

    task_id: str
    command_id: str
    operation: RelayOperation
    authenticated_principal: str
    policy_version: int
    decision_reference: str
    outcome: CommandOutcome | None
    admitted_at: datetime | None = None
    completed_at: datetime | None = None
    declared_input_bytes: int | None = None
    declared_output_bytes: int | None = None
    kubernetes_correlation_id: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.task_id, "task ID")
        _require_identifier(self.command_id, "command ID")
        if not isinstance(self.operation, RelayOperation):
            raise TypeError("operation must be a relay operation")
        _require_identifier(self.authenticated_principal, "authenticated principal")
        if type(self.policy_version) is not int or self.policy_version < 0:
            raise ValueError("policy version must be a non-negative integer")
        _require_text(self.decision_reference, "decision reference")
        if self.outcome is not None and not isinstance(self.outcome, CommandOutcome):
            raise ValueError("outcome must be a command outcome")
        for value in (self.admitted_at, self.completed_at):
            if value is not None:
                _timestamp(value)
        for size_value, field_name in (
            (self.declared_input_bytes, "declared input bytes"),
            (self.declared_output_bytes, "declared output bytes"),
        ):
            if size_value is not None and (
                type(size_value) is not int or size_value < 0
            ):
                raise ValueError(f"{field_name} must be a non-negative integer")
        if self.kubernetes_correlation_id is not None:
            _require_identifier(
                self.kubernetes_correlation_id, "Kubernetes correlation ID"
            )


class CommandAudit(Protocol):
    async def record(self, entry: CommandAuditEntry) -> None:
        """Persist one audit entry without blocking admission."""

        ...


@dataclass(frozen=True, slots=True)
class CommandResult:
    """The manager's final report for one command."""

    command_id: str
    outcome: CommandOutcome
    reason_code: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.command_id, "command ID")
        if not isinstance(self.outcome, CommandOutcome):
            raise TypeError("outcome must be a command outcome")
        if self.reason_code is not None:
            _require_text(self.reason_code, "reason code")


class InputStream(Protocol):
    def __aiter__(self) -> AsyncIterator[bytes]: ...


#: Receives each relayed output frame (channel, payload) as it arrives.
#: Awaiting the sink propagates backpressure through the relay.
OutputSink = Callable[[int, bytes], Awaitable[None]]


class CommandRelay(Protocol):
    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        """Relay one admitted command into its sandbox Pod.

        Implementations must verify the observed Pod UID, task annotation,
        labels, namespace, and container against the durable task record
        immediately before ``pods/exec``, and must classify a lost stream or a
        manager restart as ``UNKNOWN`` rather than success or confirmed
        cancellation.
        """

        ...


class CommandClock(Protocol):
    def now(self) -> datetime: ...


class SystemCommandClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class AdmissionFence:
    """The process fence invalidating admissions from prior manager processes.

    A new random generation is created at startup. The generation is recorded
    durably with every admission so a restarted manager, or a later second
    worker, refuses to act on admissions fenced by an older owner.
    """

    def __init__(self, generation: str | None = None) -> None:
        self._generation = (
            generation if generation is not None else secrets.token_urlsafe(32)
        )
        _require_identifier(self._generation, "fence generation")

    @property
    def generation(self) -> str:
        return self._generation


@dataclass(frozen=True, slots=True)
class CommandAdmission:
    """A durably begun command paired with its policy decision."""

    command: AdmittedCommand
    policy: PolicyResult


class CommandAdmissionGate:
    """Applies ownership, policy, and durable fencing to each command."""

    def __init__(
        self,
        repository: CommandRepository,
        policy: CommandPolicy,
        audit: CommandAudit,
        relay: CommandRelay,
        fence: AdmissionFence | None = None,
        clock: CommandClock | None = None,
    ) -> None:
        self._repository = repository
        self._policy = policy
        self._audit = audit
        self._relay = relay
        self._fence = fence if fence is not None else AdmissionFence()
        self._now = (clock or SystemCommandClock()).now

    @property
    def fence(self) -> AdmissionFence:
        return self._fence

    async def admit(
        self, principal: str, envelope: CommandEnvelope
    ) -> AdmittedCommand:
        """Authenticate, evaluate policy, then durably begin one command."""

        return (await self._admit(principal, envelope)).command

    async def admit_detailed(
        self, principal: str, envelope: CommandEnvelope
    ) -> CommandAdmission:
        """Begin one command and return it with its policy decision.

        Transports that must answer an admission request before any frame
        flows (the NATS request/reply) use this with ``run_admitted``.
        """

        return await self._admit(principal, envelope)

    async def run_admitted(
        self,
        admission: CommandAdmission,
        envelope: CommandEnvelope,
        principal: str,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        """Relay and durably finish one already-admitted command."""

        return await self._run_body(
            admission, envelope, principal, input_stream, output_sink
        )

    async def abandon(
        self,
        admission: CommandAdmission,
        envelope: CommandEnvelope,
        principal: str,
    ) -> None:
        """Finish an admitted command as unknown before relay ownership begins."""

        await self._abandon(
            admission.command, envelope, principal, admission.policy
        )

    async def run(
        self,
        principal: str,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        """Admit, relay, and durably finish one command.

        A relay failure maps to ``UNKNOWN``; the in-flight slot is always
        released, and a lost stream is never reported as success or as
        confirmed cancellation. The manager never re-issues the command.
        """

        admission = await self._admit(principal, envelope)
        return await self._run_body(
            admission, envelope, principal, input_stream, output_sink
        )

    async def _run_body(
        self,
        admission: CommandAdmission,
        envelope: CommandEnvelope,
        principal: str,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        try:
            result = await self._execute(
                admission.command, envelope, input_stream, output_sink
            )
        except asyncio.CancelledError:
            # The consumer abandoned the command (a closed download stream)
            # or the manager is shutting down. The in-flight slot must still
            # be released and the outcome recorded fail-closed; cancellation
            # itself always propagates.
            await self._abandon(
                admission.command, envelope, principal, admission.policy
            )
            raise
        completed_at = self._now()
        try:
            await self._repository.finish(
                admission.command, result.outcome, completed_at
            )
        except Exception as error:
            raise CommandPersistenceError(
                admission.command.task_id, "finish", error
            ) from error
        with contextlib.suppress(Exception):
            await self._audit.record(
                CommandAuditEntry(
                    task_id=admission.command.task_id,
                    command_id=admission.command.command_id,
                    operation=envelope.operation,
                    authenticated_principal=principal,
                    policy_version=admission.policy.policy_version,
                    decision_reference=admission.policy.decision_reference,
                    outcome=result.outcome,
                    admitted_at=admission.command.admitted_at,
                    completed_at=completed_at,
                    declared_input_bytes=envelope.declared_input_bytes,
                    declared_output_bytes=envelope.declared_output_bytes,
                )
            )
        return result

    async def _admit(
        self, principal: str, envelope: CommandEnvelope
    ) -> CommandAdmission:
        _require_identifier(principal, "authenticated principal")
        task_id = envelope.task_id
        state = await self._repository.admission_state(task_id)
        if state is None:
            raise TaskNotAdmissibleError(AdmissionRefusal.UNKNOWN_TASK, task_id)
        if state.agent_session_id != principal:
            raise CommandOwnershipError(task_id, principal)

        result = await self._policy.evaluate(
            PolicyEvaluation(
                schema_version=1,
                operation=envelope.operation,
                profile=state.profile,
                authenticated_principal=principal,
                declared_size=envelope.declared_input_bytes,
            )
        )
        if result.decision is PolicyDecision.DENY:
            await self._audit.record(
                CommandAuditEntry(
                    task_id=task_id,
                    command_id=envelope.command_id,
                    operation=envelope.operation,
                    authenticated_principal=principal,
                    policy_version=result.policy_version,
                    decision_reference=result.decision_reference,
                    outcome=CommandOutcome.REJECTED,
                    admitted_at=self._now(),
                    declared_input_bytes=envelope.declared_input_bytes,
                    declared_output_bytes=envelope.declared_output_bytes,
                )
            )
            raise PolicyDeniedError(task_id, result.decision_reference)

        request = AdmissionRequest(
            task_id=task_id,
            command_id=envelope.command_id,
            authenticated_principal=principal,
            fence_generation=self._fence.generation,
            admitted_at=self._now(),
        )
        repository_admission = asyncio.create_task(self._repository.admit(request))
        try:
            decision = await asyncio.shield(repository_admission)
        except asyncio.CancelledError:
            # Admission is a durable transition. Let it settle so cancellation
            # cannot land after the store commits but before ownership returns,
            # leaking the task's single in-flight slot.
            decision = await repository_admission
            if decision.command is not None:
                await self._abandon(decision.command, envelope, principal, result)
            raise
        except Exception as error:
            raise CommandPersistenceError(task_id, "admit", error) from error
        if decision.refusal is not None:
            refusal = decision.refusal
            if refusal is AdmissionRefusal.WRONG_PRINCIPAL:
                raise CommandOwnershipError(task_id, principal)
            if refusal is AdmissionRefusal.COMMAND_IN_FLIGHT:
                raise CommandInFlightError(task_id)
            raise TaskNotAdmissibleError(refusal, task_id)
        command = decision.command
        if command is None:  # pragma: no cover - decision invariant
            raise CommandPersistenceError(
                task_id, "admit", RuntimeError("decision carried no command")
            )
        return CommandAdmission(command=command, policy=result)

    async def _execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        try:
            return await self._relay.execute(
                command, envelope, input_stream, output_sink
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - fail-closed UNKNOWN mapping
            return CommandResult(
                command_id=command.command_id,
                outcome=CommandOutcome.UNKNOWN,
                reason_code=f"relay_stream_lost:{type(error).__name__}",
            )

    async def _abandon(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        principal: str,
        policy: PolicyResult,
    ) -> None:
        """Finish one cancelled command as ``UNKNOWN`` and release its slot."""

        completed_at = self._now()
        try:
            await self._repository.finish(
                command, CommandOutcome.UNKNOWN, completed_at
            )
            await self._audit.record(
                CommandAuditEntry(
                    task_id=command.task_id,
                    command_id=command.command_id,
                    operation=envelope.operation,
                    authenticated_principal=principal,
                    policy_version=policy.policy_version,
                    decision_reference=policy.decision_reference,
                    outcome=CommandOutcome.UNKNOWN,
                    admitted_at=command.admitted_at,
                    completed_at=completed_at,
                    declared_input_bytes=envelope.declared_input_bytes,
                    declared_output_bytes=envelope.declared_output_bytes,
                )
            )
        except Exception:  # noqa: BLE001 - cancellation must still propagate
            # A persistence failure here must not mask the in-flight
            # cancellation; the broken store surfaces on the next operation.
            return


class InMemoryCommandRepository:
    """Single-worker command repository implementation.

    Mirrors ``CapacityLimiter``: suitable for one manager process and seeded
    from durable recovery state. It is not cross-worker authority.

    With a journal attached, every mutation is written to the durable log
    ahead of its in-memory application, under the repository lock, so the
    event order in the log matches the order the live view advanced and a
    journal failure leaves the mutation unapplied.
    """

    def __init__(
        self,
        states: Mapping[str, TaskAdmissionState] | None = None,
        journal: CommandEventJournal | None = None,
    ) -> None:
        self._states: dict[str, TaskAdmissionState] = dict(states or {})
        self._lock = asyncio.Lock()
        self._outcomes: dict[str, CommandOutcome] = {}
        self._journal = journal

    def attach_journal(self, journal: CommandEventJournal) -> None:
        """Attach the durable journal once replay has seeded this repository.

        Startup folds the log into a journal-less repository first;
        attaching afterwards guarantees replay does not re-append the
        events it reads. Intended for the single manager start-up path,
        before any admission can arrive.
        """

        self._journal = journal

    async def _journal_append(self, event: CommandEvent) -> None:
        """Write one transition ahead of the mutation it describes."""

        if self._journal is not None:
            await self._journal.append(event)

    async def admission_state(self, task_id: str) -> TaskAdmissionState | None:
        async with self._lock:
            return self._states.get(task_id)

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        async with self._lock:
            state = self._states.get(request.task_id)
            if state is None:
                return AdmissionDecision(refusal=AdmissionRefusal.UNKNOWN_TASK)
            if state.agent_session_id != request.authenticated_principal:
                return AdmissionDecision(refusal=AdmissionRefusal.WRONG_PRINCIPAL)
            if state.fence_generation != request.fence_generation:
                return AdmissionDecision(refusal=AdmissionRefusal.STALE_FENCE)
            if state.lifecycle is AdmissionLifecycle.TERMINATING:
                return AdmissionDecision(refusal=AdmissionRefusal.NOT_ADMISSIBLE)
            if state.in_flight_command_id is not None:
                return AdmissionDecision(refusal=AdmissionRefusal.COMMAND_IN_FLIGHT)
            await self._journal_append(
                CommandEvent(
                    kind=CommandEventKind.ADMITTED,
                    task_id=request.task_id,
                    command_id=request.command_id,
                    authenticated_principal=request.authenticated_principal,
                    fence_generation=request.fence_generation,
                    admitted_at=request.admitted_at,
                )
            )
            activated = state.lifecycle is AdmissionLifecycle.READY
            self._states[request.task_id] = TaskAdmissionState(
                task_id=state.task_id,
                agent_session_id=state.agent_session_id,
                profile=state.profile,
                lifecycle=(
                    AdmissionLifecycle.ACTIVE if activated else state.lifecycle
                ),
                fence_generation=state.fence_generation,
                in_flight_command_id=request.command_id,
            )
            return AdmissionDecision(
                command=AdmittedCommand(
                    task_id=request.task_id,
                    command_id=request.command_id,
                    agent_session_id=state.agent_session_id,
                    profile=state.profile,
                    fence_generation=request.fence_generation,
                    activated=activated,
                    admitted_at=request.admitted_at,
                )
            )

    async def finish(
        self, command: AdmittedCommand, outcome: CommandOutcome, completed_at: datetime
    ) -> None:
        async with self._lock:
            state = self._states.get(command.task_id)
            if state is None or state.in_flight_command_id != command.command_id:
                return
            await self._journal_append(
                CommandEvent(
                    kind=CommandEventKind.FINISHED,
                    task_id=command.task_id,
                    command_id=command.command_id,
                    outcome=outcome,
                    completed_at=completed_at,
                )
            )
            self._outcomes[command.command_id] = outcome
            self._states[command.task_id] = TaskAdmissionState(
                task_id=state.task_id,
                agent_session_id=state.agent_session_id,
                profile=state.profile,
                lifecycle=state.lifecycle,
                fence_generation=state.fence_generation,
                in_flight_command_id=None,
            )

    async def outcome(self, command_id: str) -> CommandOutcome | None:
        async with self._lock:
            return self._outcomes.get(command_id)

    async def lifecycle(self, task_id: str) -> AdmissionLifecycle | None:
        async with self._lock:
            state = self._states.get(task_id)
            return None if state is None else state.lifecycle

    async def begin_termination(self, task_id: str) -> None:
        """Close admission for a task, refusing any further commands."""

        async with self._lock:
            state = self._states.get(task_id)
            if state is None:
                return
            if state.lifecycle is not AdmissionLifecycle.TERMINATING:
                await self._journal_append(
                    CommandEvent(
                        kind=CommandEventKind.TERMINATION_BEGUN, task_id=task_id
                    )
                )
            self._states[task_id] = TaskAdmissionState(
                task_id=state.task_id,
                agent_session_id=state.agent_session_id,
                profile=state.profile,
                lifecycle=AdmissionLifecycle.TERMINATING,
                fence_generation=state.fence_generation,
                in_flight_command_id=state.in_flight_command_id,
            )

    async def seed_ready(self, state: TaskAdmissionState) -> None:
        """Seed admission for a newly ready task under the current fence.

        Idempotent per task: re-seeding the same owner and profile refreshes
        the fence generation (a restarted process re-publishes ready tasks)
        while preserving activation and any in-flight command. Reopening a
        terminating task, or silently switching its owner or profile, is
        refused.
        """

        if state.lifecycle is not AdmissionLifecycle.READY:
            raise ValueError("seeded admission state must be READY")
        # The event records the incoming state; folding it back through
        # seed_ready reproduces both the fresh seed and the fence refresh.
        event = CommandEvent(
            kind=CommandEventKind.SEED_READY,
            task_id=state.task_id,
            agent_session_id=state.agent_session_id,
            profile=state.profile,
            fence_generation=state.fence_generation,
        )
        async with self._lock:
            existing = self._states.get(state.task_id)
            if existing is None:
                await self._journal_append(event)
                self._states[state.task_id] = state
                return
            if existing.lifecycle is AdmissionLifecycle.TERMINATING:
                raise ValueError(
                    f"task {state.task_id} is terminating; admission stays closed"
                )
            if (
                existing.agent_session_id != state.agent_session_id
                or existing.profile != state.profile
            ):
                raise ValueError(
                    f"task {state.task_id} already admits a different owner or profile"
                )
            await self._journal_append(event)
            self._states[state.task_id] = TaskAdmissionState(
                task_id=existing.task_id,
                agent_session_id=existing.agent_session_id,
                profile=existing.profile,
                lifecycle=existing.lifecycle,
                fence_generation=state.fence_generation,
                in_flight_command_id=existing.in_flight_command_id,
            )

    async def recover(self, fence_generation: str) -> AdmissionRecovery:
        """Adopt a new fence generation, classifying stale in-flight commands.

        A restarted manager cannot confirm what happened to commands admitted
        by a prior process: each is recorded ``UNKNOWN`` and its slot released,
        and nothing is re-issued. Every surviving state then
        carries the new generation, so the task stays admissible under its
        current owner.
        """

        _require_identifier(fence_generation, "fence generation")
        await self._journal_append(
            CommandEvent(
                kind=CommandEventKind.FENCE_ADOPTED,
                fence_generation=fence_generation,
            )
        )
        entries: list[AdmissionRecoveryEntry] = []
        async with self._lock:
            for task_id, state in list(self._states.items()):
                in_flight = state.in_flight_command_id
                stale_in_flight = (
                    in_flight is not None
                    and state.fence_generation != fence_generation
                )
                if stale_in_flight:
                    assert in_flight is not None
                    self._outcomes[in_flight] = CommandOutcome.UNKNOWN
                    entries.append(AdmissionRecoveryEntry(task_id, in_flight))
                elif state.fence_generation == fence_generation:
                    continue
                self._states[task_id] = TaskAdmissionState(
                    task_id=state.task_id,
                    agent_session_id=state.agent_session_id,
                    profile=state.profile,
                    lifecycle=state.lifecycle,
                    fence_generation=fence_generation,
                    in_flight_command_id=None if stale_in_flight else in_flight,
                )
            return AdmissionRecovery(tuple(entries))


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise ValueError(f"{field} contains invalid text")


def _require_identifier(value: object, field: str) -> None:
    if not is_identifier(value):
        raise ValueError(
            f"{field} must contain 1-64 ASCII letters, digits, underscores, or hyphens"
        )


def _timestamp(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(UTC)
