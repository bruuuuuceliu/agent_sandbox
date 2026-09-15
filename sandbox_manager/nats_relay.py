"""The authenticated Core NATS command API between agent and manager.

Subject layout (one token per identifier; the server's authorization
configuration restricts each agent credential to its own prefix, so a
message arriving under ``sandbox.agent.<principal>.`` was published with
that credential — the subject namespace is the transport authentication,
and a payload field is never evidence of identity):

===========================  =============================================
Subject                      Direction and content
===========================  =============================================
``<base>.admit``             agent -> manager: encoded ``CommandEnvelope``
``<reply subject>``          manager -> agent: admission reply JSON
``<task>.in``                agent -> manager: encoded relay frames
``<task>.closed``            agent -> manager: input complete, no payload
``<task>.out``               manager -> agent: output frames then terminal
===========================  =============================================

with ``base = sandbox.agent.<principal>`` and
``task = <base>.task.<task_id>.<command_id>``.

The admission request/reply runs first; only a granted reply names the
task-scoped subjects, and both sides unsubscribe when the terminal frame
lands, so the subjects are ephemeral. Subscription ordering closes the
at-most-once delivery window on both sides: the manager subscribes to a
command's input subjects before sending the grant (same-connection ordering
means the agent can never publish ahead of them), and the agent subscribes to
the output subject before requesting admission (a fast manager-side failure
publishes its terminal frame immediately after the grant). Core NATS is
at-most-once: a manager that loses its connection mid-command publishes no
terminal frame, and the agent classifies the command as ``UNKNOWN`` — it never
re-issues it.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

from .commands import (
    CommandAdmission,
    CommandAdmissionGate,
    CommandInFlightError,
    CommandOwnershipError,
    CommandPersistenceError,
    CommandResult,
    OutputSink,
    PolicyDeniedError,
    TaskNotAdmissibleError,
)
from .identifiers import is_identifier
from .nats_client import (
    AsyncNatsConnection,
    NatsClosedError,
    NatsMessage,
    NatsSubscription,
    NatsTransportError,
)
from .relay_protocol import (
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataFrame,
    FrameSequencer,
    RelayFrame,
    TerminalFrame,
    data_channel_from_index,
    data_channel_index,
    decode_command_envelope,
    decode_frame,
    encode_command_envelope,
    encode_frame,
)

_AGENT_PREFIX = "sandbox.agent"

_ADMIT_SUFFIX = "admit"

_INPUT_SUFFIX = "in"

_CLOSED_SUFFIX = "closed"

_OUTPUT_SUFFIX = "out"

#: Default bound on each relay subject's pending queue before drops.
PENDING_LIMIT = 256


class NatsRelayError(RuntimeError):
    """Raised when the relay command API is used incorrectly."""


def require_subject_token(value: str, field: str) -> str:
    """Validate one subject token so identifiers cannot split a subject."""

    if not is_identifier(value):
        raise NatsRelayError(f"{field} must be a short single subject token")
    return value


def agent_base_subject(principal: str) -> str:
    return f"{_AGENT_PREFIX}.{require_subject_token(principal, 'principal')}"


def admission_subject(principal: str) -> str:
    return f"{agent_base_subject(principal)}.{_ADMIT_SUFFIX}"


def task_subjects(
    principal: str, task_id: str, command_id: str
) -> tuple[str, str, str]:
    """Return the (input, closed, output) subjects for one command."""

    base = (
        f"{agent_base_subject(principal)}.task"
        f".{require_subject_token(task_id, 'task ID')}"
        f".{require_subject_token(command_id, 'command ID')}"
    )
    return f"{base}.{_INPUT_SUFFIX}", f"{base}.{_CLOSED_SUFFIX}", f"{base}.{_OUTPUT_SUFFIX}"


def principal_from_admission_subject(subject: str) -> str:
    """Recover the transport principal from a wildcard-matched subject."""

    parts = subject.split(".")
    if (
        len(parts) != 4
        or parts[0] != "sandbox"
        or parts[1] != "agent"
        or parts[3] != _ADMIT_SUFFIX
    ):
        raise NatsRelayError(f"not an admission subject: {subject}")
    return require_subject_token(parts[2], "principal")


@dataclass(frozen=True, slots=True)
class AdmissionReply:
    """Transport-level answer to one admission request."""

    admitted: bool
    input_subject: str | None = None
    closed_subject: str | None = None
    output_subject: str | None = None
    refusal: str | None = None

    def encode(self) -> bytes:
        return json.dumps(
            {
                "admitted": self.admitted,
                "input_subject": self.input_subject,
                "closed_subject": self.closed_subject,
                "output_subject": self.output_subject,
                "refusal": self.refusal,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def decode(cls, payload: bytes) -> AdmissionReply:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise NatsRelayError("admission reply is not JSON") from error
        if not isinstance(decoded, dict):
            raise NatsRelayError("admission reply must be an object")
        admitted = decoded.get("admitted")
        if not isinstance(admitted, bool):
            raise NatsRelayError("admission reply must carry a boolean")
        input_subject = decoded.get("input_subject")
        closed_subject = decoded.get("closed_subject")
        output_subject = decoded.get("output_subject")
        refusal = decoded.get("refusal")
        for value in (input_subject, closed_subject, output_subject, refusal):
            if value is not None and not isinstance(value, str):
                raise NatsRelayError("admission reply fields must be text")
        if admitted and (
            input_subject is None or closed_subject is None or output_subject is None
        ):
            raise NatsRelayError("granted reply must name every subject")
        if not admitted and refusal is None:
            raise NatsRelayError("refused reply must carry a refusal")
        return cls(
            admitted=admitted,
            input_subject=input_subject,
            closed_subject=closed_subject,
            output_subject=output_subject,
            refusal=refusal,
        )


class NatsCommandServer:
    """Manager-side bridge from admission subjects to the command gate."""

    def __init__(
        self,
        connection: AsyncNatsConnection,
        gate: CommandAdmissionGate,
        pending_limit: int = PENDING_LIMIT,
    ) -> None:
        self._connection = connection
        self._gate = gate
        self._pending_limit = pending_limit
        self._sessions: set[asyncio.Task[None]] = set()
        self._admit_task: asyncio.Task[None] | None = None

    @property
    def principal(self) -> str:
        return self._connection.principal

    async def start(self) -> None:
        """Subscribe every agent's admission subject and serve requests."""

        if self._admit_task is not None:
            raise NatsRelayError("command server is already started")
        subscription = await self._connection.subscribe(
            f"{_AGENT_PREFIX}.*.{_ADMIT_SUFFIX}", self._pending_limit
        )
        try:
            await self._connection.flush()
        except BaseException:
            await _unsubscribe_quietly(self._connection, subscription)
            raise
        self._admit_task = asyncio.create_task(
            self._serve_admissions(subscription), name="nats-admissions"
        )

    async def stop(self) -> None:
        """Cancel serving and abandon every in-flight command fail-closed."""

        if self._admit_task is not None:
            self._admit_task.cancel()
            await asyncio.gather(self._admit_task, return_exceptions=True)
            self._admit_task = None
        sessions = list(self._sessions)
        for session in sessions:
            session.cancel()
        await asyncio.gather(*sessions, return_exceptions=True)
        self._sessions.clear()

    async def _serve_admissions(self, subscription: NatsSubscription) -> None:
        try:
            while True:
                message = await subscription.queue.get()
                try:
                    await self._handle_admission(message)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - isolate malformed requests
                    continue
        finally:
            await _unsubscribe_quietly(self._connection, subscription)

    async def _handle_admission(self, message: NatsMessage) -> None:
        if message.reply is None:
            return  # unanswerable request; nothing was begun
        try:
            principal = principal_from_admission_subject(message.subject)
        except NatsRelayError:
            await self._reply(message.reply, AdmissionReply(False, refusal="bad_subject"))
            return
        try:
            envelope = decode_command_envelope(message.payload)
        except Exception:  # noqa: BLE001 - any decode failure is a refusal
            await self._reply(
                message.reply, AdmissionReply(False, refusal="invalid_envelope")
            )
            return
        try:
            admission = await self._gate.admit_detailed(principal, envelope)
        except CommandOwnershipError:
            await self._reply(
                message.reply, AdmissionReply(False, refusal="wrong_principal")
            )
            return
        except CommandInFlightError:
            await self._reply(
                message.reply, AdmissionReply(False, refusal="command_in_flight")
            )
            return
        except PolicyDeniedError:
            await self._reply(
                message.reply, AdmissionReply(False, refusal="policy_denied")
            )
            return
        except CommandPersistenceError:
            await self._reply(
                message.reply, AdmissionReply(False, refusal="store_unavailable")
            )
            return
        except TaskNotAdmissibleError as error:
            await self._reply(message.reply, AdmissionReply(False, refusal=error.refusal.value))
            return

        input_sub: NatsSubscription | None = None
        closed_sub: NatsSubscription | None = None
        session_started = False
        try:
            input_subject, closed_subject, output_subject = task_subjects(
                principal, envelope.task_id, envelope.command_id
            )
            input_sub = await self._connection.subscribe(
                input_subject, self._pending_limit
            )
            closed_sub = await self._connection.subscribe(
                closed_subject, self._pending_limit
            )
            replied = await self._reply(
                message.reply,
                AdmissionReply(
                    True,
                    input_subject=input_subject,
                    closed_subject=closed_subject,
                    output_subject=output_subject,
                ),
            )
            if not replied:
                return
            session = asyncio.create_task(
                self._run_session(
                    admission,
                    envelope,
                    principal,
                    input_sub,
                    closed_sub,
                    output_subject,
                ),
                name=f"nats-command-{envelope.command_id}",
            )
            self._sessions.add(session)
            session.add_done_callback(self._sessions.discard)
            session_started = True
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - admitted work must fail closed
            return
        finally:
            if not session_started:
                if closed_sub is not None:
                    await _unsubscribe_quietly(self._connection, closed_sub)
                if input_sub is not None:
                    await _unsubscribe_quietly(self._connection, input_sub)
                await self._gate.abandon(admission, envelope, principal)

    async def _run_session(
        self,
        admission: CommandAdmission,
        envelope: CommandEnvelope,
        principal: str,
        input_sub: NatsSubscription,
        closed_sub: NatsSubscription,
        output_subject: str,
    ) -> None:
        sequences: dict[int, int] = {}

        async def output_sink(channel: int, payload: bytes) -> None:
            sequence = sequences.get(channel, 0)
            sequences[channel] = sequence + 1
            frame = encode_frame(
                DataFrame(
                    envelope.command_id,
                    data_channel_from_index(channel),
                    sequence,
                    payload,
                )
            )
            await self._connection.publish(output_subject, frame)

        try:
            result = await self._gate.run_admitted(
                admission,
                envelope,
                principal,
                _merged_input(input_sub.queue, closed_sub.queue),
                output_sink,
            )
            await self._publish_terminal(output_subject, result)
        finally:
            await _unsubscribe_quietly(self._connection, input_sub)
            await _unsubscribe_quietly(self._connection, closed_sub)

    async def _publish_terminal(
        self, output_subject: str, result: CommandResult
    ) -> None:
        terminal = encode_frame(
            TerminalFrame(
                schema_version=RELAY_SCHEMA_VERSION,
                command_id=result.command_id,
                outcome=result.outcome,
                reason_code=result.reason_code,
            )
        )
        await self._safe_publish(output_subject, terminal)

    async def _safe_publish(self, subject: str, payload: bytes) -> None:
        try:
            await self._connection.publish(subject, payload)
        except (NatsTransportError, NatsClosedError):
            # At-most-once transport: an undeliverable frame is a stream the
            # agent will classify as UNKNOWN. Never re-issue.
            return

    async def _reply(self, subject: str, reply: AdmissionReply) -> bool:
        try:
            await self._connection.publish(subject, reply.encode())
        except (NatsTransportError, NatsClosedError):
            return False
        return True


async def _merged_input(
    input_queue: asyncio.Queue[NatsMessage],
    closed_queue: asyncio.Queue[NatsMessage],
) -> AsyncIterator[bytes]:
    """Yield relay frames until the agent signals closed input.

    The close signal is honored only after every frame that per-publisher
    ordering placed ahead of it has been yielded.
    """

    next_input = asyncio.create_task(input_queue.get())
    closed = asyncio.create_task(closed_queue.get())
    try:
        while True:
            done, _ = await asyncio.wait(
                {next_input, closed}, return_when=asyncio.FIRST_COMPLETED
            )
            if next_input in done:
                yield next_input.result().payload
                next_input = asyncio.create_task(input_queue.get())
            elif closed in done:
                # Core NATS preserves per-publisher ordering across subjects,
                # so every `.in` frame of this command was queued ahead of its
                # `.closed` message; drain them instead of dropping frames
                # that arrived before this consumer woke up.
                while not input_queue.empty():
                    yield input_queue.get_nowait().payload
                return
    finally:
        for pending in (next_input, closed):
            pending.cancel()
        await asyncio.gather(next_input, closed, return_exceptions=True)


async def _unsubscribe_quietly(
    connection: AsyncNatsConnection, subscription: NatsSubscription
) -> None:
    try:
        await connection.unsubscribe(subscription)
    except (NatsTransportError, NatsClosedError):
        return


@dataclass(frozen=True, slots=True)
class RelayCommandResult:
    """The agent-side terminal view of one relay command."""

    command_id: str
    outcome: CommandOutcome
    reason_code: str | None


class NatsCommandClient:
    """Agent-side driver for one command over the relay subjects."""

    def __init__(self, connection: AsyncNatsConnection) -> None:
        self._connection = connection

    @property
    def principal(self) -> str:
        return self._connection.principal

    async def execute(
        self,
        envelope: CommandEnvelope,
        input_frames: AsyncIterator[bytes] | None,
        on_output: OutputSink | None = None,
        admission_timeout: float = 5.0,
        terminal_timeout: float = 30.0,
    ) -> RelayCommandResult:
        """Run one command and return its terminal outcome.

        The output subscription is opened before the admission request is
        sent: task subjects are a pure function of the principal, task, and
        command, and a command that fails fast on the manager side publishes
        its terminal frame immediately after the grant — Core NATS delivers
        it only to a subscriber that already exists. Stream loss at any stage
        fails closed to ``UNKNOWN``; the command is never automatically
        re-issued.
        """

        input_subject, closed_subject, output_subject = task_subjects(
            self._connection.principal, envelope.task_id, envelope.command_id
        )
        try:
            output_sub = await self._connection.subscribe(
                output_subject, PENDING_LIMIT
            )
        except (NatsTransportError, NatsClosedError):
            return RelayCommandResult(
                envelope.command_id, CommandOutcome.UNKNOWN, "transport_closed"
            )
        sequencer = FrameSequencer(envelope.command_id)
        try:
            try:
                reply_message = await self._connection.request(
                    admission_subject(self._connection.principal),
                    encode_command_envelope(envelope),
                    admission_timeout,
                )
            except NatsTransportError:
                return RelayCommandResult(
                    envelope.command_id,
                    CommandOutcome.UNKNOWN,
                    "admission_unanswered",
                )
            try:
                reply = AdmissionReply.decode(reply_message.payload)
            except NatsRelayError:
                return RelayCommandResult(
                    envelope.command_id,
                    CommandOutcome.UNKNOWN,
                    "invalid_admission_reply",
                )
            if not reply.admitted:
                return RelayCommandResult(
                    envelope.command_id, CommandOutcome.REJECTED, reply.refusal
                )
            assert (
                reply.input_subject is not None
                and reply.closed_subject is not None
                and reply.output_subject is not None
            )
            if (
                reply.input_subject,
                reply.closed_subject,
                reply.output_subject,
            ) != (input_subject, closed_subject, output_subject):
                # A granted reply must name this command's deterministic
                # task subjects; anything else is not the approved contract,
                # and input must not be published to it.
                return RelayCommandResult(
                    envelope.command_id, CommandOutcome.UNKNOWN, "subject_mismatch"
                )

            try:
                if input_frames is not None:
                    async for frame in input_frames:
                        await self._connection.publish(input_subject, frame)
                await self._connection.publish(closed_subject, b"")
            except (NatsTransportError, NatsClosedError):
                return RelayCommandResult(
                    envelope.command_id, CommandOutcome.UNKNOWN, "input_unpublished"
                )
            return await self._await_terminal(
                envelope.command_id,
                output_sub,
                sequencer,
                on_output,
                terminal_timeout,
            )
        finally:
            await _unsubscribe_quietly(self._connection, output_sub)

    async def _await_terminal(
        self,
        command_id: str,
        subscription: NatsSubscription,
        sequencer: FrameSequencer,
        on_output: OutputSink | None,
        timeout: float,
    ) -> RelayCommandResult:
        while True:
            try:
                message = await asyncio.wait_for(
                    subscription.queue.get(), timeout=timeout
                )
            except TimeoutError:
                return RelayCommandResult(
                    command_id, CommandOutcome.UNKNOWN, "no_terminal_frame"
                )
            try:
                frame = decode_frame(message.payload)
            except Exception:  # noqa: BLE001 - malformed frames fail closed
                return RelayCommandResult(
                    command_id, CommandOutcome.UNKNOWN, "malformed_frame"
                )
            outcome = await _consume_frame(frame, command_id, sequencer, on_output)
            if outcome is not None:
                return outcome


async def _consume_frame(
    frame: RelayFrame,
    command_id: str,
    sequencer: FrameSequencer,
    on_output: OutputSink | None,
) -> RelayCommandResult | None:
    """Observe one output frame; return a terminal result when it is one."""

    if isinstance(frame, TerminalFrame):
        if frame.command_id != command_id:
            return RelayCommandResult(
                command_id, CommandOutcome.UNKNOWN, "terminal_command_mismatch"
            )
        return RelayCommandResult(command_id, frame.outcome, frame.reason_code)
    if isinstance(frame, DataFrame):
        if frame.command_id != command_id:
            return RelayCommandResult(
                command_id, CommandOutcome.UNKNOWN, "frame_command_mismatch"
            )
        try:
            sequencer.observe(frame)
        except Exception:  # noqa: BLE001 - a sequence gap is stream loss
            return RelayCommandResult(
                command_id, CommandOutcome.UNKNOWN, "frame_sequence_gap"
            )
        if on_output is not None:
            await on_output(data_channel_index(frame.channel), frame.payload)
    return None
