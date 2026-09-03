"""JetStream lifecycle transport: durable spawn intake and ready publication.

The command relay (``nats_relay.py``) is at-most-once Core NATS by design.
Lifecycle traffic is different: spawn requests must survive a manager
restart and never be lost, and ready responses are the durable handoff to
the owning agent session. This module adapts the stdlib Core NATS client to
JetStream for exactly those two flows and nothing else:

- ``JetStreamSpawnIntake`` drives a durable pull consumer over the spawn
  stream. A delivery is acknowledged only after the repository durably
  accepted the task (at-least-once delivery with an idempotent
  repository), a payload that cannot decode, claims a principal other than
  the transport-authenticated one, or conflicts with durable identifiers is
  terminated (never redelivered — redelivery cannot fix any of those), and
  any other failure is negatively acknowledged for a bounded redelivery.
- ``JetStreamReadyPublisher`` publishes one payload-free ready response per
  task onto a deterministic manager-owned subject with a deterministic
  message ID, so republication after a lost acknowledgment deduplicates at
  the stream.
- ``JetStreamCommandEventJournal`` durably records every command-admission
  transition (identifiers, lifecycle facts, and outcomes only) and folds
  the log back into a fresh single-worker repository at startup, so the
  termination fence and in-flight slots survive a manager restart.

The transport principal comes from the subject alone, exactly as the
command relay recovers it: a spawn published on
``sandbox.agent.<principal>.spawn`` whose payload names a different agent
session is a poison message, not evidence of ownership (Decision 22).

Ready responses carry identifiers and relay routing only (Decision 4): task,
request, agent session, profile type, relay protocol version and admission
route, and the sandbox/result-volume references and UIDs. No endpoint, no
credential, no payload data.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from .commands import (
    COMMAND_EVENT_FIELDS,
    AdmissionFence,
    AdmittedCommand,
    CommandEvent,
    CommandEventKind,
    InMemoryCommandRepository,
)
from .intake import IdentifierConflictError, SpawnIntakeProcessor, TaskRepository
from .nats_client import (
    AsyncNatsConnection,
    NatsMessage,
    NatsTransportError,
)
from .nats_relay import NatsRelayError, require_subject_token
from .provisioning import ReadyTask
from .relay_protocol import CommandOutcome
from .spawn import SpawnRequestError, decode_spawn_request

#: Document schema of one ready response (independent of the relay schema).
READY_SCHEMA_VERSION = 1

#: Document schema of one command-admission journal event.
COMMAND_EVENT_SCHEMA_VERSION = 1

#: Where every command-admission journal event is published.
COMMAND_EVENT_SUBJECT = "sandbox.manager.command.event"

_JS_API_PREFIX = "$JS.API"
_ACK_PREFIX = "$JS.ACK"

_STATUS_NO_MESSAGES = "408"


class NatsLifecycleError(RuntimeError):
    """Raised when lifecycle subjects or documents are malformed."""


class JetStreamError(RuntimeError):
    """Raised when the JetStream transport misbehaves."""


class JetStreamApiError(JetStreamError):
    """Raised when a JetStream API request returns an error response."""

    def __init__(self, code: int, description: str, err_code: int | None = None) -> None:
        self.code = code
        self.err_code = err_code
        self.description = description
        detail = f"JetStream API error {code}"
        if err_code is not None:
            detail = f"{detail}/{err_code}"
        super().__init__(f"{detail}: {description}")


@dataclass(frozen=True, slots=True)
class JetStreamPubAck:
    """One confirmed JetStream publish."""

    stream: str
    sequence: int


def _require_token(value: str, field: str) -> str:
    try:
        return require_subject_token(value, field)
    except NatsRelayError as error:
        raise NatsLifecycleError(str(error)) from error


def spawn_subject(principal: str) -> str:
    """Where one agent session's spawn requests are published."""

    return f"sandbox.agent.{_require_token(principal, 'principal')}.spawn"


def principal_from_spawn_subject(subject: str) -> str:
    """Recover the transport principal from a spawn subject."""

    tokens = subject.split(".")
    if len(tokens) != 4 or tokens[:2] != ["sandbox", "agent"] or tokens[3] != "spawn":
        raise NatsLifecycleError(f"not a spawn subject: {subject}")
    return _require_token(tokens[2], "principal")


def ready_subject(principal: str, task_id: str) -> str:
    """Where one task's ready response is published (manager-owned)."""

    return (
        f"sandbox.manager.{_require_token(principal, 'principal')}"
        f".task.{_require_token(task_id, 'task_id')}.ready"
    )


def principal_from_ready_subject(subject: str) -> str:
    """Recover the owning principal from a ready-response subject."""

    tokens = subject.split(".")
    if (
        len(tokens) != 6
        or tokens[:2] != ["sandbox", "manager"]
        or tokens[3] != "task"
        or tokens[5] != "ready"
    ):
        raise NatsLifecycleError(f"not a ready subject: {subject}")
    return _require_token(tokens[2], "principal")


@dataclass(frozen=True, slots=True)
class ReadyResponse:
    """The payload-free durable handoff for one provisioned sandbox."""

    schema_version: int
    task_id: str
    request_id: str
    agent_session_id: str
    sandbox_type: str
    relay_protocol_version: int
    relay_route: str
    sandbox_reference: str
    sandbox_uid: str
    result_volume_reference: str
    result_volume_uid: str


_RESPONSE_FIELDS = frozenset(
    {
        "schema_version",
        "task_id",
        "request_id",
        "agent_session_id",
        "sandbox_type",
        "relay_protocol_version",
        "relay_route",
        "sandbox_reference",
        "sandbox_uid",
        "result_volume_reference",
        "result_volume_uid",
    }
)


def encode_ready_response(task: ReadyTask) -> bytes:
    """Encode one ready response; identifiers and routing only."""

    request = task.provisioning_task.queued_task.request
    sandbox = task.resources
    document = {
        "schema_version": READY_SCHEMA_VERSION,
        "task_id": request.task_id,
        "request_id": request.request_id,
        "agent_session_id": request.agent_session_id,
        "sandbox_type": request.sandbox_type,
        "relay_protocol_version": task.relay.protocol_version,
        "relay_route": task.relay.route,
        "sandbox_reference": sandbox.resource_reference,
        "sandbox_uid": sandbox.resource_uid,
        "result_volume_reference": sandbox.result_volume_reference,
        "result_volume_uid": sandbox.result_volume_uid,
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def decode_ready_response(payload: bytes) -> ReadyResponse:
    """Decode and validate one ready response."""

    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NatsLifecycleError("ready response must be UTF-8 JSON") from error
    if not isinstance(decoded, dict):
        raise NatsLifecycleError("ready response must be a JSON object")
    values: Mapping[str, object] = decoded
    if set(values) != _RESPONSE_FIELDS:
        raise NatsLifecycleError("ready response fields do not match the schema")
    if values["schema_version"] != READY_SCHEMA_VERSION:
        raise NatsLifecycleError("unsupported ready response schema version")
    for field in _RESPONSE_FIELDS - {"schema_version", "relay_protocol_version"}:
        value = values[field]
        if not isinstance(value, str) or not value:
            raise NatsLifecycleError(f"ready response field {field} must be text")
    if type(values["relay_protocol_version"]) is not int:
        raise NatsLifecycleError("relay protocol version must be an integer")
    return ReadyResponse(
        schema_version=READY_SCHEMA_VERSION,
        task_id=str(values["task_id"]),
        request_id=str(values["request_id"]),
        agent_session_id=str(values["agent_session_id"]),
        sandbox_type=str(values["sandbox_type"]),
        relay_protocol_version=int(values["relay_protocol_version"]),
        relay_route=str(values["relay_route"]),
        sandbox_reference=str(values["sandbox_reference"]),
        sandbox_uid=str(values["sandbox_uid"]),
        result_volume_reference=str(values["result_volume_reference"]),
        result_volume_uid=str(values["result_volume_uid"]),
    )


def _decode_api_document(payload: bytes, source: str) -> dict[str, object]:
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as failure:
        raise JetStreamError(f"invalid JetStream response from {source}") from failure
    if not isinstance(decoded, dict):
        raise JetStreamError(f"invalid JetStream response from {source}")
    return decoded


def _api_failure(document: dict[str, object]) -> JetStreamApiError | None:
    value = document.get("error")
    if not isinstance(value, dict):
        return None
    code = value.get("code")
    description = value.get("description")
    err_code = value.get("err_code")
    return JetStreamApiError(
        code if isinstance(code, int) else 0,
        description if isinstance(description, str) else "",
        err_code if isinstance(err_code, int) else None,
    )


def _config_field(info: dict[str, object], field: str) -> object | None:
    config = info.get("config")
    if not isinstance(config, dict):
        return None
    return config.get(field)


def encode_command_event(event: CommandEvent) -> bytes:
    """Encode one journal event as canonical JSON (sorted keys)."""

    document: dict[str, object] = {
        "schema_version": COMMAND_EVENT_SCHEMA_VERSION,
        "kind": event.kind.value,
    }
    for field in COMMAND_EVENT_FIELDS[event.kind]:
        value: object = getattr(event, field)
        if isinstance(value, datetime):
            document[field] = value.isoformat()
        elif isinstance(value, CommandOutcome):
            document[field] = value.value
        else:
            document[field] = value
    return json.dumps(document, sort_keys=True).encode("utf-8")


def decode_command_event(payload: bytes) -> CommandEvent:
    """Decode and validate one journal event, failing closed on drift."""

    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NatsLifecycleError("command event must be UTF-8 JSON") from error
    if not isinstance(decoded, dict):
        raise NatsLifecycleError("command event must be a JSON object")
    values: Mapping[str, object] = decoded
    if values.get("schema_version") != COMMAND_EVENT_SCHEMA_VERSION:
        raise NatsLifecycleError("unsupported command event schema version")
    try:
        kind = CommandEventKind(str(values.get("kind", "")))
    except ValueError as error:
        raise NatsLifecycleError("unknown command event kind") from error
    expected = {"schema_version", "kind", *COMMAND_EVENT_FIELDS[kind]}
    if set(values) != expected:
        raise NatsLifecycleError("command event fields do not match its kind")
    arguments: dict[str, Any] = {}
    for field in COMMAND_EVENT_FIELDS[kind]:
        value = values[field]
        if field in ("admitted_at", "completed_at"):
            arguments[field] = _event_datetime(value, field)
        elif field == "outcome":
            arguments[field] = _event_outcome(value)
        else:
            if not isinstance(value, str) or not value:
                raise NatsLifecycleError(f"command event field {field} must be text")
            arguments[field] = value
    return CommandEvent(kind=kind, **arguments)


def _event_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise NatsLifecycleError(f"command event field {field} must be text")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise NatsLifecycleError(f"command event {field} is not a timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise NatsLifecycleError(f"command event {field} must include a timezone")
    return parsed.astimezone(UTC)


def _event_outcome(value: object) -> CommandOutcome:
    if not isinstance(value, str):
        raise NatsLifecycleError("command event outcome must be text")
    try:
        return CommandOutcome(value)
    except ValueError as error:
        raise NatsLifecycleError("unknown command outcome") from error


class JetStream:
    """Minimal JetStream semantics over one authenticated connection.

    Stream and consumer management goes through the JSON request/reply API
    on ``$JS.API``; publishes carry headers (message-ID deduplication) and
    await the server's publish acknowledgment; pulls use one-message
    ``CONSUMER.MSG.NEXT`` requests with a server-side wait window. Anything
    the API reports as an error fails closed as ``JetStreamApiError``.
    """

    def __init__(
        self,
        connection: AsyncNatsConnection,
        *,
        request_timeout: float = 5.0,
    ) -> None:
        self._connection = connection
        self._request_timeout = request_timeout

    @property
    def connection(self) -> AsyncNatsConnection:
        """The underlying Core NATS connection (for acknowledgment publishes)."""

        return self._connection

    async def _api(
        self, endpoint: str, payload: Mapping[str, object] | None = None
    ) -> dict[str, object]:
        body = b"" if payload is None else json.dumps(payload).encode("utf-8")
        message = await self._connection.request(
            f"{_JS_API_PREFIX}.{endpoint}", body, self._request_timeout
        )
        decoded = _decode_api_document(message.payload, endpoint)
        failure = _api_failure(decoded)
        if failure is not None:
            raise failure
        return decoded

    async def ensure_stream(
        self, name: str, subjects: tuple[str, ...]
    ) -> dict[str, object]:
        """Create a stream or verify the existing one covers the subjects."""

        config: dict[str, object] = {
            "name": name,
            "subjects": list(subjects),
            "retention": "limits",
            "storage": "file",
        }
        try:
            return await self._api(f"STREAM.CREATE.{name}", config)
        except JetStreamApiError as failure:
            if "already in use" not in failure.description:
                raise
        info = await self._api(f"STREAM.INFO.{name}")
        stored = _config_field(info, "subjects")
        if not isinstance(stored, list) or not set(subjects) <= set(stored):
            raise JetStreamError(f"stream {name} does not capture the required subjects")
        return info

    async def ensure_consumer(
        self,
        stream: str,
        durable: str,
        filter_subject: str,
        *,
        ack_wait_seconds: float,
        max_deliver: int,
        max_ack_pending: int = 1,
    ) -> dict[str, object]:
        """Create a durable explicit-ack pull consumer or verify the existing one."""

        # The consumer create request nests the consumer config under
        # "config"; flat fields are silently ignored by the server.
        config: dict[str, object] = {
            "durable_name": durable,
            "filter_subject": filter_subject,
            "ack_policy": "explicit",
            "ack_wait": int(ack_wait_seconds * 1_000_000_000),
            "max_deliver": max_deliver,
            "max_ack_pending": max_ack_pending,
            "deliver_policy": "all",
        }
        request: dict[str, object] = {"stream_name": stream, "config": config}
        try:
            return await self._api(f"CONSUMER.CREATE.{stream}.{durable}", request)
        except JetStreamApiError as failure:
            if "exist" not in failure.description:
                raise
        info = await self._api(f"CONSUMER.INFO.{stream}.{durable}")
        # Server 2.11 reports the durable as "name"; older servers (and the
        # in-process fake) use "durable_name". Either spelling identifies it.
        durable_value = _config_field(info, "name")
        if durable_value is None:
            durable_value = _config_field(info, "durable_name")
        if durable_value != durable:
            raise JetStreamError(
                f"consumer {stream}/{durable} has an incompatible durable name"
            )
        for field, expected in (
            ("filter_subject", filter_subject),
            ("ack_policy", "explicit"),
        ):
            if _config_field(info, field) != expected:
                raise JetStreamError(
                    f"consumer {stream}/{durable} has an incompatible {field}"
                )
        return info

    async def create_ephemeral_consumer(
        self,
        stream: str,
        filter_subject: str,
        *,
        inactive_threshold_seconds: float = 60.0,
    ) -> str:
        """Create an ephemeral pull consumer reading from the beginning.

        Replay-only: no durable name, no acknowledgment tracking, no
        redelivery. The server reaps it once it goes inactive, so callers
        that finish early still delete it to keep the consumer count flat.
        """

        request: dict[str, object] = {
            "stream_name": stream,
            "config": {
                "filter_subject": filter_subject,
                "deliver_policy": "all",
                "ack_policy": "none",
                "inactive_threshold": int(inactive_threshold_seconds * 1_000_000_000),
                "max_waiting": 15,
            },
        }
        info = await self._api(f"CONSUMER.CREATE.{stream}", request)
        name = info.get("name")
        if not isinstance(name, str) or not name:
            raise JetStreamError("ephemeral consumer creation returned no name")
        return name

    async def delete_consumer(self, stream: str, consumer: str) -> None:
        """Delete one consumer; an already-absent consumer is success."""

        try:
            await self._api(f"CONSUMER.DELETE.{stream}.{consumer}")
        except JetStreamApiError as failure:
            if "not found" not in failure.description:
                raise

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        msg_id: str | None = None,
        timeout: float | None = None,
    ) -> JetStreamPubAck:
        """Publish with a server-side acknowledgment and optional dedup."""

        headers = {"Nats-Msg-Id": msg_id} if msg_id is not None else None
        message = await self._connection.request(
            subject, payload, timeout or self._request_timeout, headers=headers
        )
        status = None if message.headers is None else message.headers.get("Status")
        if status is not None:
            raise JetStreamError(
                f"publish was not acknowledged (status {status})"
            )
        decoded = _decode_api_document(message.payload, "publish")
        failure = _api_failure(decoded)
        if failure is not None:
            raise failure
        stream = decoded.get("stream")
        sequence = decoded.get("seq")
        if not isinstance(stream, str) or not isinstance(sequence, int):
            raise JetStreamError("publish acknowledgment is missing stream or sequence")
        return JetStreamPubAck(stream, sequence)

    async def fetch(
        self, stream: str, durable: str, *, wait_seconds: float
    ) -> NatsMessage | None:
        """Pull the next delivery, holding the request server-side.

        Returns ``None`` when the window elapses with nothing to deliver or
        the server reports no messages; any other status is a failure.
        """

        # ``expires`` is a Go time.Duration on the wire: integer nanoseconds.
        request = json.dumps(
            {"batch": 1, "expires": int(wait_seconds * 1_000_000_000)}
        ).encode("ascii")
        try:
            message = await self._connection.request(
                f"{_JS_API_PREFIX}.CONSUMER.MSG.NEXT.{stream}.{durable}",
                request,
                wait_seconds + 2.0,
            )
        except NatsTransportError:
            # A genuine empty window surfaces as a reply timeout on a live
            # connection; a dead connection must fail closed instead of
            # looking like "no messages" (and hot-looping the caller).
            if self._connection.closed:
                raise
            return None
        if message.headers is not None:
            status = message.headers.get("Status")
            if status == _STATUS_NO_MESSAGES:
                return None
            if status is not None:
                raise JetStreamError(
                    f"pull delivery ended with status {status}"
                    f" {message.headers.get('Description', '')}".strip()
                )
        if message.reply is None or not message.reply.startswith(_ACK_PREFIX):
            raise JetStreamError("pull delivery carried no acknowledgment subject")
        return message


class JetStreamMessage:
    """One delivered JetStream message with its acknowledgment operations."""

    def __init__(self, message: NatsMessage, connection: AsyncNatsConnection) -> None:
        self._message = message
        self._connection = connection

    @property
    def subject(self) -> str:
        return self._message.subject

    @property
    def payload(self) -> bytes:
        return self._message.payload

    @property
    def headers(self) -> Mapping[str, str] | None:
        return self._message.headers

    async def acknowledge(self) -> None:
        await self._connection.publish(self._message.reply or "", b"+ACK")

    async def terminate(self) -> None:
        """Acknowledge terminally: the message must never be redelivered."""

        await self._connection.publish(self._message.reply or "", b"+TERM")

    async def nak(self, delay_seconds: float = 0.0) -> None:
        """Negatively acknowledge for redelivery, optionally delayed."""

        payload = b"-NAK"
        if delay_seconds > 0:
            nanoseconds = int(delay_seconds * 1_000_000_000)
            payload += b" " + json.dumps({"delay": nanoseconds}).encode("ascii")
        await self._connection.publish(self._message.reply or "", payload)


class SpawnIntakeObserver(Protocol):
    def poisoned(self, subject: str, reason: str) -> None:
        """A delivery was terminated; ``reason`` names why (payload-free)."""
        ...

    def deferred(self, subject: str, reason: str) -> None:
        """A delivery was negatively acknowledged for redelivery."""
        ...


class _NullObserver:
    def poisoned(self, subject: str, reason: str) -> None:
        return

    def deferred(self, subject: str, reason: str) -> None:
        return


class JetStreamSpawnIntake:
    """Durable spawn-request intake over a JetStream pull consumer.

    Acknowledges only after the repository durably accepted the task,
    terminates poison (undecodable, principal-mismatching, or conflicting)
    deliveries, and negatively acknowledges anything else for a bounded
    redelivery. The transport principal recovered from the spawn subject is
    the only identity evidence; the payload must agree with it.
    """

    def __init__(
        self,
        jetstream: JetStream,
        repository: TaskRepository,
        *,
        stream: str = "SANDBOX_SPAWN",
        subjects: tuple[str, ...] = ("sandbox.agent.*.spawn",),
        durable: str = "sandbox-manager-intake",
        ack_wait_seconds: float = 30.0,
        max_deliver: int = 20,
        pull_wait_seconds: float = 1.0,
        redelivery_delay_seconds: float = 1.0,
        retry_backoff_seconds: float = 1.0,
        observer: SpawnIntakeObserver | None = None,
    ) -> None:
        if not subjects:
            raise ValueError("at least one spawn subject is required")
        self._jetstream = jetstream
        self._repository = repository
        self._stream = stream
        self._subjects = subjects
        self._durable = durable
        self._ack_wait_seconds = ack_wait_seconds
        self._max_deliver = max_deliver
        self._pull_wait_seconds = pull_wait_seconds
        self._redelivery_delay_seconds = redelivery_delay_seconds
        self._retry_backoff_seconds = retry_backoff_seconds
        self._observer: SpawnIntakeObserver = observer or _NullObserver()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Ensure the stream and durable consumer exist, then consume."""

        await self._jetstream.ensure_stream(self._stream, self._subjects)
        await self._jetstream.ensure_consumer(
            self._stream,
            self._durable,
            self._subjects[0],
            ack_wait_seconds=self._ack_wait_seconds,
            max_deliver=self._max_deliver,
        )
        self._task = asyncio.create_task(
            self._run(), name=f"spawn-intake-{self._durable}"
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        processor = SpawnIntakeProcessor(self._repository)
        while True:
            try:
                message = await self._jetstream.fetch(
                    self._stream, self._durable, wait_seconds=self._pull_wait_seconds
                )
            except Exception:  # noqa: BLE001 - any transport failure backs off
                await asyncio.sleep(self._retry_backoff_seconds)
                continue
            if message is None:
                continue
            await self._process(
                processor, JetStreamMessage(message, self._jetstream.connection)
            )

    async def _process(
        self, processor: SpawnIntakeProcessor, delivery: JetStreamMessage
    ) -> None:
        try:
            principal = principal_from_spawn_subject(delivery.subject)
            request = decode_spawn_request(delivery.payload)
        except (NatsLifecycleError, SpawnRequestError):
            await delivery.terminate()
            self._observer.poisoned(delivery.subject, "invalid_request")
            return
        if request.agent_session_id != principal:
            await delivery.terminate()
            self._observer.poisoned(delivery.subject, "principal_mismatch")
            return
        try:
            await processor.process(_SpawnDelivery(delivery))
        except IdentifierConflictError:
            await delivery.terminate()
            self._observer.poisoned(delivery.subject, "identifier_conflict")
        except Exception as failure:  # noqa: BLE001 - unknown failures redeliver
            # Transport, repository-persistence, or unknown failures cannot be
            # classified as poison: redeliver on a delay, bounded by the
            # consumer's max_deliver.
            await self._redeliver(delivery, type(failure).__name__)

    async def _redeliver(self, delivery: JetStreamMessage, reason: str) -> None:
        try:
            await delivery.nak(delay_seconds=self._redelivery_delay_seconds)
        except (JetStreamError, NatsTransportError):
            # The ack wait elapses server-side and the delivery returns.
            self._observer.deferred(delivery.subject, f"{reason}:nak_lost")
            return
        self._observer.deferred(delivery.subject, reason)


class _SpawnDelivery:
    """Adapter from one JetStream message to the intake's delivery port."""

    def __init__(self, message: JetStreamMessage) -> None:
        self._message = message

    @property
    def payload(self) -> bytes:
        return self._message.payload

    async def acknowledge(self) -> None:
        await self._message.acknowledge()


class JetStreamReadyPublisher:
    """Publishes durable ready responses onto deterministic subjects."""

    def __init__(
        self,
        jetstream: JetStream,
        *,
        stream: str = "SANDBOX_READY",
        subjects: tuple[str, ...] = ("sandbox.manager.>",),
    ) -> None:
        self._jetstream = jetstream
        self._stream = stream
        self._subjects = subjects

    async def start(self) -> None:
        await self._jetstream.ensure_stream(self._stream, self._subjects)

    async def publish_ready(self, task: ReadyTask) -> None:
        request = task.provisioning_task.queued_task.request
        subject = ready_subject(request.agent_session_id, request.task_id)
        payload = encode_ready_response(task)
        await self._jetstream.publish(
            subject, payload, msg_id=f"ready-{request.task_id}"
        )


class JetStreamCommandEventJournal:
    """Durable, payload-free journal of command-admission transitions.

    The repository writes every transition here ahead of its in-memory
    application, so the log never trails the live view. A restarted
    manager folds the log back into a fresh single-worker repository and
    adopts a new fence. Replay goes through the public repository
    methods, which keeps it idempotent under duplicate events (a lost
    publish acknowledgment can store one event twice) and makes the fold
    reuse exactly the semantics the live path enforced.
    """

    def __init__(
        self,
        jetstream: JetStream,
        *,
        stream: str = "SANDBOX_COMMANDS",
        subjects: tuple[str, ...] = (COMMAND_EVENT_SUBJECT,),
        pull_wait_seconds: float = 0.5,
    ) -> None:
        self._jetstream = jetstream
        self._stream = stream
        self._subjects = subjects
        self._pull_wait_seconds = pull_wait_seconds

    @property
    def stream(self) -> str:
        return self._stream

    async def start(self) -> None:
        """Ensure the journal stream exists."""

        await self._jetstream.ensure_stream(self._stream, self._subjects)

    async def append(self, event: CommandEvent) -> None:
        """Durably append one event; a failure must abort the mutation."""

        await self._jetstream.publish(
            COMMAND_EVENT_SUBJECT, encode_command_event(event)
        )

    async def replay_into(self, repository: InMemoryCommandRepository) -> int:
        """Fold the whole journal into ``repository``; returns event count.

        Must run before any journal is attached to ``repository``, so
        replay does not re-append the events it reads. An undecodable
        entry fails closed: starting with a hole in the log is worse than
        not starting.
        """

        consumer = await self._jetstream.create_ephemeral_consumer(
            self._stream, COMMAND_EVENT_SUBJECT
        )
        admitted: dict[str, AdmittedCommand] = {}
        count = 0
        try:
            while True:
                delivery = await self._jetstream.fetch(
                    self._stream, consumer, wait_seconds=self._pull_wait_seconds
                )
                if delivery is None:
                    return count
                try:
                    event = decode_command_event(delivery.payload)
                except NatsLifecycleError as failure:
                    raise JetStreamError(
                        "command event journal carries an undecodable entry"
                    ) from failure
                await _fold_command_event(repository, event, admitted)
                count += 1
        finally:
            await self._jetstream.delete_consumer(self._stream, consumer)

    async def load_repository(
        self, fence: AdmissionFence
    ) -> InMemoryCommandRepository:
        """Rebuild the single-worker repository after a restart.

        Folds the journal into a journal-less repository, attaches this
        journal for live mutations, then adopts the new fence generation
        (classifying any in-flight command from a prior process
        ``UNKNOWN``). The returned repository is the live authority.
        """

        repository = InMemoryCommandRepository()
        await self.replay_into(repository)
        repository.attach_journal(self)
        await repository.recover(fence.generation)
        return repository


async def _fold_command_event(
    repository: InMemoryCommandRepository,
    event: CommandEvent,
    admitted: dict[str, AdmittedCommand],
) -> None:
    """Apply one replayed event through the public repository methods."""

    if event.kind is CommandEventKind.SEED_READY:
        await repository.seed_ready(event.seed_state())
    elif event.kind is CommandEventKind.ADMITTED:
        decision = await repository.admit(event.admission_request())
        command = decision.command
        if command is not None:
            admitted[command.command_id] = command
    elif event.kind is CommandEventKind.FINISHED:
        command_id = event.command_id
        command = admitted.get(command_id) if command_id is not None else None
        if command is not None and event.outcome is not None and event.completed_at is not None:
            await repository.finish(command, event.outcome, event.completed_at)
    elif event.kind is CommandEventKind.TERMINATION_BEGUN:
        if event.task_id is not None:
            await repository.begin_termination(event.task_id)
    elif (
        event.kind is CommandEventKind.FENCE_ADOPTED
        and event.fence_generation is not None
    ):
        await repository.recover(event.fence_generation)
