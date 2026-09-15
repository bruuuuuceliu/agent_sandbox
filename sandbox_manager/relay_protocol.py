"""Versioned wire schemas for the manager-mediated command relay."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal, cast

from .identifiers import is_identifier

RELAY_SCHEMA_VERSION: Literal[1] = 1

#: Fixed maximum size of one encoded data frame, envelope to end.
MAX_FRAME_BYTES = 64 * 1024

#: Largest payload one data frame may carry after its header budget.
MAX_FRAME_PAYLOAD_BYTES = MAX_FRAME_BYTES - 64

#: Fixed maximum size a command may declare for one direction.
MAX_DECLARED_BYTES = 256 * 1024 * 1024

_FRAME_HEADER_BUDGET = MAX_FRAME_BYTES - MAX_FRAME_PAYLOAD_BYTES
_MAX_LENGTH_PREFIX_VALUE = (1 << 16) - 1

_REQUIRED_ENVELOPE_FIELDS = frozenset(
    {
        "schema_version",
        "command_id",
        "task_id",
        "operation",
        "correlation_id",
        "declared_input_bytes",
        "declared_output_bytes",
    }
)
_ALL_ENVELOPE_FIELDS = _REQUIRED_ENVELOPE_FIELDS


class RelayProtocolError(ValueError):
    """Raised when relay wire data cannot be decoded or validated."""


class UnsupportedRelaySchemaVersion(RelayProtocolError):
    """Raised when relay wire data uses an unsupported schema version."""

    def __init__(self, version: int) -> None:
        self.version = version
        super().__init__(f"unsupported relay schema version: {version}")


class FrameLimitError(RelayProtocolError):
    """Raised when an encoded frame exceeds the fixed frame maximum."""

    def __init__(self, size: int) -> None:
        self.size = size
        super().__init__(f"encoded frame exceeds {MAX_FRAME_BYTES} bytes")


class FrameSequenceError(RelayProtocolError):
    """Raised when a channel receives a missing or duplicate frame."""

    def __init__(self, command_id: str, expected: int, received: int) -> None:
        self.command_id = command_id
        self.expected = expected
        self.received = received
        super().__init__(f"expected frame sequence {expected}, received {received}")


class RelayOperation(StrEnum):
    EXEC = "exec"
    PTY = "pty"
    UPLOAD = "upload"
    DOWNLOAD = "download"


class CommandOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class DataChannel(StrEnum):
    STDIN = "stdin"
    STDOUT = "stdout"
    STDERR = "stderr"


#: Outcomes that end a command. ``UNKNOWN`` is terminal for reporting but
#: records that the manager cannot confirm what happened remotely.
_TERMINAL_OUTCOMES = frozenset(CommandOutcome)


@dataclass(frozen=True, slots=True)
class CommandEnvelope:
    """Versioned control envelope admitting exactly one relay operation."""

    schema_version: Literal[1]
    command_id: str
    task_id: str
    operation: RelayOperation
    correlation_id: str
    declared_input_bytes: int
    declared_output_bytes: int

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != RELAY_SCHEMA_VERSION
        ):
            raise RelayProtocolError(
                f"unsupported relay schema version: {self.schema_version}"
            )
        _require_identifier(self.command_id, "command ID")
        _require_identifier(self.task_id, "task ID")
        _require_identifier(self.correlation_id, "correlation ID")
        if not isinstance(self.operation, RelayOperation):
            raise RelayProtocolError("operation must be a relay operation")
        _require_size(self.declared_input_bytes, "declared input size")
        _require_size(self.declared_output_bytes, "declared output size")


def encode_command_envelope(envelope: CommandEnvelope) -> bytes:
    """Encode one control envelope as canonical JSON."""

    return json.dumps(
        {
            "schema_version": envelope.schema_version,
            "command_id": envelope.command_id,
            "task_id": envelope.task_id,
            "operation": envelope.operation.value,
            "correlation_id": envelope.correlation_id,
            "declared_input_bytes": envelope.declared_input_bytes,
            "declared_output_bytes": envelope.declared_output_bytes,
        },
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_command_envelope(payload: bytes | str) -> CommandEnvelope:
    """Decode one versioned JSON control envelope."""

    if isinstance(payload, bytes):
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise RelayProtocolError("command envelope must be UTF-8 JSON") from error
    else:
        text = payload

    try:
        decoded = json.loads(text, object_pairs_hook=_unique_object)
    except json.JSONDecodeError as error:
        raise RelayProtocolError("command envelope must be valid JSON") from error

    if not isinstance(decoded, dict):
        raise RelayProtocolError("command envelope must be a JSON object")
    values = cast(dict[str, object], decoded)

    missing = _REQUIRED_ENVELOPE_FIELDS - values.keys()
    if missing:
        raise RelayProtocolError(f"missing command envelope field: {min(missing)}")
    unknown = values.keys() - _ALL_ENVELOPE_FIELDS
    if unknown:
        raise RelayProtocolError(f"unknown command envelope field: {min(unknown)}")

    schema_version = values["schema_version"]
    if type(schema_version) is not int:
        raise RelayProtocolError("schema_version must be an integer")
    if schema_version != RELAY_SCHEMA_VERSION:
        raise UnsupportedRelaySchemaVersion(schema_version)

    operation = values["operation"]
    if not isinstance(operation, str):
        raise RelayProtocolError("operation must be a string")
    try:
        parsed_operation = RelayOperation(operation)
    except ValueError as error:
        raise RelayProtocolError(f"unsupported relay operation: {operation}") from error

    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=_required_identifier(values, "command_id"),
        task_id=_required_identifier(values, "task_id"),
        operation=parsed_operation,
        correlation_id=_required_identifier(values, "correlation_id"),
        declared_input_bytes=_require_size_value(values, "declared_input_bytes"),
        declared_output_bytes=_require_size_value(values, "declared_output_bytes"),
    )


@dataclass(frozen=True, slots=True)
class DataFrame:
    """One bounded, sequenced frame of relay stream data."""

    command_id: str
    channel: DataChannel
    sequence: int
    payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        _require_identifier(self.command_id, "command ID")
        if not isinstance(self.channel, DataChannel):
            raise RelayProtocolError("data channel must be a relay channel")
        if type(self.sequence) is not int or self.sequence < 0:
            raise RelayProtocolError("frame sequence must be a non-negative integer")
        if not isinstance(self.payload, (bytes, bytearray)):
            raise RelayProtocolError("frame payload must be bytes")
        payload = bytes(self.payload)
        object.__setattr__(self, "payload", payload)
        if len(payload) > MAX_FRAME_BYTES - _FRAME_HEADER_BUDGET:
            raise FrameLimitError(len(payload) + _FRAME_HEADER_BUDGET)


@dataclass(frozen=True, slots=True)
class CancelSignal:
    """Requests best-effort cancellation of one in-flight command."""

    schema_version: Literal[1]
    command_id: str

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != RELAY_SCHEMA_VERSION
        ):
            raise RelayProtocolError(
                f"unsupported relay schema version: {self.schema_version}"
            )
        _require_identifier(self.command_id, "command ID")


@dataclass(frozen=True, slots=True)
class TerminalFrame:
    """Carries the terminal command outcome and a payload-free reason code."""

    schema_version: Literal[1]
    command_id: str
    outcome: CommandOutcome
    reason_code: str | None = None

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != RELAY_SCHEMA_VERSION
        ):
            raise RelayProtocolError(
                f"unsupported relay schema version: {self.schema_version}"
            )
        _require_identifier(self.command_id, "command ID")
        if not isinstance(self.outcome, CommandOutcome):
            raise RelayProtocolError("terminal outcome must be a command outcome")
        if self.reason_code is not None:
            _require_wire_text(self.reason_code, "reason code")


RelayFrame = DataFrame | CancelSignal | TerminalFrame

_DATA_FRAME = 0x01
_CANCEL_FRAME = 0x02
_TERMINAL_FRAME = 0x03

_CHANNEL_INDEX = {
    DataChannel.STDIN: 0,
    DataChannel.STDOUT: 1,
    DataChannel.STDERR: 2,
}
_INDEX_CHANNEL = {index: channel for channel, index in _CHANNEL_INDEX.items()}


def data_channel_index(channel: DataChannel) -> int:
    """Return the wire index of one data channel."""

    return _CHANNEL_INDEX[channel]


def data_channel_from_index(index: int) -> DataChannel:
    """Return the data channel for one wire index, or raise."""

    try:
        return _INDEX_CHANNEL[index]
    except KeyError as error:
        raise RelayProtocolError(
            f"unknown data channel index: {index}"
        ) from error

_OUTCOME_INDEX = {outcome: index for index, outcome in enumerate(CommandOutcome)}
_INDEX_OUTCOME = {index: outcome for outcome, index in _OUTCOME_INDEX.items()}


def encode_frame(frame: RelayFrame) -> bytes:
    """Encode one relay frame into its bounded binary wire form."""

    if isinstance(frame, DataFrame):
        channel_index = _CHANNEL_INDEX[frame.channel]
        command_id = frame.command_id.encode("ascii")
        payload = frame.payload
        encoded = (
            bytes((_DATA_FRAME, channel_index))
            + frame.sequence.to_bytes(4, "big")
            + len(command_id).to_bytes(2, "big")
            + command_id
            + len(payload).to_bytes(4, "big")
            + payload
        )
    elif isinstance(frame, CancelSignal):
        command_id = frame.command_id.encode("ascii")
        encoded = (
            bytes((_CANCEL_FRAME,))
            + len(command_id).to_bytes(2, "big")
            + command_id
        )
    elif isinstance(frame, TerminalFrame):
        command_id = frame.command_id.encode("ascii")
        reason = b"" if frame.reason_code is None else frame.reason_code.encode("ascii")
        encoded = (
            bytes((_TERMINAL_FRAME, _OUTCOME_INDEX[frame.outcome]))
            + len(command_id).to_bytes(2, "big")
            + command_id
            + len(reason).to_bytes(2, "big")
            + reason
        )
    else:
        raise RelayProtocolError("unsupported relay frame type")
    if len(encoded) > MAX_FRAME_BYTES:
        raise FrameLimitError(len(encoded))
    return encoded


def decode_frame(data: bytes | bytearray) -> RelayFrame:
    """Decode exactly one bounded relay frame."""

    raw = bytes(data)
    if len(raw) > MAX_FRAME_BYTES:
        raise FrameLimitError(len(raw))
    if not raw:
        raise RelayProtocolError("empty relay frame")

    kind = raw[0]
    if kind == _DATA_FRAME:
        if len(raw) < 8:
            raise RelayProtocolError("data frame header is truncated")
        channel_index = raw[1]
        if channel_index not in _INDEX_CHANNEL:
            raise RelayProtocolError(f"unknown data channel index: {channel_index}")
        sequence = int.from_bytes(raw[2:6], "big")
        command_id, offset = _take_required_text(raw, 6, 2)
        payload, offset = _take_length_prefixed_raw(raw, offset, 4)
        if offset != len(raw):
            raise RelayProtocolError("data frame has trailing bytes")
        return DataFrame(
            command_id=command_id,
            channel=_INDEX_CHANNEL[channel_index],
            sequence=sequence,
            payload=payload,
        )
    if kind == _CANCEL_FRAME:
        command_id, offset = _take_required_text(raw, 1, 2)
        if offset != len(raw):
            raise RelayProtocolError("cancel frame has trailing bytes")
        return CancelSignal(
            schema_version=RELAY_SCHEMA_VERSION, command_id=command_id
        )
    if kind == _TERMINAL_FRAME:
        if len(raw) < 2:
            raise RelayProtocolError("terminal frame header is truncated")
        outcome_index = raw[1]
        if outcome_index not in _INDEX_OUTCOME:
            raise RelayProtocolError(f"unknown terminal outcome index: {outcome_index}")
        command_id, offset = _take_required_text(raw, 2, 2)
        reason, offset = _take_length_prefixed(raw, offset, 2, allow_empty_none=True)
        if offset != len(raw):
            raise RelayProtocolError("terminal frame has trailing bytes")
        return TerminalFrame(
            schema_version=RELAY_SCHEMA_VERSION,
            command_id=command_id,
            outcome=_INDEX_OUTCOME[outcome_index],
            reason_code=reason,
        )
    raise RelayProtocolError(f"unknown relay frame kind: {kind}")


class FrameSequencer:
    """Enforces gap-free, duplicate-free frame order per channel."""

    def __init__(self, command_id: str) -> None:
        _require_identifier(command_id, "command ID")
        self._command_id = command_id
        self._expected: dict[DataChannel, int] = {}

    def observe(self, frame: DataFrame) -> None:
        """Accept the next frame of one channel or report a sequence error."""

        if frame.command_id != self._command_id:
            raise RelayProtocolError("frame does not belong to this command")
        expected = self._expected.get(frame.channel, 0)
        if frame.sequence != expected:
            raise FrameSequenceError(self._command_id, expected, frame.sequence)
        self._expected[frame.channel] = expected + 1


@dataclass(frozen=True, slots=True)
class OutcomeReport:
    """Payload-free terminal record for one command."""

    command_id: str
    outcome: CommandOutcome
    reason_code: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.command_id, "command ID")
        if not isinstance(self.outcome, CommandOutcome):
            raise RelayProtocolError("outcome must be a command outcome")
        if self.outcome not in _TERMINAL_OUTCOMES:
            raise RelayProtocolError("outcome must be terminal")
        if self.reason_code is not None:
            _require_wire_text(self.reason_code, "reason code")


def _take_length_prefixed_raw(raw: bytes, offset: int, width: int) -> tuple[bytes, int]:
    """Read one length-prefixed byte string without decoding it as text."""

    end = offset + width
    if len(raw) < end:
        raise RelayProtocolError("relay frame is truncated")
    length = int.from_bytes(raw[offset:end], "big")
    value_end = end + length
    if len(raw) < value_end:
        raise RelayProtocolError("relay frame is truncated")
    return raw[end:value_end], value_end


def _take_length_prefixed(
    raw: bytes, offset: int, width: int, *, allow_empty_none: bool = False
) -> tuple[str | None, int]:
    """Read one length-prefixed ASCII identifier (None when empty and allowed)."""

    end = offset + width
    if len(raw) < end:
        raise RelayProtocolError("relay frame is truncated")
    length = int.from_bytes(raw[offset:end], "big")
    value_end = end + length
    if len(raw) < value_end:
        raise RelayProtocolError("relay frame is truncated")
    value = raw[end:value_end]
    if allow_empty_none and not value:
        return None, value_end
    try:
        text = value.decode("ascii")
    except UnicodeDecodeError as error:
        raise RelayProtocolError("relay frame identifiers must be ASCII") from error
    _require_wire_text(text, "frame text")
    return text, value_end


def _take_required_text(raw: bytes, offset: int, width: int) -> tuple[str, int]:
    """Read one length-prefixed ASCII identifier that must be present."""

    text, value_end = _take_length_prefixed(raw, offset, width)
    if text is None:  # pragma: no cover - only the optional variant returns None
        raise RelayProtocolError("relay frame identifiers must be ASCII")
    _require_identifier(text, "frame identifier")
    return text, value_end


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, value in pairs:
        if key in values:
            raise RelayProtocolError(f"duplicate JSON field: {key}")
        values[key] = value
    return values


def _required_text(values: dict[str, object], field: str) -> str:
    value = values[field]
    if not isinstance(value, str) or not value or value != value.strip():
        raise RelayProtocolError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise RelayProtocolError(f"{field} contains invalid text")
    return value


def _required_identifier(values: dict[str, object], field: str) -> str:
    value = values[field]
    if not is_identifier(value):
        raise RelayProtocolError(
            f"{field} must contain 1-64 ASCII letters, digits, underscores, or hyphens"
        )
    return value


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RelayProtocolError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise RelayProtocolError(f"{field} contains invalid text")


def _require_identifier(value: object, field: str) -> None:
    if not is_identifier(value):
        raise RelayProtocolError(
            f"{field} must contain 1-64 ASCII letters, digits, underscores, or hyphens"
        )


def _require_wire_text(value: object, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RelayProtocolError(f"{field} must be a non-empty string")
    if len(value) > _MAX_LENGTH_PREFIX_VALUE or any(
        not 32 <= ord(character) <= 126 for character in value
    ):
        raise RelayProtocolError(f"{field} must be bounded printable ASCII")


def _require_size(value: int, field: str) -> None:
    if type(value) is not int or not 0 <= value <= MAX_DECLARED_BYTES:
        raise RelayProtocolError(
            f"{field} must be an integer between 0 and {MAX_DECLARED_BYTES}"
        )


def _require_size_value(values: dict[str, object], field: str) -> int:
    value = values[field]
    if type(value) is not int:
        raise RelayProtocolError(f"{field} must be an integer")
    _require_size(value, field)
    return value
