"""Spawn request domain model and wire decoding."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

from .identifiers import is_identifier

SCHEMA_VERSION: Literal[1] = 1

_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "task_id",
        "agent_session_id",
        "sandbox_type",
    }
)
_OPTIONAL_FIELDS = frozenset({"requested_deadline"})
_ALL_FIELDS = _REQUIRED_FIELDS | _OPTIONAL_FIELDS


class SpawnRequestError(ValueError):
    """Raised when a spawn request cannot be decoded or validated."""


class UnsupportedSchemaVersion(SpawnRequestError):
    """Raised when a spawn request uses an unsupported schema version."""

    def __init__(self, version: int) -> None:
        self.version = version
        super().__init__(f"unsupported spawn request schema version: {version}")


@dataclass(frozen=True, slots=True)
class SpawnRequest:
    schema_version: Literal[1]
    request_id: str
    task_id: str
    agent_session_id: str
    sandbox_type: str
    requested_deadline: datetime | None = None

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise UnsupportedSchemaVersion(self.schema_version)
        for value, field in (
            (self.request_id, "request_id"),
            (self.task_id, "task_id"),
            (self.agent_session_id, "agent_session_id"),
        ):
            _require_identifier(value, field)
        _require_text(self.sandbox_type, "sandbox_type")
        if self.requested_deadline is not None:
            deadline = _aware_deadline(self.requested_deadline)
            object.__setattr__(self, "requested_deadline", deadline)

    def fingerprint(self) -> str:
        """Return a stable digest of the complete immutable request."""

        encoded = json.dumps(
            self._canonical_values(),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _canonical_values(self) -> dict[str, object]:
        deadline = self.requested_deadline
        return {
            "agent_session_id": self.agent_session_id,
            "request_id": self.request_id,
            "requested_deadline": (
                None
                if deadline is None
                else deadline.astimezone(UTC).isoformat().replace("+00:00", "Z")
            ),
            "sandbox_type": self.sandbox_type,
            "schema_version": self.schema_version,
            "task_id": self.task_id,
        }


def decode_spawn_request(payload: bytes | str) -> SpawnRequest:
    """Decode one versioned JSON spawn request."""

    if isinstance(payload, bytes):
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SpawnRequestError("spawn request must be UTF-8 JSON") from error
    else:
        text = payload

    try:
        decoded = json.loads(text, object_pairs_hook=_unique_object)
    except json.JSONDecodeError as error:
        raise SpawnRequestError("spawn request must be valid JSON") from error

    if not isinstance(decoded, dict):
        raise SpawnRequestError("spawn request must be a JSON object")
    values = cast(dict[str, object], decoded)

    missing = _REQUIRED_FIELDS - values.keys()
    if missing:
        raise SpawnRequestError(f"missing spawn request field: {min(missing)}")
    unknown = values.keys() - _ALL_FIELDS
    if unknown:
        raise SpawnRequestError(f"unknown spawn request field: {min(unknown)}")

    schema_version = values["schema_version"]
    if type(schema_version) is not int:
        raise SpawnRequestError("schema_version must be an integer")
    if schema_version != SCHEMA_VERSION:
        raise UnsupportedSchemaVersion(schema_version)

    return SpawnRequest(
        schema_version=SCHEMA_VERSION,
        request_id=_required_identifier(values, "request_id"),
        task_id=_required_identifier(values, "task_id"),
        agent_session_id=_required_identifier(values, "agent_session_id"),
        sandbox_type=_required_text(values, "sandbox_type"),
        requested_deadline=_deadline(values.get("requested_deadline")),
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, value in pairs:
        if key in values:
            raise SpawnRequestError(f"duplicate JSON field: {key}")
        values[key] = value
    return values


def _required_text(values: dict[str, object], field: str) -> str:
    value = values[field]
    _require_text(value, field)
    assert isinstance(value, str)
    return value


def _require_text(value: object, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise SpawnRequestError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise SpawnRequestError(f"{field} contains invalid text")


def _required_identifier(values: dict[str, object], field: str) -> str:
    value = values[field]
    _require_identifier(value, field)
    assert isinstance(value, str)
    return value


def _require_identifier(value: object, field: str) -> None:
    if not is_identifier(value):
        raise SpawnRequestError(
            f"{field} must contain 1-64 ASCII letters, digits, underscores, or hyphens"
        )


def _deadline(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise SpawnRequestError("requested_deadline must be an ISO 8601 timestamp")
    try:
        deadline = datetime.fromisoformat(value)
    except ValueError as error:
        raise SpawnRequestError(
            "requested_deadline must be an ISO 8601 timestamp"
        ) from error
    return _aware_deadline(deadline)


def _aware_deadline(deadline: datetime) -> datetime:
    if deadline.tzinfo is None or deadline.utcoffset() is None:
        raise SpawnRequestError("requested_deadline must include a timezone")
    return deadline.astimezone(UTC)
