from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime

from sandbox_manager.spawn import (
    SCHEMA_VERSION,
    SpawnRequest,
    SpawnRequestError,
    UnsupportedSchemaVersion,
    decode_spawn_request,
)


def valid_payload(**overrides: object) -> bytes:
    values: dict[str, object] = {
        "schema_version": 1,
        "request_id": "request-1",
        "task_id": "task-1",
        "agent_session_id": "session-1",
        "sandbox_type": "python",
    }
    values.update(overrides)
    return json.dumps(values).encode()


class SpawnRequestTests(unittest.TestCase):
    def test_decodes_versioned_request(self) -> None:
        request = decode_spawn_request(
            valid_payload(requested_deadline="2026-08-28T12:30:00Z")
        )

        self.assertEqual(request.schema_version, 1)
        self.assertEqual(request.request_id, "request-1")
        self.assertEqual(request.task_id, "task-1")
        self.assertEqual(request.agent_session_id, "session-1")
        self.assertEqual(request.sandbox_type, "python")
        self.assertEqual(
            request.requested_deadline,
            datetime(2026, 8, 28, 12, 30, tzinfo=UTC),
        )

    def test_fingerprint_is_independent_of_json_format_and_timezone(self) -> None:
        first = decode_spawn_request(
            valid_payload(requested_deadline="2026-08-28T12:30:00Z")
        )
        second = decode_spawn_request(
            b'{"task_id":"task-1","sandbox_type":"python",'
            b'"agent_session_id":"session-1",'
            b'"request_id":"request-1","schema_version":1,'
            b'"requested_deadline":"2026-08-28T14:30:00+02:00"}'
        )

        self.assertEqual(first.fingerprint(), second.fingerprint())
        self.assertEqual(len(first.fingerprint()), 64)
        self.assertEqual(second.requested_deadline, first.requested_deadline)

    def test_fingerprint_changes_with_immutable_input(self) -> None:
        first = decode_spawn_request(valid_payload())
        second = decode_spawn_request(valid_payload(sandbox_type="node"))

        self.assertNotEqual(first.fingerprint(), second.fingerprint())

    def test_rejects_invalid_contracts(self) -> None:
        cases = {
            "malformed UTF-8": b"\xff",
            "malformed JSON": b"{",
            "non-object JSON": b"[]",
            "boolean version": valid_payload(schema_version=True),
            "unsupported version": valid_payload(schema_version=2),
            "missing field": json.dumps(
                {
                    "schema_version": 1,
                    "request_id": "request-1",
                    "task_id": "task-1",
                    "agent_session_id": "session-1",
                }
            ).encode(),
            "unknown field": valid_payload(image="unapproved:latest"),
            "empty identifier": valid_payload(request_id=""),
            "control character": valid_payload(task_id="task\n1"),
            "subject separator": valid_payload(task_id="task.1"),
            "non-ASCII identifier": valid_payload(agent_session_id="sessión-1"),
            "oversized identifier": valid_payload(request_id="x" * 65),
            "naive deadline": valid_payload(requested_deadline="2026-08-28T12:30:00"),
            "duplicate field": (
                b'{"schema_version":1,"request_id":"first",'
                b'"request_id":"second","task_id":"task-1",'
                b'"agent_session_id":"session-1","sandbox_type":"python"}'
            ),
        }

        for name, payload in cases.items():
            with self.subTest(name=name), self.assertRaises(SpawnRequestError):
                decode_spawn_request(payload)

    def test_identifies_unsupported_schema_version(self) -> None:
        with self.assertRaises(UnsupportedSchemaVersion) as raised:
            decode_spawn_request(valid_payload(schema_version=2))

        self.assertEqual(raised.exception.version, 2)

    def test_direct_construction_enforces_the_wire_invariants(self) -> None:
        with self.assertRaises(SpawnRequestError):
            SpawnRequest(
                schema_version=SCHEMA_VERSION,
                request_id="request.1",
                task_id="task-1",
                agent_session_id="session-1",
                sandbox_type="python",
            )

        with self.assertRaises(SpawnRequestError):
            SpawnRequest(
                schema_version=SCHEMA_VERSION,
                request_id="request-1",
                task_id="task-1",
                agent_session_id="session-1",
                sandbox_type="python",
                requested_deadline=datetime(2026, 8, 28, 12, 30),  # noqa: DTZ001
            )


if __name__ == "__main__":
    unittest.main()
