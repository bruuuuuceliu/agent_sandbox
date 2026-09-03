"""Sentinel-driven payload- and credential-redaction suite (AC-025).

Secrets are planted only in payload positions — command argv, stream
bytes, transfer file content, and connection credentials — never in
identifiers. Every artifact the manager produces in process is then
scanned for them: command audit entries, durable command-event journal
entries, ready documents, protocol error messages, and the Kubernetes
request metadata an API server (and its audit) would record. Identifiers
may appear; payload may not.

The scan deliberately excludes the data-plane channels the fakes model:
the ``argv``/``stdin`` a fake sandbox helper observes and the bearer
header carry payloads by design and never reach an artifact surface.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import unittest
from collections.abc import AsyncIterator

from sandbox_manager.commands import (
    AdmissionFence,
    CommandAdmissionGate,
    InMemoryCommandRepository,
)
from sandbox_manager.config import ConnectionURL
from sandbox_manager.nats_lifecycle import (
    JetStreamCommandEventJournal,
    decode_command_event,
    encode_ready_response,
)
from sandbox_manager.relay_protocol import (
    RELAY_SCHEMA_VERSION,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    RelayProtocolError,
    decode_command_envelope,
    decode_frame,
    encode_frame,
)
from tests.test_commands import (
    _FENCE,
    _PRINCIPAL,
    AllowPolicy,
    FixedClock,
    RecordingAudit,
    RecordingRelay,
    admission_state,
    envelope,
)
from tests.test_nats_lifecycle import LifecycleEnvironment, ready_task
from tests.test_relay_flow import (
    RelayFlowEnvironment,
    argv_frames,
    payload_frames,
)
from tests.test_relay_flow import (
    admission_state as flow_state,
)
from tests.test_relay_flow import (
    envelope as flow_envelope,
)

_SENTINEL_ARGV = "SENTINEL.ARGV.7f31"

_SENTINEL_STREAM = b"SENTINEL.STREAM.4c9d"

_SENTINEL_TRANSFER = b"SENTINEL.TRANSFER.e2a6"

_SENTINEL_SECRET = "SENTINEL.SECRET.b84c"

_SENTINELS = (
    _SENTINEL_ARGV,
    _SENTINEL_STREAM.decode("ascii"),
    _SENTINEL_TRANSFER.decode("ascii"),
    _SENTINEL_SECRET,
)

#: The record fields the API server and its audit actually observe. The
#: fakes also capture ``argv``/``stdin``/``authorization``, which model the
#: in-sandbox helper channel and transport auth — data planes, not artifacts.
_REQUEST_METADATA_FIELDS = ("kind", "path", "query", "subprotocol")


def _assert_payload_free(test: unittest.TestCase, artifact: object, label: str) -> None:
    text = repr(artifact)
    for sentinel in _SENTINELS:
        test.assertNotIn(sentinel, text, f"{label} leaked a payload sentinel")


class RelayArtifactRedactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_exec_payloads_never_reach_artifacts(self) -> None:
        environment = RelayFlowEnvironment({"task-1": flow_state()})
        environment.seed_pod()
        # The sandbox output stream itself carries a sentinel: stream data
        # must reach the caller's sink, and nothing else.
        environment.kube.exec_frames = [(1, _SENTINEL_STREAM), (3, b"")]
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    flow_envelope("command-1"),
                    argv_frames("command-1", ("run-job", _SENTINEL_ARGV)),
                    None,
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            self.assertTrue(environment.audit.entries)
            for entry in environment.audit.entries:
                _assert_payload_free(
                    self, dataclasses.asdict(entry), "command audit entry"
                )
            for record in environment.kube.records:
                metadata = {
                    key: record[key]
                    for key in _REQUEST_METADATA_FIELDS
                    if key in record
                }
                _assert_payload_free(self, metadata, "Kubernetes request record")
        finally:
            await environment.stop()

    async def test_transfer_content_never_reaches_artifacts(self) -> None:
        payload = _SENTINEL_TRANSFER * 256  # spans several bounded frames
        environment = RelayFlowEnvironment({"task-1": flow_state()})
        environment.seed_pod()
        environment.kube.exec_frames = [(3, b"")]
        await environment.start()
        try:

            async def upload_input() -> AsyncIterator[bytes]:
                async for frame in argv_frames(
                    "command-upload",
                    (
                        "upload",
                        "--dest",
                        "/workspace/input.bin",
                        "--bytes",
                        str(len(payload)),
                        "--sha256",
                        "0" * 64,
                    ),
                ):
                    yield frame
                async for frame in payload_frames("command-upload", payload):
                    yield frame

            uploaded = await asyncio.wait_for(
                environment.client().execute(
                    flow_envelope(
                        "command-upload",
                        operation=RelayOperation.UPLOAD,
                        declared_input=len(payload),
                        declared_output=0,
                    ),
                    upload_input(),
                ),
                timeout=10.0,
            )

            self.assertIs(uploaded.outcome, CommandOutcome.SUCCESS)
            for entry in environment.audit.entries:
                _assert_payload_free(
                    self, dataclasses.asdict(entry), "command audit entry"
                )
            for record in environment.kube.records:
                metadata = {
                    key: record[key]
                    for key in _REQUEST_METADATA_FIELDS
                    if key in record
                }
                _assert_payload_free(self, metadata, "Kubernetes request record")
        finally:
            await environment.stop()

    async def test_unknown_outcomes_stay_payload_free(self) -> None:
        environment = RelayFlowEnvironment({"task-1": flow_state()})
        environment.seed_pod()
        environment.kube.exec_drop = True  # the stream vanishes mid-command
        await environment.start()
        try:
            result = await asyncio.wait_for(
                environment.client().execute(
                    flow_envelope("command-1"),
                    argv_frames("command-1", (_SENTINEL_ARGV,)),
                    None,
                ),
                timeout=10.0,
            )

            self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
            _assert_payload_free(self, result.reason_code, "unknown reason code")
            for entry in environment.audit.entries:
                _assert_payload_free(
                    self, dataclasses.asdict(entry), "command audit entry"
                )
        finally:
            await environment.stop()


class DurableArtifactRedactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_journal_ready_and_audit_stay_payload_free(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        try:
            assert environment.jetstream is not None
            journal = JetStreamCommandEventJournal(
                environment.jetstream, pull_wait_seconds=0.05
            )
            await journal.start()
            repository = InMemoryCommandRepository(journal=journal)
            await repository.seed_ready(admission_state())
            audit = RecordingAudit()
            gate = CommandAdmissionGate(
                repository=repository,
                policy=AllowPolicy(),
                audit=audit,
                relay=RecordingRelay(),
                fence=AdmissionFence(_FENCE),
                clock=FixedClock(),
            )

            async def sentinel_input() -> AsyncIterator[bytes]:
                yield encode_frame(
                    DataFrame("command-1", DataChannel.STDIN, 0, b"")
                )
                yield encode_frame(
                    DataFrame("command-1", DataChannel.STDIN, 1, _SENTINEL_STREAM)
                )

            result = await gate.run(_PRINCIPAL, envelope(), sentinel_input())

            self.assertIs(result.outcome, CommandOutcome.SUCCESS)
            for message in environment.server.streams[journal.stream].messages:
                _assert_payload_free(self, message.payload, "command journal event")
                decode_command_event(message.payload)
            for entry in audit.entries:
                _assert_payload_free(
                    self, dataclasses.asdict(entry), "command audit entry"
                )
            _assert_payload_free(
                self, encode_ready_response(ready_task()), "ready response"
            )
        finally:
            await environment.stop()


class ErrorRedactionTests(unittest.TestCase):
    def test_frame_decode_errors_never_quote_payload_bytes(self) -> None:
        sentinel = _SENTINEL_STREAM
        malformed = (
            # Unknown channel index; the sentinel is payload.
            bytes((0x01, 0x77))
            + (1).to_bytes(4, "big")
            + (3).to_bytes(2, "big")
            + b"cmd"
            + len(sentinel).to_bytes(4, "big")
            + sentinel,
            # Unknown frame kind followed by payload bytes.
            bytes((0x77,)) + sentinel,
            # Declared lengths end early: trailing payload bytes.
            bytes((0x01, 0x00))
            + (1).to_bytes(4, "big")
            + (3).to_bytes(2, "big")
            + b"cmd"
            + (2).to_bytes(4, "big")
            + sentinel,
        )
        for raw in malformed:
            with self.subTest(head=raw[:2].hex()), self.assertRaises(
                RelayProtocolError
            ) as raised:
                decode_frame(raw)
            _assert_payload_free(self, str(raised.exception), "frame decode error")

    def test_envelope_decode_errors_never_quote_payload_values(self) -> None:
        document = {"schema_version": RELAY_SCHEMA_VERSION, "rogue": _SENTINEL_ARGV}
        with self.assertRaises(RelayProtocolError) as raised:
            decode_command_envelope(json.dumps(document).encode("utf-8"))
        _assert_payload_free(self, str(raised.exception), "envelope decode error")

    def test_connection_urls_redact_credentials(self) -> None:
        url = ConnectionURL(
            f"nats://manager:{_SENTINEL_SECRET}@127.0.0.1:4222"
        )

        _assert_payload_free(self, str(url), "connection URL")
        _assert_payload_free(self, repr(url), "connection URL repr")
        _assert_payload_free(self, f"{url!r}", "connection URL format")


if __name__ == "__main__":
    unittest.main()
