from __future__ import annotations

import asyncio
import json
import unittest
from collections.abc import AsyncGenerator, AsyncIterator
from datetime import UTC, datetime
from typing import cast

from sandbox_manager.command_policy import (
    PolicyDecision,
    PolicyEvaluation,
    PolicyResult,
)
from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    AdmittedCommand,
    CommandAdmissionGate,
    CommandAuditEntry,
    CommandOwnershipError,
    CommandResult,
    InMemoryCommandRepository,
    InputStream,
    OutputSink,
    TaskAdmissionState,
)
from sandbox_manager.relay_protocol import (
    MAX_FRAME_PAYLOAD_BYTES,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    decode_frame,
)
from sandbox_manager.relay_workspace import (
    RelayWorkspaceService,
    WorkspaceLimitError,
    WorkspaceTransferError,
)
from sandbox_manager.workspace import (
    WorkspaceError,
    WorkspaceEscapeError,
    WorkspaceLayout,
    checksum_bytes,
    require_checksum,
)

_NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)

_PRINCIPAL = "agent-session-1"

_FENCE = "fence-generation-1"


class FixedClock:
    def now(self) -> datetime:
        return _NOW


class AllowPolicy:
    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        return PolicyResult(
            decision=PolicyDecision.ALLOW,
            policy_version=3,
            decision_reference="allow-v3",
        )


class RecordingAudit:
    def __init__(self) -> None:
        self.entries: list[CommandAuditEntry] = []

    async def record(self, entry: CommandAuditEntry) -> None:
        self.entries.append(entry)


class FakeInspector:
    def __init__(
        self,
        symlinks: frozenset[str] = frozenset(),
        special: frozenset[str] = frozenset(),
    ) -> None:
        self._symlinks = symlinks
        self._special = special
        self.checked: list[str] = []

    def is_symlink(self, path: str) -> bool:
        self.checked.append(path)
        return path in self._symlinks

    def is_special_file(self, path: str) -> bool:
        self.checked.append(path)
        return path in self._special


class ScriptedRelay:
    """Drains relay input frames and replays scripted output frames.

    Stands in for ``KubernetesCommandRelay``: sequence-0 STDIN frames are the
    helper operation vectors, later STDIN frames the streamed payload, and
    each scripted output pair is handed to the sink before the result.
    """

    def __init__(
        self,
        result: CommandResult,
        outputs: tuple[tuple[int, bytes], ...] = (),
        hang: bool = False,
    ) -> None:
        self._result = result
        self._outputs = outputs
        self._hang = hang
        self.commands: list[AdmittedCommand] = []
        self.envelopes: list[CommandEnvelope] = []
        self.argv_frames: list[list[str]] = []
        self.payloads: list[bytes] = []
        self.sinks: list[OutputSink | None] = []

    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        self.commands.append(command)
        self.envelopes.append(envelope)
        self.sinks.append(output_sink)
        if input_stream is not None:
            async for frame_bytes in input_stream:
                frame = decode_frame(frame_bytes)
                if (
                    isinstance(frame, DataFrame)
                    and frame.channel is DataChannel.STDIN
                ):
                    if frame.sequence == 0:
                        decoded = json.loads(frame.payload.decode("utf-8"))
                        self.argv_frames.append(list(decoded["argv"]))
                    else:
                        self.payloads.append(frame.payload)
        if output_sink is not None:
            for channel, chunk in self._outputs:
                await output_sink(channel, chunk)
        if self._hang:
            await asyncio.Event().wait()
        return self._result


def success(command_id: str) -> CommandResult:
    return CommandResult(command_id=command_id, outcome=CommandOutcome.SUCCESS)


def admission_state(task_id: str = "task-1") -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id=task_id,
        agent_session_id=_PRINCIPAL,
        profile="default",
        lifecycle=AdmissionLifecycle.READY,
        fence_generation=_FENCE,
    )


def repository(*states: TaskAdmissionState) -> InMemoryCommandRepository:
    return InMemoryCommandRepository({state.task_id: state for state in states})


def command_gate(
    repository_: InMemoryCommandRepository, relay: ScriptedRelay
) -> CommandAdmissionGate:
    return CommandAdmissionGate(
        repository=repository_,
        policy=AllowPolicy(),
        audit=RecordingAudit(),
        relay=relay,
        fence=AdmissionFence(_FENCE),
        clock=FixedClock(),
    )


def layout() -> WorkspaceLayout:
    return WorkspaceLayout(
        workspace_root="/workspace", readable_roots=("/results",)
    )


def service(
    repository_: InMemoryCommandRepository,
    relay: ScriptedRelay,
    inspector: FakeInspector | None = None,
    principal: str = _PRINCIPAL,
) -> RelayWorkspaceService:
    return RelayWorkspaceService(
        command_gate(repository_, relay),
        layout(),
        inspector if inspector is not None else FakeInspector(),
        principal,
    )


async def _chunks(*pieces: bytes) -> AsyncIterator[bytes]:
    for piece in pieces:
        yield piece


class RelayWorkspaceUploadTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_streams_argv_then_bounded_payload_frames(self) -> None:
        data = bytes(range(256)) * 782  # 200,352 bytes, four frame bounds
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())

        result = await service(repo, relay).upload(
            "task-1",
            "upload-1",
            "payloads/file.bin",
            len(data),
            checksum_bytes(data),
            _chunks(data[:1000], data[1000:]),
        )

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertEqual(len(relay.argv_frames), 1)
        self.assertEqual(
            relay.argv_frames[0],
            [
                "upload",
                "--dest",
                "/workspace/payloads/file.bin",
                "--bytes",
                str(len(data)),
                "--sha256",
                checksum_bytes(data),
            ],
        )
        self.assertEqual(b"".join(relay.payloads), data)
        self.assertTrue(all(len(p) <= MAX_FRAME_PAYLOAD_BYTES for p in relay.payloads))
        self.assertEqual(
            await repo.outcome("upload-1"), CommandOutcome.SUCCESS
        )
        envelope = relay.envelopes[0]
        self.assertEqual(envelope.declared_input_bytes, len(data))
        self.assertEqual(envelope.declared_output_bytes, 0)

    async def test_upload_records_audit_without_payload(self) -> None:
        data = b"transfer-bytes"
        audit = RecordingAudit()
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())
        command_gate_ = CommandAdmissionGate(
            repository=repo,
            policy=AllowPolicy(),
            audit=audit,
            relay=relay,
            fence=AdmissionFence(_FENCE),
            clock=FixedClock(),
        )
        service_ = RelayWorkspaceService(
            command_gate_, layout(), FakeInspector(), _PRINCIPAL
        )

        await service_.upload(
            "task-1",
            "upload-1",
            "file.bin",
            len(data),
            checksum_bytes(data),
            _chunks(data),
        )

        entry = audit.entries[0]
        self.assertEqual(entry.operation.value, "upload")
        self.assertEqual(entry.declared_input_bytes, len(data))
        self.assertEqual(entry.declared_output_bytes, 0)

    async def test_upload_rejects_escape_before_any_side_effect(self) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())

        for destination in ("../escape.bin", "/etc/passwd", "a/../b", "."):
            with self.assertRaises(WorkspaceError):
                await service(repo, relay).upload(
                    "task-1",
                    "upload-1",
                    destination,
                    4,
                    checksum_bytes(b"data"),
                    _chunks(b"data"),
                )

        self.assertEqual(relay.commands, [])
        self.assertEqual(await repo.outcome("upload-1"), None)

    async def test_upload_rejects_symlinked_destination(self) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())
        inspector = FakeInspector(symlinks=frozenset({"/workspace/link"}))

        with self.assertRaises(WorkspaceEscapeError):
            await service(repo, relay, inspector).upload(
                "task-1",
                "upload-1",
                "link/file.bin",
                4,
                checksum_bytes(b"data"),
                _chunks(b"data"),
            )

        self.assertEqual(relay.commands, [])

    async def test_upload_rejects_invalid_declaration_before_admission(self) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())
        service_ = service(repo, relay)

        with self.assertRaises(WorkspaceError):
            await service_.upload(
                "task-1",
                "upload-1",
                "file.bin",
                4,
                "not-a-checksum",
                _chunks(b"data"),
            )
        with self.assertRaises(WorkspaceLimitError):
            await service_.upload(
                "task-1", "upload-1", "file.bin", 0, "a" * 64, _chunks(b"")
            )
        with self.assertRaises(WorkspaceLimitError):
            await service_.upload(
                "task-1",
                "upload-1",
                "file.bin",
                256 * 1024 * 1024 + 1,
                "a" * 64,
                _chunks(b"data"),
            )

        self.assertEqual(relay.commands, [])
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)

    async def test_upload_over_long_payload_fails_closed_and_releases_slot(
        self,
    ) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())
        service_ = service(repo, relay)

        result = await service_.upload(
            "task-1",
            "upload-1",
            "file.bin",
            4,
            checksum_bytes(b"data"),
            _chunks(b"too much data for the declaration"),
        )

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertIn("WorkspaceLimitError", result.reason_code or "")
        self.assertEqual(await repo.outcome("upload-1"), CommandOutcome.UNKNOWN)

        second = await service_.upload(
            "task-1",
            "upload-2",
            "file.bin",
            4,
            checksum_bytes(b"data"),
            _chunks(b"data"),
        )

        self.assertIs(second.outcome, CommandOutcome.SUCCESS)

    async def test_upload_short_payload_fails_closed(self) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())

        result = await service(repo, relay).upload(
            "task-1",
            "upload-1",
            "file.bin",
            16,
            checksum_bytes(b"sixteen payload "),
            _chunks(b"short"),
        )

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertIn("WorkspaceLimitError", result.reason_code or "")
        self.assertEqual(await repo.outcome("upload-1"), CommandOutcome.UNKNOWN)

    async def test_foreign_principal_is_refused_before_relay(self) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())

        with self.assertRaises(CommandOwnershipError):
            await service(repo, relay, principal="agent-session-2").upload(
                "task-1",
                "upload-1",
                "file.bin",
                4,
                checksum_bytes(b"data"),
                _chunks(b"data"),
            )

        self.assertEqual(relay.commands, [])

    async def test_upload_checksum_format_is_enforced(self) -> None:
        relay = ScriptedRelay(success("upload-1"))
        repo = repository(admission_state())

        with self.assertRaises(WorkspaceError):
            await service(repo, relay).upload(
                "task-1",
                "upload-1",
                "file.bin",
                4,
                "A" * 64,
                _chunks(b"data"),
            )

        self.assertEqual(relay.commands, [])


class RelayWorkspaceDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_download_streams_stdout_frames(self) -> None:
        outputs = ((1, b"head"), (1, b"tail"), (2, b"noise on stderr"))
        relay = ScriptedRelay(success("download-1"), outputs=outputs)
        repo = repository(admission_state())

        received = [
            chunk
            async for chunk in service(repo, relay).download(
                "task-1", "download-1", "/results/report.bin", 1024
            )
        ]

        self.assertEqual(received, [b"head", b"tail"])
        self.assertEqual(await repo.outcome("download-1"), CommandOutcome.SUCCESS)
        envelope = relay.envelopes[0]
        self.assertEqual(envelope.declared_input_bytes, 0)
        self.assertEqual(envelope.declared_output_bytes, 1024)

    async def test_download_failure_raises_after_yielding_prefix(self) -> None:
        outputs = ((1, b"partial"),)
        failed = CommandResult(
            command_id="download-1", outcome=CommandOutcome.FAILED
        )
        relay = ScriptedRelay(failed, outputs=outputs)
        repo = repository(admission_state())

        received: list[bytes] = []
        with self.assertRaises(WorkspaceTransferError) as raised:
            async for chunk in service(repo, relay).download(
                "task-1", "download-1", "/results/report.bin", 1024
            ):
                received.append(chunk)

        self.assertEqual(received, [b"partial"])
        self.assertIs(raised.exception.result.outcome, CommandOutcome.FAILED)
        self.assertEqual(await repo.outcome("download-1"), CommandOutcome.FAILED)

    async def test_download_backpressures_after_two_buffered_frames(self) -> None:
        fourth_delivered = asyncio.Event()

        class BackpressureRelay(ScriptedRelay):
            async def execute(
                self,
                command: AdmittedCommand,
                envelope: CommandEnvelope,
                input_stream: InputStream | None,
                output_sink: OutputSink | None = None,
            ) -> CommandResult:
                assert output_sink is not None
                for chunk in (b"one", b"two", b"three", b"four"):
                    await output_sink(1, chunk)
                    if chunk == b"four":
                        fourth_delivered.set()
                return self._result

        repo = repository(admission_state())
        stream = cast(
            AsyncGenerator[bytes, None],
            service(repo, BackpressureRelay(success("download-1"))).download(
                "task-1", "download-1", "/results/report.bin", 1024
            ),
        )

        self.assertEqual(await anext(stream), b"one")
        await asyncio.sleep(0)
        self.assertFalse(fourth_delivered.is_set())
        self.assertEqual(await anext(stream), b"two")
        await asyncio.wait_for(fourth_delivered.wait(), timeout=1.0)
        self.assertEqual([chunk async for chunk in stream], [b"three", b"four"])

    async def test_abandoning_a_full_download_queue_does_not_deadlock(self) -> None:
        fourth_attempted = asyncio.Event()

        class SaturatingRelay(ScriptedRelay):
            async def execute(
                self,
                command: AdmittedCommand,
                envelope: CommandEnvelope,
                input_stream: InputStream | None,
                output_sink: OutputSink | None = None,
            ) -> CommandResult:
                assert output_sink is not None
                for chunk in (b"one", b"two", b"three", b"four"):
                    if chunk == b"four":
                        fourth_attempted.set()
                    await output_sink(1, chunk)
                return self._result

        repo = repository(admission_state())
        stream = cast(
            AsyncGenerator[bytes, None],
            service(repo, SaturatingRelay(success("download-1"))).download(
                "task-1", "download-1", "/results/report.bin", 1024
            ),
        )

        self.assertEqual(await anext(stream), b"one")
        await asyncio.wait_for(fourth_attempted.wait(), timeout=1.0)
        await asyncio.wait_for(stream.aclose(), timeout=1.0)

        self.assertIs(await repo.outcome("download-1"), CommandOutcome.UNKNOWN)
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)

    async def test_download_over_limit_fails_closed(self) -> None:
        outputs = ((1, b"x" * 600), (1, b"y" * 600))
        relay = ScriptedRelay(success("download-1"), outputs=outputs)
        repo = repository(admission_state())

        with self.assertRaises(WorkspaceTransferError) as raised:
            async for _chunk in service(repo, relay).download(
                "task-1", "download-1", "/results/report.bin", 1024
            ):
                pass

        self.assertIs(raised.exception.result.outcome, CommandOutcome.UNKNOWN)
        self.assertIn("WorkspaceLimitError", raised.exception.result.reason_code or "")
        self.assertEqual(await repo.outcome("download-1"), CommandOutcome.UNKNOWN)

    async def test_download_rejects_sources_outside_readable_roots(self) -> None:
        relay = ScriptedRelay(success("download-1"))
        repo = repository(admission_state())

        with self.assertRaises(WorkspaceEscapeError):
            await anext(
                service(repo, relay).download(
                    "task-1", "download-1", "/etc/passwd", 1024
                )
            )

        self.assertEqual(relay.commands, [])

    async def test_download_rejects_special_files(self) -> None:
        relay = ScriptedRelay(success("download-1"))
        repo = repository(admission_state())
        inspector = FakeInspector(special=frozenset({"/results/device"}))

        with self.assertRaises(WorkspaceError):
            await anext(
                service(repo, relay, inspector).download(
                    "task-1", "download-1", "/results/device", 1024
                )
            )

        self.assertEqual(relay.commands, [])

    async def test_download_rejects_invalid_limit(self) -> None:
        relay = ScriptedRelay(success("download-1"))
        repo = repository(admission_state())

        with self.assertRaises(WorkspaceLimitError):
            await anext(
                service(repo, relay).download(
                    "task-1", "download-1", "/results/report.bin", 0
                )
            )

        self.assertEqual(relay.commands, [])

    async def test_abandoned_download_releases_slot_fail_closed(self) -> None:
        relay = ScriptedRelay(success("download-1"), hang=True)
        repo = repository(admission_state())

        stream = cast(
            AsyncGenerator[bytes, None],
            service(repo, relay).download(
                "task-1", "download-1", "/results/report.bin", 1024
            ),
        )
        reader = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.2)  # admit, start the relay, suspend on the queue
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        await asyncio.wait_for(stream.aclose(), timeout=10.0)

        self.assertEqual(await repo.outcome("download-1"), CommandOutcome.UNKNOWN)
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)

        upload = await service(repo, ScriptedRelay(success("upload-1"))).upload(
            "task-1",
            "upload-1",
            "file.bin",
            4,
            checksum_bytes(b"data"),
            _chunks(b"data"),
        )

        self.assertIs(upload.outcome, CommandOutcome.SUCCESS)


class RelayWorkspaceContractTests(unittest.TestCase):
    def test_service_requires_a_principal(self) -> None:
        relay = ScriptedRelay(success("upload-1"))

        with self.assertRaises(WorkspaceError):
            RelayWorkspaceService(
                command_gate(repository(admission_state()), relay),
                layout(),
                FakeInspector(),
                "  ",
            )

        with self.assertRaises(WorkspaceError):
            RelayWorkspaceService(
                command_gate(repository(admission_state()), relay),
                layout(),
                FakeInspector(),
                "agent.session",
            )

    def test_checksum_format_is_required_for_construction(self) -> None:
        digest = checksum_bytes(b"payload")

        self.assertEqual(require_checksum(digest), digest)
        with self.assertRaises(WorkspaceError):
            require_checksum(digest.upper())


if __name__ == "__main__":
    unittest.main()
