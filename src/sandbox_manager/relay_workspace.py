"""Manager-side relay workspace transfers (plan Section 5.3).

Uploads stream caller-supplied bytes through the durable admission gate as
relay ``STDIN`` frames: sequence 0 carries the helper operation vector
(destination, declared size, declared checksum) and the remaining frames
carry the payload. The fixed helper inside the Pod stages to a bounded
temporary file, verifies the declared size and checksum, and renames into
place only on a match, so a partial or corrupted upload never becomes
visible at its destination.

Downloads run a relay command with an output sink and stream ``STDOUT``
frames onward as they arrive, bounded by the caller's byte limit.

Both operations inherit the admission ordering from
``CommandAdmissionGate``: the transport-authenticated principal supplied at
construction (never a payload field) owns the task, policy runs before any
side effect, and one command may be in flight per task.

Fail-closed accounting: a transfer the manager cut short — payload
disagreeing with its declared size, a download past its byte limit, a lost
relay stream — finishes durably as ``UNKNOWN`` and is returned or raised as
exactly that, because the manager cannot confirm what the helper observed.
Only pre-admission validation raises before any side effect.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from .commands import CommandAdmissionGate, CommandResult, OutputSink
from .relay_protocol import (
    MAX_DECLARED_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    encode_frame,
)
from .workspace import (
    PathInspector,
    WorkspaceError,
    WorkspaceLayout,
    require_checksum,
)

#: ``v4.channel.k8s.io`` stdout channel index, mirrored from the exec client
#: so this module does not import the client itself.
_STDOUT_CHANNEL = 1


class WorkspaceTransferError(WorkspaceError):
    """Raised when a streaming download does not complete successfully."""

    def __init__(self, operation: RelayOperation, result: CommandResult) -> None:
        self.operation = operation
        self.result = result
        reason = f"/{result.reason_code}" if result.reason_code else ""
        super().__init__(
            f"{operation.value} did not complete: "
            f"{result.outcome.value}{reason}"
        )


class WorkspaceLimitError(WorkspaceError):
    """Raised when a declared size or byte limit is invalid or exceeded."""


class RelayWorkspaceService:
    """Implements ``CommandWorkspace`` over the relay command path.

    One instance serves one transport-authenticated principal; the principal
    is a constructor input precisely because a payload field can never be
    evidence of ownership.
    """

    def __init__(
        self,
        gate: CommandAdmissionGate,
        layout: WorkspaceLayout,
        inspector: PathInspector,
        principal: str,
    ) -> None:
        if not isinstance(principal, str) or not principal.strip():
            raise WorkspaceError("principal must identify the agent session")
        self._gate = gate
        self._layout = layout
        self._inspector = inspector
        self._principal = principal

    async def upload(
        self,
        task_id: str,
        command_id: str,
        destination: str,
        declared_size: int,
        declared_checksum: str,
        frames: AsyncIterator[bytes],
    ) -> CommandResult:
        """Stream one upload into the task's workspace, atomically.

        Raises ``WorkspaceError`` for an invalid destination, checksum, or
        declared size before admission (no policy evaluation, no audit
        entry, no side effect). A payload whose streamed length disagrees
        with ``declared_size`` cuts the stream mid-command and the returned
        outcome is fail-closed ``UNKNOWN``.
        """

        resolved = self._layout.resolve_upload(destination)
        self._layout.ensure_transfer_safe(resolved, self._inspector)
        require_checksum(declared_checksum)
        if (
            type(declared_size) is not int
            or not 0 < declared_size <= MAX_DECLARED_BYTES
        ):
            raise WorkspaceLimitError(
                "declared upload size must be a positive integer within the relay bound"
            )

        envelope = CommandEnvelope(
            schema_version=RELAY_SCHEMA_VERSION,
            command_id=command_id,
            task_id=task_id,
            operation=RelayOperation.UPLOAD,
            correlation_id=f"upload:{command_id}",
            declared_input_bytes=declared_size,
            declared_output_bytes=0,
        )
        return await self._gate.run(
            self._principal,
            envelope,
            _upload_frames(
                command_id,
                resolved,
                declared_size,
                declared_checksum,
                frames,
            ),
        )

    def download(
        self,
        task_id: str,
        command_id: str,
        source: str,
        max_bytes: int,
    ) -> AsyncIterator[bytes]:
        """Stream one approved download source in bounded frames."""

        return self._download(task_id, command_id, source, max_bytes)

    async def _download(
        self,
        task_id: str,
        command_id: str,
        source: str,
        max_bytes: int,
    ) -> AsyncIterator[bytes]:
        resolved = self._layout.resolve_download(source)
        self._layout.ensure_transfer_safe(resolved, self._inspector)
        if type(max_bytes) is not int or not 0 < max_bytes <= MAX_DECLARED_BYTES:
            raise WorkspaceLimitError(
                "download byte limit must be a positive integer within the relay bound"
            )

        envelope = CommandEnvelope(
            schema_version=RELAY_SCHEMA_VERSION,
            command_id=command_id,
            task_id=task_id,
            operation=RelayOperation.DOWNLOAD,
            correlation_id=f"download:{command_id}",
            declared_input_bytes=0,
            declared_output_bytes=max_bytes,
        )

        queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        received = 0

        def sink(channel: int, payload: bytes) -> None:
            nonlocal received
            if channel != _STDOUT_CHANNEL:
                return
            received += len(payload)
            if received > max_bytes:
                raise WorkspaceLimitError("download exceeded its declared byte limit")
            queue.put_nowait(payload)

        async def drive() -> CommandResult:
            try:
                return await self._gate.run(self._principal, envelope, None, sink)
            finally:
                queue.put_nowait(None)

        command = asyncio.create_task(drive())
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
            result = await command
            if result.outcome is not CommandOutcome.SUCCESS:
                raise WorkspaceTransferError(RelayOperation.DOWNLOAD, result)
        finally:
            command.cancel()
            await asyncio.gather(command, return_exceptions=True)


async def _upload_frames(
    command_id: str,
    resolved_path: str,
    declared_size: int,
    declared_checksum: str,
    frames: AsyncIterator[bytes],
) -> AsyncIterator[bytes]:
    """Encode one upload as relay frames: the argv first, payload after.

    The helper operation vector never carries file content; the payload is
    chunked to the frame bound while the streamed total is accounted against
    the declared size. An over-long payload is cut as soon as it crosses the
    declaration; a short one is cut at stream end.
    """

    argv = [
        "upload",
        "--dest",
        resolved_path,
        "--bytes",
        str(declared_size),
        "--sha256",
        declared_checksum,
    ]
    yield encode_frame(
        DataFrame(
            command_id=command_id,
            channel=DataChannel.STDIN,
            sequence=0,
            payload=json.dumps({"argv": argv}, separators=(",", ":")).encode("utf-8"),
        )
    )
    sent = 0
    sequence = 1
    async for chunk in frames:
        offset = 0
        while offset < len(chunk):
            piece = chunk[offset : offset + MAX_FRAME_PAYLOAD_BYTES]
            yield encode_frame(
                DataFrame(
                    command_id=command_id,
                    channel=DataChannel.STDIN,
                    sequence=sequence,
                    payload=piece,
                )
            )
            sequence += 1
            offset += len(piece)
        sent += len(chunk)
        if sent > declared_size:
            raise WorkspaceLimitError("upload payload exceeds its declared size")
    if sent != declared_size:
        raise WorkspaceLimitError("upload payload is shorter than its declared size")


__all__ = [
    "OutputSink",
    "RelayWorkspaceService",
    "WorkspaceLimitError",
    "WorkspaceTransferError",
]
