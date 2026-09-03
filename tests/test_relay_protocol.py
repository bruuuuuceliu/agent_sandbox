from __future__ import annotations

import unittest

from sandbox_manager.relay_protocol import (
    MAX_FRAME_BYTES,
    RELAY_SCHEMA_VERSION,
    CancelSignal,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    FrameLimitError,
    FrameSequenceError,
    FrameSequencer,
    OutcomeReport,
    RelayOperation,
    RelayProtocolError,
    TerminalFrame,
    UnsupportedRelaySchemaVersion,
    decode_command_envelope,
    decode_frame,
    encode_command_envelope,
    encode_frame,
)

_ENVELOPE = CommandEnvelope(
    schema_version=RELAY_SCHEMA_VERSION,
    command_id="command-1",
    task_id="task-1",
    operation=RelayOperation.EXEC,
    correlation_id="correlation-1",
    declared_input_bytes=16,
    declared_output_bytes=32,
)


class CommandEnvelopeTests(unittest.TestCase):
    def test_encode_decode_round_trip(self) -> None:
        decoded = decode_command_envelope(encode_command_envelope(_ENVELOPE))

        self.assertEqual(decoded, _ENVELOPE)

    def test_decode_rejects_duplicate_json_fields(self) -> None:
        payload = (
            b'{"schema_version":1,"schema_version":1,"command_id":"c",'
            b'"task_id":"t","operation":"exec","correlation_id":"r",'
            b'"declared_input_bytes":0,"declared_output_bytes":0}'
        )

        with self.assertRaises(RelayProtocolError):
            decode_command_envelope(payload)

    def test_decode_rejects_unknown_fields(self) -> None:
        payload = encode_command_envelope(_ENVELOPE)[:-1] + b',"argv":[]}'

        with self.assertRaises(RelayProtocolError):
            decode_command_envelope(payload)

    def test_decode_rejects_unsupported_schema_version(self) -> None:
        payload = encode_command_envelope(_ENVELOPE).replace(b'"schema_version":1', b'"schema_version":2')

        with self.assertRaises(UnsupportedRelaySchemaVersion):
            decode_command_envelope(payload)

    def test_decode_rejects_unknown_operation(self) -> None:
        payload = encode_command_envelope(_ENVELOPE).replace(b'"exec"', b'"shell"')

        with self.assertRaises(RelayProtocolError):
            decode_command_envelope(payload)

    def test_envelope_rejects_oversized_declared_bytes(self) -> None:
        with self.assertRaises(RelayProtocolError):
            CommandEnvelope(
                schema_version=RELAY_SCHEMA_VERSION,
                command_id="command-1",
                task_id="task-1",
                operation=RelayOperation.UPLOAD,
                correlation_id="correlation-1",
                declared_input_bytes=256 * 1024 * 1024 + 1,
                declared_output_bytes=0,
            )


class FrameCodecTests(unittest.TestCase):
    def test_data_frame_round_trip(self) -> None:
        frame = DataFrame("command-1", DataChannel.STDIN, 0, b"payload")

        decoded = decode_frame(encode_frame(frame))

        self.assertEqual(decoded, frame)

    def test_cancel_and_terminal_round_trip(self) -> None:
        cancel = CancelSignal(RELAY_SCHEMA_VERSION, "command-1")
        terminal = TerminalFrame(
            RELAY_SCHEMA_VERSION, "command-1", CommandOutcome.UNKNOWN, "stream_lost"
        )

        self.assertEqual(decode_frame(encode_frame(cancel)), cancel)
        self.assertEqual(decode_frame(encode_frame(terminal)), terminal)

    def test_terminal_frame_without_reason_round_trip(self) -> None:
        terminal = TerminalFrame(RELAY_SCHEMA_VERSION, "command-1", CommandOutcome.SUCCESS)

        self.assertEqual(decode_frame(encode_frame(terminal)), terminal)

    def test_oversized_payload_is_refused_at_construction(self) -> None:
        with self.assertRaises(FrameLimitError):
            DataFrame("command-1", DataChannel.STDOUT, 0, b"x" * MAX_FRAME_BYTES)

    def test_oversized_frame_is_refused_at_decode(self) -> None:
        with self.assertRaises(FrameLimitError):
            decode_frame(b"\x01\x00" + b"\x00" * (MAX_FRAME_BYTES - 1))

    def test_truncated_frames_are_refused(self) -> None:
        encoded = encode_frame(DataFrame("command-1", DataChannel.STDIN, 3, b"abc"))

        for cut in range(1, len(encoded)):
            with self.assertRaises(RelayProtocolError):
                decode_frame(encoded[:cut])

    def test_trailing_bytes_are_refused(self) -> None:
        encoded = encode_frame(DataFrame("command-1", DataChannel.STDIN, 0, b"abc")) + b"junk"

        with self.assertRaises(RelayProtocolError):
            decode_frame(encoded)

    def test_unknown_frame_kind_is_refused(self) -> None:
        with self.assertRaises(RelayProtocolError):
            decode_frame(b"\xffcommand-1")


class FrameSequencerTests(unittest.TestCase):
    def test_accepts_ordered_frames_per_channel(self) -> None:
        sequencer = FrameSequencer("command-1")

        sequencer.observe(DataFrame("command-1", DataChannel.STDIN, 0, b"a"))
        sequencer.observe(DataFrame("command-1", DataChannel.STDOUT, 0, b"b"))
        sequencer.observe(DataFrame("command-1", DataChannel.STDIN, 1, b"c"))

    def test_rejects_duplicate_sequence(self) -> None:
        sequencer = FrameSequencer("command-1")
        sequencer.observe(DataFrame("command-1", DataChannel.STDIN, 0, b"a"))

        with self.assertRaises(FrameSequenceError):
            sequencer.observe(DataFrame("command-1", DataChannel.STDIN, 0, b"a"))

    def test_rejects_missing_sequence(self) -> None:
        sequencer = FrameSequencer("command-1")

        with self.assertRaises(FrameSequenceError):
            sequencer.observe(DataFrame("command-1", DataChannel.STDIN, 1, b"a"))

    def test_rejects_foreign_command_frame(self) -> None:
        sequencer = FrameSequencer("command-1")

        with self.assertRaises(RelayProtocolError):
            sequencer.observe(DataFrame("command-2", DataChannel.STDIN, 0, b"a"))


class OutcomeReportTests(unittest.TestCase):
    def test_accepts_unknown_outcome_with_reason(self) -> None:
        report = OutcomeReport("command-1", CommandOutcome.UNKNOWN, "stream_lost")

        self.assertEqual(report.outcome, CommandOutcome.UNKNOWN)

    def test_rejects_control_text_in_reason(self) -> None:
        with self.assertRaises(RelayProtocolError):
            OutcomeReport("command-1", CommandOutcome.FAILED, "bad\x01reason")


if __name__ == "__main__":
    unittest.main()
