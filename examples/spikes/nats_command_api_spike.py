"""Stage 4b spike: the production NATS command API on the real local server.

Exercises the shipped components — ``AsyncNatsConnection``,
``NatsCommandServer``, ``NatsCommandClient``, and the ``CommandAdmissionGate``
over ``InMemoryCommandRepository`` — against the local Core NATS container
(``examples/local/compose.yaml``, nats:2.11 with single-user auth) instead of
the in-process test fake:

1. Basic-auth CONNECT binds the transport principal the server recovers from
   the admission subject; wrong credentials are refused by the server.
2. A command round-trips: admission request/reply, argv frame on the task
   subject, streamed output frames, terminal frame; the durable repository
   records SUCCESS.
3. A task owned by another agent session is refused ``wrong_principal`` from
   the transport principal alone, before any relay side effect.
4. The single in-flight rule refuses a second command for the same task.
5. Losing the manager connection mid-command leaves the agent with UNKNOWN
   and, once the server abandons the session, a durable UNKNOWN outcome with
   the in-flight slot released.

Run with the local container up:

    PYTHONPATH=src python3 examples/spikes/nats_command_api_spike.py

Credentials come from ``config/local.env`` (git-ignored, local only). Evidence
lines are payload-free: identifiers, sizes, counts, and outcomes only.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

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
    CommandResult,
    InMemoryCommandRepository,
    InputStream,
    OutputSink,
    TaskAdmissionState,
)
from sandbox_manager.nats_client import AsyncNatsConnection, NatsAuthError
from sandbox_manager.nats_relay import NatsCommandClient, NatsCommandServer
from sandbox_manager.relay_protocol import (
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    DataChannel,
    DataFrame,
    RelayOperation,
    encode_frame,
)

_HOST = "127.0.0.1"

_PORT = 15181

_TASK_ID = "task-spike-1"

_TIMEOUT = 10.0


class AllowPolicy:
    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        return PolicyResult(
            decision=PolicyDecision.ALLOW,
            policy_version=1,
            decision_reference="spike-allow",
        )


class RecordingAudit:
    def __init__(self) -> None:
        self.count = 0

    async def record(self, entry: CommandAuditEntry) -> None:
        self.count += 1


class ScriptedRelay:
    """Drains input frames and replays fixed outputs; can hold mid-command."""

    def __init__(
        self,
        outputs: tuple[tuple[int, bytes], ...] = ((1, b"spike-out"),),
        hold: asyncio.Event | None = None,
    ) -> None:
        self._outputs = outputs
        self._hold = hold
        self.calls = 0
        self.input_frames = 0
        self.input_bytes = 0

    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        self.calls += 1
        if input_stream is not None:
            async for payload in input_stream:
                self.input_frames += 1
                self.input_bytes += len(payload)
        if output_sink is not None:
            for channel, chunk in self._outputs:
                output_sink(channel, chunk)
        if self._hold is not None:
            await self._hold.wait()
        return CommandResult(command.command_id, CommandOutcome.SUCCESS)


class RelayHarness:
    """One manager-side command server and one agent, both real connections."""

    def __init__(
        self, username: str, password: str, relay: ScriptedRelay, owner: str
    ) -> None:
        self.relay = relay
        self.fence = AdmissionFence()
        self.repository = InMemoryCommandRepository(
            {
                _TASK_ID: TaskAdmissionState(
                    task_id=_TASK_ID,
                    agent_session_id=owner,
                    profile="default",
                    lifecycle=AdmissionLifecycle.READY,
                    fence_generation=self.fence.generation,
                )
            }
        )
        self.manager = AsyncNatsConnection(
            _HOST, _PORT, username, password, "spike-manager"
        )
        self.command_server = NatsCommandServer(
            self.manager,
            CommandAdmissionGate(
                repository=self.repository,
                policy=AllowPolicy(),
                audit=RecordingAudit(),
                relay=relay,
                fence=self.fence,
            ),
        )
        self.agent = AsyncNatsConnection(
            _HOST, _PORT, username, password, "spike-agent"
        )
        self.client = NatsCommandClient(self.agent)

    async def start(self) -> None:
        await self.manager.connect()
        await self.command_server.start()
        await self.agent.connect()

    async def stop(self) -> None:
        await self.command_server.stop()
        await self.agent.close()
        if not self.manager.closed:
            await self.manager.close()


def _envelope(command_id: str) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=command_id,
        task_id=_TASK_ID,
        operation=RelayOperation.EXEC,
        correlation_id=f"correlation-{command_id}",
        declared_input_bytes=128,
        declared_output_bytes=256,
    )


async def _argv_frames(command_id: str) -> AsyncIterator[bytes]:
    payload = json.dumps({"argv": ["spike-probe", "--mode", "quiet"]}).encode()
    yield encode_frame(DataFrame(command_id, DataChannel.STDIN, 0, payload))


async def _wait_until(condition: Callable[[], bool], timeout: float) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            raise AssertionError("condition was not reached in time")
        await asyncio.sleep(0.05)


class SpikeNatsError(RuntimeError):
    """Raised when the spike violates its own expectations."""


async def scenario_authenticated_principal(username: str, password: str) -> list[str]:
    connection = AsyncNatsConnection(_HOST, _PORT, username, password, "spike-auth")
    await connection.connect()
    try:
        await connection.flush()
        return [
            f"CONNECT accepted; transport principal={connection.principal}",
            f"server max_payload={connection.max_payload} bytes",
        ]
    finally:
        await connection.close()


async def scenario_rejects_wrong_credentials(username: str) -> list[str]:
    connection = AsyncNatsConnection(
        _HOST, _PORT, username, "not-the-password", "spike-auth-bad"
    )
    try:
        await connection.connect()
    except NatsAuthError as error:
        return [
            f"server refused the wrong password: {error}",
            "the subject namespace only carries already-authenticated principals",
        ]
    await connection.close()
    raise SpikeNatsError("server accepted a wrong password")


async def scenario_command_round_trip(username: str, password: str) -> list[str]:
    relay = ScriptedRelay(outputs=((1, b"spike-out"), (2, b"spike-err")))
    harness = RelayHarness(username, password, relay, owner=username)
    await harness.start()
    try:
        observed: list[tuple[int, int]] = []

        def record(channel: int, payload: bytes) -> None:
            observed.append((channel, len(payload)))

        result = await asyncio.wait_for(
            harness.client.execute(
                _envelope("command-roundtrip"),
                _argv_frames("command-roundtrip"),
                record,
            ),
            timeout=_TIMEOUT,
        )
        outcome = await harness.repository.outcome("command-roundtrip")

        assert result.outcome is CommandOutcome.SUCCESS, result
        assert outcome is CommandOutcome.SUCCESS, outcome
        assert relay.input_frames == 1, relay.input_frames
        return [
            "admission granted task-scoped subjects for one command",
            (
                f"relay consumed {relay.input_frames} input frame(s),"
                f" {relay.input_bytes} bytes total"
            ),
            (
                f"agent observed {len(observed)} output chunk(s) on channels"
                f" {[channel for channel, _ in observed]}"
            ),
            "terminal frame carried SUCCESS; repository agrees",
        ]
    finally:
        await harness.stop()


async def scenario_wrong_principal_refusal(username: str, password: str) -> list[str]:
    relay = ScriptedRelay()
    harness = RelayHarness(username, password, relay, owner="someone-else")
    await harness.start()
    try:
        result = await asyncio.wait_for(
            harness.client.execute(_envelope("command-foreign"), None),
            timeout=_TIMEOUT,
        )
        outcome = await harness.repository.outcome("command-foreign")

        assert result.outcome is CommandOutcome.REJECTED, result
        assert result.reason_code == "wrong_principal", result
        assert relay.calls == 0, relay.calls
        assert outcome is None, outcome
        return [
            f"transport principal {username!r} does not own the task",
            "refused wrong_principal before any relay side effect",
            "no durable outcome and zero relay calls",
        ]
    finally:
        await harness.stop()


async def scenario_single_in_flight_command(
    username: str, password: str
) -> list[str]:
    hold = asyncio.Event()
    relay = ScriptedRelay(outputs=(), hold=hold)
    harness = RelayHarness(username, password, relay, owner=username)
    await harness.start()
    try:
        first = asyncio.create_task(
            harness.client.execute(_envelope("command-first"), None)
        )
        await _wait_until(lambda: relay.calls == 1, timeout=_TIMEOUT)
        second = await asyncio.wait_for(
            harness.client.execute(_envelope("command-second"), None),
            timeout=_TIMEOUT,
        )
        hold.set()
        first_result = await asyncio.wait_for(first, timeout=_TIMEOUT)

        assert second.outcome is CommandOutcome.REJECTED, second
        assert second.reason_code == "command_in_flight", second
        assert first_result.outcome is CommandOutcome.SUCCESS, first_result
        assert relay.calls == 1, relay.calls
        return [
            "second command for the same task refused command_in_flight",
            "first command finished SUCCESS after release",
            "exactly one relay call for the task throughout",
        ]
    finally:
        hold.set()
        await harness.stop()


async def scenario_manager_loss_is_unknown(username: str, password: str) -> list[str]:
    hold = asyncio.Event()
    relay = ScriptedRelay(outputs=(), hold=hold)
    harness = RelayHarness(username, password, relay, owner=username)
    await harness.start()
    try:
        executing = asyncio.create_task(
            harness.client.execute(
                _envelope("command-loss"), None, terminal_timeout=1.0
            )
        )
        await _wait_until(lambda: relay.calls == 1, timeout=_TIMEOUT)
        await harness.manager.close()  # abrupt manager transport loss
        result = await asyncio.wait_for(executing, timeout=_TIMEOUT)
        assert result.outcome is CommandOutcome.UNKNOWN, result
        assert result.reason_code == "no_terminal_frame", result
    finally:
        # Never release the hold: stopping the server must abandon the
        # in-flight command, which is the durable outcome under test.
        await harness.stop()

    state = await harness.repository.admission_state(_TASK_ID)
    outcome = await harness.repository.outcome("command-loss")
    assert outcome is CommandOutcome.UNKNOWN, outcome
    assert state is not None and state.in_flight_command_id is None, state
    return [
        "manager connection lost mid-command; agent saw no terminal frame",
        "agent classified UNKNOWN and never re-issued the command",
        "abandoned session recorded UNKNOWN durably and released the slot",
    ]


async def _load_credentials() -> tuple[str, str]:
    env_path = Path(__file__).resolve().parents[2] / "config" / "local.env"
    values: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    try:
        return values["NATS_USERNAME"], values["NATS_PASSWORD"]
    except KeyError as error:
        raise SpikeNatsError(
            f"config/local.env is missing {error}; start the local environment first"
        ) from error


async def _run() -> int:
    username, password = await _load_credentials()
    scenarios: list[tuple[str, Callable[[], Awaitable[list[str]]]]] = [
        ("authenticated_transport_principal", lambda: scenario_authenticated_principal(username, password)),
        ("rejects_wrong_credentials", lambda: scenario_rejects_wrong_credentials(username)),
        ("command_round_trip", lambda: scenario_command_round_trip(username, password)),
        ("wrong_principal_refusal", lambda: scenario_wrong_principal_refusal(username, password)),
        ("single_in_flight_command", lambda: scenario_single_in_flight_command(username, password)),
        ("manager_loss_is_unknown", lambda: scenario_manager_loss_is_unknown(username, password)),
    ]

    passed = 0
    for name, scenario in scenarios:
        try:
            evidence = await asyncio.wait_for(scenario(), timeout=60.0)
            passed += 1
            print(f"[PASS] {name}")
            for line in evidence:
                print(f"    evidence: {line}")
        except Exception as error:  # noqa: BLE001 - spike reports and continues
            print(f"[FAIL] {name}")
            print(f"    failure: {type(error).__name__}: {error}")
    print(f"\n{passed}/{len(scenarios)} scenarios passed")
    return 0 if passed == len(scenarios) else 1


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
