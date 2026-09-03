"""Stage 4c spike: the JetStream lifecycle transports on the real local server.

Exercises the shipped components — ``JetStream``,
``JetStreamSpawnIntake``, and ``JetStreamReadyPublisher`` — against the
local NATS container with JetStream enabled (``examples/local/compose.yaml``,
nats:2.11, single-user auth) instead of the in-process test fake:

1. A spawn request published while no intake is running is durable: it waits
   in the stream and is consumed, acknowledged, and deduplicated only after
   the intake starts.
2. The full round trip (publish with message-ID dedup -> durable pull ->
   repository create -> explicit ack) leaves no pending acknowledgments.
3. A payload claiming a principal other than the spawn subject's is
   terminated, never redelivered, and never reaches the repository.
4. A transient repository failure is negatively acknowledged and redelivered;
   the second delivery creates the task and acknowledges.
5. A ready response reaches its deterministic subject over Core NATS while
   also being stored durably, and republication with the same task message ID
   does not add a second copy.
6. A pull window that expires empty returns promptly with no delivery.
7. Command-admission transitions journaled write-ahead before a simulated
   crash rebuild under a new fence: the interrupted command is classified
   ``UNKNOWN``, the durable termination still refuses admission, and the
   previous process's fence is stale.

Run with the local container up:

    PYTHONPATH=src python3 examples/spikes/nats_lifecycle_spike.py

Credentials come from ``config/local.env`` (git-ignored, local only). Evidence
lines are payload-free: identifiers, sizes, counts, and outcomes only.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    AdmissionRefusal,
    AdmissionRequest,
    InMemoryCommandRepository,
    TaskAdmissionState,
)
from sandbox_manager.intake import QueuedTask, StoreResult
from sandbox_manager.nats_client import AsyncNatsConnection, NatsMessage
from sandbox_manager.nats_lifecycle import (
    COMMAND_EVENT_SUBJECT,
    JetStream,
    JetStreamCommandEventJournal,
    JetStreamReadyPublisher,
    JetStreamSpawnIntake,
    decode_ready_response,
    ready_subject,
    spawn_subject,
)
from sandbox_manager.nats_relay import admission_subject
from sandbox_manager.profiles import (
    PROFILE_SCHEMA_VERSION,
    ProfileMount,
    SandboxProfile,
)
from sandbox_manager.provisioning import (
    ProvisionedSandbox,
    ProvisioningTask,
    ReadyTask,
    RelayReference,
)
from sandbox_manager.relay_protocol import RELAY_SCHEMA_VERSION, CommandOutcome
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest

_HOST = "127.0.0.1"

_PORT = 15181

_TIMEOUT = 10.0


class SpikeNatsError(RuntimeError):
    """Raised when a lifecycle spike assertion fails."""


class SpikeRepository:
    """TaskRepository double with one injection point."""

    def __init__(self, failures_remaining: int = 0) -> None:
        self.tasks: dict[str, QueuedTask] = {}
        self.calls = 0
        self.failures_remaining = failures_remaining

    async def accept_spawn(self, task: QueuedTask) -> StoreResult:
        self.calls += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise RuntimeError("simulated durable-store outage")
        existing = self.tasks.get(task.request.task_id)
        if existing is not None:
            if existing.request_fingerprint != task.request_fingerprint:
                return StoreResult.CONFLICT
            return StoreResult.DUPLICATE
        self.tasks[task.request.task_id] = task
        return StoreResult.CREATED


class RecordingObserver:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def poisoned(self, subject: str, reason: str) -> None:
        self.events.append(("poisoned", reason))

    def deferred(self, subject: str, reason: str) -> None:
        self.events.append(("deferred", reason))


def _spawn_payload(principal: str, task_id: str) -> bytes:
    document = {
        "schema_version": SCHEMA_VERSION,
        "request_id": f"request-{task_id}",
        "task_id": task_id,
        "agent_session_id": principal,
        "sandbox_type": "python",
    }
    return json.dumps(document).encode("utf-8")


def _ready_task(principal: str, task_id: str) -> ReadyTask:
    request = SpawnRequest(
        schema_version=SCHEMA_VERSION,
        request_id=f"request-{task_id}",
        task_id=task_id,
        agent_session_id=principal,
        sandbox_type="python",
    )
    profile = SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )
    return ReadyTask(
        provisioning_task=ProvisioningTask(QueuedTask.from_request(request), profile),
        resources=ProvisionedSandbox(
            f"pods/{task_id}",
            f"uid-{task_id}",
            f"pvc/{task_id}-results",
            f"uid-{task_id}-results",
        ),
        relay=RelayReference(RELAY_SCHEMA_VERSION, admission_subject(principal)),
    )


async def _connect(username: str, password: str, name: str) -> AsyncNatsConnection:
    connection = AsyncNatsConnection(_HOST, _PORT, username, password, name)
    await connection.connect(timeout=_TIMEOUT)
    return connection


async def _until(condition: Callable[[], bool], what: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() >= deadline:
            raise SpikeNatsError(f"timed out waiting for {what}")
        await asyncio.sleep(0.05)


class LifecycleHarness:
    """One manager and one agent connection over the real JetStream server."""

    def __init__(self, username: str, password: str) -> None:
        self.run = secrets.token_hex(4)
        self.principal = f"spike-{self.run}"
        self.spawn_stream = f"SANDBOX_SPAWN_{self.run.upper()}"
        self.ready_stream = f"SANDBOX_READY_{self.run.upper()}"
        self.command_stream = f"SANDBOX_COMMANDS_{self.run.upper()}"
        self.durable = f"intake-{self.run}"
        self._username = username
        self._password = password
        self.manager: AsyncNatsConnection | None = None
        self.agent: AsyncNatsConnection | None = None
        self.jetstream: JetStream | None = None
        self.intake: JetStreamSpawnIntake | None = None

    async def start(self) -> None:
        self.manager = await _connect(
            self._username, self._password, f"spike-manager-{self.run}"
        )
        self.jetstream = JetStream(self.manager)
        self.agent = await _connect(
            self._username, self._password, f"spike-agent-{self.run}"
        )

    async def stop(self) -> None:
        if self.intake is not None:
            await self.intake.stop()
        assert self.manager is not None
        for stream in (
            self.spawn_stream,
            self.ready_stream,
            self.command_stream,
        ):
            await self.manager.request(f"$JS.API.STREAM.DELETE.{stream}", b"", 5.0)
        for connection in (self.agent, self.manager):
            if connection is not None:
                await connection.close()

    def intake_for(
        self, repository: SpikeRepository, observer: RecordingObserver
    ) -> JetStreamSpawnIntake:
        assert self.jetstream is not None
        return JetStreamSpawnIntake(
            self.jetstream,
            repository,
            stream=self.spawn_stream,
            subjects=(spawn_subject(self.principal),),
            durable=self.durable,
            pull_wait_seconds=0.2,
            redelivery_delay_seconds=0.5,
            retry_backoff_seconds=0.5,
            observer=observer,
        )

    async def consumer_state(self) -> dict[str, int]:
        """Consumer counts: the API reports them at the top level."""

        assert self.jetstream is not None
        info = await self.jetstream.ensure_consumer(
            self.spawn_stream,
            self.durable,
            spawn_subject(self.principal),
            ack_wait_seconds=30.0,
            max_deliver=10,
        )
        counts: dict[str, int] = {}
        for field in ("num_pending", "num_ack_pending", "num_redelivered"):
            value = info.get(field)
            counts[field] = value if isinstance(value, int) else -1
        return counts

    async def until_settled(self, timeout: float = 15.0) -> dict[str, int]:
        """Wait until the consumer holds nothing pending or unacknowledged."""

        deadline = time.monotonic() + timeout
        while True:
            state = await self.consumer_state()
            if state["num_ack_pending"] == 0 and state["num_pending"] == 0:
                return state
            if time.monotonic() >= deadline:
                raise SpikeNatsError(f"consumer never settled: {state}")
            await asyncio.sleep(0.1)

    async def stream_messages(self, name: str) -> int:
        assert self.jetstream is not None
        if name == self.ready_stream:
            subjects = (f"sandbox.manager.{self.principal}.>",)
        elif name == self.command_stream:
            subjects = (COMMAND_EVENT_SUBJECT,)
        else:
            subjects = (spawn_subject(self.principal),)
        info = await self.jetstream.ensure_stream(name, subjects)
        state = info.get("state")
        if not isinstance(state, dict):
            raise SpikeNatsError("stream info carried no state")
        messages = state.get("messages")
        return messages if isinstance(messages, int) else -1


def _admission_state(principal: str, task_id: str, fence: str) -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id=task_id,
        agent_session_id=principal,
        profile="python",
        lifecycle=AdmissionLifecycle.READY,
        fence_generation=fence,
    )


def _admission(principal: str, task_id: str, command_id: str, fence: str) -> AdmissionRequest:
    return AdmissionRequest(
        task_id=task_id,
        command_id=command_id,
        authenticated_principal=principal,
        fence_generation=fence,
        admitted_at=datetime.now(UTC),
    )


async def scenario_backlog_survives_until_intake_starts(
    username: str, password: str
) -> list[str]:
    """A spawn published with no intake running is durable until consumption."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None and harness.agent is not None
        await harness.jetstream.ensure_stream(
            harness.spawn_stream, (spawn_subject(harness.principal),)
        )
        await harness.agent.publish(
            spawn_subject(harness.principal),
            _spawn_payload(harness.principal, "task-backlog"),
        )
        stored = await harness.stream_messages(harness.spawn_stream)
        assert stored == 1, stored

        repository = SpikeRepository()
        harness.intake = harness.intake_for(repository, RecordingObserver())
        await harness.intake.start()
        await _until(lambda: "task-backlog" in repository.tasks, "backlog intake")
        await harness.until_settled()
        assert (await harness.consumer_state())["num_pending"] == 0
        return [
            "spawn stored in the stream with no consumer attached",
            "intake started afterwards consumed the backlog and stored the task",
            "consumer left with zero pending and zero unacknowledged deliveries",
        ]
    finally:
        await harness.stop()


async def scenario_intake_round_trip_with_dedup(
    username: str, password: str
) -> list[str]:
    """Message-ID dedup stores one copy; the delivery is explicitly acked."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None
        repository = SpikeRepository()
        harness.intake = harness.intake_for(repository, RecordingObserver())
        await harness.intake.start()
        subject = spawn_subject(harness.principal)
        first = await harness.jetstream.publish(
            subject,
            _spawn_payload(harness.principal, "task-round-trip"),
            msg_id=f"request-{harness.run}",
        )
        duplicate = await harness.jetstream.publish(
            subject,
            _spawn_payload(harness.principal, "task-round-trip"),
            msg_id=f"request-{harness.run}",
        )
        assert duplicate.sequence == first.sequence, (first, duplicate)
        await _until(lambda: "task-round-trip" in repository.tasks, "intake")
        await harness.until_settled()
        await harness.intake.stop()
        harness.intake = None
        stored = await harness.stream_messages(harness.spawn_stream)
        assert stored == 1, stored
        assert repository.calls == 1, repository.calls
        return [
            "duplicate message ID republished to the same stream sequence",
            "repository created the task exactly once",
            "stored messages stayed at one; delivery explicitly acknowledged",
        ]
    finally:
        await harness.stop()


async def scenario_principal_mismatch_is_terminated(
    username: str, password: str
) -> list[str]:
    """A payload claiming another principal is terminated, not redelivered."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None and harness.agent is not None
        repository = SpikeRepository()
        observer = RecordingObserver()
        harness.intake = harness.intake_for(repository, observer)
        await harness.intake.start()
        await harness.agent.publish(
            spawn_subject(harness.principal),
            _spawn_payload("someone-else", "task-mismatch"),
        )
        await _until(
            lambda: ("poisoned", "principal_mismatch") in observer.events,
            "poison classification",
        )
        await harness.until_settled()
        await harness.until_settled()
        assert not repository.tasks, repository.tasks
        assert (await harness.consumer_state())["num_redelivered"] == 0
        return [
            "payload principal disagreed with the spawn subject principal",
            "delivery terminated: no redelivery, no repository write",
            "observer recorded payload-free reason principal_mismatch",
        ]
    finally:
        await harness.stop()


async def scenario_transient_failure_redelivers(
    username: str, password: str
) -> list[str]:
    """A repository outage is negatively acknowledged, then redelivered."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None and harness.agent is not None
        repository = SpikeRepository(failures_remaining=1)
        observer = RecordingObserver()
        harness.intake = harness.intake_for(repository, observer)
        await harness.intake.start()
        await harness.agent.publish(
            spawn_subject(harness.principal),
            _spawn_payload(harness.principal, "task-transient"),
        )
        await _until(lambda: "task-transient" in repository.tasks, "redelivery")
        await harness.until_settled()
        assert repository.calls == 2, repository.calls
        assert ("deferred", "RuntimeError") in observer.events, observer.events
        return [
            "first delivery failed in the repository and was negatively acknowledged",
            "redelivery created the task and acknowledged",
            "observer recorded one payload-free deferred event",
        ]
    finally:
        await harness.stop()


async def scenario_ready_publication_is_durable(
    username: str, password: str
) -> list[str]:
    """Ready responses land on the deterministic subject and deduplicate."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None and harness.agent is not None
        subscription = await harness.agent.subscribe(
            ready_subject(harness.principal, "task-ready")
        )
        publisher = JetStreamReadyPublisher(
            harness.jetstream,
            stream=harness.ready_stream,
            subjects=(f"sandbox.manager.{harness.principal}.>",),
        )
        await publisher.start()
        task = _ready_task(harness.principal, "task-ready")
        await publisher.publish_ready(task)
        message = await harness.agent.next_message(subscription, _TIMEOUT)
        decoded = decode_ready_response(message.payload)
        assert decoded.task_id == "task-ready", decoded.task_id
        assert decoded.agent_session_id == harness.principal, decoded
        assert decoded.relay_route == admission_subject(harness.principal), decoded
        await publisher.publish_ready(task)
        stored = await harness.stream_messages(harness.ready_stream)
        assert stored == 1, stored
        return [
            "ready response observed live on its deterministic manager subject",
            "document decoded with identifiers and relay routing only",
            "republication with the same task message ID kept one stored copy",
        ]
    finally:
        await harness.stop()


async def scenario_pull_expiry_returns_promptly(
    username: str, password: str
) -> list[str]:
    """An expiring pull window over an empty stream returns no delivery."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None
        await harness.jetstream.ensure_stream(
            harness.spawn_stream, (spawn_subject(harness.principal),)
        )
        await harness.jetstream.ensure_consumer(
            harness.spawn_stream,
            harness.durable,
            spawn_subject(harness.principal),
            ack_wait_seconds=30.0,
            max_deliver=10,
        )
        started = time.monotonic()
        message: NatsMessage | None = await harness.jetstream.fetch(
            harness.spawn_stream, harness.durable, wait_seconds=0.5
        )
        elapsed = time.monotonic() - started
        assert message is None, message
        assert 0.3 < elapsed < 3.0, elapsed
        return [
            "empty pull window expired with a 408 status reply",
            f"fetch returned no delivery after {elapsed:.2f}s",
        ]
    finally:
        await harness.stop()


async def scenario_command_events_survive_restart(
    username: str, password: str
) -> list[str]:
    """Journaled admission transitions rebuild under a new fence."""

    harness = LifecycleHarness(username, password)
    await harness.start()
    try:
        assert harness.jetstream is not None
        journal = JetStreamCommandEventJournal(
            harness.jetstream,
            stream=harness.command_stream,
            pull_wait_seconds=0.2,
        )
        await journal.start()

        # Process A seeds two tasks, finishes one command, durably terminates
        # its task, and crashes mid-command on the other.
        fence_a = AdmissionFence(f"fence-a-{harness.run}")
        live = InMemoryCommandRepository(journal=journal)
        await live.seed_ready(
            _admission_state(harness.principal, "task-done", fence_a.generation)
        )
        done = await live.admit(
            _admission(harness.principal, "task-done", "command-done", fence_a.generation)
        )
        assert done.command is not None
        await live.finish(done.command, CommandOutcome.SUCCESS, datetime.now(UTC))
        await live.begin_termination("task-done")
        await live.seed_ready(
            _admission_state(harness.principal, "task-live", fence_a.generation)
        )
        running = await live.admit(
            _admission(
                harness.principal, "task-live", "command-running", fence_a.generation
            )
        )
        assert running.command is not None
        before = await harness.stream_messages(harness.command_stream)
        assert before == 6, before

        # Process B: the repository object is gone; only the journal remains.
        fence_b = AdmissionFence(f"fence-b-{harness.run}")
        recovered = await journal.load_repository(fence_b)
        terminated = await recovered.admission_state("task-done")
        assert terminated is not None
        assert terminated.lifecycle is AdmissionLifecycle.TERMINATING, terminated
        resumed = await recovered.admission_state("task-live")
        assert resumed is not None
        assert resumed.in_flight_command_id is None, resumed
        assert resumed.fence_generation == fence_b.generation, resumed
        assert await recovered.outcome("command-done") is CommandOutcome.SUCCESS
        assert await recovered.outcome("command-running") is CommandOutcome.UNKNOWN
        refused = await recovered.admit(
            _admission(harness.principal, "task-done", "command-x", fence_b.generation)
        )
        assert refused.refusal is AdmissionRefusal.NOT_ADMISSIBLE, refused
        stale = await recovered.admit(
            _admission(harness.principal, "task-live", "command-x", fence_a.generation)
        )
        assert stale.refusal is AdmissionRefusal.STALE_FENCE, stale
        after = await harness.stream_messages(harness.command_stream)
        assert after == before + 1, after
        return [
            "six write-ahead events journaled before the simulated crash",
            "replay rebuilt both tasks; recovery classified the interrupted command UNKNOWN",
            "termination survived the restart: admission refused not_admissible",
            "an admission fenced by the previous process refused stale_fence",
        ]
    finally:
        await harness.stop()


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
        (
            "backlog_survives_until_intake_starts",
            lambda: scenario_backlog_survives_until_intake_starts(username, password),
        ),
        (
            "intake_round_trip_with_dedup",
            lambda: scenario_intake_round_trip_with_dedup(username, password),
        ),
        (
            "principal_mismatch_is_terminated",
            lambda: scenario_principal_mismatch_is_terminated(username, password),
        ),
        (
            "transient_failure_redelivers",
            lambda: scenario_transient_failure_redelivers(username, password),
        ),
        (
            "ready_publication_is_durable",
            lambda: scenario_ready_publication_is_durable(username, password),
        ),
        (
            "pull_expiry_returns_promptly",
            lambda: scenario_pull_expiry_returns_promptly(username, password),
        ),
        (
            "command_events_survive_restart",
            lambda: scenario_command_events_survive_restart(username, password),
        ),
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
