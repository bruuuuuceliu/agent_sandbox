from __future__ import annotations

import asyncio
import dataclasses
import unittest
from datetime import UTC, datetime

from sandbox_manager.command_policy import (
    CommandPolicy,
    PolicyDecision,
    PolicyEvaluation,
    PolicyResult,
)
from sandbox_manager.commands import (
    AdmissionDecision,
    AdmissionFence,
    AdmissionLifecycle,
    AdmissionRefusal,
    AdmissionRequest,
    AdmittedCommand,
    CommandAdmissionGate,
    CommandAuditEntry,
    CommandEvent,
    CommandEventKind,
    CommandInFlightError,
    CommandOwnershipError,
    CommandPersistenceError,
    CommandRelay,
    CommandResult,
    InMemoryCommandRepository,
    InputStream,
    OutputSink,
    PolicyDeniedError,
    TaskAdmissionState,
    TaskNotAdmissibleError,
)
from sandbox_manager.relay_protocol import (
    RELAY_SCHEMA_VERSION,
    CommandEnvelope,
    CommandOutcome,
    RelayOperation,
)

_NOW = datetime(2026, 8, 28, 12, tzinfo=UTC)

_PRINCIPAL = "agent-session-1"

_FENCE = "fence-generation-1"


class FixedClock:
    def now(self) -> datetime:
        return _NOW


class AllowPolicy:
    def __init__(self) -> None:
        self.evaluations: list[PolicyEvaluation] = []

    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        self.evaluations.append(request)
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


class FailingAudit:
    async def record(self, entry: CommandAuditEntry) -> None:
        raise RuntimeError("audit transport unavailable")


class RecordingRelay:
    def __init__(self) -> None:
        self.calls: list[tuple[AdmittedCommand, CommandEnvelope]] = []
        self.sinks: list[OutputSink | None] = []

    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        self.calls.append((command, envelope))
        self.sinks.append(output_sink)
        return CommandResult(command_id=command.command_id, outcome=CommandOutcome.SUCCESS)


class FailingRelay:
    async def execute(
        self,
        command: AdmittedCommand,
        envelope: CommandEnvelope,
        input_stream: InputStream | None,
        output_sink: OutputSink | None = None,
    ) -> CommandResult:
        raise ConnectionError("relay stream lost")


class FailingAdmitRepository(InMemoryCommandRepository):
    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        raise RuntimeError("durable store unavailable")


class FailingFinishRepository(InMemoryCommandRepository):
    async def finish(
        self,
        command: AdmittedCommand,
        outcome: CommandOutcome,
        completed_at: datetime,
    ) -> None:
        raise RuntimeError("durable store unavailable")


class SlowReturningAdmitRepository(InMemoryCommandRepository):
    def __init__(self, states: dict[str, TaskAdmissionState]) -> None:
        super().__init__(states)
        self.committed = asyncio.Event()
        self.release = asyncio.Event()

    async def admit(self, request: AdmissionRequest) -> AdmissionDecision:
        decision = await super().admit(request)
        self.committed.set()
        await self.release.wait()
        return decision


def admission_state(
    task_id: str = "task-1",
    lifecycle: AdmissionLifecycle = AdmissionLifecycle.READY,
    fence: str = _FENCE,
) -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id=task_id,
        agent_session_id=_PRINCIPAL,
        profile="default",
        lifecycle=lifecycle,
        fence_generation=fence,
    )


def envelope(
    command_id: str = "command-1", task_id: str = "task-1"
) -> CommandEnvelope:
    return CommandEnvelope(
        schema_version=RELAY_SCHEMA_VERSION,
        command_id=command_id,
        task_id=task_id,
        operation=RelayOperation.EXEC,
        correlation_id=f"correlation-{command_id}",
        declared_input_bytes=16,
        declared_output_bytes=32,
    )


def gate(
    repository: InMemoryCommandRepository,
    policy: CommandPolicy | None = None,
    relay: CommandRelay | None = None,
    audit: RecordingAudit | None = None,
    fence: AdmissionFence | None = None,
) -> CommandAdmissionGate:
    return CommandAdmissionGate(
        repository=repository,
        policy=policy or AllowPolicy(),
        audit=audit or RecordingAudit(),
        relay=relay or RecordingRelay(),
        fence=fence or AdmissionFence(_FENCE),
        clock=FixedClock(),
    )


def repository(*states: TaskAdmissionState) -> InMemoryCommandRepository:
    return InMemoryCommandRepository({state.task_id: state for state in states})


class CommandAdmissionGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_command_activates_ready_task(self) -> None:
        repo = repository(admission_state())

        admitted = await gate(repo).admit(_PRINCIPAL, envelope())

        self.assertTrue(admitted.activated)
        self.assertEqual(admitted.task_id, "task-1")
        self.assertEqual(admitted.profile, "default")
        self.assertEqual(
            await repo.lifecycle("task-1"), AdmissionLifecycle.ACTIVE
        )

    async def test_second_command_does_not_reactivate(self) -> None:
        repo = repository(admission_state())
        command_gate = gate(repo)

        await command_gate.admit(_PRINCIPAL, envelope())
        await repo.finish(
            await _only_command(repo, "task-1"), CommandOutcome.SUCCESS, _NOW
        )

        admitted = await command_gate.admit(_PRINCIPAL, envelope("command-2"))

        self.assertFalse(admitted.activated)

    async def test_unknown_task_is_refused_before_policy(self) -> None:
        repo = repository(admission_state())
        policy = AllowPolicy()
        audit = RecordingAudit()

        with self.assertRaises(TaskNotAdmissibleError) as raised:
            await gate(repo, policy=policy, audit=audit).admit(
                _PRINCIPAL, envelope(task_id="absent")
            )

        self.assertIs(raised.exception.refusal, AdmissionRefusal.UNKNOWN_TASK)
        self.assertEqual(policy.evaluations, [])
        self.assertEqual(audit.entries, [])

    async def test_wrong_principal_is_refused_without_policy_or_side_effect(self) -> None:
        repo = repository(admission_state())
        policy = AllowPolicy()
        audit = RecordingAudit()

        with self.assertRaises(CommandOwnershipError):
            await gate(repo, policy=policy, audit=audit).admit(
                "agent-session-2", envelope()
            )

        self.assertEqual(policy.evaluations, [])
        self.assertEqual(audit.entries, [])
        self.assertEqual(await repo.lifecycle("task-1"), AdmissionLifecycle.READY)

    async def test_policy_evaluates_transport_principal_and_durable_profile(self) -> None:
        repo = repository(admission_state())
        policy = AllowPolicy()

        await gate(repo, policy=policy).admit(_PRINCIPAL, envelope())

        evaluation = policy.evaluations[0]
        self.assertEqual(evaluation.authenticated_principal, _PRINCIPAL)
        self.assertEqual(evaluation.profile, "default")
        self.assertEqual(evaluation.operation, RelayOperation.EXEC)
        self.assertEqual(evaluation.declared_size, 16)

    async def test_policy_denial_audits_rejection_without_side_effect(self) -> None:
        repo = repository(admission_state())
        audit = RecordingAudit()

        with self.assertRaises(PolicyDeniedError) as raised:
            await gate(
                repo,
                policy=_deny_policy(),
                audit=audit,
            ).admit(_PRINCIPAL, envelope())

        self.assertEqual(raised.exception.decision_reference, "deny-v2")
        self.assertEqual(len(audit.entries), 1)
        entry = audit.entries[0]
        self.assertIs(entry.outcome, CommandOutcome.REJECTED)
        self.assertEqual(entry.policy_version, 2)
        self.assertEqual(entry.decision_reference, "deny-v2")
        self.assertEqual(await repo.lifecycle("task-1"), AdmissionLifecycle.READY)

        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)

    async def test_terminating_task_refuses_admission(self) -> None:
        repo = repository(admission_state(lifecycle=AdmissionLifecycle.TERMINATING))
        policy = AllowPolicy()

        with self.assertRaises(TaskNotAdmissibleError) as raised:
            await gate(repo, policy=policy).admit(_PRINCIPAL, envelope())

        self.assertIs(raised.exception.refusal, AdmissionRefusal.NOT_ADMISSIBLE)
        self.assertEqual(len(policy.evaluations), 1)

    async def test_stale_fence_refuses_admission(self) -> None:
        repo = repository(admission_state(fence="older-generation"))

        with self.assertRaises(TaskNotAdmissibleError) as raised:
            await gate(repo).admit(_PRINCIPAL, envelope())

        self.assertIs(raised.exception.refusal, AdmissionRefusal.STALE_FENCE)

    async def test_one_in_flight_command_per_task(self) -> None:
        repo = repository(admission_state())

        await gate(repo).admit(_PRINCIPAL, envelope())

        with self.assertRaises(CommandInFlightError):
            await gate(repo).admit(_PRINCIPAL, envelope("command-2"))

    async def test_cancellation_after_durable_admission_releases_slot(self) -> None:
        state = admission_state()
        repo = SlowReturningAdmitRepository({state.task_id: state})
        admitting = asyncio.create_task(gate(repo).admit(_PRINCIPAL, envelope()))
        await repo.committed.wait()

        admitting.cancel()
        await asyncio.sleep(0)
        repo.release.set()
        with self.assertRaises(asyncio.CancelledError):
            await admitting

        current = await repo.admission_state("task-1")
        assert current is not None
        self.assertIsNone(current.in_flight_command_id)
        self.assertIs(await repo.outcome("command-1"), CommandOutcome.UNKNOWN)

    async def test_run_records_success_and_releases_slot(self) -> None:
        repo = repository(admission_state())
        audit = RecordingAudit()
        relay = RecordingRelay()
        command_gate = gate(repo, relay=relay, audit=audit)

        result = await command_gate.run(_PRINCIPAL, envelope(), None)

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertEqual(len(relay.calls), 1)
        self.assertEqual(
            await repo.outcome("command-1"), CommandOutcome.SUCCESS
        )
        self.assertEqual(len(audit.entries), 1)
        entry = audit.entries[0]
        self.assertIs(entry.outcome, CommandOutcome.SUCCESS)
        self.assertEqual(entry.policy_version, 3)
        self.assertEqual(entry.admitted_at, _NOW)
        self.assertEqual(entry.completed_at, _NOW)

        await command_gate.run(_PRINCIPAL, envelope("command-2"), None)

        self.assertEqual(len(relay.calls), 2)

    async def test_relay_failure_reports_unknown_and_releases_slot(self) -> None:
        repo = repository(admission_state())
        audit = RecordingAudit()
        command_gate = gate(repo, relay=FailingRelay(), audit=audit)

        result = await command_gate.run(_PRINCIPAL, envelope(), None)

        self.assertIs(result.outcome, CommandOutcome.UNKNOWN)
        self.assertEqual(result.reason_code, "relay_stream_lost:ConnectionError")
        self.assertEqual(await repo.outcome("command-1"), CommandOutcome.UNKNOWN)
        self.assertIs(audit.entries[0].outcome, CommandOutcome.UNKNOWN)

        await command_gate.run(_PRINCIPAL, envelope("command-2"), None)

        self.assertEqual(await repo.outcome("command-2"), CommandOutcome.UNKNOWN)

    async def test_run_refusal_never_reaches_relay(self) -> None:
        repo = repository(admission_state())
        relay = RecordingRelay()

        with self.assertRaises(CommandOwnershipError):
            await gate(repo, relay=relay).run("agent-session-2", envelope(), None)

        self.assertEqual(relay.calls, [])

    async def test_run_forwards_output_sink_to_relay(self) -> None:
        repo = repository(admission_state())
        relay = RecordingRelay()

        async def sink(channel: int, payload: bytes) -> None:
            return None

        await gate(repo, relay=relay).run(_PRINCIPAL, envelope(), None, sink)

        self.assertIs(relay.sinks[0], sink)

    async def test_audit_failure_does_not_hide_the_durable_result(self) -> None:
        repo = repository(admission_state())
        relay = RecordingRelay()
        command_gate = CommandAdmissionGate(
            repository=repo,
            policy=AllowPolicy(),
            audit=FailingAudit(),
            relay=relay,
            fence=AdmissionFence(_FENCE),
            clock=FixedClock(),
        )

        result = await command_gate.run(_PRINCIPAL, envelope(), None)

        self.assertIs(result.outcome, CommandOutcome.SUCCESS)
        self.assertIs(
            await repo.outcome("command-1"),
            CommandOutcome.SUCCESS,
        )

    async def test_admit_persistence_failure_is_wrapped(self) -> None:
        repo = FailingAdmitRepository(
            {state.task_id: state for state in (admission_state(),)}
        )

        with self.assertRaises(CommandPersistenceError) as raised:
            await gate(repo).admit(_PRINCIPAL, envelope())

        self.assertEqual(raised.exception.operation, "admit")

    async def test_finish_persistence_failure_is_wrapped(self) -> None:
        repo = FailingFinishRepository(
            {state.task_id: state for state in (admission_state(),)}
        )

        with self.assertRaises(CommandPersistenceError) as raised:
            await gate(repo).run(_PRINCIPAL, envelope(), None)

        self.assertEqual(raised.exception.operation, "finish")


class InMemoryCommandRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_admit_is_atomic_against_concurrent_requests(self) -> None:
        repo = repository(admission_state())
        requests = [
            AdmissionRequest(
                task_id="task-1",
                command_id=f"command-{index}",
                authenticated_principal=_PRINCIPAL,
                fence_generation=_FENCE,
                admitted_at=_NOW,
            )
            for index in range(8)
        ]

        decisions = await asyncio.gather(
            *(repo.admit(request) for request in requests)
        )

        admitted = [d for d in decisions if d.command is not None]
        refused = [d for d in decisions if d.refusal is not None]
        self.assertEqual(len(admitted), 1)
        self.assertEqual(len(refused), 7)
        self.assertTrue(
            all(d.refusal is AdmissionRefusal.COMMAND_IN_FLIGHT for d in refused)
        )

    async def test_finish_ignores_foreign_command(self) -> None:
        repo = repository(admission_state())
        decision = await repo.admit(
            AdmissionRequest(
                task_id="task-1",
                command_id="command-1",
                authenticated_principal=_PRINCIPAL,
                fence_generation=_FENCE,
                admitted_at=_NOW,
            )
        )
        assert decision.command is not None

        foreign = AdmittedCommand(
            task_id="task-1",
            command_id="command-9",
            agent_session_id=_PRINCIPAL,
            profile="default",
            fence_generation=_FENCE,
            activated=False,
            admitted_at=_NOW,
        )
        await repo.finish(foreign, CommandOutcome.SUCCESS, _NOW)

        self.assertIsNone(await repo.outcome("command-9"))
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertEqual(state.in_flight_command_id, "command-1")

        await repo.finish(decision.command, CommandOutcome.SUCCESS, _NOW)

        self.assertEqual(await repo.outcome("command-1"), CommandOutcome.SUCCESS)
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)

    async def test_begin_termination_closes_admission(self) -> None:
        repo = repository(admission_state())

        await repo.begin_termination("task-1")

        self.assertEqual(await repo.lifecycle("task-1"), AdmissionLifecycle.TERMINATING)
        decision = await repo.admit(
            AdmissionRequest(
                task_id="task-1",
                command_id="command-1",
                authenticated_principal=_PRINCIPAL,
                fence_generation=_FENCE,
                admitted_at=_NOW,
            )
        )
        self.assertIs(decision.refusal, AdmissionRefusal.NOT_ADMISSIBLE)


class AdmissionContractTests(unittest.TestCase):
    def test_decision_requires_exactly_one_outcome(self) -> None:
        command = AdmittedCommand(
            task_id="task-1",
            command_id="command-1",
            agent_session_id=_PRINCIPAL,
            profile="default",
            fence_generation=_FENCE,
            activated=True,
            admitted_at=_NOW,
        )

        with self.assertRaises(ValueError):
            AdmissionDecision()
        with self.assertRaises(ValueError):
            AdmissionDecision(command=command, refusal=AdmissionRefusal.UNKNOWN_TASK)

    def test_request_rejects_naive_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            AdmissionRequest(
                task_id="task-1",
                command_id="command-1",
                authenticated_principal=_PRINCIPAL,
                fence_generation=_FENCE,
                admitted_at=datetime(2026, 8, 28, 12),  # noqa: DTZ001 - naive on purpose
            )

    def test_fence_generations_are_random_per_process(self) -> None:
        self.assertNotEqual(AdmissionFence().generation, AdmissionFence().generation)

    def test_command_identifiers_are_validated_on_direct_construction(self) -> None:
        for identifier in ("bad.id", "caf\N{LATIN SMALL LETTER E WITH ACUTE}", "x" * 65):
            with self.subTest(identifier=identifier):
                with self.assertRaises(ValueError):
                    AdmissionFence(identifier)
                with self.assertRaises(ValueError):
                    AdmissionRequest(
                        task_id=identifier,
                        command_id="command-1",
                        authenticated_principal=_PRINCIPAL,
                        fence_generation=_FENCE,
                        admitted_at=_NOW,
                    )

    def test_admitted_command_requires_a_boolean_activation_flag(self) -> None:
        with self.assertRaises(TypeError):
            AdmittedCommand(
                task_id="task-1",
                command_id="command-1",
                agent_session_id=_PRINCIPAL,
                profile="default",
                fence_generation=_FENCE,
                activated=1,  # type: ignore[arg-type]
                admitted_at=_NOW,
            )

    def test_audit_entry_cannot_carry_payload_fields(self) -> None:
        names = {entry.name for entry in dataclasses.fields(CommandAuditEntry)}

        for forbidden in (
            "argv",
            "environment",
            "payload",
            "stream",
            "stdin",
            "stdout",
            "stderr",
            "content",
        ):
            for name in names:
                self.assertNotIn(forbidden, name)


class _DenyPolicy:
    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        return PolicyResult(
            decision=PolicyDecision.DENY,
            policy_version=2,
            decision_reference="deny-v2",
        )


def _deny_policy() -> _DenyPolicy:
    return _DenyPolicy()


async def _only_command(
    repo: InMemoryCommandRepository, task_id: str
) -> AdmittedCommand:
    state = await repo.admission_state(task_id)
    assert state is not None and state.in_flight_command_id is not None
    return AdmittedCommand(
        task_id=task_id,
        command_id=state.in_flight_command_id,
        agent_session_id=state.agent_session_id,
        profile=state.profile,
        fence_generation=state.fence_generation,
        activated=True,
        admitted_at=_NOW,
    )


class RecordingJournal:
    def __init__(self) -> None:
        self.events: list[CommandEvent] = []

    async def append(self, event: CommandEvent) -> None:
        self.events.append(event)


class ExplodingJournal:
    async def append(self, event: CommandEvent) -> None:
        raise ConnectionError("journal unavailable")


def _admission_request(
    task_id: str = "task-1",
    command_id: str = "command-1",
    principal: str = _PRINCIPAL,
    fence: str = _FENCE,
) -> AdmissionRequest:
    return AdmissionRequest(
        task_id=task_id,
        command_id=command_id,
        authenticated_principal=principal,
        fence_generation=fence,
        admitted_at=_NOW,
    )


class CommandEventTests(unittest.TestCase):
    def test_event_rejects_fields_outside_its_kind(self) -> None:
        with self.assertRaises(ValueError):
            CommandEvent(
                kind=CommandEventKind.TERMINATION_BEGUN,
                task_id="task-1",
                command_id="command-1",
            )

    def test_event_requires_its_own_fields(self) -> None:
        with self.assertRaises(ValueError):
            CommandEvent(kind=CommandEventKind.ADMITTED, task_id="task-1")

    def test_event_rejects_naive_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            CommandEvent(
                kind=CommandEventKind.ADMITTED,
                task_id="task-1",
                command_id="command-1",
                authenticated_principal=_PRINCIPAL,
                fence_generation=_FENCE,
                admitted_at=datetime(2026, 8, 28, 12),  # noqa: DTZ001 - naive on purpose
            )

    def test_accessors_reconstruct_domain_objects(self) -> None:
        seed = CommandEvent(
            kind=CommandEventKind.SEED_READY,
            task_id="task-1",
            agent_session_id=_PRINCIPAL,
            profile="default",
            fence_generation=_FENCE,
        )
        state = seed.seed_state()
        self.assertEqual(state.task_id, "task-1")
        self.assertEqual(state.lifecycle, AdmissionLifecycle.READY)

        admitted = CommandEvent(
            kind=CommandEventKind.ADMITTED,
            task_id="task-1",
            command_id="command-1",
            authenticated_principal=_PRINCIPAL,
            fence_generation=_FENCE,
            admitted_at=_NOW,
        )
        request = admitted.admission_request()
        self.assertEqual(request.command_id, "command-1")
        self.assertEqual(request.admitted_at, _NOW)

    def test_accessors_refuse_the_wrong_kind(self) -> None:
        event = CommandEvent(
            kind=CommandEventKind.FENCE_ADOPTED, fence_generation=_FENCE
        )
        with self.assertRaises(ValueError):
            event.seed_state()
        with self.assertRaises(ValueError):
            event.admission_request()


class CommandEventJournalTests(unittest.IsolatedAsyncioTestCase):
    async def test_mutations_are_journaled_in_application_order(self) -> None:
        journal = RecordingJournal()
        repo = InMemoryCommandRepository(journal=journal)

        await repo.seed_ready(admission_state())
        decision = await repo.admit(_admission_request())
        assert decision.command is not None
        await repo.finish(decision.command, CommandOutcome.SUCCESS, _NOW)
        await repo.begin_termination("task-1")
        await repo.recover("fence-generation-2")

        self.assertEqual(
            [event.kind for event in journal.events],
            [
                CommandEventKind.SEED_READY,
                CommandEventKind.ADMITTED,
                CommandEventKind.FINISHED,
                CommandEventKind.TERMINATION_BEGUN,
                CommandEventKind.FENCE_ADOPTED,
            ],
        )
        admitted_event = journal.events[1]
        self.assertEqual(admitted_event.command_id, "command-1")
        self.assertEqual(admitted_event.authenticated_principal, _PRINCIPAL)
        self.assertEqual(admitted_event.admitted_at, _NOW)
        self.assertEqual(journal.events[2].outcome, CommandOutcome.SUCCESS)
        self.assertEqual(journal.events[3].task_id, "task-1")
        self.assertEqual(journal.events[4].fence_generation, "fence-generation-2")

    async def test_refusals_are_never_journaled(self) -> None:
        journal = RecordingJournal()
        repo = InMemoryCommandRepository(journal=journal)
        await repo.seed_ready(admission_state())

        journal.events.clear()
        unknown = await repo.admit(_admission_request(task_id="task-9"))
        wrong_principal = await repo.admit(_admission_request(principal="other"))
        stale = await repo.admit(_admission_request(fence="older-fence"))
        admitted = await repo.admit(_admission_request())
        busy = await repo.admit(_admission_request(command_id="command-2"))

        self.assertIsNotNone(unknown.refusal)
        self.assertIsNotNone(wrong_principal.refusal)
        self.assertIsNotNone(stale.refusal)
        self.assertIsNotNone(busy.refusal)
        self.assertIsNotNone(admitted.command)
        self.assertEqual(
            [event.kind for event in journal.events],
            [CommandEventKind.ADMITTED],
        )
        self.assertEqual(journal.events[0].command_id, "command-1")

    async def test_foreign_finish_and_repeat_termination_are_not_journaled(self) -> None:
        journal = RecordingJournal()
        repo = InMemoryCommandRepository(journal=journal)
        await repo.seed_ready(admission_state())
        decision = await repo.admit(_admission_request())
        assert decision.command is not None

        journal.events.clear()
        foreign = AdmittedCommand(
            task_id="task-1",
            command_id="command-9",
            agent_session_id=_PRINCIPAL,
            profile="default",
            fence_generation=_FENCE,
            activated=False,
            admitted_at=_NOW,
        )
        await repo.finish(foreign, CommandOutcome.SUCCESS, _NOW)
        await repo.begin_termination("task-1")
        await repo.begin_termination("task-1")

        self.assertEqual(
            [event.kind for event in journal.events],
            [CommandEventKind.TERMINATION_BEGUN],
        )

    async def test_seed_refresh_is_journaled_and_updates_the_fence(self) -> None:
        journal = RecordingJournal()
        repo = InMemoryCommandRepository(journal=journal)

        await repo.seed_ready(admission_state())
        await repo.seed_ready(admission_state(fence="fence-generation-2"))

        self.assertEqual(
            [event.kind for event in journal.events],
            [CommandEventKind.SEED_READY, CommandEventKind.SEED_READY],
        )
        self.assertEqual(
            journal.events[1].fence_generation, "fence-generation-2"
        )
        state = await repo.admission_state("task-1")
        assert state is not None
        self.assertEqual(state.fence_generation, "fence-generation-2")

    async def test_journal_failure_leaves_the_mutation_unapplied(self) -> None:
        journal = ExplodingJournal()
        repo = InMemoryCommandRepository(journal=journal)
        with self.assertRaises(ConnectionError):
            await repo.seed_ready(admission_state())
        self.assertIsNone(await repo.admission_state("task-1"))

        live = repository(admission_state())
        live.attach_journal(journal)
        with self.assertRaises(ConnectionError):
            await live.admit(_admission_request())
        state = await live.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)
        with self.assertRaises(ConnectionError):
            await live.begin_termination("task-1")
        self.assertEqual(await live.lifecycle("task-1"), AdmissionLifecycle.READY)

    async def test_journal_attaches_after_construction(self) -> None:
        journal = RecordingJournal()
        repo = repository(admission_state())
        repo.attach_journal(journal)

        await repo.begin_termination("task-1")

        self.assertEqual(
            [event.kind for event in journal.events],
            [CommandEventKind.TERMINATION_BEGUN],
        )


if __name__ == "__main__":
    unittest.main()
