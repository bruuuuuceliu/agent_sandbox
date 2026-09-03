"""Fault injection: process and transport loss under live traffic.

Three composed scenarios the unit suites bracket but never run together:

* spawn intake riding out the loss of its NATS connection — the loop
  survives without crashing and never classifies transport loss as
  poison, and a replacement process resumes the durable backlog;
* a command-event journal whose transport dies mid-mutation — admission
  and termination fail closed with repository state unchanged, and the
  same mutations succeed once a replacement journal is attached;
* a full manager restart under traffic — the journal replays, a new
  fence adopts, the interrupted command is classified ``UNKNOWN``, and
  the spawn backlog drains alongside fresh admissions.
"""

from __future__ import annotations

import asyncio
import unittest

from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    InMemoryCommandRepository,
)
from sandbox_manager.nats_client import AsyncNatsConnection, NatsTransportError
from sandbox_manager.nats_lifecycle import (
    JetStream,
    JetStreamCommandEventJournal,
    JetStreamError,
    JetStreamSpawnIntake,
    spawn_subject,
)
from sandbox_manager.relay_protocol import CommandOutcome
from tests.test_commands import _FENCE, _admission_request, admission_state
from tests.test_nats_lifecycle import (
    _AGENT_USER,
    _MANAGER_USER,
    IntakeRepository,
    LifecycleEnvironment,
    RecordingObserver,
    _until,
    spawn_payload,
)


def _intake(
    jetstream: JetStream, repository: IntakeRepository, observer: RecordingObserver
) -> JetStreamSpawnIntake:
    return JetStreamSpawnIntake(
        jetstream,
        repository,
        pull_wait_seconds=0.05,
        redelivery_delay_seconds=0.01,
        retry_backoff_seconds=0.05,
        observer=observer,
    )


class IntakeConnectionLossTests(unittest.IsolatedAsyncioTestCase):
    async def test_intake_rides_out_loss_and_a_replacement_drains_the_backlog(
        self,
    ) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        replacement: AsyncNatsConnection | None = None
        intake_a: JetStreamSpawnIntake | None = None
        intake_b: JetStreamSpawnIntake | None = None
        try:
            assert environment.jetstream is not None
            assert environment.connection is not None
            repository_a = IntakeRepository()
            observer = RecordingObserver()
            intake_a = _intake(environment.jetstream, repository_a, observer)
            await intake_a.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            stream = environment.server.streams["SANDBOX_SPAWN"]
            await _until(lambda: "task-1" in repository_a.tasks)
            await _until(lambda: stream.messages[0].acked)

            # The manager process loses its transport; the durable backlog
            # keeps growing while it is gone.
            await environment.connection.close()
            await agent.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(task_id="task-2", request_id="request-2"),
            )
            await asyncio.sleep(0.2)  # several backoff windows elapse

            assert intake_a._task is not None
            self.assertFalse(intake_a._task.done())
            # Transport loss is neither poison nor a deferral.
            self.assertEqual(observer.events, [])

            # A replacement process resumes the same durable consumer.
            replacement = AsyncNatsConnection(
                "127.0.0.1",
                environment.server.port,
                _MANAGER_USER,
                "unused",
                "manager-replacement",
            )
            await replacement.connect()
            repository_b = IntakeRepository()
            observer_b = RecordingObserver()
            intake_b = _intake(JetStream(replacement), repository_b, observer_b)
            await intake_b.start()
            await _until(lambda: "task-2" in repository_b.tasks)
            await _until(
                lambda: len(stream.messages) == 2
                and all(message.acked for message in stream.messages)
            )
            # The first spawn was durably accepted before the loss; only the
            # backlog crosses the restart.
            self.assertNotIn("task-1", repository_b.tasks)
            self.assertEqual(observer_b.events, [])
        finally:
            for intake in (intake_a, intake_b):
                if intake is not None:
                    await intake.stop()
            if replacement is not None:
                await replacement.close()
            await environment.stop()


class JournalTransportFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_journal_transport_failure_fails_admission_closed(self) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        replacement: AsyncNatsConnection | None = None
        try:
            assert environment.jetstream is not None
            assert environment.connection is not None
            journal = JetStreamCommandEventJournal(
                environment.jetstream, pull_wait_seconds=0.05
            )
            await journal.start()
            repository = InMemoryCommandRepository(journal=journal)
            await repository.seed_ready(admission_state())
            events = environment.server.streams[journal.stream].messages
            self.assertEqual(len(events), 1)

            # The journal transport dies mid-flight.
            await environment.connection.close()

            with self.assertRaises((JetStreamError, NatsTransportError)):
                await repository.admit(_admission_request())
            with self.assertRaises((JetStreamError, NatsTransportError)):
                await repository.begin_termination("task-1")

            # Fail-closed: neither mutation was applied or journaled.
            state = await repository.admission_state("task-1")
            assert state is not None
            self.assertIsNone(state.in_flight_command_id)
            self.assertIs(state.lifecycle, AdmissionLifecycle.READY)
            self.assertEqual(len(events), 1)

            # A replacement journal admits the same command cleanly.
            replacement = AsyncNatsConnection(
                "127.0.0.1",
                environment.server.port,
                _MANAGER_USER,
                "unused",
                "manager-replacement",
            )
            await replacement.connect()
            journal_b = JetStreamCommandEventJournal(
                JetStream(replacement), pull_wait_seconds=0.05
            )
            await journal_b.start()
            repository.attach_journal(journal_b)
            decision = await repository.admit(
                _admission_request(command_id="command-2")
            )

            self.assertIsNotNone(decision.command)
            state = await repository.admission_state("task-1")
            assert state is not None
            self.assertEqual(state.in_flight_command_id, "command-2")
            self.assertEqual(len(events), 2)
        finally:
            if replacement is not None:
                await replacement.close()
            await environment.stop()


class ManagerRestartUnderTrafficTests(unittest.IsolatedAsyncioTestCase):
    async def test_restart_replays_commands_adopts_a_fence_and_drains_the_backlog(
        self,
    ) -> None:
        environment = LifecycleEnvironment()
        await environment.start()
        replacement: AsyncNatsConnection | None = None
        intake_a: JetStreamSpawnIntake | None = None
        intake_b: JetStreamSpawnIntake | None = None
        try:
            assert environment.jetstream is not None
            assert environment.connection is not None
            # Process A: a journaled repository with a command that never
            # finishes, plus a live spawn intake.
            journal_a = JetStreamCommandEventJournal(
                environment.jetstream, pull_wait_seconds=0.05
            )
            await journal_a.start()
            repository_a = InMemoryCommandRepository(journal=journal_a)
            await repository_a.seed_ready(admission_state())
            await repository_a.admit(_admission_request())
            spawn_store_a = IntakeRepository()
            observer_a = RecordingObserver()
            intake_a = _intake(environment.jetstream, spawn_store_a, observer_a)
            await intake_a.start()
            agent = environment.agent
            assert agent is not None
            await agent.publish(spawn_subject(_AGENT_USER), spawn_payload())
            spawn_stream = environment.server.streams["SANDBOX_SPAWN"]
            await _until(lambda: "task-1" in spawn_store_a.tasks)
            await _until(lambda: spawn_stream.messages[0].acked)

            # Process A dies: one command in flight, one backlog delivery.
            await environment.connection.close()
            await agent.publish(
                spawn_subject(_AGENT_USER),
                spawn_payload(task_id="task-2", request_id="request-2"),
            )

            # Process B: replay the journal, adopt a fresh fence, serve.
            replacement = AsyncNatsConnection(
                "127.0.0.1",
                environment.server.port,
                _MANAGER_USER,
                "unused",
                "manager-replacement",
            )
            await replacement.connect()
            jetstream_b = JetStream(replacement)
            journal_b = JetStreamCommandEventJournal(
                jetstream_b, pull_wait_seconds=0.05
            )
            repository_b = await journal_b.load_repository(
                AdmissionFence("fence-generation-2")
            )
            recovered = await repository_b.admission_state("task-1")
            assert recovered is not None
            self.assertEqual(recovered.fence_generation, "fence-generation-2")
            self.assertIsNone(recovered.in_flight_command_id)
            self.assertIs(
                await repository_b.outcome("command-1"), CommandOutcome.UNKNOWN
            )

            # The spawn backlog drains on the replacement's intake.
            spawn_store_b = IntakeRepository()
            observer_b = RecordingObserver()
            intake_b = _intake(jetstream_b, spawn_store_b, observer_b)
            await intake_b.start()
            await _until(lambda: "task-2" in spawn_store_b.tasks)
            self.assertNotIn("task-1", spawn_store_b.tasks)

            # Fresh traffic flows through the recovered repository.
            decision = await repository_b.admit(
                _admission_request(command_id="command-2", fence="fence-generation-2")
            )
            self.assertIsNotNone(decision.command)
            state = await repository_b.admission_state("task-1")
            assert state is not None
            self.assertEqual(state.in_flight_command_id, "command-2")
            self.assertIs(
                await repository_b.outcome("command-1"), CommandOutcome.UNKNOWN
            )
            stale = await repository_b.admit(
                _admission_request(command_id="command-3", fence=_FENCE)
            )
            self.assertIsNotNone(stale.refusal)
            self.assertEqual(
                len(environment.server.streams[journal_b.stream].messages), 4
            )
        finally:
            for intake in (intake_a, intake_b):
                if intake is not None:
                    await intake.stop()
            if replacement is not None:
                await replacement.close()
            await environment.stop()


if __name__ == "__main__":
    unittest.main()
