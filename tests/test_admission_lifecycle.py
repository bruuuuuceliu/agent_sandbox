"""Durable admission wiring across provisioning, termination, and restart.

Covers the three Stage 4b lifecycle integrations: seeding admission state
when provisioning marks a task READY, closing admission when termination
wins the durable race (tested in ``test_lifecycle.py`` against the
manager's cleanup ordering), and recovering prior-fence in-flight commands
as ``UNKNOWN`` after a manager restart without ever re-issuing them.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

from sandbox_manager.admission_lifecycle import AdmissionSeedingPublisher
from sandbox_manager.commands import (
    AdmissionFence,
    AdmissionLifecycle,
    AdmissionRefusal,
    AdmissionRequest,
    AdmittedCommand,
    InMemoryCommandRepository,
    TaskAdmissionState,
)
from sandbox_manager.intake import QueuedTask
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
from sandbox_manager.relay_protocol import CommandOutcome
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest

_ADMITTED_AT = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def python_profile() -> SandboxProfile:
    return SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )


def ready_task(
    task_id: str = "task-1", agent_session_id: str = "session-1"
) -> ReadyTask:
    return ReadyTask(
        provisioning_task=ProvisioningTask(
            queued_task=QueuedTask.from_request(
                SpawnRequest(
                    schema_version=SCHEMA_VERSION,
                    request_id=f"request-{task_id}",
                    task_id=task_id,
                    agent_session_id=agent_session_id,
                    sandbox_type="python",
                )
            ),
            profile=python_profile(),
        ),
        resources=ProvisionedSandbox(
            resource_reference="pod/default/task-1",
            resource_uid="uid-pod-task-1",
            result_volume_reference="pvc/default/task-1",
            result_volume_uid="uid-pvc-task-1",
        ),
        relay=RelayReference(
            protocol_version=1, route="sandbox.agent.session-1.admit"
        ),
    )


def ready_state(
    fence: str,
    task_id: str = "task-1",
    agent_session_id: str = "session-1",
    profile: str = "python",
) -> TaskAdmissionState:
    return TaskAdmissionState(
        task_id=task_id,
        agent_session_id=agent_session_id,
        profile=profile,
        lifecycle=AdmissionLifecycle.READY,
        fence_generation=fence,
    )


async def admit_command(
    repository: InMemoryCommandRepository,
    fence: str,
    command_id: str = "command-1",
    principal: str = "session-1",
    task_id: str = "task-1",
) -> AdmittedCommand:
    decision = await repository.admit(
        AdmissionRequest(
            task_id=task_id,
            command_id=command_id,
            authenticated_principal=principal,
            fence_generation=fence,
            admitted_at=_ADMITTED_AT,
        )
    )
    assert decision.command is not None
    return decision.command


class SeedReadyTests(unittest.IsolatedAsyncioTestCase):
    async def test_seed_makes_a_ready_task_admissible(self) -> None:
        repository = InMemoryCommandRepository()
        fence = AdmissionFence("fence-1")

        await repository.seed_ready(ready_state(fence.generation))

        state = await repository.admission_state("task-1")
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state.lifecycle, AdmissionLifecycle.READY)
        command = await admit_command(repository, fence.generation)
        self.assertEqual(command.task_id, "task-1")

    async def test_reseed_refreshes_fence_preserving_activity_and_in_flight(
        self,
    ) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))
        await admit_command(repository, "fence-1")

        await repository.seed_ready(ready_state("fence-2"))

        state = await repository.admission_state("task-1")
        assert state is not None
        self.assertEqual(state.fence_generation, "fence-2")
        self.assertEqual(state.lifecycle, AdmissionLifecycle.ACTIVE)
        self.assertEqual(state.in_flight_command_id, "command-1")
        # The current fence is authoritative, so the slot — not the fence —
        # is what refuses a second command.
        decision = await repository.admit(
            AdmissionRequest(
                task_id="task-1",
                command_id="command-2",
                authenticated_principal="session-1",
                fence_generation="fence-2",
                admitted_at=_ADMITTED_AT,
            )
        )
        self.assertEqual(decision.refusal, AdmissionRefusal.COMMAND_IN_FLIGHT)

    async def test_reseed_refuses_a_terminating_task(self) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))
        await repository.begin_termination("task-1")

        with self.assertRaises(ValueError):
            await repository.seed_ready(ready_state("fence-2"))

    async def test_reseed_refuses_owner_or_profile_change(self) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))

        with self.assertRaises(ValueError):
            await repository.seed_ready(
                ready_state("fence-1", agent_session_id="session-2")
            )
        with self.assertRaises(ValueError):
            await repository.seed_ready(ready_state("fence-1", profile="node"))

    async def test_seed_requires_the_ready_lifecycle(self) -> None:
        repository = InMemoryCommandRepository()
        state = TaskAdmissionState(
            task_id="task-1",
            agent_session_id="session-1",
            profile="python",
            lifecycle=AdmissionLifecycle.ACTIVE,
            fence_generation="fence-1",
        )

        with self.assertRaises(ValueError):
            await repository.seed_ready(state)


class RestartRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_prior_fence_in_flight_commands_become_unknown(self) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))
        await admit_command(repository, "fence-1", command_id="command-1")

        recovery = await repository.recover("fence-2")

        self.assertEqual(len(recovery.entries), 1)
        self.assertEqual(recovery.entries[0].task_id, "task-1")
        self.assertEqual(recovery.entries[0].command_id, "command-1")
        self.assertIs(
            await repository.outcome("command-1"), CommandOutcome.UNKNOWN
        )
        state = await repository.admission_state("task-1")
        assert state is not None
        self.assertIsNone(state.in_flight_command_id)
        self.assertEqual(state.fence_generation, "fence-2")

    async def test_current_fence_in_flight_commands_are_untouched(self) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))
        await admit_command(repository, "fence-1", command_id="command-1")

        recovery = await repository.recover("fence-1")

        self.assertEqual(recovery.entries, ())
        self.assertIsNone(await repository.outcome("command-1"))
        state = await repository.admission_state("task-1")
        assert state is not None
        self.assertEqual(state.in_flight_command_id, "command-1")

    async def test_recovery_lets_surviving_tasks_admit_under_the_new_fence(
        self,
    ) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))

        recovery = await repository.recover("fence-2")

        self.assertEqual(recovery.entries, ())
        command = await admit_command(repository, "fence-2")
        self.assertEqual(command.task_id, "task-1")

    async def test_surviving_terminated_tasks_stay_terminated(self) -> None:
        repository = InMemoryCommandRepository()
        await repository.seed_ready(ready_state("fence-1"))
        await repository.begin_termination("task-1")

        recovery = await repository.recover("fence-2")

        self.assertEqual(recovery.entries, ())
        decision = await repository.admit(
            AdmissionRequest(
                task_id="task-1",
                command_id="command-1",
                authenticated_principal="session-1",
                fence_generation="fence-2",
                admitted_at=_ADMITTED_AT,
            )
        )
        self.assertEqual(decision.refusal, AdmissionRefusal.NOT_ADMISSIBLE)


class RecordingStore:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.seeded: list[TaskAdmissionState] = []

    async def seed_ready(self, state: TaskAdmissionState) -> None:
        self.seeded.append(state)
        self.events.append("seed")


class FailingStore:
    async def seed_ready(self, state: TaskAdmissionState) -> None:
        raise OSError("state unavailable")


class RecordingPublisher:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.published: list[ReadyTask] = []

    async def publish_ready(self, task: ReadyTask) -> None:
        self.published.append(task)
        self.events.append("publish")


class AdmissionSeedingPublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_seeds_durable_state_before_publishing(self) -> None:
        events: list[str] = []
        store = RecordingStore(events)
        publisher = RecordingPublisher(events)
        task = ready_task()

        await AdmissionSeedingPublisher(
            store, AdmissionFence("fence-1"), publisher
        ).publish_ready(task)

        self.assertEqual(events, ["seed", "publish"])
        self.assertEqual(publisher.published, [task])

    async def test_seeded_state_binds_owner_profile_and_fence(self) -> None:
        events: list[str] = []
        store = RecordingStore(events)
        fence = AdmissionFence("fence-1")

        await AdmissionSeedingPublisher(
            store, fence, RecordingPublisher(events)
        ).publish_ready(ready_task())

        assert len(store.seeded) == 1
        state = store.seeded[0]
        self.assertEqual(state.task_id, "task-1")
        self.assertEqual(state.agent_session_id, "session-1")
        self.assertEqual(state.profile, "python")
        self.assertEqual(state.lifecycle, AdmissionLifecycle.READY)
        self.assertEqual(state.fence_generation, fence.generation)

    async def test_seed_failure_prevents_publication(self) -> None:
        events: list[str] = []
        publisher = RecordingPublisher(events)

        with self.assertRaises(OSError):
            await AdmissionSeedingPublisher(
                FailingStore(), AdmissionFence("fence-1"), publisher
            ).publish_ready(ready_task())

        self.assertEqual(publisher.published, [])
        self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
