from __future__ import annotations

import asyncio
import unittest

from sandbox_manager.intake import QueuedTask
from sandbox_manager.profiles import (
    PROFILE_SCHEMA_VERSION,
    ProfileMount,
    SandboxProfile,
    SandboxProfileRegistry,
    UnknownSandboxTypeError,
)
from sandbox_manager.provisioning import (
    CapacityLimiter,
    FailureStage,
    ProvisionedSandbox,
    ProvisioningError,
    ProvisioningOrchestrator,
    ProvisioningState,
    ProvisioningStatus,
    ProvisioningTask,
    ReadyPublicationError,
    ReadyTask,
    RelayReference,
)
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest


def queued_task(sandbox_type: str = "python") -> QueuedTask:
    return QueuedTask.from_request(
        SpawnRequest(
            schema_version=SCHEMA_VERSION,
            request_id="request-1",
            task_id="task-1",
            agent_session_id="session-1",
            sandbox_type=sandbox_type,
        )
    )


def profile() -> SandboxProfile:
    return SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image="registry.example/sandbox/python@sha256:" + "a" * 64,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/results"),),
    )


class RecordingCapacity:
    def __init__(self, events: list[str], available: bool = True) -> None:
        self.events = events
        self.available = available

    async def try_reserve(self, task_id: str) -> bool:
        self.events.append("reserve")
        return self.available

    async def release(self, task_id: str) -> None:
        self.events.append("release")


class RecordingRepository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.provisioning_error: BaseException | None = None
        self.ready_error: BaseException | None = None
        self.failure_record_error: Exception | None = None
        self.resources_error: Exception | None = None
        self.failure_stage: FailureStage | None = None
        self.committed_ready: ReadyTask | None = None

    async def mark_provisioning(
        self, task: QueuedTask, resolved_profile: SandboxProfile
    ) -> ProvisioningTask:
        self.events.append("mark_provisioning")
        if self.provisioning_error is not None:
            raise self.provisioning_error
        return ProvisioningTask(task, resolved_profile)

    async def mark_ready(
        self,
        task: ProvisioningTask,
        resources: ProvisionedSandbox,
        relay: RelayReference,
    ) -> ReadyTask:
        self.events.append("mark_ready")
        ready_task = ReadyTask(task, resources, relay)
        if self.ready_error is not None:
            self.committed_ready = ready_task
            raise self.ready_error
        self.committed_ready = ready_task
        return ready_task

    async def record_resources(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        self.events.append("record_resources")
        if self.resources_error is not None:
            raise self.resources_error

    async def record_failure(self, task_id: str, stage: FailureStage) -> None:
        self.events.append("record_failure")
        if self.failure_record_error is not None:
            raise self.failure_record_error
        self.failure_stage = stage

    async def record_rollback(self, task_id: str) -> None:
        self.events.append("record_rollback")


class RecordingProvisioner:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.provision_error: BaseException | None = None
        self.readiness_error: BaseException | None = None
        self.rollback_error: Exception | None = None
        self.resources = ProvisionedSandbox(
            "pod/default/task-1",
            "uid-pod-task-1",
            "pvc/task-1",
            "uid-pvc-task-1",
        )
        self.relay = RelayReference(1, "sandbox.agent.session-1.admit")

    async def provision(self, task: ProvisioningTask) -> ProvisionedSandbox:
        self.events.append("provision")
        if self.provision_error is not None:
            raise self.provision_error
        return self.resources

    async def wait_ready(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        self.events.append("wait_ready")
        if self.readiness_error is not None:
            raise self.readiness_error

    async def rollback(self, task_id: str) -> None:
        self.events.append("rollback")
        if self.rollback_error is not None:
            raise self.rollback_error


class ConcurrentProvisioner(RecordingProvisioner):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.active = 0
        self.maximum_active = 0

    async def provision(self, task: ProvisioningTask) -> ProvisionedSandbox:
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        await asyncio.sleep(0)
        resources = await super().provision(task)
        self.active -= 1
        return resources


class RecordingPublisher:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.error: Exception | None = None
        self.tasks: list[ReadyTask] = []

    async def publish_ready(self, task: ReadyTask) -> None:
        self.events.append("publish_ready")
        if self.error is not None:
            raise self.error
        self.tasks.append(task)


class ProvisioningOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.events: list[str] = []
        self.capacity = RecordingCapacity(self.events)
        self.repository = RecordingRepository(self.events)
        self.provisioner = RecordingProvisioner(self.events)
        self.publisher = RecordingPublisher(self.events)
        self.orchestrator = ProvisioningOrchestrator(
            SandboxProfileRegistry([profile()]),
            self.capacity,
            self.repository,
            self.provisioner,
            self.publisher,
        )

    async def test_persists_each_state_before_its_side_effect(self) -> None:
        result = await self.orchestrator.provision(queued_task())

        self.assertEqual(result.status, ProvisioningStatus.READY)
        self.assertIsNotNone(result.ready_task)
        ready_task = result.ready_task
        assert ready_task is not None
        self.assertEqual(ready_task.state, ProvisioningState.READY)
        self.assertEqual(
            self.events,
            [
                "reserve",
                "mark_provisioning",
                "provision",
                "record_resources",
                "wait_ready",
                "mark_ready",
                "publish_ready",
            ],
        )
        self.assertEqual(ready_task.resources.result_volume_reference, "pvc/task-1")

    async def test_leaves_task_pending_when_capacity_is_unavailable(self) -> None:
        self.capacity.available = False

        result = await self.orchestrator.provision(queued_task())

        self.assertEqual(result.status, ProvisioningStatus.AT_CAPACITY)
        self.assertIsNone(result.ready_task)
        self.assertEqual(self.events, ["reserve"])

    async def test_unknown_profile_consumes_no_capacity(self) -> None:
        with self.assertRaises(UnknownSandboxTypeError):
            await self.orchestrator.provision(queued_task("node"))

        self.assertEqual(self.events, ["record_failure"])
        self.assertEqual(self.repository.failure_stage, FailureStage.PROFILE_RESOLUTION)

    async def test_provisioning_persistence_failure_retains_capacity(self) -> None:
        self.repository.provisioning_error = OSError("state unavailable")

        with self.assertRaises(ProvisioningError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertEqual(raised.exception.stage, FailureStage.PROVISIONING_PERSISTENCE)
        self.assertEqual(self.events, ["reserve", "mark_provisioning"])
        self.assertNotIn("provision", self.events)

    async def test_cancelled_provisioning_transition_retains_capacity(self) -> None:
        self.repository.provisioning_error = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.orchestrator.provision(queued_task())

        self.assertEqual(self.events, ["reserve", "mark_provisioning"])

    async def test_readiness_failure_is_recorded_and_rolled_back(self) -> None:
        self.provisioner.readiness_error = TimeoutError("readiness timed out")

        with self.assertRaises(ProvisioningError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertEqual(raised.exception.stage, FailureStage.READINESS)
        self.assertIsInstance(raised.exception.cause, TimeoutError)
        self.assertEqual(self.repository.failure_stage, FailureStage.READINESS)
        self.assertEqual(
            self.events,
            [
                "reserve",
                "mark_provisioning",
                "provision",
                "record_resources",
                "wait_ready",
                "record_failure",
                "rollback",
                "record_rollback",
                "release",
            ],
        )

    async def test_cancelled_resource_creation_is_rolled_back(self) -> None:
        self.provisioner.provision_error = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.orchestrator.provision(queued_task())

        self.assertEqual(
            self.events,
            [
                "reserve",
                "mark_provisioning",
                "provision",
                "record_failure",
                "rollback",
                "record_rollback",
                "release",
            ],
        )

    async def test_cancelled_readiness_wait_is_rolled_back(self) -> None:
        self.provisioner.readiness_error = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.orchestrator.provision(queued_task())

        self.assertEqual(self.repository.failure_stage, FailureStage.READINESS)
        self.assertIn("rollback", self.events)
        self.assertEqual(self.events[-1], "release")

    async def test_cancelled_ready_transition_is_left_for_reconciliation(self) -> None:
        self.repository.ready_error = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.orchestrator.provision(queued_task())

        self.assertIsNotNone(self.repository.committed_ready)
        self.assertNotIn("rollback", self.events)
        self.assertNotIn("release", self.events)
        self.assertNotIn("publish_ready", self.events)

    async def test_rollback_failure_retains_capacity_and_is_reported(self) -> None:
        self.provisioner.provision_error = OSError("cluster unavailable")
        self.provisioner.rollback_error = OSError("cleanup unavailable")

        with self.assertRaises(ProvisioningError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertEqual(raised.exception.stage, FailureStage.RESOURCE_CREATION)
        self.assertIsNotNone(raised.exception.rollback_error)
        self.assertNotIn("release", self.events)
        self.assertNotIn("record_rollback", self.events)

    async def test_failure_record_error_prevents_rollback(self) -> None:
        self.provisioner.provision_error = OSError("cluster unavailable")
        self.repository.failure_record_error = OSError("state unavailable")

        with self.assertRaises(ProvisioningError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertIsNotNone(raised.exception.failure_record_error)
        self.assertEqual(
            self.events,
            ["reserve", "mark_provisioning", "provision", "record_failure"],
        )

    async def test_resource_persistence_failure_preserves_resources(self) -> None:
        self.repository.resources_error = OSError("state unavailable")

        with self.assertRaises(ProvisioningError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertEqual(raised.exception.stage, FailureStage.RESOURCE_PERSISTENCE)
        self.assertNotIn("wait_ready", self.events)
        self.assertNotIn("rollback", self.events)
        self.assertNotIn("release", self.events)

    async def test_ready_persistence_failure_is_left_for_reconciliation(self) -> None:
        self.repository.ready_error = OSError("state unavailable")

        with self.assertRaises(ProvisioningError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertEqual(raised.exception.stage, FailureStage.READY_PERSISTENCE)
        self.assertIsNotNone(self.repository.committed_ready)
        self.assertNotIn("publish_ready", self.events)
        self.assertNotIn("rollback", self.events)
        self.assertNotIn("release", self.events)

    async def test_publication_failure_preserves_durable_ready_sandbox(self) -> None:
        self.publisher.error = ConnectionError("transport unavailable")

        with self.assertRaises(ReadyPublicationError) as raised:
            await self.orchestrator.provision(queued_task())

        self.assertIsInstance(raised.exception.cause, ConnectionError)
        self.assertEqual(raised.exception.ready_task.state, ProvisioningState.READY)
        self.assertEqual(self.events[-2:], ["mark_ready", "publish_ready"])
        self.assertNotIn("rollback", self.events)
        self.assertNotIn("release", self.events)

    async def test_serializes_concurrent_reconciliation_for_one_task(self) -> None:
        provisioner = ConcurrentProvisioner(self.events)
        orchestrator = ProvisioningOrchestrator(
            SandboxProfileRegistry([profile()]),
            self.capacity,
            self.repository,
            provisioner,
            self.publisher,
        )

        results = await asyncio.gather(
            orchestrator.provision(queued_task()),
            orchestrator.provision(queued_task()),
        )

        self.assertEqual([result.status for result in results], ["ready", "ready"])
        self.assertEqual(provisioner.maximum_active, 1)


class CapacityLimiterTests(unittest.IsolatedAsyncioTestCase):
    async def test_reservations_never_exceed_capacity(self) -> None:
        capacity = CapacityLimiter(10)

        results = await asyncio.gather(
            *(capacity.try_reserve(f"task-{number}") for number in range(11))
        )

        self.assertEqual(results.count(True), 10)
        self.assertFalse(results[-1])
        self.assertTrue(await capacity.try_reserve("task-0"))

        await capacity.release("task-0")
        self.assertTrue(await capacity.try_reserve("task-10"))

    async def test_initializes_from_active_tasks(self) -> None:
        capacity = CapacityLimiter(2, {"task-1", "task-2"})

        self.assertFalse(await capacity.try_reserve("task-3"))
        with self.assertRaisesRegex(ValueError, "exceed"):
            CapacityLimiter(1, {"task-1", "task-2"})


if __name__ == "__main__":
    unittest.main()
