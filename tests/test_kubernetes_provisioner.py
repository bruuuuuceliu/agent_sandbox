from __future__ import annotations

import asyncio
import unittest
from collections.abc import Mapping
from copy import deepcopy
from datetime import timedelta
from typing import cast

from sandbox_manager.intake import QueuedTask
from sandbox_manager.kubernetes import (
    KubernetesProvisionerConfig,
    KubernetesResourceError,
    KubernetesSandboxProvisioner,
    sandbox_resource_name,
)
from sandbox_manager.profiles import (
    PROFILE_SCHEMA_VERSION,
    ProfileMount,
    SandboxProfile,
    SandboxProfileRegistry,
)
from sandbox_manager.provisioning import (
    FailureStage,
    ProvisionedSandbox,
    ProvisioningOrchestrator,
    ProvisioningStatus,
    ProvisioningTask,
    ReadyTask,
    RelayReference,
)
from sandbox_manager.spawn import SCHEMA_VERSION, SpawnRequest

_IMAGE = "registry.example/sandbox/python@sha256:" + "a" * 64


def queued_task() -> QueuedTask:
    return QueuedTask.from_request(
        SpawnRequest(
            schema_version=SCHEMA_VERSION,
            request_id="request-1",
            task_id="task-1",
            agent_session_id="session-1",
            sandbox_type="python",
        )
    )


def profile() -> SandboxProfile:
    return SandboxProfile(
        schema_version=PROFILE_SCHEMA_VERSION,
        profile_version=1,
        sandbox_type="python",
        image=_IMAGE,
        command=("python", "-m", "sandbox_agent"),
        volume_mounts=(ProfileMount("results", "/workspace/results"),),
    )


def provisioner(
    client: RecordingKubernetesClient,
    readiness_timeout: timedelta = timedelta(seconds=60),
) -> KubernetesSandboxProvisioner:
    return KubernetesSandboxProvisioner(
        KubernetesProvisionerConfig(
            namespace="sandboxes",
            runtime_class_name="runsc",
            storage_class_name="encrypted-rwo",
            result_storage="2Gi",
            readiness_timeout=readiness_timeout,
        ),
        client,
    )


def manifest_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"expected mapping, got {type(value).__name__}")
    return cast(dict[str, object], value)


class RecordingKubernetesClient:
    def __init__(self, fail_at: int | None = None) -> None:
        self.fail_at = fail_at
        self.manifests: dict[tuple[str, str, str, str], dict[str, object]] = {}
        self.apply_attempts: list[tuple[str, str, str, str]] = []
        self.ready_waits: list[tuple[str, str]] = []
        self.deletions: list[tuple[str, str, str, str]] = []
        self.ready_gate: asyncio.Event | None = None

    async def apply(self, resource: Mapping[str, object]) -> str:
        metadata = manifest_mapping(resource["metadata"])
        key = (
            cast(str, resource["apiVersion"]),
            cast(str, resource["kind"]),
            cast(str, metadata["namespace"]),
            cast(str, metadata["name"]),
        )
        if self.fail_at == len(self.apply_attempts):
            self.fail_at = None
            raise OSError("apply interrupted")

        copied = deepcopy(dict(resource))
        existing = self.manifests.get(key)
        if existing is not None and existing != copied:
            raise RuntimeError("conflicting resource apply")
        self.manifests[key] = copied
        self.apply_attempts.append(key)
        return f"uid-{key[1].lower()}-{key[3]}"

    async def wait_pod_ready(self, namespace: str, name: str) -> None:
        self.ready_waits.append((namespace, name))
        if self.ready_gate is not None:
            await self.ready_gate.wait()

    async def delete(
        self, api_version: str, kind: str, namespace: str, name: str
    ) -> None:
        key = (api_version, kind, namespace, name)
        self.deletions.append(key)
        self.manifests.pop(key, None)


class RecordingCapacity:
    def __init__(self) -> None:
        self.reserved: set[str] = set()

    async def try_reserve(self, task_id: str) -> bool:
        self.reserved.add(task_id)
        return True

    async def release(self, task_id: str) -> None:
        self.reserved.discard(task_id)


class RecordingRepository:
    def __init__(self) -> None:
        self.ready_task: ReadyTask | None = None

    async def mark_provisioning(
        self, task: QueuedTask, resolved_profile: SandboxProfile
    ) -> ProvisioningTask:
        return ProvisioningTask(task, resolved_profile)

    async def mark_ready(
        self,
        task: ProvisioningTask,
        resources: ProvisionedSandbox,
        relay: RelayReference,
    ) -> ReadyTask:
        self.ready_task = ReadyTask(task, resources, relay)
        return self.ready_task

    async def record_resources(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        return None

    async def record_failure(self, task_id: str, stage: FailureStage) -> None:
        raise AssertionError("unexpected failure")

    async def record_rollback(self, task_id: str) -> None:
        raise AssertionError("unexpected rollback")


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[ReadyTask] = []

    async def publish_ready(self, task: ReadyTask) -> None:
        self.published.append(task)


class KubernetesSandboxProvisionerTests(unittest.IsolatedAsyncioTestCase):
    async def test_orchestrator_reaches_ready_through_kubernetes_boundary(self) -> None:
        client = RecordingKubernetesClient()
        repository = RecordingRepository()
        publisher = RecordingPublisher()
        orchestrator = ProvisioningOrchestrator(
            SandboxProfileRegistry([profile()]),
            RecordingCapacity(),
            repository,
            provisioner(client),
            publisher,
        )

        result = await orchestrator.provision(queued_task())

        self.assertEqual(result.status, ProvisioningStatus.READY)
        self.assertIs(result.ready_task, repository.ready_task)
        self.assertEqual(publisher.published, [result.ready_task])
        name = sandbox_resource_name("task-1")
        pod = client.manifests[("v1", "Pod", "sandboxes", name)]
        pod_spec = manifest_mapping(pod["spec"])
        container = manifest_mapping(cast(list[object], pod_spec["containers"])[0])
        self.assertEqual(container["command"], ["python", "-m", "sandbox_agent"])
        self.assertIn(
            {"mountPath": "/workspace/results", "name": "results"},
            cast(list[object], container["volumeMounts"]),
        )
        self.assertEqual(client.ready_waits, [("sandboxes", name)])
        assert result.ready_task is not None
        self.assertEqual(
            result.ready_task.relay.route, "sandbox.agent.session-1.admit"
        )
        self.assertNotIn("connection-secret", repr(result.ready_task))
        self.assertEqual(
            result.ready_task.resources.resource_uid,
            f"uid-pod-{name}",
        )
        self.assertEqual(
            result.ready_task.resources.result_volume_uid,
            f"uid-persistentvolumeclaim-{name}",
        )

    async def test_partial_apply_retries_against_existing_resources(self) -> None:
        client = RecordingKubernetesClient(fail_at=1)
        adapter = provisioner(client)
        task = ProvisioningTask(queued_task(), profile())

        with self.assertRaisesRegex(OSError, "apply interrupted"):
            await adapter.provision(task)
        self.assertEqual(len(client.manifests), 1)

        resources = await adapter.provision(task)

        self.assertEqual(len(client.manifests), 3)
        self.assertEqual(
            resources.result_volume_reference,
            f"persistentvolumeclaim/sandboxes/{sandbox_resource_name('task-1')}",
        )

    async def test_readiness_rejects_mismatched_resource_references(self) -> None:
        client = RecordingKubernetesClient()
        adapter = provisioner(client)
        task = ProvisioningTask(queued_task(), profile())

        with self.assertRaisesRegex(KubernetesResourceError, "do not match"):
            await adapter.wait_ready(
                task,
                ProvisionedSandbox(
                    "pod/sandboxes/other",
                    "uid-pod-other",
                    "pvc/sandboxes/other",
                    "uid-pvc-other",
                ),
            )

        self.assertEqual(client.ready_waits, [])

    async def test_readiness_timeout_is_enforced(self) -> None:
        client = RecordingKubernetesClient()
        client.ready_gate = asyncio.Event()
        adapter = provisioner(client, timedelta(milliseconds=1))
        task = ProvisioningTask(queued_task(), profile())
        resources = await adapter.provision(task)

        with self.assertRaises(TimeoutError):
            await adapter.wait_ready(task, resources)

    async def test_rollback_is_repeatable_and_preserves_results(self) -> None:
        client = RecordingKubernetesClient()
        adapter = provisioner(client)
        task = ProvisioningTask(queued_task(), profile())
        await adapter.provision(task)

        await adapter.rollback(task.task_id)
        await adapter.rollback(task.task_id)

        name = sandbox_resource_name("task-1")
        self.assertEqual(
            set(client.manifests),
            {("v1", "PersistentVolumeClaim", "sandboxes", name)},
        )
        self.assertEqual(len(client.deletions), 4)
        self.assertEqual(client.deletions[0][1], "Pod")
        self.assertEqual(client.deletions[1][1], "NetworkPolicy")


if __name__ == "__main__":
    unittest.main()
