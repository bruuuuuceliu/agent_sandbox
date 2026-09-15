from __future__ import annotations

import asyncio
import unittest
from collections.abc import Mapping
from typing import cast

from sandbox_manager.kubernetes import (
    CPU_LIMIT,
    CPU_REQUEST,
    MEMORY_LIMIT,
    MEMORY_REQUEST,
    TASK_ANNOTATION,
    WORKSPACE_MOUNT_PATH,
    KubernetesResourceError,
    SandboxWorkload,
    apply_sandbox_resources,
    build_sandbox_resources,
)

_IMAGE = "registry.example/sandbox/python@sha256:" + "a" * 64

_RESOURCE_KINDS = ["PersistentVolumeClaim", "NetworkPolicy", "Pod"]


def workload(**overrides: object) -> SandboxWorkload:
    values: dict[str, object] = {
        "namespace": "sandboxes",
        "task_id": "Task_Customer_42",
        "image": _IMAGE,
        "command": ("python", "-m", "sandbox_agent"),
        "runtime_class_name": "runsc",
        "storage_class_name": "encrypted-rwo",
        "result_storage": "1Gi",
        "results_mount_path": "/results",
    }
    values.update(overrides)
    return SandboxWorkload(**values)  # type: ignore[arg-type]


def mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"expected mapping, got {type(value).__name__}")
    return cast(dict[str, object], value)


def sequence(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"expected list, got {type(value).__name__}")
    return cast(list[object], value)


class KubernetesResourceTests(unittest.TestCase):
    def test_builds_traceable_resources_with_stable_safe_names(self) -> None:
        first = build_sandbox_resources(workload())
        second = build_sandbox_resources(workload())

        first_manifests = first.apply_order()
        self.assertEqual(first_manifests, second.apply_order())
        self.assertEqual(
            [manifest["kind"] for manifest in first_manifests],
            _RESOURCE_KINDS,
        )
        for manifest in first_manifests:
            metadata = mapping(manifest["metadata"])
            name = cast(str, metadata["name"])
            annotations = mapping(metadata["annotations"])
            self.assertLessEqual(len(name), 63)
            self.assertRegex(name, r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
            self.assertEqual(
                annotations[TASK_ANNOTATION], "Task_Customer_42"
            )

        other = build_sandbox_resources(workload(task_id="task-customer-42"))
        first_name = mapping(first.pod["metadata"])["name"]
        other_name = mapping(other.pod["metadata"])["name"]
        self.assertNotEqual(first_name, other_name)

    def test_pvc_is_retention_safe_and_results_are_mounted(self) -> None:
        resources = build_sandbox_resources(workload())
        pvc_metadata = mapping(resources.persistent_volume_claim["metadata"])
        pvc_annotations = mapping(pvc_metadata["annotations"])
        pvc_spec = mapping(resources.persistent_volume_claim["spec"])
        pod_spec = mapping(resources.pod["spec"])
        container = mapping(sequence(pod_spec["containers"])[0])
        mounts = [mapping(item) for item in sequence(container["volumeMounts"])]
        volumes = [mapping(item) for item in sequence(pod_spec["volumes"])]

        self.assertEqual(pvc_annotations["sandbox-manager.io/persistence"], "retained")
        self.assertNotIn("ownerReferences", pvc_metadata)
        self.assertEqual(pvc_spec["accessModes"], ["ReadWriteOnce"])
        self.assertEqual(pvc_spec["storageClassName"], "encrypted-rwo")
        self.assertIn({"mountPath": "/results", "name": "results"}, mounts)
        self.assertIn(
            {"mountPath": WORKSPACE_MOUNT_PATH, "name": "workspace"}, mounts
        )
        self.assertIn({"name": "workspace", "emptyDir": {}}, volumes)
        self.assertTrue(any("persistentVolumeClaim" in volume for volume in volumes))
        self.assertFalse(any("hostPath" in volume for volume in volumes))

    def test_pod_uses_gvisor_fixed_resources_and_restricted_security(self) -> None:
        resources = build_sandbox_resources(workload())
        pod_spec = mapping(resources.pod["spec"])
        pod_security = mapping(pod_spec["securityContext"])
        containers = sequence(pod_spec["containers"])
        container = mapping(containers[0])
        container_security = mapping(container["securityContext"])
        resource_budget = mapping(container["resources"])

        self.assertEqual(len(containers), 1)
        self.assertEqual(pod_spec["runtimeClassName"], "runsc")
        self.assertFalse(cast(bool, pod_spec["automountServiceAccountToken"]))
        self.assertFalse(cast(bool, pod_spec["hostNetwork"]))
        self.assertFalse(cast(bool, pod_spec["hostPID"]))
        self.assertFalse(cast(bool, pod_spec["hostIPC"]))
        self.assertTrue(cast(bool, pod_security["runAsNonRoot"]))
        self.assertEqual(
            mapping(resource_budget["requests"]),
            {"cpu": CPU_REQUEST, "memory": MEMORY_REQUEST},
        )
        self.assertEqual(
            mapping(resource_budget["limits"]),
            {"cpu": CPU_LIMIT, "memory": MEMORY_LIMIT},
        )
        self.assertFalse(cast(bool, container_security["privileged"]))
        self.assertFalse(cast(bool, container_security["allowPrivilegeEscalation"]))
        self.assertTrue(cast(bool, container_security["readOnlyRootFilesystem"]))
        self.assertEqual(container_security["capabilities"], {"drop": ["ALL"]})

    def test_pod_uses_workload_resource_overrides(self) -> None:
        resources = build_sandbox_resources(
            workload(
                cpu_request="500m",
                memory_request="768Mi",
                cpu_limit="2",
                memory_limit="1536Mi",
            )
        )
        pod_spec = mapping(resources.pod["spec"])
        container = mapping(sequence(pod_spec["containers"])[0])
        resource_budget = mapping(container["resources"])

        self.assertEqual(
            mapping(resource_budget["requests"]),
            {"cpu": "500m", "memory": "768Mi"},
        )
        self.assertEqual(
            mapping(resource_budget["limits"]),
            {"cpu": "2", "memory": "1536Mi"},
        )

    def test_sandbox_exposes_no_inbound_network_surface(self) -> None:
        resources = build_sandbox_resources(workload())
        pod_spec = mapping(resources.pod["spec"])
        container = mapping(sequence(pod_spec["containers"])[0])
        deny_spec = mapping(resources.default_deny_network_policy["spec"])

        # The manager relay is the only command path: no Service fronting the
        # Pod, no ingress-allow policy, no listening port, and no credential
        # Secret or volume exists to leak.
        self.assertNotIn("Service", _RESOURCE_KINDS)
        self.assertNotIn("Secret", _RESOURCE_KINDS)
        self.assertFalse(hasattr(resources, "service"))
        self.assertFalse(hasattr(resources, "secret"))
        self.assertFalse(hasattr(resources, "ingress_network_policy"))
        self.assertNotIn("ports", container)
        self.assertNotIn("readinessProbe", container)
        self.assertNotIn(
            "secret",
            [mapping(item).get("secret") for item in sequence(pod_spec["volumes"])],
        )
        self.assertEqual(deny_spec["policyTypes"], ["Ingress", "Egress"])
        self.assertNotIn("ingress", deny_spec)
        self.assertNotIn("egress", deny_spec)

    def test_rejects_unusable_workload_values(self) -> None:
        cases: dict[str, dict[str, object]] = {
            "blank task": {"task_id": ""},
            "control character": {"task_id": "task\n1"},
            "oversized task": {"task_id": "x" * 65},
            "task with dot": {"task_id": "task.customer"},
            "Unicode task": {"task_id": "task-caf\u00e9"},
            "namespace": {"namespace": "Sandbox"},
            "runtime class": {"runtime_class_name": "RunSC"},
            "runtime class segment": {"runtime_class_name": "runsc..sandbox"},
            "storage class": {"storage_class_name": ""},
            "mutable image": {"image": "registry.example/sandbox:latest"},
            "string command": {"command": "python"},
            "storage": {"result_storage": "0Gi"},
            "results at workspace": {"results_mount_path": "/workspace"},
            "results below workspace": {"results_mount_path": "/workspace/results"},
            "results contain workspace": {"results_mount_path": "/"},
            "blank CPU request": {"cpu_request": ""},
            "blank memory request": {"memory_request": ""},
            "blank CPU limit": {"cpu_limit": ""},
            "blank memory limit": {"memory_limit": ""},
        }

        for name, overrides in cases.items():
            with self.subTest(name=name), self.assertRaises(KubernetesResourceError):
                workload(**overrides)


class RecordingApplier:
    def __init__(self, fail_at: int | None = None) -> None:
        self.applied: list[Mapping[str, object]] = []
        self._fail_at = fail_at

    async def apply(self, resource: Mapping[str, object]) -> str:
        if self._fail_at == len(self.applied):
            raise RuntimeError("apply failed")
        await asyncio.sleep(0)
        self.applied.append(resource)
        return f"uid-{len(self.applied)}"


class KubernetesApplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_applies_pod_last(self) -> None:
        resources = build_sandbox_resources(workload())
        applier = RecordingApplier()

        applied = await apply_sandbox_resources(applier, resources)

        self.assertEqual(
            [resource["kind"] for resource in applier.applied],
            _RESOURCE_KINDS,
        )
        self.assertEqual(applied.result_volume_uid, "uid-1")
        self.assertEqual(applied.pod_uid, "uid-3")

    async def test_failure_does_not_start_pod_and_retry_converges(self) -> None:
        resources = build_sandbox_resources(workload())
        failed = RecordingApplier(fail_at=2)

        with self.assertRaisesRegex(RuntimeError, "apply failed"):
            await apply_sandbox_resources(failed, resources)

        self.assertNotIn("Pod", [resource["kind"] for resource in failed.applied])

        recovered = RecordingApplier()
        await apply_sandbox_resources(recovered, resources)
        self.assertEqual(recovered.applied, list(resources.apply_order()))


if __name__ == "__main__":
    unittest.main()
