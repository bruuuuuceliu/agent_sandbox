from __future__ import annotations

import hashlib
import unittest
from datetime import UTC, datetime

from sandbox_manager.kubernetes import sandbox_resource_name
from sandbox_manager.kubernetes_lifecycle import (
    KubernetesEphemeralCleaner,
    KubernetesIdentityError,
    KubernetesLifecycleClient,
    KubernetesLifecycleError,
    KubernetesWorkloadObserver,
    ObjectReference,
    ObservedObject,
    ResultVolumeMissingError,
)


def pod_reference(task_id: str = "task-1") -> str:
    return f"pod/sandboxes/{sandbox_resource_name(task_id)}"


def pod_uid(task_id: str = "task-1") -> str:
    return f"uid-Pod-{sandbox_resource_name(task_id)}"


def result_volume_uid(task_id: str = "task-1") -> str:
    return f"uid-PersistentVolumeClaim-{sandbox_resource_name(task_id)}"


def observed(reference: ObjectReference, task_id: str = "task-1") -> ObservedObject:
    name = sandbox_resource_name(task_id)

    return ObservedObject(
        reference=reference,
        uid=f"uid-{reference.kind}-{reference.name}",
        labels={
            "app.kubernetes.io/instance": name,
            "app.kubernetes.io/managed-by": "sandbox-manager",
            "sandbox-manager.io/task-hash": hashlib.sha256(
                task_id.encode("utf-8")
            ).hexdigest()[:32],
        },
        annotations={"sandbox-manager.io/task-id": task_id},
    )


class RecordingClient(KubernetesLifecycleClient):
    def __init__(self, task_id: str = "task-1") -> None:
        name = sandbox_resource_name(task_id)
        references = (
            ObjectReference("v1", "Pod", "sandboxes", name),
            ObjectReference(
                "networking.k8s.io/v1",
                "NetworkPolicy",
                "sandboxes",
                f"{name}-deny",
            ),
            ObjectReference("v1", "PersistentVolumeClaim", "sandboxes", name),
        )
        self.objects = {
            reference: observed(reference, task_id) for reference in references
        }
        self.deletions: list[tuple[ObjectReference, str]] = []
        self.waits: list[tuple[ObjectReference, str]] = []
        self.replace_during_wait: ObjectReference | None = None
        self.keep_during_wait: ObjectReference | None = None
        self.fail_delete_once: ObjectReference | None = None

    async def observe(self, reference: ObjectReference) -> ObservedObject | None:
        return self.objects.get(reference)

    async def delete_if_uid(self, reference: ObjectReference, uid: str) -> None:
        if self.fail_delete_once == reference:
            self.fail_delete_once = None
            raise OSError("delete interrupted")
        current = self.objects.get(reference)
        if current is None or current.uid != uid:
            raise RuntimeError("UID precondition failed")
        self.deletions.append((reference, uid))

    async def wait_uid_absent(self, reference: ObjectReference, uid: str) -> None:
        self.waits.append((reference, uid))
        current = self.objects.get(reference)
        if (
            current is not None
            and current.uid == uid
            and self.keep_during_wait != reference
        ):
            self.objects.pop(reference)
        if self.replace_during_wait == reference:
            original = observed(reference)
            self.objects[reference] = ObservedObject(
                reference,
                "replacement-uid",
                original.labels,
                original.annotations,
            )


class KubernetesLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_cleanup_removes_only_ephemeral_resources(self) -> None:
        client = RecordingClient()
        cleaner = KubernetesEphemeralCleaner("sandboxes", client)

        await cleaner.stop_workload("task-1", pod_reference(), pod_uid())
        await cleaner.delete_ephemeral_resources(
            "task-1", pod_reference(), pod_uid(), result_volume_uid()
        )

        self.assertEqual(
            {reference.kind for reference in client.objects},
            {"PersistentVolumeClaim"},
        )
        self.assertEqual(len(client.deletions), 2)
        self.assertTrue(all(uid.startswith("uid-") for _, uid in client.deletions))
        self.assertEqual(client.deletions[0][0].kind, "Pod")
        self.assertEqual(client.deletions[1][0].kind, "NetworkPolicy")

    async def test_cleanup_is_repeatable_after_partial_deletion(self) -> None:
        client = RecordingClient()
        cleaner = KubernetesEphemeralCleaner("sandboxes", client)

        await cleaner.stop_workload("task-1", pod_reference(), pod_uid())
        await cleaner.stop_workload("task-1", pod_reference(), pod_uid())
        await cleaner.delete_ephemeral_resources(
            "task-1", pod_reference(), pod_uid(), result_volume_uid()
        )

        self.assertEqual(len(client.deletions), 2)

    async def test_cleanup_recovers_after_interrupted_delete(self) -> None:
        client = RecordingClient()
        pod = next(reference for reference in client.objects if reference.kind == "Pod")
        client.fail_delete_once = pod
        cleaner = KubernetesEphemeralCleaner("sandboxes", client)

        with self.assertRaisesRegex(OSError, "interrupted"):
            await cleaner.delete_ephemeral_resources(
                "task-1", pod_reference(), pod_uid(), result_volume_uid()
            )

        self.assertIn(pod, client.objects)
        await cleaner.delete_ephemeral_resources(
            "task-1", pod_reference(), pod_uid(), result_volume_uid()
        )
        self.assertEqual(
            {reference.kind for reference in client.objects},
            {"PersistentVolumeClaim"},
        )

    async def test_identity_mismatch_fails_before_delete(self) -> None:
        client = RecordingClient()
        pod = next(reference for reference in client.objects if reference.kind == "Pod")
        current = client.objects[pod]
        client.objects[pod] = ObservedObject(
            current.reference,
            current.uid,
            current.labels,
            {"sandbox-manager.io/task-id": "another-task"},
        )

        with self.assertRaises(KubernetesIdentityError):
            await KubernetesEphemeralCleaner("sandboxes", client).stop_workload(
                "task-1", pod_reference(), pod_uid()
            )

        self.assertEqual(client.deletions, [])

    async def test_replacement_race_fails_closed(self) -> None:
        client = RecordingClient()
        pod = next(reference for reference in client.objects if reference.kind == "Pod")
        client.replace_during_wait = pod

        with self.assertRaisesRegex(KubernetesIdentityError, "replaced"):
            await KubernetesEphemeralCleaner("sandboxes", client).stop_workload(
                "task-1", pod_reference(), pod_uid()
            )

        self.assertIn(pod, client.objects)

    async def test_replaced_workload_fails_before_delete(self) -> None:
        client = RecordingClient()
        pod = next(reference for reference in client.objects if reference.kind == "Pod")
        current = client.objects[pod]
        client.objects[pod] = ObservedObject(
            current.reference,
            "replacement-uid",
            current.labels,
            current.annotations,
        )

        with self.assertRaisesRegex(KubernetesIdentityError, "workload UID"):
            await KubernetesEphemeralCleaner("sandboxes", client).stop_workload(
                "task-1", pod_reference(), pod_uid()
            )

        self.assertEqual(client.deletions, [])

    async def test_cleanup_requires_observed_disappearance(self) -> None:
        client = RecordingClient()
        pod = next(reference for reference in client.objects if reference.kind == "Pod")
        client.keep_during_wait = pod

        with self.assertRaisesRegex(KubernetesLifecycleError, "still exists"):
            await KubernetesEphemeralCleaner(
                "sandboxes", client
            ).delete_ephemeral_resources(
                "task-1", pod_reference(), pod_uid(), result_volume_uid()
            )

        self.assertIn(pod, client.objects)

    async def test_missing_result_volume_prevents_cleanup_completion(self) -> None:
        client = RecordingClient()
        pvc = next(
            reference
            for reference in client.objects
            if reference.kind == "PersistentVolumeClaim"
        )
        client.objects.pop(pvc)
        cleaner = KubernetesEphemeralCleaner("sandboxes", client)

        with self.assertRaises(ResultVolumeMissingError):
            await cleaner.delete_ephemeral_resources(
                "task-1", pod_reference(), pod_uid(), result_volume_uid()
            )

        self.assertTrue(
            all(
                reference.kind != "PersistentVolumeClaim"
                for reference, _ in client.deletions
            )
        )

    async def test_terminating_result_volume_prevents_cleanup_completion(self) -> None:
        client = RecordingClient()
        pvc = next(
            reference
            for reference in client.objects
            if reference.kind == "PersistentVolumeClaim"
        )
        current = client.objects[pvc]
        client.objects[pvc] = ObservedObject(
            current.reference,
            current.uid,
            current.labels,
            current.annotations,
            datetime(2026, 8, 28, 12, tzinfo=UTC),
        )

        with self.assertRaisesRegex(ResultVolumeMissingError, "in progress"):
            await KubernetesEphemeralCleaner(
                "sandboxes", client
            ).delete_ephemeral_resources(
                "task-1", pod_reference(), pod_uid(), result_volume_uid()
            )

    async def test_replaced_result_volume_prevents_cleanup_completion(self) -> None:
        client = RecordingClient()
        pvc = next(
            reference
            for reference in client.objects
            if reference.kind == "PersistentVolumeClaim"
        )
        current = client.objects[pvc]
        client.objects[pvc] = ObservedObject(
            current.reference,
            "replacement-uid",
            current.labels,
            current.annotations,
        )

        with self.assertRaisesRegex(KubernetesIdentityError, "volume UID"):
            await KubernetesEphemeralCleaner(
                "sandboxes", client
            ).delete_ephemeral_resources(
                "task-1", pod_reference(), pod_uid(), result_volume_uid()
            )

        self.assertIn(pvc, client.objects)

    async def test_observer_detects_missing_and_terminating_pods(self) -> None:
        client = RecordingClient()
        observer = KubernetesWorkloadObserver("sandboxes", client)

        self.assertTrue(
            await observer.workload_exists("task-1", pod_reference(), pod_uid())
        )
        pod = next(reference for reference in client.objects if reference.kind == "Pod")
        current = client.objects[pod]
        client.objects[pod] = ObservedObject(
            current.reference,
            current.uid,
            current.labels,
            current.annotations,
            datetime(2026, 8, 28, 12, tzinfo=UTC),
        )
        self.assertFalse(
            await observer.workload_exists("task-1", pod_reference(), pod_uid())
        )
        client.objects.pop(pod)
        self.assertFalse(
            await observer.workload_exists("task-1", pod_reference(), pod_uid())
        )

    async def test_observer_rejects_replaced_workload_uid(self) -> None:
        client = RecordingClient()
        observer = KubernetesWorkloadObserver("sandboxes", client)

        with self.assertRaisesRegex(KubernetesIdentityError, "UID"):
            await observer.workload_exists("task-1", pod_reference(), "another-uid")

    async def test_reference_mismatch_performs_no_observation(self) -> None:
        client = RecordingClient()
        observer = KubernetesWorkloadObserver("sandboxes", client)

        with self.assertRaises(KubernetesIdentityError):
            await observer.workload_exists("task-1", "pod/other/wrong", pod_uid())


if __name__ == "__main__":
    unittest.main()
