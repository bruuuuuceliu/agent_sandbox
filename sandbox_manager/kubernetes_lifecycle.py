"""Kubernetes observation and cleanup for sandbox lifecycle management."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Protocol

from .kubernetes import TASK_ANNOTATION, sandbox_resource_name


class KubernetesLifecycleError(RuntimeError):
    """Raised when Kubernetes lifecycle work cannot proceed safely."""


class KubernetesIdentityError(KubernetesLifecycleError):
    """Raised when an observed object does not belong to the expected task."""


class ResultVolumeMissingError(KubernetesLifecycleError):
    """Raised when retained result storage cannot be observed after cleanup."""


@dataclass(frozen=True, slots=True)
class ObjectReference:
    api_version: str
    kind: str
    namespace: str
    name: str

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.api_version, "API version"),
            (self.kind, "kind"),
            (self.namespace, "namespace"),
            (self.name, "name"),
        ):
            _require_text(value, field_name)


@dataclass(frozen=True, slots=True)
class ObservedObject:
    reference: ObjectReference
    uid: str
    labels: Mapping[str, str]
    annotations: Mapping[str, str]
    deletion_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        _require_text(self.uid, "UID")
        object.__setattr__(self, "labels", MappingProxyType(dict(self.labels)))
        object.__setattr__(
            self, "annotations", MappingProxyType(dict(self.annotations))
        )
        if self.deletion_timestamp is not None and (
            self.deletion_timestamp.tzinfo is None
            or self.deletion_timestamp.utcoffset() is None
        ):
            raise ValueError("deletion timestamp must include a timezone")


class KubernetesLifecycleClient(Protocol):
    async def observe(self, reference: ObjectReference) -> ObservedObject | None:
        """Return the current object identity, or None when it is absent."""

        ...

    async def delete_if_uid(self, reference: ObjectReference, uid: str) -> None:
        """Request deletion only when the object's UID still matches."""

        ...

    async def wait_uid_absent(self, reference: ObjectReference, uid: str) -> None:
        """Wait until the specified object generation no longer exists."""

        ...


class KubernetesEphemeralCleaner:
    def __init__(self, namespace: str, client: KubernetesLifecycleClient) -> None:
        _require_text(namespace, "namespace")
        self._namespace = namespace
        self._client = client

    async def stop_workload(
        self, task_id: str, resource_reference: str, resource_uid: str
    ) -> None:
        self._validate_workload_reference(task_id, resource_reference)
        _require_text(resource_uid, "workload UID")
        await self._delete_owned(
            task_id,
            (_references(self._namespace, task_id).pod,),
            expected_pod_uid=resource_uid,
        )

    async def delete_ephemeral_resources(
        self,
        task_id: str,
        resource_reference: str,
        resource_uid: str,
        result_volume_uid: str,
    ) -> None:
        self._validate_workload_reference(task_id, resource_reference)
        _require_text(resource_uid, "workload UID")
        _require_text(result_volume_uid, "result volume UID")
        references = _references(self._namespace, task_id)
        await self._delete_owned(
            task_id, references.cleanup_order, expected_pod_uid=resource_uid
        )

        for reference in references.ephemeral:
            if await self._client.observe(reference) is not None:
                raise KubernetesLifecycleError(
                    f"ephemeral resource still exists: {reference.kind}"
                )

        result_volume = await self._client.observe(references.result_volume)
        if result_volume is None:
            raise ResultVolumeMissingError("result volume is missing after cleanup")
        if result_volume.reference != references.result_volume:
            raise KubernetesIdentityError("observed result reference does not match")
        _validate_ownership(result_volume, task_id)
        if result_volume.uid != result_volume_uid:
            raise KubernetesIdentityError("observed result volume UID does not match")
        if result_volume.deletion_timestamp is not None:
            raise ResultVolumeMissingError("result volume deletion is in progress")

    async def _delete_owned(
        self,
        task_id: str,
        references: tuple[ObjectReference, ...],
        *,
        expected_pod_uid: str | None = None,
    ) -> None:
        observed_resources: list[ObservedObject | None] = []
        for reference in references:
            observed = await self._client.observe(reference)
            if observed is not None:
                if observed.reference != reference:
                    raise KubernetesIdentityError(
                        "observed resource reference does not match"
                    )
                _validate_ownership(observed, task_id)
                if reference.kind == "Pod" and observed.uid != expected_pod_uid:
                    raise KubernetesIdentityError(
                        "observed workload UID does not match"
                    )
            observed_resources.append(observed)

        for reference, observed in zip(references, observed_resources, strict=True):
            if observed is None:
                continue
            await self._client.delete_if_uid(reference, observed.uid)
            await self._client.wait_uid_absent(reference, observed.uid)

            replacement = await self._client.observe(reference)
            if replacement is not None:
                if replacement.uid == observed.uid:
                    raise KubernetesLifecycleError(
                        f"resource still exists after deletion: {reference.kind}"
                    )
                raise KubernetesIdentityError(
                    f"resource was replaced during cleanup: {reference.kind}"
                )

    def _validate_workload_reference(
        self, task_id: str, resource_reference: str
    ) -> None:
        expected = _pod_resource_reference(self._namespace, task_id)
        if resource_reference != expected:
            raise KubernetesIdentityError(
                "workload reference does not match task and namespace"
            )


class KubernetesWorkloadObserver:
    def __init__(self, namespace: str, client: KubernetesLifecycleClient) -> None:
        _require_text(namespace, "namespace")
        self._namespace = namespace
        self._client = client

    async def workload_exists(
        self,
        task_id: str,
        resource_reference: str,
        resource_uid: str,
    ) -> bool:
        expected = _pod_resource_reference(self._namespace, task_id)
        if resource_reference != expected:
            raise KubernetesIdentityError(
                "workload reference does not match task and namespace"
            )
        _require_text(resource_uid, "expected workload UID")
        expected_reference = _references(self._namespace, task_id).pod
        observed = await self._client.observe(expected_reference)
        if observed is None:
            return False
        if observed.reference != expected_reference:
            raise KubernetesIdentityError("observed workload reference does not match")
        _validate_ownership(observed, task_id)
        if observed.uid != resource_uid:
            raise KubernetesIdentityError("observed workload UID does not match")
        return observed.deletion_timestamp is None


@dataclass(frozen=True, slots=True)
class _SandboxReferences:
    pod: ObjectReference
    default_deny_policy: ObjectReference
    result_volume: ObjectReference

    @property
    def ephemeral(self) -> tuple[ObjectReference, ...]:
        return (self.pod, self.default_deny_policy)

    @property
    def cleanup_order(self) -> tuple[ObjectReference, ...]:
        return (self.pod, self.default_deny_policy)


def _references(namespace: str, task_id: str) -> _SandboxReferences:
    name = sandbox_resource_name(task_id)
    return _SandboxReferences(
        pod=ObjectReference("v1", "Pod", namespace, name),
        default_deny_policy=ObjectReference(
            "networking.k8s.io/v1", "NetworkPolicy", namespace, f"{name}-deny"
        ),
        result_volume=ObjectReference("v1", "PersistentVolumeClaim", namespace, name),
    )


def _pod_resource_reference(namespace: str, task_id: str) -> str:
    return f"pod/{namespace}/{sandbox_resource_name(task_id)}"


def _validate_ownership(observed: ObservedObject, task_id: str) -> None:
    reference = observed.reference
    expected_name = sandbox_resource_name(task_id)
    expected_names = {expected_name, f"{expected_name}-deny"}
    task_hash = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:32]
    if (
        reference.name not in expected_names
        or observed.annotations.get(TASK_ANNOTATION) != task_id
        or observed.labels.get("app.kubernetes.io/instance") != expected_name
        or observed.labels.get("app.kubernetes.io/managed-by") != "sandbox-manager"
        or observed.labels.get("sandbox-manager.io/task-hash") != task_hash
    ):
        raise KubernetesIdentityError(
            f"resource ownership does not match task: {reference.kind}"
        )


def _require_text(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{field_name} must be a non-empty string")
