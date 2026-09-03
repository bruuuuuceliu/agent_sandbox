"""Kubernetes resource construction for sandbox workloads."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from .provisioning import ProvisionedSandbox, ProvisioningTask

Manifest = dict[str, object]

CPU_REQUEST = "250m"
MEMORY_REQUEST = "256Mi"
CPU_LIMIT = "1"
MEMORY_LIMIT = "512Mi"

_DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_DNS_SUBDOMAIN_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
_STORAGE_QUANTITY_PATTERN = re.compile(r"^[1-9][0-9]*(?:Ki|Mi|Gi|Ti)$")
_IMAGE_DIGEST_PATTERN = re.compile(r"^[^@\s]+@sha256:[a-f0-9]{64}$")


class KubernetesResourceError(ValueError):
    """Raised when a workload cannot produce valid Kubernetes resources."""


@dataclass(frozen=True, slots=True)
class SandboxWorkload:
    namespace: str
    task_id: str
    image: str
    command: tuple[str, ...]
    runtime_class_name: str
    storage_class_name: str
    result_storage: str
    results_mount_path: str

    def __post_init__(self) -> None:
        _validate_dns_label(self.namespace, "namespace")
        _validate_task_id(self.task_id)
        _validate_dns_subdomain(self.runtime_class_name, "runtime class")
        _validate_dns_subdomain(self.storage_class_name, "storage class")
        if not _IMAGE_DIGEST_PATTERN.fullmatch(self.image):
            raise KubernetesResourceError("image must use a lowercase sha256 digest")
        if not isinstance(self.command, tuple):
            raise KubernetesResourceError("command must be a tuple")
        command = tuple(self.command)
        if not command or any(not _valid_text(argument) for argument in command):
            raise KubernetesResourceError("command must contain valid arguments")
        object.__setattr__(self, "command", command)
        if not _STORAGE_QUANTITY_PATTERN.fullmatch(self.result_storage):
            raise KubernetesResourceError(
                "result storage must be a positive binary quantity"
            )
        if not _valid_absolute_path(self.results_mount_path):
            raise KubernetesResourceError("results mount path must be absolute")


@dataclass(frozen=True, slots=True)
class SandboxResources:
    persistent_volume_claim: Manifest
    default_deny_network_policy: Manifest
    pod: Manifest

    def apply_order(self) -> tuple[Manifest, ...]:
        """Return resources with policy and dependencies before the workload."""

        return (
            self.persistent_volume_claim,
            self.default_deny_network_policy,
            self.pod,
        )


class KubernetesResourceApplier(Protocol):
    async def apply(self, resource: Mapping[str, object]) -> str:
        """Create or converge one resource and return its immutable UID."""

        ...


class KubernetesSandboxClient(KubernetesResourceApplier, Protocol):
    async def wait_pod_ready(self, namespace: str, name: str) -> None:
        """Wait until the named Pod reports ready or cannot become ready."""

        ...

    async def delete(
        self, api_version: str, kind: str, namespace: str, name: str
    ) -> None:
        """Delete a resource if present."""

        ...


@dataclass(frozen=True, slots=True)
class KubernetesProvisionerConfig:
    namespace: str
    runtime_class_name: str
    storage_class_name: str
    result_storage: str
    readiness_timeout: timedelta

    def __post_init__(self) -> None:
        _validate_dns_label(self.namespace, "namespace")
        _validate_dns_subdomain(self.runtime_class_name, "runtime class")
        _validate_dns_subdomain(self.storage_class_name, "storage class")
        if not _STORAGE_QUANTITY_PATTERN.fullmatch(self.result_storage):
            raise KubernetesResourceError(
                "result storage must be a positive binary quantity"
            )
        if self.readiness_timeout <= timedelta(0):
            raise KubernetesResourceError("readiness timeout must be positive")


class KubernetesSandboxProvisioner:
    def __init__(
        self,
        config: KubernetesProvisionerConfig,
        client: KubernetesSandboxClient,
    ) -> None:
        self._config = config
        self._client = client

    async def provision(self, task: ProvisioningTask) -> ProvisionedSandbox:
        profile = task.profile
        resources = build_sandbox_resources(
            SandboxWorkload(
                namespace=self._config.namespace,
                task_id=task.task_id,
                image=profile.image,
                command=profile.command,
                runtime_class_name=self._config.runtime_class_name,
                storage_class_name=self._config.storage_class_name,
                result_storage=self._config.result_storage,
                results_mount_path=profile.results_mount_path,
            )
        )
        applied = await apply_sandbox_resources(self._client, resources)
        name = sandbox_resource_name(task.task_id)
        return ProvisionedSandbox(
            resource_reference=_reference("pod", self._config.namespace, name),
            resource_uid=applied.pod_uid,
            result_volume_reference=_reference(
                "persistentvolumeclaim", self._config.namespace, name
            ),
            result_volume_uid=applied.result_volume_uid,
        )

    async def wait_ready(
        self, task: ProvisioningTask, resources: ProvisionedSandbox
    ) -> None:
        name = sandbox_resource_name(task.task_id)
        if resources.resource_reference != _reference(
            "pod", self._config.namespace, name
        ) or resources.result_volume_reference != _reference(
            "persistentvolumeclaim", self._config.namespace, name
        ):
            raise KubernetesResourceError("resource references do not match task")

        async with asyncio.timeout(self._config.readiness_timeout.total_seconds()):
            await self._client.wait_pod_ready(self._config.namespace, name)

    async def rollback(self, task_id: str) -> None:
        name = sandbox_resource_name(task_id)
        for api_version, kind, resource_name in (
            ("v1", "Pod", name),
            ("networking.k8s.io/v1", "NetworkPolicy", f"{name}-deny"),
        ):
            await self._client.delete(
                api_version, kind, self._config.namespace, resource_name
            )


@dataclass(frozen=True, slots=True)
class AppliedSandboxResources:
    pod_uid: str
    result_volume_uid: str


async def apply_sandbox_resources(
    applier: KubernetesResourceApplier, resources: SandboxResources
) -> AppliedSandboxResources:
    """Converge a sandbox resource set in startup-safe order."""

    applied_uids: list[str] = []
    for resource in resources.apply_order():
        uid = await applier.apply(resource)
        if not _valid_text(uid):
            raise KubernetesResourceError("resource UID must be a non-empty string")
        applied_uids.append(uid)
    return AppliedSandboxResources(
        pod_uid=applied_uids[-1],
        result_volume_uid=applied_uids[0],
    )


def build_sandbox_resources(workload: SandboxWorkload) -> SandboxResources:
    """Build the complete resource set for one sandbox workload."""

    name = sandbox_resource_name(workload.task_id)
    labels = {
        "app.kubernetes.io/name": "sandbox",
        "app.kubernetes.io/instance": name,
        "app.kubernetes.io/managed-by": "sandbox-manager",
        "sandbox-manager.io/task-hash": hashlib.sha256(
            workload.task_id.encode("utf-8")
        ).hexdigest()[:32],
    }
    pod_selector = {"app.kubernetes.io/instance": name}

    persistent_volume_claim: Manifest = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": _metadata(
            name,
            workload,
            labels,
            {"sandbox-manager.io/persistence": "retained"},
        ),
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": workload.storage_class_name,
            "resources": {"requests": {"storage": workload.result_storage}},
        },
    }
    default_deny_network_policy: Manifest = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": _metadata(f"{name}-deny", workload, labels),
        "spec": {
            "podSelector": {"matchLabels": pod_selector},
            "policyTypes": ["Ingress", "Egress"],
        },
    }
    pod: Manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": _metadata(name, workload, labels),
        "spec": {
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "hostIPC": False,
            "hostNetwork": False,
            "hostPID": False,
            "restartPolicy": "Never",
            "runtimeClassName": workload.runtime_class_name,
            "securityContext": {
                "fsGroup": 10_000,
                "fsGroupChangePolicy": "OnRootMismatch",
                "runAsGroup": 10_000,
                "runAsNonRoot": True,
                "runAsUser": 10_000,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "sandbox",
                    "image": workload.image,
                    "command": list(workload.command),
                    "resources": {
                        "requests": {
                            "cpu": CPU_REQUEST,
                            "memory": MEMORY_REQUEST,
                        },
                        "limits": {"cpu": CPU_LIMIT, "memory": MEMORY_LIMIT},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "privileged": False,
                        "readOnlyRootFilesystem": True,
                        "runAsNonRoot": True,
                    },
                    "volumeMounts": [
                        {
                            "mountPath": workload.results_mount_path,
                            "name": "results",
                        },
                    ],
                }
            ],
            "volumes": [
                {
                    "name": "results",
                    "persistentVolumeClaim": {"claimName": name},
                },
            ],
        },
    }

    return SandboxResources(
        persistent_volume_claim=persistent_volume_claim,
        default_deny_network_policy=default_deny_network_policy,
        pod=pod,
    )


def sandbox_resource_name(task_id: str) -> str:
    """Return the deterministic Kubernetes name for a task."""

    _validate_task_id(task_id)
    slug = re.sub(r"[^a-z0-9]+", "-", task_id.lower()).strip("-")
    slug = slug[:32].rstrip("-") or "task"
    digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
    return f"sbx-{slug}-{digest}"


def _reference(kind: str, namespace: str, name: str) -> str:
    return f"{kind}/{namespace}/{name}"


def _metadata(
    name: str,
    workload: SandboxWorkload,
    labels: Mapping[str, str],
    annotations: Mapping[str, str] | None = None,
) -> dict[str, object]:
    resource_annotations = {"sandbox-manager.io/task-id": workload.task_id}
    if annotations is not None:
        resource_annotations.update(annotations)
    return {
        "name": name,
        "namespace": workload.namespace,
        "labels": dict(labels),
        "annotations": resource_annotations,
    }


def _validate_task_id(value: str) -> None:
    if (
        not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 128
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise KubernetesResourceError("task ID is invalid")


def _validate_dns_label(value: str, description: str) -> None:
    if len(value) > 63 or _DNS_LABEL_PATTERN.fullmatch(value) is None:
        raise KubernetesResourceError(
            f"{description} must be a valid Kubernetes DNS label"
        )


def _validate_dns_subdomain(value: str, description: str) -> None:
    if not _valid_dns_subdomain(value):
        raise KubernetesResourceError(
            f"{description} must be a valid Kubernetes DNS subdomain"
        )


def _valid_text(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _valid_absolute_path(value: str) -> bool:
    return _valid_text(value) and value.startswith("/")


def _valid_dns_subdomain(value: str) -> bool:
    return (
        bool(value)
        and len(value) <= 253
        and _DNS_SUBDOMAIN_PATTERN.fullmatch(value) is not None
        and all(
            len(label) <= 63 and _DNS_LABEL_PATTERN.fullmatch(label) is not None
            for label in value.split(".")
        )
    )
