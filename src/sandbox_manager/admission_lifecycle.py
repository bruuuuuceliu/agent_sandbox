"""Seeding durable command admission from the provisioning lifecycle.

A sandbox with no admission state admits no commands: the durable
repository, not the relay, decides who may run what. This module bridges
the two lifecycles. When provisioning marks a task READY, the seeding
publisher records a ``TaskAdmissionState`` under the current fence
*before* the ready task is published onward, so a command can never
arrive for a task the durable store does not know about.
"""

from __future__ import annotations

from typing import Protocol

from .commands import (
    AdmissionFence,
    AdmissionLifecycle,
    TaskAdmissionState,
)
from .provisioning import ReadyPublisher, ReadyTask


class AdmissionSeedingStore(Protocol):
    async def seed_ready(self, state: TaskAdmissionState) -> None:
        """Durably seed admission for one newly ready task."""

        ...


class AdmissionSeedingPublisher:
    """``ReadyPublisher`` decorator that seeds admission before publishing.

    A seed failure fails the publication, so the orchestrator surfaces it
    through its existing ``ReadyPublicationError`` path instead of
    announcing a task that could never accept a command.
    """

    def __init__(
        self,
        store: AdmissionSeedingStore,
        fence: AdmissionFence,
        publisher: ReadyPublisher,
    ) -> None:
        self._store = store
        self._fence = fence
        self._publisher = publisher

    async def publish_ready(self, task: ReadyTask) -> None:
        await self._store.seed_ready(
            TaskAdmissionState(
                task_id=task.task_id,
                agent_session_id=(
                    task.provisioning_task.queued_task.request.agent_session_id
                ),
                profile=task.provisioning_task.profile.sandbox_type,
                lifecycle=AdmissionLifecycle.READY,
                fence_generation=self._fence.generation,
            )
        )
        # Seed before publish: once the task is visible as ready a command
        # may arrive at any moment, and it must find durable admission state.
        await self._publisher.publish_ready(task)
