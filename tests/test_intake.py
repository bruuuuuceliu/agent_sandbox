from __future__ import annotations

import json
import unittest

from sandbox_manager.intake import (
    IdentifierConflictError,
    IntakeResult,
    QueuedTask,
    SpawnIntakeProcessor,
    StoreResult,
)
from sandbox_manager.spawn import SpawnRequestError


class RecordingRepository:
    def __init__(
        self,
        result: StoreResult = StoreResult.CREATED,
        error: Exception | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.events = [] if events is None else events
        self.tasks: list[QueuedTask] = []

    async def accept_spawn(self, task: QueuedTask) -> StoreResult:
        self.events.append("store")
        if self.error is not None:
            raise self.error
        self.tasks.append(task)
        return self.result


class RecordingDelivery:
    def __init__(
        self,
        payload: bytes,
        events: list[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._payload = payload
        self.events = [] if events is None else events
        self.error = error
        self.acknowledgements = 0

    @property
    def payload(self) -> bytes:
        return self._payload

    async def acknowledge(self) -> None:
        self.events.append("acknowledge")
        if self.error is not None:
            raise self.error
        self.acknowledgements += 1


def valid_payload(**overrides: object) -> bytes:
    values: dict[str, object] = {
        "schema_version": 1,
        "request_id": "request-1",
        "task_id": "task-1",
        "agent_session_id": "session-1",
        "sandbox_type": "python",
    }
    values.update(overrides)
    return json.dumps(values).encode()


class SpawnIntakeProcessorTests(unittest.IsolatedAsyncioTestCase):
    async def test_persists_before_acknowledging(self) -> None:
        events: list[str] = []
        repository = RecordingRepository(events=events)
        delivery = RecordingDelivery(valid_payload(), events)

        result = await SpawnIntakeProcessor(repository).process(delivery)

        self.assertEqual(result, IntakeResult.CREATED)
        self.assertEqual(events, ["store", "acknowledge"])
        self.assertEqual(len(repository.tasks), 1)
        self.assertEqual(repository.tasks[0].state, "QUEUED")

    async def test_acknowledges_matching_duplicate(self) -> None:
        repository = RecordingRepository(StoreResult.DUPLICATE)
        delivery = RecordingDelivery(valid_payload())

        result = await SpawnIntakeProcessor(repository).process(delivery)

        self.assertEqual(result, IntakeResult.DUPLICATE)
        self.assertEqual(delivery.acknowledgements, 1)

    async def test_does_not_acknowledge_persistence_failure(self) -> None:
        repository = RecordingRepository(error=OSError("storage unavailable"))
        delivery = RecordingDelivery(valid_payload())

        with self.assertRaisesRegex(OSError, "storage unavailable"):
            await SpawnIntakeProcessor(repository).process(delivery)

        self.assertEqual(delivery.acknowledgements, 0)

    async def test_does_not_acknowledge_identifier_conflict(self) -> None:
        repository = RecordingRepository(StoreResult.CONFLICT)
        delivery = RecordingDelivery(valid_payload())

        with self.assertRaises(IdentifierConflictError):
            await SpawnIntakeProcessor(repository).process(delivery)

        self.assertEqual(delivery.acknowledgements, 0)

    async def test_redelivery_after_acknowledgement_failure_is_duplicate(self) -> None:
        repository = RecordingRepository()
        failed_delivery = RecordingDelivery(
            valid_payload(), error=ConnectionError("acknowledgement failed")
        )

        with self.assertRaisesRegex(ConnectionError, "acknowledgement failed"):
            await SpawnIntakeProcessor(repository).process(failed_delivery)

        repository.result = StoreResult.DUPLICATE
        retry_delivery = RecordingDelivery(valid_payload())
        result = await SpawnIntakeProcessor(repository).process(retry_delivery)

        self.assertEqual(result, IntakeResult.DUPLICATE)
        self.assertEqual(failed_delivery.acknowledgements, 0)
        self.assertEqual(retry_delivery.acknowledgements, 1)

    async def test_does_not_persist_or_acknowledge_invalid_request(self) -> None:
        repository = RecordingRepository()
        delivery = RecordingDelivery(valid_payload(schema_version=2))

        with self.assertRaises(SpawnRequestError):
            await SpawnIntakeProcessor(repository).process(delivery)

        self.assertEqual(repository.tasks, [])
        self.assertEqual(delivery.acknowledgements, 0)


if __name__ == "__main__":
    unittest.main()
