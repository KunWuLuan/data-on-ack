import asyncio
import json
import threading
import unittest
from unittest.mock import patch

from gcs_test_support import configure_storage
from ray_admission import queue
import ray
from ray_admission._storage import GcsStore


class ControllerFaultTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1, include_dashboard=False)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    async def asyncSetUp(self):
        configure_storage()
        self.controllers = []
        self.controller = self.restore()
        self.request = {
            "kind": "actor_creation",
            "name": "Service",
            "job_id": "job",
            "resources": {"CPU": 1},
        }

    def restore(self):
        c = queue._QueueController("digest")
        self.controllers.append(c)
        return c

    async def reserve(self, controller, ticket, request=None):
        request = request or self.request
        result = await controller.enqueue(request, ticket)
        for _ in range(100):
            state = controller.inspect(ticket)["state"]
            if state == "RESERVED":
                return True
            if controller._recovery_state != "READY":
                raise RuntimeError("Queue storage unavailable; recovery started")
            await asyncio.sleep(0.001)
        self.fail(f"ticket did not become RESERVED: {result}")

    async def asyncTearDown(self):
        tasks = []
        for c in self.controllers:
            for task in (c._monitor, c._drainer, c._writer.task):
                if task:
                    task.cancel()
                    tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def test_duplicate_actor_tracking_keeps_permit(self):
        c = self.controller
        await self.reserve(c, "ticket")
        with patch.object(c, "_start_monitor"):
            await c.track_actor("ticket", "actor-id")
            await c.track_actor("ticket", "actor-id")
            with self.assertRaises(ValueError):
                await c.track_actor("ticket", "different")
        self.assertEqual(c._usage[(queue._NAMESPACE, "job")]["CPU"], 1)

    async def test_released_ticket_cannot_be_granted_again_after_restart(self):
        c = self.controller
        await self.reserve(c, "ticket")
        await c.release("ticket")
        restored = self.restore()
        self.assertEqual(
            (await restored.enqueue(self.request, "ticket"))["state"], "TERMINAL"
        )
        await restored.release("ticket")
        self.assertFalse(restored._permits)

    async def test_ambiguous_grant_write_stops_admission_until_restore(self):
        c = self.controller
        original = c._store.commit

        def write_then_fail(changes):
            original(changes)
            raise OSError("response lost after commit")

        with patch.object(c._store, "commit", side_effect=write_then_fail):
            with self.assertRaisesRegex(RuntimeError, "recovery started"):
                await self.reserve(c, "ticket")
            with self.assertRaises(RuntimeError):
                await self.reserve(c, "second")
        await c._recovery
        self.assertEqual(c._recovery_state, "READY")
        self.assertEqual(c._usage[(queue._NAMESPACE, "job")]["CPU"], 1)

    async def test_failed_release_retains_durable_reservation(self):
        c = self.controller
        await self.reserve(c, "ticket")
        with patch.object(c._store, "commit", side_effect=OSError("write rejected")):
            with self.assertRaises(RuntimeError):
                await c.release("ticket")
        self.assertIn("ticket", self.restore()._permits)

    async def test_duplicate_waiters_share_one_reservation(self):
        c = self.controller
        c._quotas[(queue._NAMESPACE, "job")] = {"CPU": 0}
        first = await c.enqueue(self.request, "ticket")
        second = await c.enqueue(self.request, "ticket")
        await asyncio.sleep(0.02)
        self.assertEqual(len(c._waiting[(queue._NAMESPACE, "job")]), 1)
        c._quotas.clear()
        c._schedule_drain()
        await self.reserve(c, "ticket")
        self.assertEqual([first["state"], second["state"]], ["WAITING", "WAITING"])
        self.assertEqual(c._usage[(queue._NAMESPACE, "job")]["CPU"], 1)

    async def test_waiting_cancel_survives_restart(self):
        c = self.controller
        c._quotas[(queue._NAMESPACE, "job")] = {"CPU": 0}
        await c.enqueue(self.request, "cancelled")
        await asyncio.sleep(0.02)
        self.assertIsNone(c._drainer)
        self.assertEqual(await c.cancel("cancelled"), "TERMINAL")
        restored = self.restore()
        self.assertEqual(
            (await restored.enqueue(self.request, "cancelled"))["state"], "TERMINAL"
        )

    async def test_tracking_retry_after_completed_release(self):
        c = self.controller
        await self.reserve(c, "completed")
        with patch.object(c, "_start_monitor"):
            await c.track_actor("completed", "actor-id")
        await c.release("completed")
        self.assertEqual(
            await c.track_actor("completed", "actor-id"), "TRACKED_COMPLETED"
        )
        with self.assertRaises(ValueError):
            await c.track_actor("completed", "another-id")

    async def test_duplicate_actor_tracking_retry_preserves_original_charge(self):
        c = self.controller
        await self.reserve(c, "original")
        await self.reserve(c, "duplicate")
        with patch.object(c, "_start_monitor"):
            await c.track_actor("original", "actor-id")
            await c.track_actor("duplicate", "actor-id")
            self.assertEqual(
                await c.track_actor("duplicate", "actor-id"), "TRACKED_COMPLETED"
            )
        self.assertEqual(c._usage[(queue._NAMESPACE, "job")]["CPU"], 1)

    async def test_pre_journal_failure_preserves_waiter(self):
        c = self.controller
        original = c._store.commit
        failed = False

        def fail_once(changes):
            nonlocal failed
            if not failed:
                failed = True
                raise OSError("before journal")
            original(changes)

        with patch.object(c._store, "commit", side_effect=fail_once):
            await c.enqueue(self.request, "recover-waiter")
            for _ in range(300):
                if c.inspect("recover-waiter")["state"] == "RESERVED":
                    break
                await asyncio.sleep(0.01)
        self.assertEqual(c.inspect("recover-waiter")["state"], "RESERVED")
        self.assertNotIn("recover-waiter", c._waiting_tickets)

    async def test_cancel_racing_begin_submit_never_releases_submitting_work(self):
        c = self.controller
        await self.reserve(c, "ticket")
        begin, cancel = await asyncio.gather(
            c.begin_submit("ticket"), c.cancel("ticket"), return_exceptions=True
        )
        state = c.inspect("ticket")["state"]
        self.assertIn(state, ("SUBMITTING", "TERMINAL"))
        if state == "SUBMITTING":
            self.assertEqual(cancel, "SUBMISSION_UNKNOWN")
            self.assertEqual(c._usage[(queue._NAMESPACE, "job")]["CPU"], 1)
        else:
            self.assertIsInstance(begin, ValueError)
            self.assertEqual(c._usage[(queue._NAMESPACE, "job")]["CPU"], 0)

    async def test_monitor_retries_transient_query_failure(self):
        c = self.controller
        c._actors["ticket"] = "actor-id"
        calls = 0

        async def poll():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("temporary GCS failure")
            c._actors.clear()

        with patch.object(c, "_poll_completions", side_effect=poll):
            await asyncio.wait_for(c._watch_completions(), timeout=2)
        self.assertEqual(calls, 2)
        self.assertIsNone(c._monitor_error)

    async def test_partial_batch_replayed_before_recovery(self):
        store = self.controller._store
        original = store._put

        def fail_second(key, value, **kwargs):
            if key == store.PREFIX + b"permit/two":
                raise OSError("interrupted record batch")
            original(key, value, **kwargs)

        with patch.object(store, "_put", side_effect=fail_second):
            with self.assertRaises(OSError):
                store.commit(
                    [
                        ("permit/one", {"state": "RELEASED"}),
                        ("permit/two", {"state": "RELEASED"}),
                    ]
                )
        restored = GcsStore("digest")
        self.assertEqual(set(restored.load()), {"permit/one", "permit/two"})
        self.assertIsNone(restored._get(b"pending-v2"))

    async def test_legacy_migration_checks_token_and_preserves_records(self):
        store = self.controller._store
        store._delete(b"auth-v2")
        store._put(
            b"controller-state-v1",
            json.dumps(
                {
                    "version": 1,
                    "admin_token_digest": "digest",
                    "quotas": [["team", "job", {"CPU": 2}]],
                    "permits": [
                        {
                            "ticket": "old",
                            "quota_namespace": "team",
                            "job_id": "job",
                            "resources": {"CPU": 1},
                        }
                    ],
                    "released": ["done"],
                }
            ).encode(),
        )
        with self.assertRaises(PermissionError):
            GcsStore("wrong")
        self.assertIsNone(store._get(b"auth-v2"))
        restored = GcsStore("digest")
        records = restored.load()
        self.assertEqual(records["permit/old"]["state"], "RESERVED")
        self.assertEqual(records["permit/done"]["state"], "RELEASED")
        self.assertEqual(len(records), 3)
        restored.commit([("permit/old", None)])
        self.assertNotIn("permit/old", GcsStore("digest").load())

    async def test_cancelled_write_requires_recovery_and_does_not_block_loop(self):
        c = self.controller
        await self.reserve(c, "ticket")
        entered = threading.Event()
        resume = threading.Event()
        original = c._store.commit

        def slow_write(changes):
            entered.set()
            if not resume.wait(timeout=5):
                raise TimeoutError("test did not unblock writer")
            original(changes)

        with patch.object(c._store, "commit", side_effect=slow_write):
            task = asyncio.create_task(c.track_actor("ticket", "actor-id"))
            while not entered.is_set():
                await asyncio.sleep(0.001)
            # The event loop is responsive while storage I/O is blocked.
            self.assertFalse(task.done())
            task.cancel()
            try:
                with self.assertRaises(RuntimeError):
                    await task
            finally:
                resume.set()
            await c._writer.requests.join()
        self.assertIsNotNone(c._storage_error)
        with patch.object(queue._QueueController, "_start_monitor"):
            restored = self.restore()
        self.assertEqual(restored._actors["ticket"], "actor-id")

    async def test_compaction_resumes_after_watermark_commit(self):
        c = self.controller
        await c.open_session("abc")
        await self.reserve(c, "abc:1")
        await c.release("abc:1")
        original = c._store.commit

        def fail_deletion(changes):
            if any(value is None for _, value in changes):
                raise OSError("cleanup unavailable")
            original(changes)

        with patch.object(c._store, "commit", side_effect=fail_deletion):
            with self.assertRaises(RuntimeError):
                await c.acknowledge("abc", 1)
        restored = self.restore()
        self.assertEqual(
            (await restored.enqueue(self.request, "abc:1"))["state"], "TERMINAL"
        )
        async with restored._lock:
            await restored._compact_session_locked("abc")
        self.assertNotIn("permit/abc:1", restored._store.load())

    async def test_ack_compacts_tombstones_without_resurrecting_tickets(self):
        c = self.controller
        await c.open_session("abc")
        await self.reserve(c, "abc:1")
        await self.reserve(c, "abc:2")
        await c.release("abc:2")
        # An out-of-order completion cannot acknowledge the missing first one.
        await c.acknowledge("abc", 0)
        self.assertIn("abc:2", c._released)
        await c.acknowledge("abc", 2)
        self.assertNotIn("abc:2", c._released)
        self.assertIn("abc:1", c._permits)
        await c.release("abc:1")
        restored = self.restore()
        self.assertFalse(restored._permits)
        self.assertFalse(restored._released)
        self.assertEqual(
            (await restored.enqueue(self.request, "abc:2"))["state"], "TERMINAL"
        )

    async def test_batched_grants_and_constant_transition_write_size(self):
        c = self.controller
        c._quotas[(queue._NAMESPACE, "job")] = {"CPU": 0}
        replies = [await c.enqueue(self.request, f"batch-{i:04d}") for i in range(96)]
        self.assertTrue(all(reply["state"] == "WAITING" for reply in replies))
        await asyncio.sleep(0.02)
        before = c._store.batches
        c._quotas.clear()
        c._schedule_drain()
        while any(
            c.inspect(f"batch-{i:04d}")["state"] != "RESERVED" for i in range(96)
        ):
            await asyncio.sleep(0.001)
        self.assertEqual(c._store.batches - before, 3)
        before = c._store.bytes_written
        await self.reserve(c, "one-record")
        size = c._store.bytes_written - before
        self.assertLess(size, 1500)
        # Active-set size does not appear in the batch payload.
        self.assertEqual(len(c._permits), 97)
