import unittest
from unittest.mock import Mock, patch

from ray_admission import queue


class AckIsolationTest(unittest.TestCase):
    def test_disable_flushes_last_watermark_without_timer_retry(self):
        controller = Mock()
        controller._ray_actor_id.hex.return_value = "controller"
        with (
            patch.object(queue, "_ack_states", {}),
            patch.object(queue.threading, "Timer") as timer,
            patch.object(queue, "_originals", {}),
            patch.object(queue.ray, "get"),
        ):
            queue._ack_ticket(controller, "aaaa:1")
            queue.disable_admission()
            controller.acknowledge.remote.assert_called_once_with("aaaa", 1)
            self.assertEqual(queue._ack_states[("controller", "aaaa")]["sent"], 1)
            self.assertEqual(timer.call_count, 1)

    def setUp(self):
        enabled = patch.object(queue, "_ack_enabled", True)
        enabled.start()
        self.addCleanup(enabled.stop)

    def test_begin_submit_retries_recovery_with_same_ticket(self):
        controller = Mock()
        with (
            patch.object(
                queue.ray,
                "get",
                side_effect=[queue.ServiceRecovering("RECOVERING"), "SUBMITTING"],
            ),
            patch.object(queue.time, "sleep"),
        ):
            self.assertEqual(queue._begin_submit(controller, "aaaa:1"), "SUBMITTING")
        self.assertEqual(controller.begin_submit.remote.call_count, 2)
        self.assertTrue(
            all(
                call.args == ("aaaa:1",)
                for call in controller.begin_submit.remote.call_args_list
            )
        )

    def test_disable_prevents_inflight_flush_rescheduling(self):
        controller = Mock()
        controller._ray_actor_id.hex.return_value = "controller"
        with (
            patch.object(queue, "_ack_states", {}),
            patch.object(queue.threading, "Timer") as timer,
            patch.object(queue, "_originals", {}),
        ):
            queue._ack_ticket(controller, "aaaa:1")

            def fail_after_disable(*args, **kwargs):
                queue.disable_admission()
                raise RuntimeError("offline")

            with patch.object(queue.ray, "get", side_effect=fail_after_disable):
                queue._flush_ack(("controller", "aaaa"))
            self.assertEqual(timer.call_count, 1)
            self.assertIsNone(queue._ack_states[("controller", "aaaa")]["timer"])
            self.assertEqual(queue._ack_states[("controller", "aaaa")]["watermark"], 1)

    def test_unmetered_failure_acknowledges_ticket(self):
        controller, target = Mock(), Mock()
        target._ray_actor_id = "other"

        def original(target, *args, **kwargs):
            raise ValueError("invalid arguments")

        with (
            patch.object(queue, "_controller", controller),
            patch.object(queue, "_request", return_value={"resources": {}}),
            patch.object(queue, "_next_ticket", return_value="aaaa:1"),
            patch.object(queue, "_wait_for_reservation", return_value="UNMETERED"),
            patch.object(queue, "_ack_ticket") as ack,
        ):
            with self.assertRaisesRegex(ValueError, "invalid arguments"):
                queue._wrap(original, "actor_method")(target)
            ack.assert_called_once_with(controller, "aaaa:1")

    def test_old_session_timer_keeps_original_destination(self):
        controller = Mock()
        controller._ray_actor_id.hex.return_value = "controller"
        with (
            patch.object(queue, "_ack_states", {}),
            patch.object(queue.threading, "Timer"),
            patch.object(queue.ray, "get"),
        ):
            queue._ack_ticket(controller, "aaaa:1")
            queue._ack_ticket(controller, "bbbb:2")
            with patch.object(queue, "_identity", ("namespace", "bbbb")):
                queue._flush_ack(("controller", "aaaa"))
            controller.acknowledge.remote.assert_called_once_with("aaaa", 1)
            self.assertEqual(queue._ack_states[("controller", "bbbb")]["watermark"], 0)

    def test_overload_propagates_without_status_query(self):
        controller = Mock()
        with patch.object(
            queue.ray, "get", side_effect=RuntimeError("Queue overloaded")
        ):
            with self.assertRaisesRegex(RuntimeError, "overloaded"):
                queue._wait_for_reservation(controller, {}, "aaaa:1")
        controller.enqueue.remote.assert_called_once()
        controller.status.remote.assert_not_called()
