import time
import unittest

import ray
from ray_admission import queue
from gcs_test_support import configure_storage


class FaultController(queue._QueueController):
    def lose_next_commit_reply(self):
        original = self._store.commit

        def commit(changes):
            original(changes)
            raise OSError("durable write, reply lost")

        self._store.commit = commit


class BeginRecoveryRpcTest(unittest.TestCase):
    def test_begin_submit_write_failure_retries_across_rpc(self):
        ray.init(num_cpus=1, include_dashboard=False)
        try:
            configure_storage()
            controller = ray.remote(FaultController).remote("digest")
            request = {
                "kind": "task",
                "name": "work",
                "job_id": "job",
                "resources": {"CPU": 1},
            }
            ray.get(controller.enqueue.remote(request, "ticket"))
            deadline = time.monotonic() + 10
            while ray.get(controller.inspect.remote("ticket"))["state"] != "RESERVED":
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.01)
            ray.get(controller.lose_next_commit_reply.remote())
            self.assertEqual(queue._begin_submit(controller, "ticket"), "SUBMITTING")
            state = ray.get(controller.status.remote())
            self.assertEqual(state["usage"][queue._NAMESPACE]["job"], {"CPU": 1})
            self.assertGreater(state["health"]["recovery_attempts"], 0)
            ray.kill(controller)
        finally:
            ray.shutdown()
