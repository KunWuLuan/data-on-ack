"""Real CLI processes, GCS KV, and intercepted Ray submissions; no RPC mocks."""

import asyncio
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import ray
from ray_admission.queue import disable_admission, enable_admission


@ray.remote(num_cpus=0)
class Gate:
    def __init__(self):
        self.events = {}
        self.entered = []

    async def wait(self, token):
        self.entered.append(token)
        await self.events.setdefault(token, asyncio.Event()).wait()
        return token

    def open(self, token):
        self.events.setdefault(token, asyncio.Event()).set()

    def entries(self):
        return self.entered


@ray.remote(num_cpus=1)
def work(gate, token):
    return ray.get(gate.wait.remote(token))


@ray.remote(num_cpus=1)
class Service:
    def ready(self):
        return True


class QueueEndToEndTest(unittest.TestCase):
    def setUp(self):
        # A fresh local cluster per case: never clear a user's GCS namespace.
        self.context = ray.init(num_cpus=3, include_dashboard=False)
        self.addCleanup(ray.shutdown)
        self.addCleanup(disable_admission)
        self.address = self.context.address_info["gcs_address"]
        self.job = ray.get_runtime_context().get_job_id()
        self.token = secrets.token_urlsafe(32)
        self.env = {
            **os.environ,
            "RAY_ADMISSION_ADMIN_TOKEN": self.token,
            "RAY_NAMESPACE": "e2e-team",
        }
        self.namespace = patch.dict(os.environ, {"RAY_NAMESPACE": "e2e-team"})
        self.namespace.start()
        self.addCleanup(self.namespace.stop)
        self.controller = None
        self.gate = Gate.remote()
        self.pool = ThreadPoolExecutor(max_workers=2)
        self.addCleanup(self.cleanup_work)

    def cli(self, *args, token=None, success=True):
        executable = Path(sys.executable).parent / "ray-admission"
        env = dict(self.env)
        if token is not None:
            env["RAY_ADMISSION_ADMIN_TOKEN"] = token
        result = subprocess.run(
            [str(executable), *args],
            env=env,
            text=True,
            capture_output=True,
            timeout=45,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def command(self, *args, **kwargs):
        return self.cli("queue", "--address", self.address, *args, **kwargs)

    def start(self):
        result = json.loads(self.command("start").stdout)
        self.assertEqual(
            result,
            {"name": "ray-submission-queue", "namespace": "ray-submission-queue"},
        )
        self.controller = enable_admission()

    def quota(self, cpu):
        self.command("quota", "set", self.job, json.dumps({"CPU": cpu}))

    def state(self):
        return ray.get(self.controller.status.remote(), timeout=30)

    def usage(self):
        return self.state()["usage"].get("e2e-team", {}).get(self.job, {}).get("CPU", 0)

    def wait_for(self, predicate):
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail("Timed out; controller status: " + repr(self.state()))

    def cleanup_work(self):
        # Open barriers and remove quota before joining submission threads.
        disable_admission()
        for token in ("first", "second"):
            ray.get(self.gate.open.remote(token), timeout=10)
        if self.controller is not None:
            try:
                ray.get(
                    self.controller.set_job_quota.remote(
                        self.job,
                        None,
                        quota_namespace="e2e-team",
                        admin_token=self.token,
                    ),
                    timeout=15,
                )
            finally:
                self.pool.shutdown(wait=False, cancel_futures=True)
                ray.kill(self.controller)
        else:
            self.pool.shutdown(wait=False, cancel_futures=True)

    def test_cli_start_identity_authentication_and_discovery(self):
        self.assertIn("queue\t", self.cli("list").stdout)
        with self.assertRaisesRegex(RuntimeError, "not running"):
            enable_admission()
        self.start()
        original = self.controller._ray_actor_id
        self.command("start")
        self.assertEqual(enable_admission()._ray_actor_id, original)
        self.quota(1)
        self.command("start", token="wrong", success=False)
        self.command(
            "quota", "set", self.job, '{"CPU": 9}', token="wrong", success=False
        )
        self.command("quota", "delete", self.job, token="wrong", success=False)
        status = json.loads(self.command("status").stdout)
        self.assertEqual(status["quotas"], {"e2e-team": {self.job: {"CPU": 1}}})
        self.assertEqual(status["storage"]["backend"], "gcs-kv")

    def test_remote_waits_for_quota_updates_and_deletion(self):
        self.start()
        self.quota(0)
        first = self.pool.submit(work.remote, self.gate, "first")
        self.wait_for(lambda: len(self.state()["waiting"]) == 1)
        self.assertFalse(first.done())
        self.assertEqual(ray.get(self.gate.entries.remote()), [])
        self.quota(1)
        first_ref = first.result(timeout=30)
        self.wait_for(lambda: self.usage() == 1)
        self.quota(0)  # Lowering quota must not preempt the running task.
        self.assertEqual(self.usage(), 1)
        second = self.pool.submit(work.remote, self.gate, "second")
        self.wait_for(lambda: len(self.state()["waiting"]) == 1)
        self.assertFalse(second.done())
        self.command("quota", "delete", self.job)
        second_ref = second.result(timeout=30)
        self.wait_for(lambda: self.usage() == 2)
        for token in ("first", "second"):
            ray.get(self.gate.open.remote(token))
        self.assertEqual(
            ray.get([first_ref, second_ref], timeout=30), ["first", "second"]
        )
        self.wait_for(lambda: self.usage() == 0)

    def test_running_task_and_waiter_survive_controller_restart(self):
        self.start()
        self.quota(1)
        first = work.remote(self.gate, "first")
        self.wait_for(lambda: ray.get(self.gate.entries.remote()) == ["first"])
        second = self.pool.submit(work.remote, self.gate, "second")
        self.wait_for(lambda: len(self.state()["waiting"]) == 1)
        ray.kill(self.controller, no_restart=False)
        # RPC retries wait for reconstruction; inspect the recovered service.
        self.wait_for(lambda: len(self.state()["waiting"]) == 1)
        self.assertEqual(self.usage(), 1)
        self.assertEqual(self.state()["quotas"]["e2e-team"][self.job], {"CPU": 1})
        self.assertFalse(second.done())
        self.assertEqual(ray.get(self.gate.entries.remote()), ["first"])
        ray.get(self.gate.open.remote("first"))
        self.assertEqual(ray.get(first, timeout=30), "first")
        second_ref = second.result(timeout=30)
        self.wait_for(
            lambda: ray.get(self.gate.entries.remote()) == ["first", "second"]
        )
        self.assertEqual(self.usage(), 1)
        ray.get(self.gate.open.remote("second"))
        self.assertEqual(ray.get(second_ref, timeout=30), "second")
        self.wait_for(lambda: self.usage() == 0)

    def test_actor_dies_during_controller_downtime(self):
        self.start()
        self.quota(1)
        actor = Service.remote()
        self.assertTrue(ray.get(actor.ready.remote(), timeout=30))
        self.assertEqual(self.usage(), 1)
        disable_admission()
        ray.kill(self.controller, no_restart=True)
        self.controller = None

        def absent():
            try:
                ray.get_actor("ray-submission-queue", namespace="ray-submission-queue")
                return False
            except ValueError:
                return True

        self.wait_for(absent)
        ray.kill(actor)
        self.start()  # A new CLI process restores persisted GCS records.
        self.wait_for(lambda: self.usage() == 0)
        self.assertEqual(self.state()["quotas"]["e2e-team"][self.job], {"CPU": 1})
        replacement = Service.remote()
        self.assertTrue(ray.get(replacement.ready.remote(), timeout=30))
        self.assertEqual(self.usage(), 1)
        ray.kill(replacement)
        self.wait_for(lambda: self.usage() == 0)


if __name__ == "__main__":
    unittest.main()
