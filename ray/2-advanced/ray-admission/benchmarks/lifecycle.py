"""Measure complete short-task admission lifecycle on an isolated local Ray cluster."""

import json
import os
import statistics
import time

import ray
from ray_admission import queue


@ray.remote(num_cpus=0.01)
def work(started):
    return (time.perf_counter() - started) * 1000


def percentile(values, fraction):
    return sorted(values)[min(int(len(values) * fraction), len(values) - 1)]


def main():
    count = int(os.environ.get("RAY_ADMISSION_BENCHMARK_TASKS", "1000"))
    ray.init(num_cpus=2, include_dashboard=False)
    token = "isolated-local-benchmark"
    controller = queue.start(admin_token=token)
    try:
        queue.enable_admission()
        job = ray.get_runtime_context().get_job_id()
        ray.get(controller.set_job_quota.remote(job, {"CPU": 2}, admin_token=token))
        submitted = []
        submit_ms = []
        started = time.perf_counter()
        for _ in range(count):
            before = time.perf_counter()
            submitted.append(work.remote(before))
            submit_ms.append((time.perf_counter() - before) * 1000)
        execution_ms = ray.get(submitted)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = ray.get(controller.status.remote())
            usage = state["usage"].get(queue._NAMESPACE, {}).get(job, {})
            if not any(usage.values()):
                break
            time.sleep(0.02)
        state = ray.get(controller.status.remote())
        elapsed = time.perf_counter() - started
        print(
            json.dumps(
                {
                    "tasks": count,
                    "throughput_per_second": round(count / elapsed, 2),
                    "submit_p50_ms": round(statistics.median(submit_ms), 3),
                    "submit_p95_ms": round(percentile(submit_ms, 0.95), 3),
                    "submit_p99_ms": round(percentile(submit_ms, 0.99), 3),
                    "execution_start_p50_ms": round(statistics.median(execution_ms), 3),
                    "execution_start_p95_ms": round(percentile(execution_ms, 0.95), 3),
                    "rpc_counts": state["rpc_counts"],
                    "storage": state["storage"],
                    "retained_permits": len(state["permits"]),
                    "retained_waiters": len(state["waiting"]),
                },
                indent=2,
            )
        )
    finally:
        queue.disable_admission()
        ray.kill(controller)
        ray.shutdown()


if __name__ == "__main__":
    main()
