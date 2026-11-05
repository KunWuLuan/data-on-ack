"""Submission admission prototype (uses private Ray interfaces).

An administrator first calls ``start(admin_token=...)`` to launch the queue.
Business jobs call ``enable_admission()`` to initialize Ray and take over submissions.
Each submission
waits for the named detached controller's approval, then returns Ray's native result.
The controller configures resource ceilings per job with ``set_job_quota()``.
Each operator charges its declared Ray resources, including ``.options()``
overrides. Requests are FIFO within a job; jobs do not block each other.
Worker startup hooks automatically install interception for nested submissions.
Ray Client and non-Python submissions are not supported by this prototype.

Example::

    import ray
    import secrets
    from ray_admission.queue import (
        disable_admission, enable_admission, policy, start,
    )

    # Administrator process: persist the token privately for future updates.
    token = secrets.token_urlsafe(32)
    controller = start(admin_token=token)
    job_id = ray.get_runtime_context().get_job_id()
    ray.get(controller.set_job_quota.remote(
        job_id, {"CPU": 2, "GPU": 0}, admin_token=token
    ))

    # Business job: connects without the administrator token.
    controller = enable_admission()

    @ray.remote(num_cpus=1)
    @policy(queue="batch", tenant="example")
    def work(value):
        return value + 1

    assert ray.get(work.remote(1)) == 2
    print(ray.get(controller.snapshot.remote()))
    disable_admission()
    ray.kill(controller)  # Explicit cleanup of the detached service, if desired.
    ray.shutdown()

Request kind, job ID, resources, target name/actor ID, and annotation metadata go
to the controller. Arguments stay on the original submission path. Approval is not
execution: original submission failures still propagate normally. Registration
does not cover existing actors, unrelated drivers, or Ray's internal retries.
FIFO means controller arrival order, not execution order. A full quota at the
head blocks later submissions in the same job. Unconfigured jobs and resource
dimensions are unlimited. Task resources are held until all returns are ready;
actor lifetime resources are held until GCS reports DEAD (including restarts).
Actor methods do not charge the actor resources again. Default actors reserve
zero lifetime CPUs, matching Ray. Resource-bearing tasks require positive fixed
returns. Nested submissions can deadlock if the parent holds the job's budget.
Requests larger than the current quota wait until the quota is increased.
Quota configuration and admitted resource reservations survive a controller actor
restart within the same Ray cluster. In-flight admission RPCs are retried by Ray.
State lives in Ray GCS KV and follows the cluster GCS persistence configuration.
There is no lease expiry for submitters lost before tracking.
Administrator tokens authorize configuration RPCs; they are not a sandbox for
mutually untrusted code with full access to the same Ray cluster. Submission job
IDs and resource claims remain cooperative metadata in this prototype.
"""

import argparse
import functools
import hashlib
import inspect
import json
import math
import os
import threading
import time
import uuid
from contextlib import contextmanager

import ray
from ray.actor import ActorClass, ActorHandle
from ray.remote_function import RemoteFunction
from ray_admission._controller import QueueController as _QueueController
from ray_admission._controller import ServiceRecovering

_NAME = "ray-submission-queue"
_NAMESPACE = "ray-submission-queue"
_lock = threading.RLock()
_local = threading.local()
_controller = None
_identity = None
_quota_namespace = None
_originals = {}
_runtime_env_cache = {}
_MARKER = "RAY_SUBMISSION_QUEUE_IDENTITY"
_session_sequence = 0
_session_watermark = 0
_session_done = set()
_session_opened = False
_ack_timer = None
_ack_controller = None
_ack_states = {}
_ack_enabled = True
_submissions = {}
_MAX_UNRESOLVED_SUBMISSIONS = 1024
DESCRIPTION = "Per-Job resource quota and FIFO submission queue"


class SubmissionOutcomeUnknown(RuntimeError):
    """Ray submission was entered and may have created work."""

    def __init__(self, ticket, phase, cause):
        super().__init__(
            f"Submission outcome unknown for {ticket} during {phase}: {cause}"
        )
        self.ticket = ticket
        self.phase = phase
        self.cause = cause


class SubmissionTrackingError(RuntimeError):
    """Execution exists, but its durable permit tracking did not complete."""

    def __init__(self, ticket, handle, cause):
        super().__init__(f"Submission {ticket} succeeded but tracking failed: {cause}")
        self.ticket = ticket
        self.handle = handle
        self.cause = cause

    def retry_tracking(self):
        return retry_tracking(self.ticket)


def _next_ticket():
    global _session_sequence, _session_opened
    with _lock:
        if not _session_opened:
            with _bypass():
                ray.get(_controller.open_session.remote(_identity[1]))
            _session_opened = True
        _session_sequence += 1
        return f"{_identity[1]}:{_session_sequence}"


def _ack_ticket(controller, ticket):
    session, sequence = ticket.split(":")
    key = (controller._ray_actor_id.hex(), session)
    with _lock:
        state = _ack_states.setdefault(
            key,
            dict(
                controller=controller,
                watermark=0,
                done=set(),
                timer=None,
                inflight=False,
                sent=0,
            ),
        )
        if int(sequence) > state["watermark"]:
            state["done"].add(int(sequence))
        while state["watermark"] + 1 in state["done"]:
            state["watermark"] += 1
            state["done"].remove(state["watermark"])
        _schedule_ack(key, state, 0.05)


def _schedule_ack(key, state, delay):
    if (
        _ack_enabled
        and state["timer"] is None
        and not state["inflight"]
        and state["sent"] < state["watermark"]
    ):
        timer = threading.Timer(delay, _flush_ack, args=(key,))
        timer.daemon = True
        state["timer"] = timer
        timer.start()


def _flush_ack(key=None, *, deadline=None):
    if key is None:
        with _lock:
            keys = list(_ack_states)
        for item in keys:
            _flush_ack(item)
        return
    with _lock:
        if not _ack_enabled and deadline is None:
            return
        if deadline is not None and time.monotonic() >= deadline:
            return
        state = _ack_states[key]
        if state["inflight"]:
            return
        if state["timer"] is not None:
            state["timer"].cancel()
        state["timer"] = None
        if state["sent"] >= state["watermark"]:
            return
        state["inflight"] = True
        controller, watermark = state["controller"], state["watermark"]
    succeeded = False
    try:
        with _bypass():
            timeout = 10 if deadline is None else max(0, deadline - time.monotonic())
            ray.get(controller.acknowledge.remote(key[1], watermark), timeout=timeout)
        succeeded = True
    except Exception:
        pass
    finally:
        with _lock:
            state["inflight"] = False
            if succeeded:
                state["sent"] = watermark
            _schedule_ack(key, state, 0.05 if succeeded else 0.5)


def _wait_for_reservation(controller, request, ticket):
    """Use only short actor calls; waiting lives in the client process."""
    delay = 0.01
    while True:
        try:
            with _bypass():
                result = ray.get(controller.enqueue.remote(request, ticket))
        except ServiceRecovering:
            time.sleep(delay)
            delay = min(delay * 1.5, 0.25)
            continue
        state = result["state"]
        if state in ("RESERVED", "UNMETERED"):
            return state
        if state == "TERMINAL":
            raise RuntimeError("Admission ticket is already terminal")
        if state not in ("WAITING", "NOT_REGISTERED"):
            raise RuntimeError(f"Unexpected admission state: {state}")
        time.sleep(delay)
        delay = min(delay * 1.5, 0.25)


def get_submission(ticket):
    """Return the process-local association for a submitted ticket, if retained."""
    with _lock:
        value = _submissions.get(ticket)
        return None if value is None else dict(value)


def _begin_submit(controller, ticket):
    delay = 0.01
    while True:
        try:
            with _bypass():
                return ray.get(controller.begin_submit.remote(ticket))
        except ServiceRecovering:
            time.sleep(delay)
            delay = min(delay * 1.5, 0.25)


def retry_tracking(ticket):
    """Retry durable tracking without resubmitting the user's task or actor."""
    with _lock:
        entry = _submissions.get(ticket)
    if entry is None or entry.get("handle") is None:
        raise ValueError("No process-local execution handle for ticket")
    controller = entry["controller"]
    handle = entry["handle"]
    try:
        with _bypass():
            if entry["kind"] == "task":
                refs = [handle] if isinstance(handle, ray.ObjectRef) else handle
                ray.get(controller.track.remote(ticket, refs))
            else:
                ray.get(
                    controller.track_actor.remote(ticket, handle._ray_actor_id.hex())
                )
    except Exception as error:
        with _lock:
            entry["last_error"] = str(error)
        raise SubmissionTrackingError(ticket, handle, error) from error
    with _lock:
        _submissions.pop(ticket, None)
    _ack_ticket(controller, ticket)
    return handle


def _current_quota_namespace():
    from ray._private.ray_constants import RAY_NAMESPACE_ENVIRONMENT_VARIABLE

    return os.environ.get(RAY_NAMESPACE_ENVIRONMENT_VARIABLE, _NAMESPACE)


def _setup_worker(quota_namespace, previous_key):
    if previous_key:
        from ray._private.runtime_env.setup_hook import load_and_execute_setup_hook

        error = load_and_execute_setup_hook(previous_key)
        if error:
            raise RuntimeError(error)
    # Never create a controller from a worker startup hook.
    controller = ray.get_actor(_NAME, namespace=_NAMESPACE)
    with _lock:
        _install(controller, quota_namespace)


def _propagated_runtime_env(runtime_env):
    from ray._private import ray_constants
    from ray._private.runtime_env.setup_hook import (
        upload_worker_process_setup_hook_if_needed,
    )
    from ray._private.worker import global_worker
    from ray.runtime_env import RuntimeEnv

    hook_key = ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR
    # Preserve inherited job hooks as well as per-task/actor hooks. Serialize
    # first so callable hooks are represented by their existing GCS keys.
    env = RuntimeEnv(**dict(runtime_env or {}))
    env = upload_worker_process_setup_hook_if_needed(env, global_worker)
    inherited = json.loads(global_worker.runtime_env or "{}")
    inherited_vars = inherited.get("env_vars", {})
    env_vars = dict(env.get("env_vars", {}))
    identity = json.dumps(_identity)
    effective_vars = {**inherited_vars, **env_vars}
    previous_key = effective_vars.get(hook_key)
    if effective_vars.get(_MARKER) == identity and (
        hook_key not in env_vars or env_vars[hook_key] == inherited_vars.get(hook_key)
    ):
        # Reuse the installed hook instead of chaining it at every nesting level.
        env_vars[hook_key] = previous_key
        env_vars[_MARKER] = identity
        env["env_vars"] = env_vars
        return env
    cache_key = (env.serialize(), previous_key, identity)
    if cache_key not in _runtime_env_cache:
        quota_namespace = _quota_namespace

        def setup():
            _setup_worker(quota_namespace, previous_key)

        env_vars.pop(hook_key, None)
        env_vars[_MARKER] = identity
        env["env_vars"] = env_vars
        env["worker_process_setup_hook"] = setup
        env = upload_worker_process_setup_hook_if_needed(env, global_worker)
        _runtime_env_cache[cache_key] = env
    return _runtime_env_cache[cache_key]


def _validate_resources(resources):
    if not isinstance(resources, dict):
        raise ValueError("resources must be a resource-name to amount dictionary")
    for name, amount in resources.items():
        if (
            not isinstance(name, str)
            or not name
            or isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or not math.isfinite(amount)
            or amount < 0
        ):
            raise ValueError("resource amounts must be finite nonnegative numbers")
    return {name: amount for name, amount in resources.items() if amount != 0}


@contextmanager
def _bypass():
    previous = getattr(_local, "bypass", False)
    _local.bypass = True
    try:
        yield
    finally:
        _local.bypass = previous


def policy(**metadata):
    """Annotate a function or class *below* ``@ray.remote``.

    Metadata is forwarded unchanged. Resource ceilings are configured only via
    the controller's set_job_quota(), not on individual operators.
    Actor methods currently carry only their method name.
    """

    if "quota" in metadata:
        raise ValueError(
            "Set quotas on the controller with set_job_quota(job_id, resources)"
        )

    def decorate(target):
        target.__ray_admission_policy__ = dict(metadata)
        return target

    return decorate


def _request(kind, target, args, kwargs):
    if kind == "task":
        definition = target._function
        name = target._function_name
    elif kind == "actor_creation":
        definition = target.__ray_metadata__.modified_class
        name = definition.__name__
    else:
        return {
            "kind": kind,
            "name": kwargs.get("method_name", args[0] if args else ""),
            "actor_id": target._ray_actor_id.hex(),
            "metadata": {},
            "job_id": ray.get_runtime_context().get_job_id(),
            "quota_namespace": _quota_namespace,
            "resources": {},
        }
    return {
        "job_id": ray.get_runtime_context().get_job_id(),
        "quota_namespace": _quota_namespace,
        "resources": {},
        "kind": kind,
        "name": name,
        "metadata": dict(getattr(definition, "__ray_admission_policy__", {})),
    }


def _wrap(original, kind):
    signature = inspect.signature(original)

    @functools.wraps(original)
    def submit(target, *args, **kwargs):
        controller = _controller
        if controller is not None and not getattr(_local, "bypass", False):
            # Never route controller RPCs back through the controller itself.
            is_controller = (
                kind == "actor_method"
                and target._ray_actor_id == controller._ray_actor_id
            )
            if not is_controller:
                request = _request(kind, target, args, kwargs)
                bound = None
                if kind != "actor_method":
                    from ray._common import ray_option_utils
                    from ray._common.utils import resources_from_ray_options
                    from ray._private.utils import get_runtime_env_info

                    bound = signature.bind(target, *args, **kwargs)
                    options_key = "task_options" if kind == "task" else "actor_options"
                    options = bound.arguments.setdefault(options_key, {})
                    if kind == "actor_creation" and options.get("get_if_exists"):
                        # Existing handles consume no new resources. Ray recursively
                        # calls _remote(get_if_exists=False) only when creating.
                        return original(*bound.args, **bound.kwargs)
                    defaults = (
                        ray_option_utils.task_options
                        if kind == "task"
                        else ray_option_utils.actor_options
                    )
                    resource_options = {
                        key: value.default_value for key, value in defaults.items()
                    }
                    resource_options.update(options)
                    resources = resources_from_ray_options(resource_options)
                    if kind == "actor_creation":
                        default_cpu = (
                            1
                            if set(resources).difference(
                                {"memory", "object_store_memory"}
                            )
                            else 0
                        )
                        resources.setdefault("CPU", default_cpu)
                    request["resources"] = _validate_resources(resources)
                    if kind == "task":
                        returns = options.get("num_returns", target._num_returns)
                        request["trackable"] = not target._is_generator and (
                            returns is None or isinstance(returns, int) and returns > 0
                        )
                    env = _propagated_runtime_env(options.get("runtime_env"))
                    options["runtime_env"] = env
                    if kind == "task":
                        bound.arguments["serialized_runtime_env_info"] = (
                            get_runtime_env_info(env, serialize=True)
                        )
                ticket = _next_ticket()
                try:
                    state = _wait_for_reservation(controller, request, ticket)
                except BaseException as error:
                    try:
                        deadline = time.monotonic() + 10
                        while True:
                            try:
                                with _bypass():
                                    cancelled = ray.get(
                                        controller.cancel.remote(ticket), timeout=2
                                    )
                                if cancelled == "TERMINAL":
                                    _ack_ticket(controller, ticket)
                                break
                            except Exception:
                                if time.monotonic() >= deadline:
                                    raise
                                time.sleep(0.1)
                    except Exception:
                        error.add_note(f"Cancellation outcome unknown; ticket={ticket}")
                    raise
                if state == "UNMETERED":
                    try:
                        return (
                            original(*bound.args, **bound.kwargs)
                            if bound is not None
                            else original(target, *args, **kwargs)
                        )
                    finally:
                        _ack_ticket(controller, ticket)
                # Catch argument serialization errors before the durable
                # SUBMITTING boundary. Once the original Ray call begins, an
                # exception is conservatively treated as an unknown outcome.
                try:
                    ray.cloudpickle.dumps((args, kwargs))
                except BaseException:
                    with _bypass():
                        ray.get(controller.cancel.remote(ticket), timeout=10)
                    _ack_ticket(controller, ticket)
                    raise
                with _lock:
                    unresolved_full = len(_submissions) >= _MAX_UNRESOLVED_SUBMISSIONS
                if unresolved_full:
                    with _bypass():
                        ray.get(controller.cancel.remote(ticket), timeout=10)
                    _ack_ticket(controller, ticket)
                    raise RuntimeError(
                        "Too many unresolved submissions; reconcile them before submitting"
                    )
                _begin_submit(controller, ticket)
                try:
                    if bound is not None:
                        result = original(*bound.args, **bound.kwargs)
                    else:
                        result = original(target, *args, **kwargs)
                except (KeyboardInterrupt, SystemExit) as error:
                    with _lock:
                        _submissions[ticket] = {
                            "controller": controller,
                            "kind": kind,
                            "handle": None,
                            "tracking_state": "SUBMISSION_UNKNOWN",
                            "last_error": repr(error),
                        }
                    error.add_note(f"Ray submission outcome unknown; ticket={ticket}")
                    raise
                except BaseException as error:
                    with _lock:
                        _submissions[ticket] = {
                            "controller": controller,
                            "kind": kind,
                            "handle": None,
                            "tracking_state": "SUBMISSION_UNKNOWN",
                            "last_error": repr(error),
                        }
                    raise SubmissionOutcomeUnknown(
                        ticket, "ray-submit", error
                    ) from error
                if request["resources"]:
                    with _lock:
                        _submissions[ticket] = {
                            "controller": controller,
                            "kind": kind,
                            "handle": result,
                            "tracking_state": "PENDING",
                            "last_error": None,
                        }
                    return retry_tracking(ticket)
                return result
        return original(target, *args, **kwargs)

    return submit


def start(*, admin_token):
    """Administrator entry point: create or authenticate to a detached queue.

    Keep the token outside business jobs. Only its hash is stored in the actor;
    no token is included in worker startup hooks or returned by status().
    This starts the service without installing submission interception.
    """
    if not isinstance(admin_token, str) or not admin_token:
        raise ValueError("A nonempty administrator token is required")
    if not ray.is_initialized():
        ray.init(namespace=_NAMESPACE)
    from ray._private.client_mode_hook import client_mode_should_convert

    if client_mode_should_convert():
        raise NotImplementedError("Ray Client is not supported")
    with _bypass():
        controller = (
            ray.remote(_QueueController)
            .options(
                name=_NAME,
                namespace=_NAMESPACE,
                lifetime="detached",
                get_if_exists=True,
                num_cpus=0,
                max_restarts=-1,
                max_task_retries=-1,
            )
            .remote(hashlib.sha256(admin_token.encode("utf-8")).hexdigest())
        )
        # Existing queues retain their credentials; start cannot replace them.
        ray.get(controller.check_admin.remote(admin_token))
        health = ray.get(controller.status.remote())["health"]
        if health["recovery_state"] != "READY":
            raise RuntimeError(
                "Existing queue is unhealthy; run ray-admission queue recover"
            )
    return controller


def enable_admission():
    """Install interception and return the shared detached controller handle.

    Initializes Ray if necessary and connects to an administrator-started queue.
    Does not create the queue or grant configuration privileges.
    Repeated registration with the same
    identity is harmless. Controller failures propagate and prevent submission.
    Admission blocks the calling thread, including when called in an event loop.
    This affects already-defined remote objects and ``.options().remote()`` too.
    New task and actor workers automatically connect to the same controller.
    Existing actors and other drivers are not retroactively instrumented.
    """
    global _controller, _identity
    quota_namespace = _current_quota_namespace()
    if not ray.is_initialized():
        ray.init(namespace=_NAMESPACE)
    from ray._private.client_mode_hook import client_mode_should_convert

    if client_mode_should_convert():
        raise NotImplementedError("Ray Client is not supported")
    with _lock:
        if _controller is not None:
            if _quota_namespace != quota_namespace:
                raise ValueError(
                    "Disable admission before selecting another quota namespace"
                )
            return _controller
        with _bypass():
            try:
                controller = ray.get_actor(_NAME, namespace=_NAMESPACE)
            except ValueError as error:
                raise RuntimeError(
                    "Queue is not running; an administrator must call start() first"
                ) from error
            # Confirm that the controller is ready before enabling interception.
            ray.get(controller.snapshot.remote())
        _install(controller, quota_namespace)
        return controller


def _install(controller, quota_namespace):
    global _controller, _identity, _quota_namespace, _ack_enabled
    global _session_sequence, _session_watermark, _session_opened
    if _controller is not None:
        if _quota_namespace != quota_namespace:
            raise ValueError("Process already attached to another quota namespace")
        return
    session = uuid.uuid4().hex
    _session_opened = False
    _session_sequence = 0
    _session_watermark = 0
    _session_done.clear()
    for cls, method, kind in (
        (RemoteFunction, "_remote", "task"),
        (ActorClass, "_remote", "actor_creation"),
        (ActorHandle, "_actor_method_call", "actor_method"),
    ):
        original = getattr(cls, method)
        wrapper = _wrap(original, kind)
        _originals[cls, method] = (original, wrapper)
        setattr(cls, method, wrapper)
    _identity = (quota_namespace, session)
    _quota_namespace = quota_namespace
    _controller = controller
    _ack_enabled = True
    for key, state in _ack_states.items():
        _schedule_ack(key, state, 0.05)


def disable_admission():
    """Restore local submission methods; leave the detached controller alive.

    Does not disable interception in workers that have already started.
    Call before ``ray.shutdown()``. Enabling/disabling admission during concurrent
    submissions is not supported.
    """
    global \
        _controller, \
        _identity, \
        _quota_namespace, \
        _ack_timer, \
        _ack_controller, \
        _ack_enabled
    with _lock:
        _ack_enabled = False
        for state in _ack_states.values():
            if state["timer"] is not None:
                state["timer"].cancel()
                state["timer"] = None
        if _ack_timer is not None:
            _ack_timer.cancel()
            _ack_timer = None
        _ack_controller = None
        _controller = None
        _identity = None
        _quota_namespace = None
        for (cls, method), (original, wrapper) in _originals.items():
            if getattr(cls, method) is wrapper:
                setattr(cls, method, original)
        _originals.clear()
        _runtime_env_cache.clear()
        keys = list(_ack_states)
    # One shared budget, not one timeout per session. Background callbacks
    # remain disabled even when this final best-effort delivery fails.
    deadline = time.monotonic() + 2
    for key in keys:
        _flush_ack(key, deadline=deadline)


def _cli_resources(value):
    try:
        resources = json.loads(value)
        _validate_resources(resources)
        return resources
    except (ValueError, TypeError) as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _cli_token(args):
    token = args.admin_token or os.environ.get("RAY_ADMISSION_ADMIN_TOKEN")
    if not token:
        raise ValueError("Set RAY_ADMISSION_ADMIN_TOKEN or pass --admin-token")
    return token


def register_cli(parser):
    """Add this implementation's commands to the shared CLI parser."""
    parser.add_argument("--address", default=None, help="Ray cluster address")
    parser.add_argument(
        "--admin-token", help="Administrator token (environment variable is preferred)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("start", help="Start or authenticate to the detached queue")
    commands.add_parser("recover", help="Recover an unhealthy queue storage writer")
    commands.add_parser("status", help="Show quotas, usage, and waiting requests")
    quota = commands.add_parser("quota", help="Manage per-Job quotas")
    quota_commands = quota.add_subparsers(dest="quota_command", required=True)
    set_quota = quota_commands.add_parser("set")
    set_quota.add_argument("job_id")
    set_quota.add_argument(
        "resources", type=_cli_resources, help='JSON, e.g. {"CPU": 4}'
    )
    delete_quota = quota_commands.add_parser("delete")
    delete_quota.add_argument("job_id")


def run_cli(args):
    """Execute a parsed queue command."""
    try:
        if args.command == "start":
            if args.address is not None:
                ray.init(address=args.address, namespace=_NAMESPACE)
            start(admin_token=_cli_token(args))
            print(json.dumps({"name": _NAME, "namespace": _NAMESPACE}))
        else:
            ray.init(address=args.address, namespace=_NAMESPACE)
            controller = ray.get_actor(_NAME, namespace=_NAMESPACE)
            if args.command == "status":
                print(json.dumps(ray.get(controller.status.remote()), indent=2))
            elif args.command == "recover":
                state = ray.get(
                    controller.recover.remote(admin_token=_cli_token(args)), timeout=45
                )
                print(json.dumps({"recovery_state": state}))
                if state != "READY":
                    raise RuntimeError(f"Queue recovery ended in {state}")
            else:
                resources = args.resources if args.quota_command == "set" else None
                quota_namespace = _current_quota_namespace()
                ray.get(
                    controller.set_job_quota.remote(
                        args.job_id,
                        resources,
                        quota_namespace=quota_namespace,
                        admin_token=_cli_token(args),
                    )
                )
                print(
                    json.dumps(
                        {
                            "quota_namespace": quota_namespace,
                            "job_id": args.job_id,
                            "resources": resources,
                        }
                    )
                )
    finally:
        if ray.is_initialized():
            ray.shutdown()
