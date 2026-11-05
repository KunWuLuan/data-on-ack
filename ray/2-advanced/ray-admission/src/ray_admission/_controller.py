"""Single-writer queue state machine with short admission RPCs and GCS recovery."""

import asyncio
import base64
import hashlib
import hmac
import json
import random
import time
import uuid
from collections import Counter, defaultdict, deque

import ray

from ray_admission._storage import BoundedWriter, GcsStore

NAMESPACE = "ray-submission-queue"
PROTOCOL_VERSION = 3
MAX_WAITING = 100_000
MAX_WAITING_PER_KEY = 10_000


class ServiceRecovering(RuntimeError):
    """A transient recovery state; retry the same operation identity."""


def quota_key(key):
    return "quota/" + base64.urlsafe_b64encode(json.dumps(key).encode()).decode()


def ticket_parts(ticket):
    if ":" not in ticket:
        return None
    session, sequence = ticket.split(":", 1)
    if not session or not sequence.isdigit() or int(sequence) < 1:
        raise ValueError("Invalid session ticket")
    return session, int(sequence)


class QueueController:
    def __init__(self, admin_token_digest):
        from ray_admission.queue import _validate_resources

        self._validate = _validate_resources
        self._admin_token_digest = admin_token_digest
        self._lock = asyncio.Lock()
        self._store = GcsStore(admin_token_digest)
        self._writer = BoundedWriter(self._store)
        self._records = {}
        self._quotas = {}
        self._sessions = {}
        self._counts = Counter()
        self._rpc_counts = Counter()
        self._usage = {}
        self._permits = {}
        self._tracked = {}
        self._actors = {}
        self._released = set()
        self._released_by_session = defaultdict(set)
        self._waiting = {}
        self._waiting_tickets = {}
        self._monitor = None
        self._drainer = None
        self._recovery = None
        self._recovery_state = "READY"
        self._storage_error = None
        self._monitor_error = None
        self._recovery_attempts = 0
        self._recovery_started = None
        self._last_recovery_seconds = None
        self._last_successful_commit = None
        self._last_actor_poll = 0.0
        self._actor_states = {}
        self._actor_poll_cursor = 0
        self._incarnation = uuid.uuid4().hex
        self._generation = 0
        self._load_records(self._store.load())
        self._start_monitor()

    def _load_records(self, records):
        self._records = records
        self._quotas.clear()
        self._sessions.clear()
        self._usage.clear()
        self._permits.clear()
        self._tracked.clear()
        self._actors.clear()
        self._released.clear()
        self._released_by_session.clear()
        for name, record in records.items():
            if name.startswith("quota/"):
                self._validate(record["resources"])
                self._quotas[tuple(record["key"])] = record["resources"]
            elif name.startswith("session/"):
                self._sessions[name[8:]] = record["watermark"]
            elif name.startswith("permit/"):
                ticket = name[7:]
                # v2 RESERVED may have crossed the old submit boundary. Never
                # make it safely cancellable under the v3 protocol.
                if record["state"] == "RESERVED" and record.get("protocol", 2) < 3:
                    record = {**record, "state": "SUBMITTING", "protocol": 3}
                    self._records[name] = record
                self._apply_permit(ticket, record)
            else:
                raise RuntimeError("Unknown storage record: " + name)

    def _apply_permit(self, ticket, record):
        old = self._permits.pop(ticket, None)
        if old:
            key, resources = old
            self._usage[key].subtract(resources)
            for resource in resources:
                if abs(self._usage[key][resource]) <= 1e-9:
                    self._usage[key][resource] = 0
        self._tracked.pop(ticket, None)
        self._actors.pop(ticket, None)
        self._released.discard(ticket)
        parts = ticket_parts(ticket)
        if parts:
            self._released_by_session[parts[0]].discard(parts[1])
        if record is None:
            return
        state = record["state"]
        if state == "RELEASED":
            self._released.add(ticket)
            if parts:
                self._released_by_session[parts[0]].add(parts[1])
            return
        if state not in ("RESERVED", "SUBMITTING", "TRACKED"):
            raise RuntimeError("Invalid permit state")
        key = tuple(record["key"])
        resources = self._validate(record["resources"])
        self._permits[ticket] = (key, resources)
        self._usage.setdefault(key, Counter()).update(resources)
        if record.get("refs"):
            self._tracked[ticket] = {
                ray.cloudpickle.loads(base64.b64decode(value))
                for value in record["refs"]
            }
        if record.get("actor_id"):
            self._actors[ticket] = record["actor_id"]

    def _ensure_ready(self):
        if self._recovery_state in ("QUIESCING", "RECOVERING", "BACKOFF"):
            raise ServiceRecovering(self._recovery_state)
        if self._recovery_state != "READY":
            raise RuntimeError(f"Queue service is {self._recovery_state}; retry later")

    async def _commit(self, changes):
        self._ensure_ready()
        generation = self._generation
        changes = [
            (
                key,
                None
                if value is None
                else {
                    **value,
                    "revision": self._records.get(key, {}).get("revision", 0) + 1,
                },
            )
            for key, value in changes
        ]
        try:
            await self._writer.commit(changes)
        except BaseException as error:
            self._storage_error = str(error)
            self._begin_recovery()
            raise ServiceRecovering(
                "Queue storage unavailable; recovery started"
            ) from error
        if generation != self._generation:
            raise ServiceRecovering("Commit completed during recovery; retry operation")
        self._publish(changes)
        self._last_successful_commit = time.time()

    def _publish(self, changes):
        for key, record in changes:
            if record is None:
                self._records.pop(key, None)
            else:
                self._records[key] = record
            if key.startswith("permit/"):
                self._apply_permit(key[7:], record)

    def _begin_recovery(self):
        if self._recovery is None or self._recovery.done():
            self._generation += 1
            self._recovery_state = "QUIESCING"
            self._recovery_started = time.monotonic()
            self._recovery = asyncio.create_task(self._recover())

    async def _recover(self):
        delay = 0.05
        while True:
            self._recovery_attempts += 1
            try:
                self._recovery_state = "QUIESCING"
                await asyncio.wait_for(self._writer.quiesce(), timeout=30)
                self._recovery_state = "RECOVERING"
                store = await asyncio.to_thread(GcsStore, self._admin_token_digest)
                records = await asyncio.to_thread(store.load)
                async with self._lock:
                    self._store = store
                    self._writer = BoundedWriter(store)
                    self._load_records(records)
                    persisted_tickets = self._records_ticket_set()
                    for key, waiting in list(self._waiting.items()):
                        self._waiting[key] = deque(
                            x for x in waiting if x[0] not in persisted_tickets
                        )
                        if not self._waiting[key]:
                            del self._waiting[key]
                    self._waiting_tickets = {
                        t: v
                        for t, v in self._waiting_tickets.items()
                        if t not in persisted_tickets
                    }
                    self._storage_error = None
                    self._recovery_state = "READY"
                    self._last_recovery_seconds = (
                        time.monotonic() - self._recovery_started
                    )
                self._start_monitor()
                self._schedule_drain()
                return
            except asyncio.TimeoutError:
                self._recovery_state = "RESTART_REQUIRED"
                return
            except Exception as error:
                self._storage_error = str(error)
                self._recovery_state = "BACKOFF"
                await asyncio.sleep(delay + random.random() * delay)
                delay = min(delay * 2, 5)

    async def recover(self, *, admin_token=None):
        self.check_admin(admin_token)
        self._begin_recovery()
        if self._recovery is not None:
            await asyncio.shield(self._recovery)
        return self._recovery_state

    def check_admin(self, token):
        if (
            not isinstance(token, str)
            or not token
            or not hmac.compare_digest(
                hashlib.sha256(token.encode()).hexdigest(), self._admin_token_digest
            )
        ):
            raise PermissionError("Invalid queue administrator credentials")

    async def open_session(self, session):
        if not session or any(c not in "0123456789abcdef" for c in session):
            raise ValueError("Invalid session ID")
        async with self._lock:
            self._ensure_ready()
            if session not in self._sessions:
                await self._commit([("session/" + session, {"watermark": 0})])
                self._sessions[session] = 0

    def _acknowledged(self, ticket):
        parts = ticket_parts(ticket)
        return parts is not None and parts[1] <= self._sessions.get(parts[0], 0)

    async def acknowledge(self, session, watermark):
        self._rpc_counts["acknowledge"] += 1
        if (
            isinstance(watermark, bool)
            or not isinstance(watermark, int)
            or watermark < 0
        ):
            raise ValueError("Invalid acknowledgment watermark")
        async with self._lock:
            self._ensure_ready()
            if session not in self._sessions:
                raise ValueError("Unknown session")
            if watermark > self._sessions[session]:
                await self._commit([("session/" + session, {"watermark": watermark})])
                self._sessions[session] = watermark
            await self._compact_session_locked(session)

    async def _compact_session_locked(self, session):
        watermark = self._sessions.get(session, 0)
        obsolete = [
            n for n in self._released_by_session.get(session, ()) if n <= watermark
        ]
        for offset in range(0, len(obsolete), 32):
            await self._commit(
                [
                    (f"permit/{session}:{number}", None)
                    for number in obsolete[offset : offset + 32]
                ]
            )

    async def set_job_quota(
        self, job_id, resources, *, quota_namespace=NAMESPACE, admin_token=None
    ):
        self.check_admin(admin_token)
        if not job_id or not quota_namespace:
            raise ValueError("job_id and quota_namespace must be nonempty strings")
        if resources is not None:
            self._validate(resources)
        key = (quota_namespace, job_id)
        async with self._lock:
            await self._commit(
                [
                    (
                        quota_key(key),
                        None
                        if resources is None
                        else {
                            "key": list(key),
                            "resources": dict(resources),
                        },
                    )
                ]
            )
            if resources is None:
                self._quotas.pop(key, None)
            else:
                self._quotas[key] = dict(resources)
        self._schedule_drain()

    def _ticket_state(self, ticket):
        if ticket in self._released or self._acknowledged(ticket):
            return "TERMINAL"
        if ticket in self._permits:
            return self._records["permit/" + ticket]["state"]
        if ticket in self._waiting_tickets:
            return "WAITING"
        return "NOT_REGISTERED"

    async def enqueue(self, request, ticket):
        self._rpc_counts["enqueue"] += 1
        resources = self._validate(request["resources"])
        key = (request.get("quota_namespace", NAMESPACE), request["job_id"])
        if not request.get("trackable", True) and resources:
            raise ValueError("Resource accounting requires positive fixed returns")
        async with self._lock:
            self._ensure_ready()
            parts = ticket_parts(ticket)
            if parts and parts[0] not in self._sessions:
                raise ValueError("Unknown session")
            state = self._ticket_state(ticket)
            if state == "TERMINAL":
                return {"state": state, "incarnation": self._incarnation}
            if ticket in self._permits:
                if self._permits[ticket] != (key, resources):
                    raise ValueError("Ticket reused for a different request")
                return {"state": state, "incarnation": self._incarnation}
            if request["kind"] == "actor_method" or not resources:
                self._counts[request["kind"]] += 1
                return {"state": "UNMETERED", "incarnation": self._incarnation}
            if ticket in self._waiting_tickets:
                existing = self._waiting_tickets[ticket]
                if existing[:2] != (key, resources):
                    raise ValueError("Ticket reused for a different request")
            else:
                if len(self._waiting_tickets) >= MAX_WAITING:
                    raise RuntimeError("Queue overloaded")
                waiting = self._waiting.setdefault(key, deque())
                if len(waiting) >= MAX_WAITING_PER_KEY:
                    raise RuntimeError("Job queue overloaded")
                item = (ticket, request, resources)
                waiting.append(item)
                self._waiting_tickets[ticket] = (key, resources, request)
        self._schedule_drain()
        return {"state": self._ticket_state(ticket), "incarnation": self._incarnation}

    def inspect(self, ticket):
        self._rpc_counts["inspect"] += 1
        return {
            "state": self._ticket_state(ticket),
            "incarnation": self._incarnation,
            "retry_after": 0.02,
        }

    async def begin_submit(self, ticket):
        self._rpc_counts["begin_submit"] += 1
        async with self._lock:
            self._ensure_ready()
            state = self._ticket_state(ticket)
            if state == "SUBMITTING":
                return state
            if state != "RESERVED":
                raise ValueError(f"Ticket is {state}, expected RESERVED")
            await self._commit(
                [
                    (
                        "permit/" + ticket,
                        {
                            **self._records["permit/" + ticket],
                            "state": "SUBMITTING",
                            "protocol": PROTOCOL_VERSION,
                        },
                    )
                ]
            )
            return "SUBMITTING"

    def _schedule_drain(self):
        if self._recovery_state == "READY" and (
            self._drainer is None or self._drainer.done()
        ):
            self._drainer = asyncio.create_task(self._drain())

    async def _drain(self):
        budget = 32
        try:
            async with self._lock:
                self._ensure_ready()
                budget = 32
                for key, waiting in list(self._waiting.items()):
                    selected = []
                    projected = self._usage.get(key, Counter()).copy()
                    for ticket, request, resources in waiting:
                        if not budget:
                            break
                        if any(
                            projected[r] + resources.get(r, 0) > limit + 1e-9
                            for r, limit in self._quotas.get(key, {}).items()
                        ):
                            break
                        selected.append((ticket, request, resources))
                        projected.update(resources)
                        budget -= 1
                    if selected:
                        await self._commit(
                            [
                                (
                                    "permit/" + ticket,
                                    {
                                        "state": "RESERVED",
                                        "key": list(key),
                                        "resources": resources,
                                        "protocol": PROTOCOL_VERSION,
                                    },
                                )
                                for ticket, _, resources in selected
                            ]
                        )
                        for ticket, request, _ in selected:
                            waiting.popleft()
                            self._waiting_tickets.pop(ticket, None)
                            self._counts[request["kind"]] += 1
                    if not waiting:
                        self._waiting.pop(key, None)
                    if not budget:
                        break
        except Exception:
            pass
        self._drainer = None
        if self._waiting_tickets and budget == 0:
            await asyncio.sleep(0)
            self._schedule_drain()

    async def cancel(self, ticket):
        self._rpc_counts["cancel"] += 1
        async with self._lock:
            self._ensure_ready()
            if ticket in self._waiting_tickets:
                await self._release_locked([ticket])
                key, _, _ = self._waiting_tickets.pop(ticket)
                self._waiting[key] = deque(
                    x for x in self._waiting[key] if x[0] != ticket
                )
                if not self._waiting[key]:
                    del self._waiting[key]
                return "TERMINAL"
            state = self._ticket_state(ticket)
            if state == "RESERVED":
                await self._release_locked([ticket])
                return "TERMINAL"
            if state in ("SUBMITTING", "TRACKED"):
                return "SUBMISSION_UNKNOWN"
            if state == "NOT_REGISTERED":
                await self._release_locked([ticket])
                return "TERMINAL"
            return state

    def _records_ticket_set(self):
        return {key[7:] for key in self._records if key.startswith("permit/")}

    async def _release_locked(self, tickets, *, actor_id=None):
        changes = []
        for ticket in tickets:
            if ticket in self._released:
                continue
            if self._acknowledged(ticket):
                if ticket not in self._permits:
                    continue
                value = None
            else:
                old = self._records.get("permit/" + ticket, {})
                value = {
                    "state": "RELEASED",
                    "protocol": PROTOCOL_VERSION,
                    "refs": old.get("refs", []),
                    "actor_id": actor_id or old.get("actor_id"),
                }
            changes.append(("permit/" + ticket, value))
        for offset in range(0, len(changes), 32):
            await self._commit(changes[offset : offset + 32])

    async def release(self, ticket):
        """Release only tracked/completed work; use cancel before submission."""
        self._rpc_counts["release"] += 1
        async with self._lock:
            state = self._ticket_state(ticket)
            if state == "SUBMITTING":
                raise ValueError("Submission outcome unknown; reconciliation required")
            await self._release_locked([ticket])
        self._schedule_drain()

    async def track(self, ticket, refs):
        self._rpc_counts["track"] += 1
        refs = set(refs)
        async with self._lock:
            self._ensure_ready()
            if ticket not in self._permits:
                record = self._records.get("permit/" + ticket, {})
                saved = {
                    ray.cloudpickle.loads(base64.b64decode(v))
                    for v in record.get("refs", [])
                }
                if record.get("state") == "RELEASED" and saved and saved == refs:
                    return "TRACKED_COMPLETED"
                raise ValueError("Unknown permit")
            if ticket in self._actors or (
                ticket in self._tracked and self._tracked[ticket] != refs
            ):
                raise ValueError("Ticket already tracks a different execution")
            if ticket not in self._tracked:
                await self._commit(
                    [
                        (
                            "permit/" + ticket,
                            {
                                **self._records["permit/" + ticket],
                                "state": "TRACKED",
                                "refs": [
                                    base64.b64encode(
                                        ray.cloudpickle.dumps(ref)
                                    ).decode()
                                    for ref in refs
                                ],
                            },
                        )
                    ]
                )
        self._start_monitor()

    async def track_actor(self, ticket, actor_id):
        self._rpc_counts["track_actor"] += 1
        async with self._lock:
            self._ensure_ready()
            if ticket not in self._permits:
                record = self._records.get("permit/" + ticket, {})
                if (
                    record.get("state") == "RELEASED"
                    and record.get("actor_id") == actor_id
                ):
                    return "TRACKED_COMPLETED"
                raise ValueError("Unknown permit")
            if ticket in self._tracked or (
                ticket in self._actors and self._actors[ticket] != actor_id
            ):
                raise ValueError("Ticket already tracks a different execution")
            if ticket in self._actors:
                return
            if actor_id in self._actors.values():
                await self._release_locked([ticket], actor_id=actor_id)
            else:
                await self._commit(
                    [
                        (
                            "permit/" + ticket,
                            {
                                **self._records["permit/" + ticket],
                                "state": "TRACKED",
                                "actor_id": actor_id,
                            },
                        )
                    ]
                )
        self._start_monitor()
        self._schedule_drain()

    def _start_monitor(self):
        if self._monitor is None or self._monitor.done():
            self._monitor = asyncio.create_task(self._watch_completions())

    async def _watch_completions(self):
        delay = 0.1
        while self._tracked or self._actors or self._released:
            if self._recovery_state != "READY":
                return
            try:
                await self._poll_completions()
                self._monitor_error = None
                delay = 0.1
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._monitor_error = str(error)
                delay = min(delay * 2, 5)
            await asyncio.sleep(delay)

    async def _poll_completions(self):
        from ray._private.state import actors

        tracked = dict(self._tracked)
        actor_ids = dict(self._actors)
        refs = list({ref for pending in tracked.values() for ref in pending})
        ready = set()
        if refs:
            ready, _ = await asyncio.to_thread(
                ray.wait, refs, num_returns=len(refs), timeout=0, fetch_local=False
            )
            ready = set(ready)
        if actor_ids and time.monotonic() - self._last_actor_poll >= 1:
            identifiers = list(actor_ids.values())
            count = min(64, len(identifiers))
            start = self._actor_poll_cursor % len(identifiers)
            selected = [
                identifiers[(start + offset) % len(identifiers)]
                for offset in range(count)
            ]

            def query_selected():
                result = {}
                for actor_id in selected:
                    info = actors(actor_id=actor_id)
                    if info:
                        result[actor_id] = info
                return result

            self._actor_states.update(await asyncio.to_thread(query_selected))
            self._actor_poll_cursor = start + count
            self._last_actor_poll = time.monotonic()
        states = self._actor_states
        completed = [t for t, pending in tracked.items() if pending.issubset(ready)]
        completed += [
            t
            for t, actor_id in actor_ids.items()
            if states.get(actor_id, {}).get("State") == "DEAD"
        ]
        async with self._lock:
            await self._release_locked(completed)
            for session in list(self._released_by_session):
                await self._compact_session_locked(session)
        if completed:
            self._schedule_drain()

    def snapshot(self):
        self._ensure_ready()
        return dict(self._counts)

    def status(self):
        def nested(values):
            result = {}
            for (namespace, job_id), value in values.items():
                result.setdefault(namespace, {})[job_id] = dict(value)
            return result

        return {
            "health": {
                "storage_error": self._storage_error,
                "monitor_error": self._monitor_error,
                "accepting": self._recovery_state == "READY",
                "recovery_state": self._recovery_state,
                "incarnation": self._incarnation,
                "recovery_attempts": self._recovery_attempts,
                "last_recovery_seconds": self._last_recovery_seconds,
                "last_successful_commit": self._last_successful_commit,
                "writer_inflight": self._writer.requests.qsize(),
                "monitor_running": self._monitor is not None
                and not self._monitor.done(),
            },
            "storage": {
                "backend": "gcs-kv",
                "batches": self._store.batches,
                "kv_writes": self._store.kv_writes,
                "bytes_written": self._store.bytes_written,
                "released_records": len(self._released),
                "sessions": len(self._sessions),
            },
            "rpc_counts": dict(self._rpc_counts),
            "quotas": nested(self._quotas),
            "usage": nested(self._usage),
            "permits": [
                {
                    "ticket": ticket,
                    "state": self._records["permit/" + ticket]["state"],
                    "quota_namespace": key[0],
                    "job_id": key[1],
                    "resources": dict(resources),
                }
                for ticket, (key, resources) in self._permits.items()
            ],
            "waiting": [
                {
                    "ticket": ticket,
                    "name": request["name"],
                    "job_id": key[1],
                    "quota_namespace": key[0],
                    "resources": dict(resources),
                }
                for ticket, (key, resources, request) in self._waiting_tickets.items()
            ],
        }
