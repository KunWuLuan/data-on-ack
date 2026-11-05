# Ray Admission Queue Design

Status: internal iteration; experimental implementation. This document describes
the implementation at `7c8250d`, with documentation-only elaboration afterward.
It distinguishes implemented behavior from proposed follow-up work. It is not a
production-readiness claim. [中文设计文档](DESIGN_ZH.md) covers the same design.

**Review disposition:** the core reliability protocol in
[RELIABILITY_PLAN.md](RELIABILITY_PLAN.md) is implemented, but the service remains
in internal iteration until the full acceptance matrix passes.

## 1. Purpose and scope

Ray Admission adds a cooperative admission step before Python task and actor
submission. A controller limits the sum of declared resources admitted for each
`(quota namespace, Ray Job ID)`. Requests that do not fit wait before submission
to Ray. Ray remains responsible for placement, execution, retries and scheduling
against physical cluster capacity.

The package is independent of the Ray source distribution. Administrators,
drivers and workers install the same wheel. The current compatibility baseline
is Ray 2.58; private Python interfaces require validation on other versions.

Goals:

- One discoverable admission CLI with interchangeable implementations; `queue`
  is the current implementation.
- A fixed controller location, business namespace isolation in accounting, and
  administrator-controlled resource vectors.
- Explicit reservation and release semantics across controller reconstruction.
- Incremental GCS KV storage, bounded write batches and retry-safe compaction.
- Observable storage and monitoring failures that do not silently free capacity.

Non-goals in the current implementation:

- A security boundary for mutually untrusted jobs or direct GCS clients.
- Enforcement inside the Ray scheduler, or interception of Java/C++/Ray Client.
- Recovery solely from a task-table scan, cross-cluster recovery, or an external
  database dependency.
- Persistent waiting order, global fairness, preemption, automatic quota
  provisioning or automatic repair of every orphaned reservation.

## 2. Components and responsibilities

| Component | File | Responsibility |
| --- | --- | --- |
| CLI discovery | `src/ray_admission/cli.py` | Enumerate implementations and dispatch commands |
| Client integration | `src/ray_admission/queue.py` | Resource extraction, tickets, interception, worker propagation, admin entry points |
| Stateful service | `src/ray_admission/_controller.py` | Quotas, FIFO queues, reservations, tracking, completion and compaction |
| Persistence | `src/ray_admission/_storage.py` | GCS records, redo recovery, migration and background writes |
| Packaging | `build_wheel.py` | Include package modules and CLI entry point in a pure-Python wheel |

Logical request path:

```text
driver / worker Python .remote()
  -> client wrapper -> short enqueue/inspect RPCs; client waits
  -> per-quota FIFO -> durable RESERVED -> durable SUBMITTING
  -> original Ray submission -> ObjectRef(s) / actor handle
  -> controller.track / track_actor -> durable TRACKED record
  -> client acknowledgment watermark

controller completion monitor
  -> Ray ObjectRef readiness / GCS actor state
  -> durable release or delete -> update usage -> drain waiting requests
```

The CLI scans immediate modules under `ray_admission.__path__` with
`pkgutil.iter_modules`. It skips packages, private names and `cli`; modules with
a callable `register_cli` are registered. A usable implementation also supplies
`DESCRIPTION` and `run_cli`. Discovery currently imports modules, is not recursive,
and does not isolate an implementation's import failure. This is a package-local
extension mechanism, not an entry-point-based third-party plugin system.

Commands are `ray-admission list`, `ray-admission queue ... start`, `status`,
`quota set` and `quota delete`. There is currently no dedicated stop/restart CLI.

## 3. Identity, discovery and authority

| Identity | Source | Meaning |
| --- | --- | --- |
| Controller actor name | Fixed `ray-submission-queue` | Shared lookup name |
| Controller Ray namespace | Fixed `ray-submission-queue` | Shared actor lookup scope |
| GCS KV namespace | Fixed `ray-submission-queue` | Internal storage isolation |
| Business quota namespace | Driver `RAY_NAMESPACE`; fallback `ray-submission-queue` | First quota-key component |
| Job ID | Ray runtime context | Second quota-key component |
| Session ID | Random UUID hex per client installation | Ticket retry/compaction scope |
| Ticket | `<session>:<positive sequence>` | One intercepted submission |

Controller start and lookup expose neither name nor namespace overrides. A
Kubernetes Downward API can populate the driver's `RAY_NAMESPACE` from
`metadata.namespace`; this identifies business accounting, not controller
placement. Changing it requires disabling and re-enabling the local integration.
New workers receive the selected business namespace through the startup hook.

`start(admin_token=...)` creates or authenticates to the detached controller.
The digest is SHA-256; comparisons use constant-time comparison. Existing state
requires the original credential. Quota mutations check the administrator token.
Business `enable_admission()` only looks up an existing controller and confirms
readiness; it cannot create one or grant itself configuration authority.

This authentication protects the intended management API. Admission RPCs and
session watermarks trust cooperating clients. Users able to bypass wrappers,
invoke internal RPCs or mutate GCS can defeat the policy. Do not describe this as
tenant security enforcement. No automatic credential rotation is implemented.

## 4. Integration and submission flow

1. Install the wheel in administrator, driver and worker environments.
2. Start the detached controller with administrator credentials.
3. Establish the business namespace and actual Ray Job ID, and configure quota
   before business submissions that must be constrained.
4. After `ray.init(address="auto")`, call `enable_admission()` in the driver.
5. Submit normal Ray tasks and actors. No interactive Enter prompt is required.

Quota provisioning is an orchestration responsibility. Unconfigured keys are
unlimited, so starting a job before its quota exists is not a deny-by-default
workflow. This design does not introduce an implicit user-input barrier.

Interception wraps `RemoteFunction._remote`, `ActorClass._remote` and
`ActorHandle._actor_method_call`. It affects already-defined remote objects and
`.options().remote()`. Controller RPCs bypass interception to prevent recursion.
The effective Ray options determine the resource vector; actor CPU defaults
follow the adapter's lifetime-accounting logic. Use explicit actor resources when
predictable accounting matters, and validate defaults on Ray upgrades.

The client waits synchronously for admission before calling Ray's original
submission method. Thus `.remote()` can block a caller thread, including an async
event loop if invoked there. This is not an asynchronous admission API.

Worker hooks preserve an existing setup hook, install the integration and reuse
hook metadata to avoid unbounded chaining in nested submissions. Sessions open
lazily on the first intercepted submission, not inside the worker startup hook.
Existing actors and other drivers are not retroactively instrumented. Disabling
the driver restores only its local methods; previously started workers remain
instrumented. Concurrent enable/disable with submission is unsupported.

## 5. Quota and FIFO semantics

For quota key `q`, usage `U`, request `R` and configured limits `L`, a request fits
when `U[r] + R[r] <= L[r] + 1e-9` for every resource present in `L`.

- A missing quota key is unlimited. Missing resource dimensions in a configured
  vector are also unlimited. An explicit zero limit blocks positive demand.
- Resource amounts must be finite, nonnegative numbers; boolean values are
  rejected. Zero demand is removed from the request's resource vector.
- Setting quota replaces the full vector. Deleting quota removes restrictions.
- Lowering quota does not cancel or preempt admitted work. New work waits until
  usage fits, or the administrator raises/removes the limit.
- Usage is declared admitted demand, not measured CPU/GPU use and not just RUNNING
  tasks. Work waiting inside Ray after admission still consumes its reservation.

Each key has its own FIFO. A head request that does not fit blocks later requests
for that key, including smaller requests that could otherwise fit. Other keys can
be processed. There is no weighted fairness or global ordering guarantee. The
drainer selects up to 32 entries per batch, accounts projected usage, persists
reservations, then resolves their admission futures.

Nested submissions can deadlock when a parent holds the entire quota while
waiting for children using the same key. No resource lending or parent-child
deadlock detection exists. An oversized head request waits indefinitely unless
the limit changes or the request is cancelled.

## 6. Permit lifecycle and invariants

| State | Charged? | Durable representation | Exit condition |
| --- | --- | --- | --- |
| WAITING | No | No permit record; client polls short RPCs and FIFO is in memory | Fits quota or cancellation |
| RESERVED | Yes | Ticket, key, resource vector | `begin_submit` or safe cancellation |
| SUBMITTING | Yes | Reservation whose Ray submission outcome may be unknown | Tracking or evidence-based reconciliation |
| TRACKED | Yes | Reservation plus serialized ObjectRefs or actor ID | Completion observed or explicit release |
| RELEASED | No | Tombstone, until safely compacted | Covered by session watermark |

Core invariants:

1. Persist RESERVED before returning resource-consuming approval.
2. Rebuild usage from retained RESERVED/SUBMITTING/TRACKED records, never from an empty
   counter after restart.
3. Persist release before lowering usage or admitting a replacement.
4. A duplicate active ticket with the same key/resources reuses its reservation;
   conflicting key/resources are rejected. Duplicate enqueue/query calls stay idempotent.
5. A RELEASED or acknowledged ticket cannot acquire again.
6. A tracking retry may repeat the same execution identity, but cannot replace it
   with another identity. A ticket cannot track both task references and an actor.
7. Any ambiguous mutation failure stops subsequent admissions until recovery.

Task permits release only when every tracked result reference is ready. A failed
task whose error result becomes ready also completes accounting; the controller
does not need to fetch the result. Resource-consuming generators and tasks without
positive fixed returns are rejected because this mechanism cannot track their
completion correctly. Zero-resource work does not need a durable reservation.

Actor creation holds declared lifetime resources until GCS reports `DEAD`.
Actor method calls are intercepted/counted but do not reserve extra resources.
Reusing an existing named actor must not double charge: the adapter handles
`get_if_exists`, and the controller releases a duplicate reservation if it sees
the same actor ID already tracked. A temporarily restarting actor retains charge.

Acknowledgment means the client finished the submission/tracking protocol, not
that execution completed. Acknowledged active permits continue to consume quota.

## 7. Submission failure windows

| Failure point | Current behavior |
| --- | --- |
| Before approval | No Ray work submitted; client can retry short RPCs or cancel |
| RESERVED persisted, approval reply lost | Same ticket reuses reservation after retry |
| Original submission raises after SUBMITTING | Permit stays charged and `SubmissionOutcomeUnknown` identifies the ticket |
| Ray accepted work, client dies before tracking | SUBMITTING remains charged; automatic completion inference unavailable |
| Tracking fails after Ray accepted work | `SubmissionTrackingError` exposes the original handle and retry API |
| Tracking persisted, reply lost | Same identity can be retried idempotently |
| Acknowledgment reply lost | Later completed submissions resend cumulative watermark |
| Release persisted, reply lost | Repeated release remains safe; ticket cannot resurrect |

The SUBMITTING-to-TRACKED gap is real. Scanning running tasks alone cannot prove
which untracked ticket belongs to which execution, nor distinguish a delayed
submission from a failed one. Timeouts therefore do not automatically free these
permits. Operators must establish execution status before explicit reconciliation.
There is currently no dedicated safe orphan-reconciliation CLI.

## 8. GCS KV record format

All keys below live in the fixed internal KV namespace. Record values are JSON;
the administrator digest and migration marker are simple encoded values.

| Key | Contents |
| --- | --- |
| `auth-v2` | Administrator token digest |
| `pending-v2` | At most 32 record replacements/deletions for redo |
| `records-v2/quota/<encoded-key>` | `key: [namespace, job]`, `resources`, `revision` |
| `records-v2/permit/<ticket>` | `state`, active `key/resources`, optional `refs/actor_id`, `revision` |
| `records-v2/session/<session>` | Contiguous acknowledged `watermark`, `revision` |
| `controller-state-v1` | Earlier monolithic snapshot, retained for inspection |
| `migrated-v1` | Marker that legacy copy completed |

Quota suffixes use URL-safe base64 of the JSON key pair. Task references use
`ray.cloudpickle.dumps(ObjectRef)` followed by base64. Saving only an ObjectRef
hex ID is insufficient for this implementation's ownership/readiness behavior.
Restored references are cluster-specific and may retain distributed references
until permit cleanup; reference-lifetime cost needs further large-scale testing.
Actor records store the actor ID. Usage counters are derived, not independently
persisted. Revisions are diagnostics, not compare-and-set protection.

## 9. Write protocol and concurrency

Controller mutations serialize under one `asyncio.Lock`. The background writer
has a request queue bounded at 32 entries and one consumer. Synchronous GCS calls
run in a thread so storage waits do not block the actor event loop. Each commit
contains at most 32 changed records; release and compaction split larger sets.
This does not bound waiting submissions, controller memory or total retained data.

For each nonempty batch:

1. Serialize and put the complete change list at `pending-v2`.
2. Apply record puts/deletes in order; replacements and deletes are idempotent.
3. Delete `pending-v2`.
4. Publish the corresponding in-memory changes, then return successful RPCs or
   resolve admission futures.

On construction, replay a pending journal before reading records or serving
admission. A failed response may mean the write succeeded. The controller marks
storage unhealthy, fails pending waiters, and rejects later mutations; it does
not roll memory forward speculatively. Cancellation can also race a running I/O
thread and is treated as ambiguous. The writer remembers its failure.

GCS KV does not supply multi-key atomic transactions or an independent fencing
token here. Correctness assumes one fixed named actor and termination of the old
process before reconstruction. A journal is recovery intent, not a distributed
lock. Concurrent external readers may see partial batches; external mutation or
multiple writers are unsupported. A poisoned controller must be reconstructed;
transient storage recovery alone does not clear its unhealthy flag.

## 10. Restart and recovery

The detached controller is created with `max_restarts=-1` and
`max_task_retries=-1`. Its lifecycle is independent of the starting driver.
Unexpected actor failure can trigger reconstruction; explicit termination that
disables restart is different and requires an administrator to start the service.

Reconstruction order:

1. Validate credentials and legacy schema if present; authenticate `auth-v2`.
2. Replay `pending-v2` if present.
3. Complete legacy migration if needed.
4. List/read individual records and rebuild quotas, sessions and permits.
5. Sum active resource vectors into usage; deserialize task references and actor IDs.
6. Start completion monitoring and cleanup, then serve retried/new RPCs.

Running tasks and actors are not restarted by this controller reconstruction.
Their retained permits remain charged. If a task finished during downtime, its
restored references can report ready. If an actor died during downtime, a `DEAD`
state triggers release. Missing actor state is not proof of death and retains
the reservation. A terminal state purged before observation can require manual
reconciliation.

WAITING entries are not durable. Ray retries surviving in-flight actor RPCs, so
they re-enter the queue; the original global/per-key waiting order is not
preserved. A client that no longer retries does not contribute a restored waiter.
Admission counters are reset. Recovery reads O(N) retained records and is not a
constant-time startup operation.

GCS state availability follows the cluster's own GCS persistence configuration.
Controller restart tolerance does not imply survival of complete cluster/state
loss. There is no transfer of saved ObjectRefs into a replacement cluster.

## 11. Completion monitoring and health

The monitor deduplicates all tracked references and uses nonblocking `ray.wait`
in a background thread. Actor polling queries at most 64 tracked actor IDs per
one-second pass and rotates the selection, avoiding a full cluster actor-table read.

The normal polling delay is 0.1 seconds. Query failures set `monitor_error` and
back off up to 5 seconds, without freeing uncertain reservations. Successful
polling clears the error. Storage failure stops the monitor's mutations.

`status` exposes quotas, derived usage, waiting requests, active permit states,
`health.storage_error`, `health.monitor_error`, `health.accepting`, and storage
batch/write/byte counters plus retained released-record/session counts. Counters
are process-local diagnostics. `accepting` indicates absence of a storage error;
it does not guarantee quota availability, monitoring health or end-to-end latency.

## 12. Watermarks and safe compaction

A session has monotonic ticket sequences. The client records completed submission
protocols and advances only a contiguous watermark. If ticket 2 finishes before
ticket 1, the watermark cannot skip ticket 1. Watermark updates are cumulative;
a later successful completion retries an earlier failed acknowledgment.

The controller first persists the watermark, then removes covered RELEASED
records in batches. If it fails between these steps, recovery retains the
watermark and cleanup resumes. Delayed acquires below the watermark are rejected
even after their tombstones disappear. Active permits below the watermark remain
charged and are deleted directly only on release.

One session record remains per client installation. Dead clients, sequence gaps
and legacy UUID tickets can retain tombstones indefinitely. No TTL assumes that
unlimited delayed retries have stopped. If no later submission completes after
an acknowledgment failure, cleanup can remain delayed. Watermark trust is another
reason this is a cooperative protocol, not adversarial enforcement.

## 13. Storage cost and measurements

Replacing full snapshots changes write payload cost from O(all retained state)
to O(sum of changed record sizes). A K-record redo batch performs K+2 KV mutations:
one journal put, K record operations and one journal delete. It reduces payload
and serialization, not necessarily RPC count. Completion and client acknowledgment
can cause additional batches; the benchmark below is one storage update, not an
entire submission lifecycle.

Memory and recovery are O(retained quotas + permits + sessions), plus waiting
requests and client out-of-order acknowledgment sets. Compaction currently scans
retained tombstones, and legacy migration materializes its change list in memory.
These are remaining scaling costs, not eliminated by the batch bound.

Local Ray 2.58 results, 100 single-record updates per size:

| Records | Put key/value bytes per update | KV mutations | p50 / p95 ms | Record load ms |
| --- | --- | --- | --- | --- |
| 10 | 207.88 | 3 | 0.479 / 0.589 | 1.890 |
| 100 | 207.88 | 3 | 0.443 / 0.495 | 15.151 |
| 1000 | 207.88 | 3 | 0.444 / 0.503 | 151.631 |

Bytes exclude RPC framing and delete request bytes. Load time excludes complete
actor construction and task-reference recovery. Reproduce in an isolated local
environment with `PYTHONPATH=src python benchmarks/storage.py`. Results establish
constant update payload in this workload, not production throughput or capacity.

## 14. Legacy migration and operational procedure

Stop the previous named controller before starting the new implementation. Before
serving requests, validate `controller-state-v1` version and original credential,
copy quota/permit/released records in batches, then write `migrated-v1`. An
interrupted copy is replayable because service has not yet admitted new mutations.
A wrong token must not initialize `auth-v2` and block the legitimate migration.

The old snapshot remains for inspection but is non-authoritative after the marker.
Downgrading to code that only reads that snapshot would restore stale state and
is not a supported rollback. Do not erase GCS keys to repair a live controller;
this can lose charged reservations and admit too much work.

Operational diagnosis should inspect storage health first, then monitor errors,
active permit state, quota head-of-line blocking and actual execution state.
Reconstruction requires the old writer to have stopped. Explicit release of a
suspected orphan requires evidence that its execution has ended or never existed;
there is no automated proof or dedicated management command yet.

## 15. Validation and next internal iterations

The GCS conversion passed 48 regression tests. A subsequent 12-test fault-suite
run included the newly added legacy migration/token test. These runs overlap and
must not be reported as 60 distinct tests. The wheel was rebuilt/reinstalled and
the installed CLI `list`/`queue start --help` checked. Ruff and diff whitespace
checks passed.

Covered behaviors include active task/actor reconstruction, actor death during
downtime, duplicate tracking/waiters, lost commit replies, rejected/cancelled
writes, partial-batch replay, watermark cleanup, bounded grants, and transient
monitor failures. These are local tests; network partitions, GCS failover,
large clusters and long-duration memory/reference behavior need further work.

Proposed follow-ups, not current guarantees:

1. A durable submission/execution correlation protocol and safe orphan tooling
   to close or explicitly reconcile the SUBMITTING-to-TRACKED gap.
2. A session-retirement protocol with defined retry bounds before reclaiming
   watermarks; no time-only deletion while arbitrary delayed retries remain valid.
3. Indexed/incremental compaction, more selective actor queries, waiting-request
   limits and long-running storage/reference profiling.
4. Fault injection across actor termination, GCS interruption and recovery, with
   explicit verification of the single-writer assumption.
5. Explicit stop/restart/reconcile administration and noninteractive quota
   provisioning integrated with job submission.
6. Compatibility gates for Ray private APIs and a clearer supported-version matrix.

Usage commands are in [README.md](README.md); this design intentionally makes no
claim that the proposed follow-ups have already been implemented.
