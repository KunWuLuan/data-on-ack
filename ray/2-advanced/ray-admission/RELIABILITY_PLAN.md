# Queue Reliability Revision

Status: core protocol implemented and pending internal acceptance following review
of `7c8250d`.
This supplements [the current design](DESIGN.md). See the corresponding
[Chinese specification](RELIABILITY_PLAN_ZH.md).

## 1. Decisions and release boundary

| Priority | Finding | Decision |
| --- | --- | --- |
| P1 | Waiting acquires exhaust actor call slots | Short registration/query RPCs, client-side waiting, isolated capacity and bounded ingress |
| P1 | Uncertain submissions release quota | Persist SUBMITTING; only cancel reservations known not to have entered submission |
| P1 | Storage failure leaves a permanently unhealthy service | Recovery coordinator stops the writer, replays and rebuilds; administrative recovery fallback |
| P2 | Tracking failure loses the execution handle | Retain ticket-to-handle association; explicit submitted-but-untracked error and retry API |
| P2 | Whole-service cost is unbounded | Session indexes, coalesced acknowledgments, monitoring budgets and lifecycle benchmarks |

Preserve GCS KV single-writer ownership, persistence before approval, usage rebuilt
from active permits, durable release before capacity reuse, and watermark-based
retry rejection. Do not introduce another concurrently writing controller or an
external database.

Complete and validate all three P1 fixes before recovery acceptance. Include a
minimal usable handle-recovery API so an ordinary submission exception does not
hide accepted execution. Performance improvements may follow, but stable throughput
claims require measured capacity limits.

The implementation now includes short RPCs, SUBMITTING, process-local handle
recovery, coordinated writer recovery, `queue recover`, per-session tombstone
indexes, 50 ms acknowledgment coalescing, actor-table query throttling and a
lifecycle benchmark. Cross-process handle recovery, session retirement, selective
actor-ID queries and automatic forced restart of a stuck I/O thread remain open;
a stuck writer enters RESTART_REQUIRED instead of reporting false readiness.

## 2. Short-RPC admission protocol

Replace the indefinitely pending acquire with:

1. `enqueue(ticket, request)`: idempotent registration returning WAITING, RESERVED
   or an explicit error.
2. `inspect(ticket)`: current status, suggested polling delay and incarnation.
3. Client polling with jittered backoff and at most one in-flight query per ticket.
   FIFO is determined by registration, not polling frequency. An independent
   drainer grants capacity; queries cannot double-charge.
4. On RESERVED, call `begin_submit(ticket)`. Invoke the original Ray submission
   only after this transition has returned durable success.
5. If a restart loses WAITING, return NOT_REGISTERED and re-register the same
   ticket/request. Do not promise pre-failure order. Released/acknowledged tickets
   return TERMINAL, never an ambiguous missing-record result.

WAITING remains volatile; RESERVED/SUBMITTING/TRACKED are durable. Lost replies
retry/query the same ticket, never create a new ticket for the same protocol
attempt. Conflicting request contents are rejected.

Short RPCs remove permanent slot occupancy but do not alone prevent management
starvation under a flood. Reserve independent scheduling capacity for management/
recovery, tracking/release, and enqueue/query. Before using Ray concurrency groups,
validate async actor thread/event-loop semantics on Ray 2.58; an asyncio.Lock
cannot simply be shared across threads. If safe isolation in one loop is not
available, use a stateless rate-limiting ingress actor with a separate state actor
as the only writer. Increasing max_concurrency is not an acceptable fix by itself.

Use bounded internal priority queues: recovery/release/tracking before polling,
with work budgets per iteration. The drainer must not hold the mutation lock while
draining every quota indefinitely. Management snapshots return the last committed
state and health without waiting for the write lock. Bound global/per-Job waiting
counts and request bytes; return OVERLOADED explicitly. Choose defaults from
capacity tests, not by treating test loads as production settings.

## 3. Separate permit state from submission uncertainty

| State | Charged | Automatic release condition |
| --- | --- | --- |
| WAITING | No | Local cancellation plus idempotent deregistration |
| RESERVED | Yes | Submission not yet permitted; cancel serializes against begin_submit |
| SUBMITTING | Yes | Neither a client exception nor elapsed time is sufficient |
| TRACKED | Yes | Execution reaches terminal state, or evidence-backed reconciliation |
| RELEASED | No | Delete tombstone after durable acknowledgment coverage |

`begin_submit` durably changes RESERVED to SUBMITTING. The client cannot invoke
the original submission before its successful response. Lost responses retry/query
the transition without invoking user work twice. A per-ticket local mutex prevents
concurrent submission attempts; recovery retries registration, not user execution.

If cancel commits first, begin_submit must reject the terminal ticket. If
begin_submit commits first, cancel returns SUBMISSION_UNKNOWN without releasing.
Legacy RESERVED records may already represent submitted work: migrate them
conservatively as SUBMITTING. Introduce a schema/protocol version and reject
incompatible clients; old and new wrappers cannot safely share these semantics.

Classify errors by stage and evidence, not exception class:

- Validation completed before entering the submission protocol can cancel a
  reservation that has not reached SUBMITTING.
- After durable begin_submit but before invoking Ray, retain conservatively by
  default; a provable revocation protocol can be added later.
- Any exception after entering the original Ray call, including KeyboardInterrupt,
  SystemExit, timeout or an apparent serialization error, means unknown outcome
  by default. Do not release or acknowledge that ticket.
- Once a handle exists, execution was submitted; use tracking recovery rather
  than reporting a generic submission failure.

Expose `SubmissionOutcomeUnknown(ticket, phase, cause)`. Interrupt handling may
preserve KeyboardInterrupt semantics while attaching the ticket and recording
the uncertain attempt in a diagnostic registry; it must not swallow interrupts.
Without a handle, unknown execution needs operator reconciliation or a future
execution-correlation protocol. Reconciliation records evidence and rejects
unsupported release; elapsed time and absence from a task listing are insufficient.

This prevents early capacity return. It does not atomically commit across Ray
submission and GCS, nor guarantee exactly-once execution across arbitrary driver loss.

## 4. Tracking retry and handle recovery

Immediately after receiving an ObjectRef or actor handle, retain
`ticket -> {handle, execution_identity, tracking_state, last_error}` locally,
then call track. Retry the same identity idempotently. If the bounded retry period
expires, raise `SubmissionTrackingError(ticket, handle, retry_tracking)`, explicitly
meaning submitted but not registered. `SubmissionTrackingError`,
`retry_tracking()` and `get_submission()` now implement these process-local APIs.

The caller can use `ray.get(error.handle)` or the actor handle and call
`retry_tracking(ticket)`. That operation registers the original execution and
never invokes `.remote()` again. `get_submission(ticket)` exposes the retained
association. Acknowledge only after tracking is durable; do not automatically
generate another ticket to resubmit work on a tracking failure.

Bound the local unregistered-handle registry. Apply backpressure before the next
begin_submit when full; never evict unregistered handles to make room. Successfully
registered entries may be removed, with a documented lookup retention boundary.
Process death loses this registry: cross-process handle recovery requires a
separate durable execution-association protocol and is not promised here.

## 5. Storage recovery service state machine

States are READY, QUIESCING, RECOVERING, BACKOFF, RESTART_REQUIRED and
MANUAL_REQUIRED. A write uncertainty moves READY to QUIESCING, then RECOVERING.
Successful replay returns READY; transient failure enters BACKOFF and retries
RECOVERING. An unquiesced writer requires restart; corrupt/unsupported state
requires manual intervention.

Immediately close admission/mutation ingress with retryable SERVICE_RECOVERING,
while retaining health and authenticated recovery access. Stop consuming new
writer requests and fail queued-but-not-executed mutations explicitly. An existing
I/O thread may still run: cancelling an asyncio task does not terminate that thread.

One recovery coordinator handles both automatic and administrative triggers:

1. Use a local scheduling generation to prevent stale continuations from publishing
   memory or successful replies. Stop drainer/monitor mutation activity.
2. Wait for actual in-flight I/O completion; prove no old write thread can issue
   further GCS requests.
3. Replay the single redo journal, validate records and reconstruct temporary state.
4. Atomically replace memory, install a fresh writer, resume monitors/scheduling,
   then reopen ingress.
5. If GCS remains unavailable, stay closed, retry with exponential jittered backoff,
   and report attempts, phase and latest error.

The coordinator must not depend on slots exhausted by waiting admission RPCs.
Clearing `_storage_error`/writer.error without replay is forbidden, as is replacing
only the writer while retaining possibly stale usage.

If I/O cannot finish within a bounded period, enter RESTART_REQUIRED. The proposed
`ray-admission queue recover` authenticates the administrator and reports progress.
When safe in-process recovery is impossible, request termination/reconstruction
through Ray. Declare success only after the old process is terminated and old
in-flight writes cannot affect the new writer. Name disappearance, RPC timeout,
or a fixed sleep is insufficient evidence. Fault-inject and verify Ray/GCS ordering;
if that premise cannot be established, remain closed for operator intervention.
Do not claim automatic partition-safe failover.

Ordinary transient failures with completed I/O recover automatically in process;
forced reconstruction is a controlled fallback. `queue start` must diagnose an
unhealthy existing service and exit nonzero instead of reporting authentication
as readiness. Status adds recovery_state, incarnation, last_successful_commit,
attempts, writer_inflight and recovery duration. Incarnation supports diagnostics
and client re-query, not GCS fencing.

## 6. Indexes, acknowledgment coalescing and monitoring budgets

- Build ordered `released_by_session` sequence indexes on recovery. A watermark
  visits only covered records for that session; isolate legacy UUID tombstones.
  Do not scan every tombstone on each acknowledgment.
- Coalesce client watermarks by time/count with one in-flight update per session.
  Merge concurrent server updates to their maximum and batch where possible.
  Persist before deleting tombstones or responding with durable acknowledgment.
- Retain the maximum pending watermark on flush failure. Attempt bounded flush
  on orderly shutdown; crashes delay cleanup. A timer retries even when there
  are no subsequent submissions, closing the last-failed-acknowledgment gap.
- Acknowledgment never releases active work. Session record reclamation still
  needs a retirement protocol, not a TTL assumption about delayed retries.
- Budget completion polling by time/count. Validate selective actor-ID APIs first.
  If a full actor-table read remains necessary, rate-limit it and disclose
  O(cluster actors) cost rather than claiming constant overhead.

`benchmarks/lifecycle.py` now reports lifecycle RPC/storage counts, latency,
throughput and retained records. Sustained benchmarks must additionally count
enqueue/inspect/begin_submit/track/ack/release
RPCs, GCS get/list/put/delete operations and bytes, throughput, admission and
end-to-end p50/p95/p99 latency, controller CPU/memory/event-loop delay, retained
records and storage growth. Compare whole old/new protocols, not one commit.
Include short/long tasks, contention, abandoned sessions, uncompacted tombstones,
many unmanaged actors and recovery; measure growth over millions of submissions.
Report hardware, Ray version, concurrency, quotas, sample size and duration.
Do not set an unmeasured production throughput promise.

Current local Ray 2.58 smoke result (2 CPUs, 100 tasks at 0.01 CPU each):
53.37 tasks/s; submit p50/p95/p99 16.906/24.325/48.630 ms; 100 completions
coalesced into 28 acknowledgment RPCs; zero retained permits and waiters. This
small run validates the measurement path, not production capacity.

## 7. Required acceptance matrix

| ID | Fault/load | Required assertion |
| --- | --- | --- |
| R1 | quota=0; 2,000 registrations or at least twice the configured call concurrency | status, quota mutation, track/release finish within explicit test deadlines; raising quota drains work; no long-lived slot occupancy |
| R2 | enqueue/query flood with repeated management calls | Bounded overload, no management/completion starvation; record queue depth and latency |
| R3 | Original submission starts work, then raises before returning a handle, including KeyboardInterrupt | Capacity retained, next request not over-admitted, unknown ticket identifiable |
| R4 | cancel races begin_submit; lost response retries | Safe cancellation or SUBMITTING, never both; no duplicate execution for one ticket |
| R5 | Error after journal or Kth record write, then healthy GCS | Automatic service recovery without manually constructing a controller; correct quota/usage; waiting clients resume protocol |
| R6 | Block I/O thread during recovery, then unblock | No new writer before old thread ends; no stale publication; health remains accessible |
| R7 | Stuck writer, concurrent recovery triggers, corrupt state | Explicit RESTART_REQUIRED/MANUAL_REQUIRED; no dual writers or false readiness |
| R8 | Track fails after handle exists, including durable write/lost reply | Error exposes original handle/ticket; registration retry succeeds; business executes once |
| R9 | Crash while coalescing acknowledgments/deleting records; delayed acquire | Active permits preserved, no resurrection below watermark, resumable cleanup |
| R10 | Many unrelated-session tombstones and unmanaged actors | No all-tombstone scan for one session; measured lifecycle cost and long-term growth |

R1 deadlines are explicit CI budgets, not production SLOs. Tests must not pass by
inflating actor concurrency, waiting indefinitely or merely constructing a new
Python controller instead of exercising service recovery.

Implementation order: protocol/schema definitions; short RPCs and scheduling
isolation; submission phases and handle associations; writer recovery and CLI;
indexes/coalescing; fault matrix and sustained benchmarks. Update both language
documents and the validated-capabilities list at each step. Remain in internal
iteration until the acceptance matrix passes.
