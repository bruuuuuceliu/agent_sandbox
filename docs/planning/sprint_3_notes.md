# Sprint 3 Engineering Notes

## Status

The Python lifecycle slice is implemented and locally verified. Live-cluster
acceptance remains pending because this workspace has no configured Kubernetes
context, gVisor `RuntimeClass`, StorageClass, CNI test environment, qualified
sandbox image, durable-state adapter, or authenticated health transport.

The SRS is still a draft with unassigned owners and approvers. Finalizer use,
cleanup deadlines, cancellation policy, transport selection, and retention are
still open decisions. The implementation keeps those choices behind explicit
ports and does not treat an unapproved recommendation as deployment policy.

## Delivered Scope

- Async non-overlapping health checks with a 15-second cadence, five-second
  timeout, injected clocks, secure nonce generation, and a fresh process epoch.
- Validation of the transport-authenticated session, nonce, task, session, and
  process epoch. Missing, late, malformed, stale, mismatched, or unauthenticated
  responses are unresponsive outcomes.
- Constant-space nonce uniqueness from a monotonic sequence and secure random
  value within each process epoch. Challenge and response representations hide
  raw nonces.
- Durable health-success records containing the successful timestamp and a
  challenge digest. An ambiguous health persistence result stops supervision
  without initiating cleanup.
- Durable `READY`, `ACTIVE`, `TERMINATING`, and terminal lifecycle records for
  completion, cancellation, deadline, Pod loss, platform failure, and agent
  unresponsiveness.
- Per-task lifecycle serialization, repository-return validation, and ordered
  cleanup progress recorded after each idempotent side effect.
- Async deadline scheduling with one replaceable timer per task, monotonic
  waiting, immediate expired-deadline handling, owned in-flight termination,
  graceful shutdown, and an observable failure channel.
- Restart reconciliation that resumes partial cleanup and terminates missing
  `READY` or `ACTIVE` workloads as `FAILED / POD_LOST`.
- Kubernetes cleanup that persists Pod and PVC UIDs, verifies deterministic
  identity and ownership before deletion, waits for disappearance, rejects
  replacement races, and confirms the original result PVC remains present.

## Persistence Contracts

The durable repository must implement compare-and-set transitions against the
exact source record. It must reject stale sources, preserve the first selected
termination decision, and never substitute another trigger or outcome in a
returned record. Cleanup-step writes and terminal writes must be idempotent.

A successful health update may only commit while the same `READY` or `ACTIVE`
record and owning session remain current. The supervisor validates the returned
record before continuing. A deployment adapter must resolve ambiguous writes
through recovery rather than guessing whether cleanup is safe.

## Transport Contracts

The health engine accepts a response only when the adapter supplies a verified
session identity separate from the response's correlation fields. The adapter
must authenticate each response, bind the authenticated principal to the
durable agent session, enforce transport authorization, and avoid logging raw
challenge or credential material.

The manager process must create a new random process epoch at startup. Restart
recovery starts new health monitors; responses carrying a previous epoch cannot
satisfy the new challenge.

## Local Evidence

| Area | Evidence | Remaining gate |
|---|---|---|
| Health timing | Deterministic cadence, timeout, late-response, and non-overlap tests | Authenticated transport latency and outage behavior in staging |
| Response integrity | Constant-space nonce uniqueness plus task, session, epoch, malformed-response, and authenticated-identity tests | Approved transport credentials and authorization policy |
| Durable supervision | Success timestamp persists before the next round; ambiguity prevents cleanup; unresponsiveness reaches a terminal record; competing in-memory managers preserve the first outcome | Production compare-and-set repository and restart fault injection |
| Terminal lifecycle | All six triggers select the expected state and reason; cleanup steps are ordered and durable | Approved cancellation and cleanup policies |
| Deadlines | Deterministic timer, replacement, cancellation, expired deadline, setup failure, in-flight shutdown, and termination-failure tests | Runtime startup scheduling and operational failure consumer |
| Kubernetes cleanup | Persisted Pod/PVC identity, UID-preconditioned deletion, disappearance checks, ownership mismatch, replacement race, partial retry, and PVC retention tests | Real API deletion, watch behavior, RBAC, grace periods, and CNI behavior |
| Restart | Partial cleanup resumes; missing ready and active Pods terminate as Pod loss | Durable adapter recovery against a real cluster |
| End-to-end domain flow | Intake, capacity, ready publication, unresponsive cleanup, PVC reference retention, and capacity release | Queue, state, transport, and Kubernetes adapters together in staging |

The local verification run completed with:

- 112 passing unit and integration tests;
- Ruff lint and format checks passing;
- strict MyPy checks passing for `sandbox_manager` and `tests`;
- bytecode compilation passing;
- a successful wheel build; and
- a successful non-root Docker image build tagged `agent-sandbox:sprint3`.

## Staging Checklist

Before closing Sprint 3 in the delivery plan, platform, security, and QA owners
must record evidence for:

1. The durable repository atomically selects one terminal decision under
   concurrent completion, cancellation, deadline, health timeout, and Pod loss.
2. Restart recovery resumes partial cleanup, starts fresh health challenges,
   and neither duplicates nor deletes another task's resources.
3. The health adapter authenticates the responding principal, rejects replay
   and previous-process responses, and meets the five-second deadline with an
   approved safety margin.
4. UID-preconditioned deletion observes actual Pod and child-resource removal
   before capacity release, including API interruption and name-replacement
   races.
5. The expected PVC remains bound, undeleted, readable, and writable after all
   terminal paths and repeated cleanup.
6. Runtime wiring schedules health and deadline supervision for every recovered
   `READY` and `ACTIVE` task, cancels obsolete timers, and consumes background
   failure reports.
7. If a task CRD and finalizer are approved, the finalizer protects only task
   cleanup. The retained PVC must have neither the cleanup finalizer nor a
   deletion-cascading owner reference.

Operational logs, metrics, alerting, and the combined staging fault campaign
remain Sprint 4 work.
