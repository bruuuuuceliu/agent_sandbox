# Sandbox Manager Four-Sprint Delivery Plan

## Document Control

| Field | Value |
|---|---|
| Status | Accepted for planning; execution gated |
| Date | 2026-08-28 |
| Delivery horizon | Four two-week sprints |
| Requirements source | [Sandbox Manager SRS](../requirements/sandbox_manager_srs.md) |
| SRS approval | Version 0.1 Draft; owner and approver are unassigned |
| Delivery owner | Unassigned; must be named before Sprint 1 opens |
| Architecture decisions | Proposed; owner approval still required |
| Release target | Requirements-complete v1 sandbox manager |

## 1. Objective

Complete the initial Sandbox Manager in four sprints and produce auditable
evidence that it satisfies AC-001 through AC-025 in the SRS.

The current Python scaffold validates configuration and exposes health and
readiness endpoints. The delivery plan adds durable queue intake, lifecycle
state, Kubernetes and gVisor provisioning, persistent results, connection
publication, agent health checks, cleanup, recovery, security controls, and
operational evidence.

## 2. Planning Assumptions

The four-sprint forecast assumes:

- two backend engineers;
- one platform/Kubernetes engineer;
- part-time QA/SRE support throughout, increasing during Sprint 4;
- an available product owner and platform owner for time-sensitive decisions;
- four two-week sprints with stable team membership;
- an operational gVisor staging cluster, NATS JetStream, and a compatible
  StorageClass no later than the end of Sprint 1;
- at least one approved, connection-capable sandbox image no later than the end
  of Sprint 1; and
- CI access to local integration dependencies and the staging acceptance
  environment.

This forecast has medium confidence under those assumptions. It has low
confidence for a single engineer or when the required cluster and decision
owners are unavailable.

The ownership assumption is not currently satisfied: the SRS lists its owner,
approver, and reviewers as `TBD`. Sprint 1 must not open until named people
accept the product, platform, security, technical, and QA roles and the SRS
approval path is scheduled.

Story points in this document are relative sizing, not calendar-time
commitments. The team must recalibrate them against observed velocity after
Sprint 1. There is no valid velocity baseline yet, so the point totals size the
inventory but do not establish sprint capacity.

## 3. Pre-Sprint Decision and Ownership Gate

Decision work is a parallel governance workstream, not an engineering story,
and is excluded from sprint point forecasts. Before Sprint 1 planning, named
people must own each decision and its approval checkpoint.

Open Decisions 1-9 and 11-13 are the engineering-handoff subset. They receive
priority and must be approved early enough in Sprint 1 to freeze contracts.
Decision 10 is due before Sprint 2 planning. Decisions 14-16 may proceed on a
later review track and must close before their stated checkpoints; they must
not delay the handoff subset.

| Decision | Area | Recommended v1 baseline | Accountable role | Required checkpoint | Status |
|---:|---|---|---|---|---|
| 1 | Queue | NATS JetStream, durable consumer, explicit acknowledgement, versioned JSON, dead-letter subject | Product and platform | Sprint 1 contract freeze | Pending named owner |
| 2 | Durable state | Namespaced `SandboxTask` CRD as source of truth, using `resourceVersion` for optimistic concurrency | Platform and technical lead | Sprint 1 contract freeze | Pending named owner |
| 3 | Health transport | Authenticated NATS request/reply with unique nonce and task/session identity | Security and platform | Sprint 1 contract freeze | Pending named owner |
| 4 | Response channel | Authenticated NATS response subject carrying the endpoint and credential or secure reference | Product, security, and platform | Sprint 1 contract freeze | Pending named owner |
| 5 | Capacity scope | Global limit for the single v1 manager; count `PROVISIONING`, `READY`, `ACTIVE`, and `TERMINATING` | Product and technical lead | Sprint 1 contract freeze | Pending named owner |
| 6 | Manager topology | One replica for v1, with restart reconciliation; multi-replica operation deferred | Platform | Sprint 1 contract freeze | Pending named owner |
| 7 | Kubernetes topology | Direct Pod, private Service, Secret, PVC, NetworkPolicy, owner references, and cleanup finalizer | Platform and security | Sprint 1 contract freeze | Pending named owner |
| 8 | Sandbox profiles | Versioned allowlist, digest-pinned images, no requester image or resource overrides | Product, platform, and security | Sprint 1 contract freeze | Pending named owner |
| 9 | Storage | One configured RWO PVC per task, mounted at `/results`, retained independently from the Pod | Product and platform | Sprint 1 contract freeze | Pending named owner |
| 10 | Timing and retries | Explicit provisioning, readiness, maximum-lifetime, retry, and cleanup bounds | Product and SRE | Before Sprint 2 planning | Pending named owner |
| 11 | Lifecycle states | Keep `READY` and `ACTIVE` separate | Product | Sprint 1 contract freeze | Pending named owner |
| 12 | Cancellation | Add a terminal `CANCELLED` state | Product | Sprint 1 contract freeze | Pending named owner |
| 13 | Security model | Single-tenant v1 namespace, least-privilege RBAC, restricted Pod security, default-deny network policy | Security and platform | Sprint 1 contract freeze | Pending named owner |
| 14 | Record retention | Configured terminal, audit, log, metric, and result-retention policies | Product, security, and SRE | Before Sprint 4 planning | Pending named owner |
| 15 | Efficiency targets | Quantified manager resource budget and provisioning/health latency targets | Product and SRE | Before Sprint 4 planning | Pending named owner |
| 16 | Cleanup SLA | Quantified transition and physical resource-deletion bounds | Product and SRE | Before Sprint 3 planning | Pending named owner |

Approval of this delivery plan does not by itself approve the proposed choices
in this table. Each decision must be recorded in the SRS or an architecture
decision record with its named owner.

The Sprint 1 contract freeze requires the handoff decisions, state-transition
table, versioned message schemas, profile and resource schemas, and failure
matrix to be approved. At that checkpoint, the SRS owner and approver must move
the document from Draft to Approved. Non-policy scaffolding may proceed before
the checkpoint, but implementation that embeds an undecided policy may not.

## 4. Target Technical Shape

The proposed v1 processing path is:

```text
Agent
  -> NATS JetStream lifecycle request
  -> Manager validates and creates/reconciles SandboxTask CRD
  -> Manager acknowledges only after durable mutation
  -> Reconciler creates PVC, Secret, Pod, Service, and NetworkPolicy
  -> Pod starts with mandatory gVisor RuntimeClass
  -> Readiness succeeds
  -> Manager publishes connection response
  -> Manager performs authenticated 15s/5s health challenges
  -> Completion, cancellation, timeout, or failure starts cleanup
  -> Ephemeral resources are removed; results and terminal record remain
```

The `SandboxTask` record should contain the immutable request identity and
hash, profile and resolved image, current and target lifecycle states,
Kubernetes resource identities, result-volume identity, agent-session identity,
health-check state, connection reference, timestamps, cleanup status, and
terminal outcome.

Persistent result PVCs must not have ownership that causes them to be deleted
with the Pod or other ephemeral resources. Their retention and eventual
deletion require a separately approved policy.

### Transport Rewire Boundary

Decisions 3 and 4 can materially change the connection-response and agent
health implementations. They must be front-loaded in the handoff decision
review. The application must still define transport-neutral ports for:

- consuming lifecycle requests;
- publishing ready and terminal responses; and
- issuing and validating agent health challenges.

Core lifecycle and reconciliation code must depend on those ports rather than
NATS client types. NATS adapters implement the proposed baseline. If the owners
select WebSocket/TLS or another transport, the adapter and configuration change
without reopening the lifecycle state machine. A transport decision that also
changes message semantics or authentication remains a contract change and must
be resolved before the Sprint 1 freeze.

## 5. Critical Path

```text
Named owners and handoff-decision approval
  -> frozen lifecycle and schemas
  -> durable intake and idempotency
  -> reconciliation and capacity reservation
  -> secure gVisor provisioning
  -> readiness and connection publication
  -> health checking and termination
  -> recovery and fault injection
  -> acceptance evidence and release approval
```

Cluster preparation, sandbox-image qualification, test-harness development,
and security review should run in parallel from Sprint 1 onward.

## 6. Sprint 1: Freeze Contracts and Durable Intake

### Goal

Transform the draft requirements into an executable baseline and demonstrate
durable, idempotent request intake.

### Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Publish versioned lifecycle, response, terminal, and health message schemas | 5 | Technical lead |
| Define the `SandboxTask` CRD and versioned sandbox-profile schema | 8 | Backend and platform |
| Implement JetStream intake, request validation, and persistence-before-acknowledgement | 8 | Backend |
| Implement immutable request hashing, duplicate detection, and identifier conflict handling | 5 | Backend |
| Establish CI and deterministic redelivery, crash-boundary, and restart test harnesses | 5 | QA/SRE and backend |
| Define transport-neutral lifecycle, response, and health ports with a NATS adapter boundary | 3 | Backend and security |

The application track is approximately 34 points. Decision approvals are
excluded because they are governance checkpoints rather than engineering
effort. Stories must be split further if
the implementing team estimates any item above eight points.

### Platform Readiness Track

The platform engineer owns a parallel, explicitly counted Sprint 1 track:

| Story | Relative size | Primary owner |
|---|---:|---|
| Provision or verify the staging cluster and working gVisor `RuntimeClass` | 5 | Platform |
| Provision or verify NATS JetStream, StorageClass, credentials, and CI connectivity | 5 | Platform and SRE |
| Qualify the first digest-pinned sandbox image and approved connection path under gVisor | 5 | Platform and security |

This track adds approximately 15 points, for a combined provisional Sprint 1
inventory of 49 points. It sits inside team capacity and is not hidden
prerequisite work. The team must estimate and commit this inventory during
Sprint 1 planning. Environment access should begin as soon as named ownership
is established; inability to fit the combined inventory invalidates the
four-sprint forecast unless capacity is added.

### Exit Criteria

- Identical redelivery creates no additional durable task.
- Conflicting reuse of an identifier creates no second task or resource.
- An accepted message is not acknowledged before its state is durable.
- A crash after persistence but before acknowledgement is recovered through
  redelivery.
- Restart loses no accepted `QUEUED` task.
- The state, message, profile, failure, and security contracts are approved and
  versioned.
- Named SRS owners and approvers have changed the SRS status to Approved.
- Staging gVisor, NATS JetStream, StorageClass, CI connectivity, and the first
  sandbox image pass documented readiness checks.

This sprint supplies early evidence for AC-002 through AC-005. Those criteria
remain subject to full Kubernetes reconciliation testing in later sprints.

## 7. Sprint 2: Provision a Secure gVisor Sandbox

### Goal

Turn one valid queued request into one ready, connectable, resource-bounded
gVisor sandbox.

### Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Implement profile validation, allowlisted image resolution, and digest policy | 5 | Backend and security |
| Implement capacity reservation and the `QUEUED -> PROVISIONING` reconciler | 5 | Backend |
| Build PVC and Secret resources with task traceability and retention-safe ownership | 5 | Platform |
| Build the gVisor Pod with the required resources and restricted security context | 8 | Platform and backend |
| Build private Service and NetworkPolicy resources for the approved connection path | 5 | Platform and security |
| Observe readiness and publish an idempotent connection response | 5 | Backend |
| Add integration tests for capacity, gVisor, storage, readiness, and provisioning failures | 5 | QA/SRE and platform |

The initial total is approximately 38 points. Test-harness preparation begins
in Sprint 1, and backend and platform stories should proceed in parallel. The
Sprint 2 commitment must be set from observed Sprint 1 throughput and
role-specific capacity rather than from the provisional totals in this plan.

### Mid-Sprint Checkpoint

By the Sprint 2 midpoint, an accepted profile must reconcile far enough to
create its PVC, Secret, and gVisor Pod in staging, with readiness status
observable by the manager. If this vertical path is not green, escalate
immediately to the technical and delivery owners; do not wait for the sprint
exit. Connection publication may still be completed during the second half.

### Exit Criteria

- One valid request provisions exactly one sandbox.
- Every successful sandbox explicitly selects the configured gVisor
  `RuntimeClass`; unavailable gVisor never falls back to the default runtime.
- Aggregate workload resources equal `250m` CPU and `256Mi` memory requests and
  do not exceed `1` CPU and `512Mi` memory limits.
- Unknown types and attempted image/resource overrides create no Kubernetes
  resources.
- The eleventh request remains pending while ten sandboxes consume capacity.
- Connection information is published only after readiness and contains the
  required task, endpoint, and credential information.
- No credential appears in a log or test failure.

This sprint targets AC-001 and AC-006 through AC-014.

## 8. Sprint 3: Health, Termination, and Recovery

### Goal

Operate a sandbox safely through multiple rounds, health checks, completion,
failure, cleanup, and manager restart.

### Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Implement non-overlapping health challenges with injectable clock and nonce providers | 5 | Backend |
| Validate nonce, task, session, process epoch, and five-second deadline | 5 | Backend and security |
| Implement completion, cancellation, task deadline, Pod-loss, and platform-failure triggers | 5 | Backend |
| Implement idempotent finalizer-based cleanup and durable terminal outcomes | 8 | Backend and platform |
| Preserve result PVCs and report their identities in terminal records | 3 | Platform |
| Reconcile CRDs, Pods, and partial cleanup after restart | 5 | Backend and platform |
| Add deterministic timing, stale-response, restart, Pod-loss, and cleanup fault tests | 5 | QA/SRE |

The initial total is approximately 36 points. Failure to demonstrate the first
complete lifecycle by the midpoint of Sprint 3 is an escalation signal, not a
team failure. It indicates that the remaining acceptance and hardening work no
longer fits the forecast without corrective action.

### Exit Criteria

- The same sandbox and result volume support multiple interaction rounds.
- Every `READY` or `ACTIVE` task receives non-overlapping checks at the
  15-second cadence.
- Only a valid response received within five seconds updates the last-success
  timestamp.
- Missing, late, stale, malformed, nonce-mismatched, task-mismatched, and
  session-mismatched responses result in termination.
- Pre-restart health responses are rejected and a fresh challenge is issued.
- Completion, cancellation, deadline, Pod loss, and unrecoverable failure each
  produce idempotent cleanup and a durable terminal record.
- Repeated or partially failed cleanup affects no other task and creates no new
  resources.
- Result storage remains available after sandbox termination.

This sprint targets AC-005 and AC-015 through AC-023.

## 9. Sprint 4: Operational Proof and Release

### Goal

Produce the operational controls and auditable evidence required for release.

### Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Add correlated lifecycle, capacity, health, and cleanup structured logs and metrics | 5 | Backend and SRE |
| Add credential-redaction and automated manifest/security-policy tests | 5 | Security and QA |
| Run the complete AC-001 through AC-025 acceptance matrix | 8 | QA/SRE |
| Run manager, NATS, storage, Pod, readiness, health, and cleanup fault injection | 5 | QA/SRE and platform |
| Run capacity, timing, resource, and gVisor staging tests | 3 | SRE and platform |
| Produce deployment manifests, RBAC, dashboards, alerts, runbooks, and rollback procedure | 5 | Platform and SRE |
| Complete architecture, security, QA, and platform release review | 3 | Technical lead and owners |

The initial total is approximately 34 points. Acceptance automation and
operational assets must be developed continuously in Sprints 1-3; Sprint 4 is
for complete execution and closure, not first-time test implementation.

### Release Gate

- AC-001 through AC-025 have linked automated evidence.
- All confirmed and approved derived requirements are traceable to code,
  configuration, tests, or operational procedures.
- No critical or high security finding remains open.
- No test or operational log contains a plain-text connection credential.
- Restart, redelivery, partial cleanup, and rollback have been demonstrated.
- The approved manager resource and latency budgets are met.
- Required dashboards and alerts function in the staging environment.
- Product, platform, security, and QA owners approve the release evidence.

## 10. Acceptance-Criteria Allocation

| Acceptance criteria | Primary implementation sprint | Final verification |
|---|---:|---:|
| AC-001 | 2 | 4 |
| AC-002 through AC-004 | 1 | 4 |
| AC-005 | 1 and 3 | 4 |
| AC-006 through AC-014 | 2 | 4 |
| AC-015 through AC-023 | 3 | 4 |
| AC-024 through AC-025 | 4 | 4 |

No acceptance criterion is complete based only on a unit test when it depends
on live Kubernetes, gVisor, storage, network, or queue behavior.

## 11. Definition of Ready

A story may enter a sprint only when it has:

- a named owner and reviewer;
- linked SRS requirement and acceptance-criteria identifiers;
- testable acceptance criteria covering the intended behavior;
- approved architecture or policy decisions on which it depends;
- available environment, schema, image, and dependency prerequisites;
- a team estimate of eight points or less; and
- no unresolved security or ownership ambiguity that could change its design.

## 12. Definition of Done

A story is done only when:

- implementation and configuration are reviewed;
- deterministic automated coverage includes a normal path, a failure path, and
  a recovery or retry path where applicable;
- relevant integration or live-environment behavior is verified;
- logs and metrics provide the required correlation without exposing secrets;
- requirements traceability and operational documentation are updated;
- rollback or cleanup behavior is understood and tested; and
- no unresolved critical defect remains.

## 13. Delivery Controls

- Limit active implementation work to one primary story per engineer.
- Escalate any blocker older than one business day to the technical lead and
  dependency owner.
- Review decision status and critical-path blockers twice each week without
  adding a separate ceremony when an existing team sync can carry the review.
- Track story cycle time, blocked age, sprint carryover, acceptance tests
  passing, and unresolved critical/high defects.
- Target less than 10 percent sprint carryover. Repeated carryover requires
  smaller stories or a capacity correction, not optimistic recommitment.
- Keep acceptance and operational work inside each feature story rather than
  handing an untested batch to QA in Sprint 4.
- After Sprint 1, use observed throughput, carryover, and role bottlenecks to
  set Sprint 2 capacity. Do not compare velocity to a preselected threshold.

## 14. Risks and Escalation Triggers

| Risk | Control | Escalation trigger |
|---|---|---|
| Open decisions block implementation | Pre-sprint ownership gate and prioritized handoff subset | Named decision owners are absent at planning, or handoff decisions remain unresolved at the Sprint 1 contract freeze |
| No representative gVisor environment | Counted platform readiness track in Sprint 1 | Any platform readiness exit check is unavailable at the Sprint 1 midpoint |
| Sandbox image or connection path is undefined | Qualify one v1 profile and image during Sprint 1 | No end-to-end image and protocol by Sprint 1 exit |
| Sprint 2 scope exceeds observed capacity | Re-plan against Sprint 1 throughput and role capacity | Committed Sprint 2 inventory exceeds observed capacity, or the ready-sandbox vertical path is not green at midpoint |
| Health or response transport changes | Front-load Decisions 3-4 and isolate adapters behind transport-neutral ports | A proposed transport changes lifecycle semantics after the Sprint 1 contract freeze |
| Five-second health deadline is unreliable | Use low-latency authenticated transport and deterministic timing tests | Staging latency cannot maintain an approved safety margin |
| Queue and CRD cannot be mutated transactionally | Use at-least-once processing, immutable request hashes, and idempotent reconciliation | Any fault test creates duplicate resources or loses accepted work |
| PVC retention causes data loss or unbounded cost | Separate ownership and approve retention/janitor policy | Retention policy is still open before Sprint 2 planning |
| Security hardening arrives too late | Security reviews schemas and manifests from Sprint 1 onward | Critical/high finding remains at Sprint 4 midpoint |
| Sprint 4 becomes implementation overflow | Enforce sprint exit demonstrations | First ready sandbox misses Sprint 2 or full lifecycle misses Sprint 3 midpoint |

If a trigger fires, the project owner must add capacity, reduce only explicitly
optional scope, or extend the schedule. Confirmed requirements and acceptance
evidence must not be silently deferred to preserve the four-sprint label.

## 15. Immediate Actions

1. Assign named product, platform, security, technical, QA, SRS-owner, and
   SRS-approver roles; update the SRS document-control tables.
2. Schedule the handoff review for Decisions 1-9 and 11-13, with a named owner
   and prepared recommendation for each decision.
3. Schedule Decision 10 before Sprint 2 planning, Decision 16 before Sprint 3
   planning, and Decisions 14-15 before Sprint 4 planning.
4. Re-estimate and commit the 34-point application track and 15-point platform
   readiness track against actual team capacity.
5. Begin provisioning or verifying NATS, gVisor staging, StorageClass, and CI
   access as soon as the platform owner is named.
6. Select and qualify the first sandbox profile, image, and connection path.
7. Create Sprint 1 stories with requirement IDs and Definition of Ready checks.
8. Establish the acceptance-evidence index that Sprint 4 will use for release.
