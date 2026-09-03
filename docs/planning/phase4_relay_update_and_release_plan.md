# Sandbox Manager Phase 4 Relay Update and Completion Plan

## Document Control

| Field | Value |
|---|---|
| Document ID | SM-P4-001 |
| Status | Draft for owner approval; not an implementation baseline |
| Date | 2026-08-28 |
| Supersedes | The Sprint 4 scope in [the four-sprint delivery plan](./four_sprint_delivery_plan.md); Sprints 1-3 remain delivery history |
| Requirements source | [Sandbox Manager SRS](../requirements/sandbox_manager_srs.md) v0.1 Draft and [initial requirements](../requirements/init_req.md) |
| Delivery position | Sprints 1-3 application scope complete and locally verified; staging gates pending |
| Architecture change | Manager-mediated command relay replaces direct agent-to-sandbox connection; proposed, owner approval required |
| Delivery horizon | Three two-week planning stages (4a, 4b, 4c); six sprints total including Sprints 1-3 |

This document is valid for implementation only after the decision and contract
gates in Stage 4a are approved. Point totals size the current inventory; they do
not establish a commitment until they are reconciled with observed throughput.

## 1. Current Position

Sprints 1-3 delivered the application slice in Python with durable lifecycle
contracts, profile and image policy, capacity control, gVisor provisioning
manifests, health supervision, deadlines, cleanup, and restart reconciliation.
Local verification stands at 112 passing tests, clean lint and strict type
checks, a wheel build, and a non-root container image.

What does not yet exist:

- live-cluster evidence (the workspace has no Kubernetes context, gVisor
  `RuntimeClass`, StorageClass, or qualified relay-capable sandbox image);
- deployment adapters for the queue, durable repository, health transport, and
  Kubernetes client;
- operational logging, metrics, alerting, and the staging fault campaign; or
- an agent-facing command path. The operational server exposes only `/healthz`
  and `/readyz`.

The Sprint 2 and Sprint 3 staging checklists remain open gates. Stage 4a now
pulls forward the minimum cluster and Kubernetes-client work needed to validate
the relay architecture. Stage 4c closes the remaining staging and release work.

## 2. Architecture Update: Manager-Mediated Command Relay

### 2.1 Decision Summary

The agent must not connect to a sandbox endpoint. It uses one authenticated
manager transport. The owning manager worker authorizes each operation, checks
the current durable task and Pod identity, relays it through the Kubernetes API,
and returns framed output and a terminal or unknown outcome.

~~~text
Before (Sprints 1-3):                 Proposed v1:

Agent --direct--> Sandbox endpoint    Agent --authenticated transport--> Manager
  ^        Service + ingress policy                                    |
  |        per-task credential                         authorize + fence task state
  +-- manager publishes endpoint                                      |
     and credential after readiness                    Kubernetes pods/exec
                                                                        |
                                                                    Sandbox Pod
                                                           (no inbound Service)
~~~

The relay is a good v1 fit for at most ten sandboxes per worker, bounded command
streams, and capped file transfers. Kubernetes `pods/exec` is not treated as a
durable job protocol: a lost stream can leave a command with an unknown outcome.
Long-running, restart-transparent jobs or high-volume data transfer require a
later in-sandbox command supervisor or a different data plane.

Rationale:

1. **Stable agent surface.** Agents use the manager transport and task identity,
   not a Pod address or per-sandbox Service.
2. **Central authorization.** Every operation passes through one admission and
   audit point tied to the authenticated agent session and current task state.
3. **Workspace operations.** Upload and download follow one bounded, path-safe
   contract instead of depending on image-specific network servers.
4. **Future worker routing.** Task-scoped routing permits later ownership
   handoff, but multi-worker takeover remains out of v1 scope and is not a
   release claim.

Sandbox inbound exposure and sandbox connection credentials disappear. Agent
transport credentials do not disappear: their issuance, storage, rotation,
revocation, and binding to an agent session remain explicit security concerns.
The manager service account also becomes a remote-execution authority and must
receive narrowly scoped `pods/get` and `pods/exec` permissions with API audit.

### 2.2 Changes to the Delivered System

| Delivered component | Required change |
|---|---|
| `ConnectionReference` and ready publication (`provisioning.py`) | Replace with a task/session relay reference containing protocol version and route, but no sandbox endpoint or sandbox credential |
| Private Service and ingress NetworkPolicy (`kubernetes.py`) | Remove the Service and ingress allow policy; retain deny-by-default ingress and apply approved per-profile egress policy |
| Per-task connection credential (`SandboxCredentialProvider`) | Remove from the sandbox connection path; retain only explicitly profile-required workload secrets |
| Profile connection settings | Replace protocol, port, and ingress peer with relay capabilities, policy reference, workspace rules, container identity, and egress policy |
| `wait_ready` | Report readiness and verified Pod identity; do not construct a connection endpoint |
| Lifecycle access revocation | Close relay admission durably before workload cleanup; serialize command admission with termination |
| Kubernetes cleanup references | Remove Service and ingress-allow references while preserving UID-checked cleanup and result PVC retention |
| Capacity limiter | Remains the single-worker implementation; deployment recovery seeds it from durable state |

### 2.3 Decision Deltas

| Decision | Updated recommended baseline | Status |
|---:|---|---|
| 4 | Ready publication carries task ID, owning agent-session reference, relay protocol version, and route; it carries no sandbox endpoint or sandbox credential | Proposed |
| 7 | Direct Pod, `/workspace` ephemeral storage, retained results PVC, optional workload Secret, ingress-deny NetworkPolicy, approved per-profile egress rules, and Kubernetes `pods/exec` relay | Proposed |
| 13 | Single-tenant v1; sandbox ingress always denied; sandbox egress denied unless a profile explicitly permits DNS or approved destinations; agent-to-manager policy is enforced at the manager transport | Proposed |
| 17 | `pods/exec` invokes one fixed, qualified relay helper so user argv does not enter Kubernetes request/audit data; a durable in-sandbox supervisor or reverse channel is required before claiming durable command execution | Proposed |
| 18 | Versioned structured control envelopes plus bounded, sequenced data frames for `exec`, optional `pty`, `upload`, and `download`; opaque interactive content is never represented as policy-inspectable | Proposed |
| 19 | A versioned `CommandPolicy` port and profile policy reference are mandatory, but the allowlist language, filtering methods, and matching rules remain placeholders pending a separate security decision | Proposed placeholder |
| 20 | Every command has a unique command ID and a terminal outcome or `UNKNOWN`; transport loss and manager restart never imply that remote execution stopped, and the manager never automatically re-issues an unknown command | Proposed |
| 21 | `/workspace` is ephemeral and separate from retained `/results`; transfer roots, path handling, quotas, and persistence behavior are part of the profile contract | Proposed |
| 22 | The transport supplies an authenticated principal independently of message fields and binds it to the durable agent session; a caller-provided session ID is not proof of identity | Proposed |

The recommended transport to validate in Stage 4a is authenticated Core NATS:
request/reply for command admission and task-scoped ephemeral subjects for
bounded frames. JetStream remains the durable lifecycle transport and is not
used to replay stale interactive frames. The Stage 4a spike must either approve
this framing and flow-control design or replace it before schema freeze.

### 2.4 Requirements and Traceability Rewrite

The architecture changes the governing requirements, not only the ready-response
shape. Before freezing Stage 4a schemas, update both the SRS and the initial
requirements and rebuild their traceability tables. At minimum revise:

- `FR-014`, `FR-025`, and `FR-029` through `FR-035` for relay capabilities,
  readiness, multi-round use, reconnect, and unknown command outcomes;
- the `READY` and `ACTIVE` definitions and transitions so first command
  admission activates a task and termination closes admission first;
- `SR-004`, SRS Sections 10.3 and 10.5, the response-publication failure case,
  and all endpoint, credential, replay, and connection wording;
- `KR-006` and `SEC-004` through `SEC-007` for `pods/exec`, Pod UID validation,
  manager RBAC, authenticated principals, egress, and credential lifecycle;
- storage requirements for the ephemeral workspace, retained results, transfer
  roots, partial files, quotas, and Pod-loss behavior;
- operational requirements for relay frames, command outcomes, policy decisions,
  admission denials, backpressure, and manager/API-server resource use;
- `AC-013`, `AC-014`, and the new criteria in Section 7 of this plan;
- the SRS failure table, risks, use cases, glossary, traceability table, and open
  decisions; and
- the initial requirements' purpose, connection requirement, use case, initial
  acceptance criteria, and connection-method design decision.

The SRS remains a draft with unassigned owners. No command schema is frozen and
no direct path is deleted until these changes and Decisions 17-22 are approved.

## 3. Phase 4 Restructure

The previous document listed 46 points in Stage 4a while labeling it 37, and 44
points in Stage 4b while labeling it 38. Correcting the arithmetic and adding
the missing contracts produces more work than two credible planning stages.
Phase 4 is therefore split into three stages:

| Stage | Goal | Current inventory |
|---|---|---:|
| 4a | Contract correction and real-cluster relay proof | 36 points |
| 4b | Relay implementation, workspace operations, and direct-path removal | 39 points |
| 4c | Deployment integration, staging acceptance, and release | 44 points |

All totals are provisional. Each stage must be re-estimated against observed
Sprint 1-3 throughput and role capacity before commitment. The staging
environment is a long-lead platform track that starts immediately and proceeds
in parallel; its work remains visible in Stage 4c's inventory.

## 4. Stage 4a: Contract and Feasibility Gate

### 4.1 Goal

Produce an approved, testable relay contract and prove the riskiest Kubernetes,
transport, authentication, and image assumptions in a representative cluster
before committing the migration.

### 4.2 Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Update the SRS, initial requirements, traceability, decisions, and versioned relay schemas | 5 | Technical lead and product |
| Specify and spike authenticated Core NATS control/data framing, flow control, reconnect, caps, and principal-to-session binding | 5 | Backend, platform, and security |
| Define transport-neutral command, outcome, session, relay, workspace, and audit ports, including `UNKNOWN` outcomes | 5 | Backend |
| Define durable command-admission fencing, `READY -> ACTIVE`, one-in-flight-command v1 behavior, and termination races | 5 | Backend |
| Implement a thin Kubernetes client spike in staging for `pods/get` and `pods/exec`; verify RBAC, UID/ownership checks, stream loss, cancellation behavior, and one qualified image | 8 | Backend and platform |
| Add the versioned `CommandPolicy` extension point, profile reference, default-deny missing-policy behavior, and reserved evaluation fields; leave filtering methods as placeholders | 3 | Backend and security |
| Add contract tests for authentication mismatch, policy denial, stale task state, Pod replacement, frame limits, and unknown outcomes | 5 | QA/SRE and backend |

### 4.3 Contract Rules

- A command starts with a versioned control envelope containing command ID, task
  ID, operation, protocol version, declared sizes, and correlation information.
- The transport adapter supplies the authenticated principal separately. The
  domain layer compares it with the owning durable agent session and never
  trusts a session ID merely because it appears in the payload.
- Data frames identify command ID, channel, and sequence and have fixed maximum
  sizes. The protocol defines bounded buffering, backpressure, missing or
  duplicate frame handling, cancellation requests, and terminal frames.
- The v1 manager admits at most one command or transfer per task at a time.
  Admission atomically observes `READY` or `ACTIVE`, records activation when
  needed, and acquires the current process/worker fence. A durable transition to
  `TERMINATING` closes admission before cleanup begins.
- Immediately before `pods/exec`, the adapter reads the Pod and verifies its UID,
  task annotation, labels, namespace, and configured container against the
  durable task record. A deterministic name alone is insufficient.
- Every `pods/exec` request invokes a fixed helper path and fixed bootstrap argv
  from the qualified profile. The operation and user command are sent through
  the bounded stdin protocol, not as Kubernetes exec query arguments. This
  prevents user argv from entering Kubernetes request URIs and API audit events.
- `SUCCESS`, `FAILED`, `REJECTED`, and confirmed `CANCELLED` are terminal command
  outcomes. Stream loss, unconfirmed cancellation, timeout without confirmed
  process exit, and manager restart produce `UNKNOWN`.
- The manager never automatically retries an `UNKNOWN` command. A caller may
  submit a new command ID after reconnection, accepting application-level
  duplicate risk. Built-in operations may later define narrower idempotency
  keys where their implementation can prove the behavior.
- Policy evaluation is mandatory before every side effect, including exec,
  PTY creation, transfer setup, resize, and cancellation where applicable.
  Missing or invalid policy configuration denies admission.
- The policy schema reserves normalized fields for operation, executable, argv,
  working directory, environment names, path, size, profile, and authenticated
  principal. Exact allowlist syntax, filters, normalization rules, and matching
  strategy remain intentionally unspecified placeholders.
- Enabling PTY grants an opaque interactive capability after admission. The
  plan does not claim that subsequent PTY input can be inspected or filtered.
  Profiles keep PTY disabled unless an owner explicitly accepts that capability.
- Command arguments, environment values, file contents, PTY input, and stream
  data are never logged by the manager or placed in Kubernetes request metadata.
  Manager audit records contain identifiers, operation, policy version and
  decision reference, sizes, outcome, latency, and the Kubernetes audit/request
  correlation identifier when the client exposes it.

### 4.4 Exit Criteria

- Decisions 17-22 and the complete requirements rewrite are owner-approved.
- The selected agent transport demonstrates authenticated command admission,
  bounded frames, backpressure, cancellation signaling, and reconnect behavior.
- A real gVisor Pod accepts a bounded `pods/exec` command through least-privilege
  manager RBAC after stored UID and ownership validation.
- Fault injection demonstrates and documents the actual behavior when the
  manager, transport, or exec stream disappears; no test equates disconnect
  with confirmed process termination.
- The qualified image remains alive for exec and supplies the fixed relay helper
  used for command launch and transfer; the implementation does not assume that
  arbitrary images contain a shell, `tar`, or other undeclared tools.
- The policy port, profile reference, reserved evaluation model, denial audit,
  and non-bypass tests exist. Filtering and allowlist methods remain marked TBD.
- The Stage 4b estimate is refreshed using spike results. Failure of the API
  server, transport, or image contract to meet approved budgets triggers an
  architecture review before migration begins.

## 5. Stage 4b: Relay Implementation and Cutover

### 5.1 Goal

Implement the bounded authenticated relay and workspace operations, integrate
them with lifecycle fencing, and remove the unused direct connection path.

### 5.2 Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Implement the production `pods/exec` relay adapter using the fixed helper, with framed stdin/stdout/stderr, deadlines, best-effort cancellation, outcome classification, caps, and UID checks | 8 | Backend |
| Implement the authenticated agent-facing NATS command API and session routing from the approved Stage 4a contract | 8 | Backend and security |
| Integrate durable command admission, activation, one-command serialization, termination fencing, and restart recovery | 5 | Backend |
| Add `/workspace`, the qualified relay-helper image contract, and path-safe atomic upload/download under quotas and checksums | 8 | Backend and platform |
| Remove Service, ingress-allow policy, sandbox connection credentials, and connection profile fields; update cleanup references and ready publication | 5 | Backend and platform |
| Extend flow tests for normal relay use, policy denials, cross-task authorization, termination races, Pod replacement, restart, unknown outcomes, and abuse caps | 5 | QA/SRE and backend |

### 5.3 Workspace and Network Rules

- `/workspace` is ephemeral and is lost with the Pod. `/results` remains the
  retained PVC and is not repurposed as a general transfer staging area.
- Upload targets are confined beneath the configured workspace root. Absolute
  paths, `..`, symlink escapes, device files, and writes through existing
  symlinks are rejected. Writes use a bounded temporary file, verify declared
  size and checksum, and rename atomically.
- Downloads resolve beneath an explicitly readable profile root, reject symlink
  escapes and special files, and stream no more than the approved per-command
  and per-task limits. Archive extraction is not added implicitly.
- Partial or failed transfers remove temporary workspace files. They never
  modify a retained result unless a separate, explicit result-publication
  operation is approved later.
- Profiles declare workspace and ephemeral-storage quotas, readable roots,
  transfer capability, container name, and required helper capability. Numeric
  limits must be owner-approved before enabling a profile.
- Sandbox ingress remains denied. A profile with no egress declaration receives
  deny-all egress. Profiles needing DNS or external access receive explicit,
  reviewed rules or an approved egress proxy; relay traffic itself does not
  require sandbox network ingress or egress.

### 5.4 Migration and Cutover

No production deployment or external consumer exists, so no live compatibility
window is required. The sequencing is:

1. Land the approved contracts and relay behind ports while the current direct
   path remains test-only.
2. Integrate durable relay admission and workspace operations.
3. Switch ready publication to the relay reference and update profiles.
4. In the same stage, delete Service generation, ingress allow rules, sandbox
   connection credentials, and their cleanup references.
5. Keep every lifecycle, supervision, deadline, cleanup, and reconciliation test
   passing except assertions deliberately changed by the approved contract.

### 5.5 Exit Criteria

- A ready task accepts `exec`, `upload`, and `download` through the manager.
  PTY framing also works for a test profile, but production profiles keep it
  disabled unless explicitly approved.
- No agent-to-sandbox endpoint, Service, or ingress exception remains.
- Every operation is authenticated, checked against current task ownership and
  state, passed through the policy port, and tied to the current Pod UID.
- No command starts after termination wins the durable admission race.
- Manager restart or stream loss reports unfinished work as `UNKNOWN`, never as
  success or confirmed cancellation, and never triggers an automatic reissue.
- Workspace path, symlink, checksum, partial-write, quota, and payload abuse
  tests pass without modifying retained results.
- The full pre-existing test suite and the new relay suite pass.

## 6. Stage 4c: Operational Proof and Release

### 6.1 Goal

Complete deployment adapters, clear the Sprint 2-3 staging gates, execute the
full revised acceptance matrix, and produce release evidence.

### 6.2 Planned Backlog

| Story | Relative size | Primary owner |
|---|---:|---|
| Complete deployment adapters: JetStream lifecycle intake/responses, durable compare-and-set repository, authenticated health transport, and production Kubernetes client | 8 | Backend and platform |
| Complete staging: cluster, gVisor `RuntimeClass`, StorageClass, relay-capable qualified image, NATS credentials, and CI connectivity | 5 | Platform |
| Clear the Sprint 2 and Sprint 3 staging checklists with recorded evidence | 5 | Platform, security, and QA |
| Add correlated lifecycle, capacity, health, relay, command-outcome, policy, and cleanup logs and metrics, dashboards, and alerts | 5 | Backend and SRE |
| Add credential- and payload-redaction tests across logs, errors, events, metrics, traces, and test reports | 3 | Security and QA |
| Execute the complete revised acceptance matrix with linked evidence | 8 | QA/SRE |
| Run manager, NATS, storage, Pod, readiness, health, exec-stream, command-admission, and partial-cleanup fault injection | 5 | QA/SRE and platform |
| Complete deployment manifests, RBAC, API audit, runbooks, rollback procedure, and release review | 5 | Platform, SRE, and technical lead |

### 6.3 Release Gate

The four-sprint plan's release gate stands, plus:

- all relay acceptance criteria in Section 7 pass in staging;
- manager and API-server CPU, memory, connection, and throughput measurements
  under ten active sandboxes are recorded against owner-approved budgets;
- command and transfer caps are tested at and beyond their boundaries without
  unbounded memory, blocked lifecycle supervision, or API-server instability;
- no log, metric, trace, error, event, or test artifact contains command
  arguments, environment values, stream payloads, file content, or credentials;
- restart under active relay traffic loses no task or Kubernetes resource,
  labels unresolved commands `UNKNOWN`, and never silently reports success;
- manager RBAC contains only the approved resources and subresources; API audit
  confirms the manager principal, Pod, and fixed helper request, while the
  manager audit record supplies task/command correlation without user argv; and
- rollback disables new relay admission before reverting deployment components;
  it does not restore the deleted direct sandbox exposure.

## 7. Acceptance-Criteria Changes

| Criterion | Revised or new criterion |
|---|---|
| AC-013 | No command is admitted before readiness is durable; first admission durably activates the task if `READY` and `ACTIVE` remain separate |
| AC-014 | A ready response identifies the task, session reference, relay protocol version, and route; it contains no sandbox endpoint or sandbox credential |
| AC-025 | No operational or test artifact contains credentials, command arguments, environment values, stream payloads, or file contents |
| AC-026 | All agent operations transit the authenticated manager relay; sandboxes expose no inbound Service or permitted ingress path |
| AC-027 | Every operation reaches the policy port before side effects; a denial or missing policy is audited and never executed, independent of the future filtering method |
| AC-028 | Upload and download remain within approved roots and caps, reject traversal and symlink escapes, handle partial failure atomically, and do not disturb retained results |
| AC-029 | Stream loss or manager restart yields `UNKNOWN` for any command without a recorded terminal outcome; it never implies termination, success, or automatic retry |
| AC-030 | The authenticated transport principal, not a caller-provided session field, must own the task; cross-task and stale-session operations are rejected |
| AC-031 | Once termination wins the durable admission race, no new command starts; cleanup revokes relay admission before stopping or deleting the workload |
| AC-032 | Relay execution occurs only after the observed Pod UID, task annotation, labels, namespace, and container match the durable task record |
| AC-033 | Framing, sequencing, backpressure, cancellation signaling, and per-command/per-task caps remain bounded under slow consumers and oversized input |

AC-001 through AC-012 and AC-015 through AC-024 otherwise remain unchanged.
The SRS must contain the authoritative final wording and updated traceability.

## 8. Multi-Worker Boundary

The release remains single-worker. More than one manager worker, each capped at
ten sandboxes, is a later increment. The relay design reserves the following
contracts without claiming that takeover is implemented:

- The durable repository records capacity reservation, task claim, worker ID,
  lease expiry, and fencing generation. The in-process `CapacityLimiter` is
  seeded during single-worker recovery and is not cross-worker authority.
- A worker subscribes to a task route only after winning its durable claim. Each
  admission carries the current fencing generation; a stale owner is rejected.
- Core NATS command frames are not queued for later execution while no owner is
  subscribed. The caller receives no-responder or disconnect behavior and must
  reconnect after ownership recovery.
- Future takeover requires atomic claim acquisition, subscription handoff,
  reconstruction of task supervision, and explicit handling of commands left
  `UNKNOWN` by the previous owner.
- Per-worker capacity means total configured system capacity is worker count
  multiplied by ten. Changing to a global or tenant quota requires a distinct
  capacity design.

A two-worker demonstration is not a Stage 4c acceptance requirement unless the
owners explicitly promote multi-worker operation into release scope and add the
claim, fencing, handoff, and capacity stories.

## 9. Risks and Escalation Triggers

| Risk | Control | Escalation trigger |
|---|---|---|
| `pods/exec` cannot provide durable command outcomes | Explicit `UNKNOWN` outcome; no automatic retry; later supervisor option | Product requires restart-transparent or exactly-once commands for v1 |
| API server or manager becomes a data-plane bottleneck | Stage 4a cluster spike, bounded frames, one command per task, measured Stage 4c budgets | Spike or staging load exceeds approved latency/resource budgets |
| Agent transport framing or authentication is unsuitable | Freeze schemas only after authenticated streaming spike | Principal binding, flow control, or reconnect cannot be demonstrated in 4a |
| Deterministic Pod name resolves to the wrong object | Verify stored UID and ownership immediately before every exec | Any replacement-race test reaches the wrong Pod or container |
| Termination races command admission | Durable state fence and one-command serialization | A command starts after `TERMINATING` becomes durable |
| Policy work is prematurely embedded or bypassed | Mandatory versioned port, reserved fields, default deny, placeholder method | A side effect bypasses policy or a profile depends on an unapproved filter grammar |
| PTY undermines command inspection | Treat PTY as opaque broad capability and disable by default | A production profile enables PTY without explicit owner acceptance |
| File transfer escapes its workspace or exhausts resources | Declared helper contract, root confinement, atomic writes, quotas, caps | Traversal/symlink test escapes, partial writes remain, or memory grows without bound |
| Egress policy breaks workloads or weakens isolation | Per-profile rules and image qualification | A profile needs undeclared egress or broad unrestricted network access |
| Staging debt slips again | Start environment work immediately; Stage 4c exit requires evidence | Any prerequisite or Sprint 2-3 checklist item is unmet at the 4c midpoint |
| Three-stage Phase 4 still exceeds capacity | Re-estimate each stage from observed throughput and role constraints | Committed inventory exceeds demonstrated capacity or carryover exceeds ten percent |
| Payloads leak into telemetry | Payload-free schema and automated scans across all artifacts | Any argument, environment value, stream, file content, or credential appears in an artifact |

## 10. Immediate Actions

1. Name the SRS owner and approver and approve the architecture direction only,
   leaving the plan in Draft until Stage 4a closes.
2. Start the representative gVisor, NATS, StorageClass, image, credential, and CI
   environment work immediately.
3. Approve owners and decision deadlines for Decisions 17-22. Keep Decision 19's
   allowlist and filtering methods explicitly TBD while approving its mandatory
   port, reserved schema, default-deny behavior, and non-bypass requirement.
4. Execute the authenticated transport and `pods/exec` spike, including stream
   loss, cancellation, Pod replacement, RBAC, image tooling, and resource load.
5. Apply the complete requirements and traceability rewrite from Section 2.4.
6. Freeze schemas only after the spike results and requirements agree; record
   any rejected transport or relay option and its evidence.
7. Re-estimate Stages 4b and 4c from observed Sprint 1-3 throughput and Stage 4a
   evidence before committing dates.
