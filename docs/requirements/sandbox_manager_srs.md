# Sandbox Manager Software Requirements Specification

## Document Control

| Field | Value |
|---|---|
| Document ID | SM-SRS-001 |
| Version | 0.2 |
| Status | Draft for product-owner approval |
| Date | 2026-08-28 |
| Source | [Initial requirements](./init_req.md), recorded owner decisions, and the [Phase 4 relay update and completion plan](../planning/phase4_relay_update_and_release_plan.md) |
| Owner | TBD |
| Approver | TBD |
| Audience | Product owner, platform engineering, security, operations, and QA |

### Revision History

| Version | Date | Author | Summary |
|---|---|---|---|
| 0.1 | 2026-08-27 | Sandbox Manager project | Initial formal requirements baseline |
| 0.2 | 2026-08-28 | Sandbox Manager project | Manager-mediated command relay baseline (Stage 4a rewrite) |

### Approval

| Role | Name | Status | Date |
|---|---|---|---|
| Product owner | TBD | Pending | TBD |
| Platform owner | TBD | Pending | TBD |
| Security reviewer | TBD | Pending | TBD |

> This document formalizes the confirmed requirement baseline. Items marked
> **Derived**, **Open**, or **Assumption** are not approved product policy unless
> explicitly accepted during review. Relay-driven items marked **Confirmed**
> below rest on the owner architecture direction recorded on 2026-08-28 and
> remain subject to the formal approval signature above.

## 1. Requirement Authority

The following classifications apply:

- **Confirmed**: explicitly requested by the product owner or recorded in the
  accepted initial requirements, including items confirmed by the owner
  architecture direction of 2026-08-28.
- **Derived**: required to make a confirmed behavior safe or testable, but still
  requiring owner approval.
- **Open**: no final decision exists; engineering must not silently choose a
  product or policy commitment.
- **Assumption**: a working condition that must be validated before deployment.

The terms **must**, **must not**, **should**, and **may** are normative only for
Confirmed requirements. Open and Assumption statements are non-normative.

## 2. Purpose and Business Outcome

The Sandbox Manager provisions, exposes, monitors, and terminates isolated task
environments on an existing Kubernetes cluster.

An agent must be able to request a task-appropriate sandbox, operate it through
a manager-mediated command relay across multiple interaction rounds, save
results to persistent storage, and release it explicitly or through automatic
failure detection.

The agent never connects to a sandbox endpoint. Every operation is admitted,
authorized, and relayed by the manager, which returns framed output and a
terminal or unknown outcome.

The operational outcome is bounded resource use, durable lifecycle state,
central command authorization, and automatic reclamation of completed, failed,
timed-out, or abandoned sandboxes.

## 3. Scope

### 3.1 In Scope

- Receiving sandbox lifecycle requests from a durable queue.
- Preserving task and sandbox state across manager restarts.
- Provisioning task-specific Kubernetes sandboxes through gVisor.
- Resolving an image for every sandbox creation.
- Supporting multiple configured sandbox types.
- Accepting task-scoped agent operations only through an authenticated manager
  relay.
- Admitting commands only after readiness is durable.
- Enforcing per-sandbox CPU and memory requests and limits.
- Limiting concurrent sandboxes to an initial per-manager maximum of 10.
- Attaching persistent result storage and an ephemeral workspace.
- Evaluating a versioned command policy before every relayed side effect.
- Supporting <code>exec</code>, optionally <code>pty</code>, <code>upload</code>,
  and <code>download</code> operations.
- Recording a terminal outcome or <code>UNKNOWN</code> for every command.
- Checking the owning agent every 15 seconds.
- Terminating a sandbox when the agent fails to answer within 5 seconds.
- Handling completion, cancellation, deadlines, and unrecoverable failures.
- Cleaning ephemeral resources while retaining results according to policy.
- Exposing lifecycle, capacity, admission, policy, relay, failure, and
  health-check telemetry.

### 3.2 Not Yet In Scope

No commitment has been made for:

- installing or administering Kubernetes or gVisor;
- building the task images used by sandboxes beyond the relay-helper capability
  contract in KR-009;
- multi-tenant billing, chargeback, or priority scheduling;
- arbitrary requester-selected images;
- direct agent-to-sandbox network connectivity of any kind;
- a user interface;
- interpreting task results;
- durable or exactly-once command execution, including an in-sandbox command
  supervisor or reverse channel;
- automatic retry of commands whose outcome is <code>UNKNOWN</code>;
- publication of workspace content into retained results;
- an egress proxy;
- warm sandbox pools;
- Kubernetes node autoscaling; or
- multi-manager operation, whose reserved contracts are described in Section 20.

These are exclusions from this baseline, not permanent product exclusions.

## 4. Actors and Responsibilities

| Actor | Responsibility |
|---|---|
| Agent | Requests a sandbox, receives the relay route, issues commands and transfers through the manager relay, answers health checks, and reports completion or cancellation |
| Sandbox Manager | Validates requests, preserves state, enforces capacity, admits commands under policy and fencing, relays operations through the Kubernetes API, classifies command outcomes, checks agent health, reconciles failures, and cleans up |
| Durable Queue | Persists and delivers lifecycle messages over NATS JetStream |
| Command Transport | Carries authenticated command admission requests and bounded data frames over Core NATS, supplying the authenticated principal |
| Kubernetes API | Accepts desired state, reports observed sandbox-resource state, and carries the <code>pods/get</code> verification and <code>pods/exec</code> relay path |
| gVisor Runtime | Provides the mandatory sandbox execution boundary |
| Storage Provider | Provisions and retains task result volumes |
| Platform Operator | Configures the cluster, runtime class, queue, transport credentials, storage, profiles, policy, capacity, and operating policy |
| QA and Security | Verify lifecycle behavior, isolation, limits, relay authorization, recovery, and failure handling |

## 5. System Context

~~~text
                         lifecycle requests
Agent ------------------------------------------------> JetStream Queue
  ^                                                          |
  |                                                           v
  |                                           Sandbox Manager (single worker)
  |                                           validate, persist, reconcile
  | command envelopes and bounded frames      admit under policy and fencing
  +---------------------------------------->  verify Pod identity (pods/get)
        ^                                     relay via pods/exec
        |                                                           |
        | framed output, terminal or                     desired and observed state
        | UNKNOWN outcome                                           v
        |                                             Kubernetes API and Controllers
                                                        |       |       |
                                                        v       v       v
                                                     gVisor   Results  Network
                                                     Pod      PVC      (deny by default)
~~~

The agent-to-manager command transport is authenticated Core NATS with
request/reply admission and task-scoped ephemeral data subjects. This diagram
represents the approved logical topology; credential distribution details
remain subject to SEC-004.

## 6. Definitions

| Term | Definition |
|---|---|
| Sandbox | One isolated task execution environment managed as a lifecycle unit |
| Sandbox type | A supported profile resolving the image, command, relay capability set, policy reference, workspace rules, container identity, and egress declaration for a class of tasks |
| Task | The durable logical record associated with one sandbox lifecycle |
| Agent session | The specific agent process/session that owns a task and must answer its checks |
| Authenticated principal | The transport-verified identity supplied independently of message fields and bound to the durable agent session |
| Active capacity | Sandboxes counted against the per-manager concurrency limit |
| Readiness | The durable condition under which relay admission may begin for a task |
| Relay | The manager-mediated path carrying authorized operations into the sandbox |
| Command | One agent-initiated operation admitted, relayed, and outcome-recorded by the manager |
| Command ID | The unique identifier of one command, chosen by the caller and never reused |
| Control envelope | The versioned message opening a command, carrying identity, operation, declared sizes, and correlation |
| Data frame | One bounded, sequenced payload chunk on a named channel of a command |
| Command outcome | The terminal classification of a command: <code>SUCCESS</code>, <code>FAILED</code>, <code>REJECTED</code>, <code>CANCELLED</code>, or <code>UNKNOWN</code> |
| UNKNOWN | The outcome recorded when no terminal result is confirmed; it never implies the remote process stopped |
| Relay helper | The fixed, qualified in-sandbox program through which every relayed operation executes |
| Admission fencing | The durable mechanism serializing command admission against activation and termination |
| Agent health check | A manager-initiated challenge containing a unique nonce, task ID, and agent-session ID |
| Result storage | The persistent volume mounted so task results survive sandbox termination |
| Workspace | The ephemeral <code>/workspace</code> area lost with the sandbox Pod, separate from retained results |
| Ephemeral resources | Kubernetes resources removed during sandbox termination |
| Terminal state | A retained final outcome such as <code>COMPLETED</code>, <code>FAILED</code>, or <code>TIMED_OUT</code> |
| Immediate termination | Durably initiating termination as soon as the 5-second response deadline expires; physical deletion has a separate Open SLA |

## 7. Dependencies and Assumptions

### 7.1 Deployment Dependencies

- **DEP-001 [Confirmed]:** An operational Kubernetes cluster must exist in the
  target environment.
- **DEP-002 [Confirmed]:** The cluster must expose a working gVisor
  <code>RuntimeClass</code>.
- **DEP-003 [Confirmed]:** A queue capable of durable message storage must be
  available; NATS JetStream is the baseline selected by owner direction.
- **DEP-004 [Confirmed]:** A Kubernetes-compatible persistent storage provider
  must be available.
- **DEP-005 [Confirmed]:** An authenticated Core NATS endpoint must be available
  for command admission and data frames, alongside the JetStream lifecycle
  endpoint of the same product.

### 7.2 Assumptions Requiring Validation

- **ASM-001:** The manager can reach the Kubernetes API and has sufficient RBAC
  permissions for its scoped operations.
- **ASM-002:** The CNI and storage provider support the security and persistence
  policies selected later.
- **ASM-003:** Approved task images are compatible with gVisor and supply the
  relay-helper capability.
- **ASM-004:** The owning agent is reachable through a transport capable of a
  challenge/response exchange within 5 seconds.
- **ASM-005:** Cluster capacity accounts for gVisor overhead in addition to
  declared sandbox workload resources.
- **ASM-006:** Clocks are sufficiently synchronized for deadlines and auditing.
- **ASM-007:** The owner accepts that a single 5-second missed response can
  terminate healthy work during a transient agent, manager, network, or queue
  delay.
- **ASM-008:** The Kubernetes control plane can carry bounded
  <code>pods/exec</code> streams for up to 10 concurrent sandboxes within the
  approved budgets.

## 8. Functional Requirements

### 8.1 Queue Intake and Idempotency

- **FR-001 [Confirmed]:** The manager must receive spawn and lifecycle requests
  from a durable queue.
- **FR-002 [Confirmed]:** Every accepted request must contain a unique request
  ID and task ID.
- **FR-003 [Confirmed]:** Redelivery of a request must not create a duplicate
  sandbox or result volume.
- **FR-004 [Confirmed]:** The manager must acknowledge a queue message only
  after the corresponding state mutation is durable.
- **FR-005 [Derived]:** The manager must distinguish a duplicate request from
  conflicting reuse of an identifier.
- **FR-006 [Derived]:** Invalid requests must not consume a sandbox slot or
  create Kubernetes resources.
- **FR-007 [Open]:** Delivery policy, retry limits, dead-letter behavior,
  ordering, message schemas, and retention for the JetStream baseline remain
  undecided.

### 8.2 Durable Task State

- **FR-008 [Confirmed]:** Accepted task state must survive manager restarts.
- **FR-009 [Confirmed]:** The durable task record must contain:
  - request ID and task ID;
  - sandbox type and resolved image;
  - lifecycle state;
  - Kubernetes resource identity, including the Pod UID used by the relay;
  - result volume identity;
  - relay route and owning agent-session reference, when available;
  - admission fencing state, including activation and the in-flight command ID
    when one exists;
  - owning agent-session ID;
  - last successful agent health-check timestamp;
  - creation, update, deadline, and completion timestamps; and
  - terminal outcome or error reason.
- **FR-010 [Confirmed]:** After restart, the manager must reconcile durable
  state with observed Kubernetes state and continue, repair, or terminate
  incomplete work.
- **FR-011 [Derived]:** Reconciliation must be idempotent.
- **FR-012 [Open]:** The state technology, source of truth, and consistency
  model remain undecided. A Kubernetes custom resource is recommended but not
  approved.

### 8.3 Sandbox Types and Images

- **FR-013 [Confirmed]:** Every spawn request must identify a supported sandbox
  type.
- **FR-014 [Confirmed]:** A sandbox type must resolve at least an image, a
  command, a relay capability set, a command-policy reference, workspace rules
  including transfer roots and quotas, the container identity used by the
  relay, an egress declaration, and the results mount.
- **FR-015 [Confirmed]:** Unknown sandbox types must be rejected.
- **FR-016 [Confirmed]:** An image must be resolved for every spawn.
- **FR-017 [Open]:** Requester overrides of images or profile fields remain
  undecided.
- **FR-018 [Open]:** Registry policy, digest pinning, image validation, pull
  policy, and profile versioning remain undecided.

### 8.4 Capacity and Scheduling

- **FR-019 [Confirmed]:** The concurrency limit must be configurable, with an
  initial and default per-manager maximum of 10.
- **FR-020 [Confirmed]:** At capacity, additional valid requests must remain
  queued or pending and must not create another active sandbox.
- **FR-021 [Confirmed]:** The limit applies per manager worker; total system
  capacity is the worker count multiplied by 10. Multi-manager coordination is
  out of scope for v1 and reserved per Section 20.
- **FR-022 [Open]:** The lifecycle states counted against capacity remain
  undecided. At minimum, <code>READY</code> and <code>ACTIVE</code> consume a
  slot.
- **FR-023 [Open]:** Scheduling order, fairness, priority, and starvation
  prevention remain undecided.

### 8.5 Provisioning and Readiness

- **FR-024 [Confirmed]:** When capacity exists, the manager must provision
  storage and an isolated sandbox for the task.
- **FR-025 [Confirmed]:** No command may be admitted for a task before its
  readiness is durable.
- **FR-026 [Confirmed]:** Kubernetes names and labels must trace resources to
  their owning task.
- **FR-027 [Derived]:** Provisioning failure must produce a durable outcome and
  must not leave untracked resources.
- **FR-028 [Open]:** Workload type, readiness probe, provisioning timeout,
  retry policy, and resource-creation order remain undecided.

### 8.6 Command Relay and Multi-Round Use

- **FR-029 [Confirmed]:** After readiness, the manager must accept task-scoped
  commands over the authenticated relay; no other agent-to-sandbox path may
  exist.
- **FR-030 [Confirmed]:** The ready response must carry the task ID, the owning
  agent-session reference, the relay protocol version, and the relay route. It
  must not contain a sandbox endpoint or a sandbox credential.
- **FR-031 [Confirmed]:** Command arguments, environment values, stream
  payloads, PTY input, and file contents must not appear in plain-text logs or
  unprotected metadata.
- **FR-032 [Confirmed]:** Command admission uses authenticated Core NATS
  request/reply; bounded data frames use task-scoped ephemeral subjects.
  JetStream must not be used to replay stale interactive frames. Credential
  lifetime, rotation, and replay hardening remain subject to SEC-004.
- **FR-033 [Confirmed]:** The agent must be able to issue repeated commands
  against the same sandbox for multiple interaction rounds until termination
  starts.
- **FR-034 [Confirmed]:** Repeated interaction must not reset or replace the
  result volume. The workspace persists for the sandbox lifetime and is lost
  with the Pod.
- **FR-035 [Confirmed]:** After transport loss or manager restart, a caller may
  reconnect and submit new commands under new command IDs; the manager must
  never automatically re-issue a command whose outcome is
  <code>UNKNOWN</code>.

### 8.7 Active Agent Health Checking

- **FR-036 [Confirmed]:** While a sandbox is <code>READY</code> or
  <code>ACTIVE</code>, the manager must initiate a health check of its owning
  agent every 15 seconds.
- **FR-037 [Confirmed]:** Every check must contain a unique nonce, current task
  ID, and current agent-session ID.
- **FR-038 [Confirmed]:** The agent must return a valid matching response within
  5 seconds of check initiation.
- **FR-039 [Confirmed]:** Late, missing, stale, malformed, nonce-mismatched,
  task-mismatched, or session-mismatched responses must not satisfy a check.
- **FR-040 [Confirmed]:** Checks for the same task must not overlap.
- **FR-041 [Confirmed]:** If no valid response arrives within 5 seconds, the
  manager must immediately:
  1. durably move the task to <code>TERMINATING</code> with target outcome
     <code>TIMED_OUT</code>;
  2. close relay admission; and
  3. after cleanup, record <code>TIMED_OUT</code> with reason
     <code>AGENT_UNRESPONSIVE</code>.
- **FR-042 [Confirmed]:** After restart, the manager must issue a new check and
  reject responses to checks issued by the previous manager process.
- **FR-043 [Open]:** Health-check transport authentication details, cadence
  tolerance, and manager-unavailability behavior remain undecided.
- **FR-044 [Open]:** The physical cleanup SLA after the response deadline
  remains undecided.

### 8.8 Completion, Cancellation, and Cleanup

- **FR-045 [Confirmed]:** Agent completion must initiate termination.
- **FR-046 [Confirmed]:** Explicit cancellation, health-check failure, deadline,
  unrecoverable provisioning/runtime error, or unexpected Pod deletion must
  initiate termination.
- **FR-047 [Confirmed]:** Termination must remove ephemeral Kubernetes
  resources associated with the sandbox.
- **FR-048 [Confirmed]:** Cleanup must be idempotent.
- **FR-049 [Confirmed]:** Sandbox termination alone must not delete persistent
  results before the retention policy permits.
- **FR-050 [Confirmed]:** Completion, timeout, and failure must leave a durable
  terminal task record.
- **FR-051 [Open]:** The explicit-cancellation terminal state remains
  undecided; the working baseline is <code>CANCELLED</code>.
- **FR-052 [Open]:** Termination ordering, grace period, forced deletion,
  finalizers, and cleanup deadline remain undecided.

### 8.9 Command Admission, Policy, and Outcomes

- **FR-053 [Confirmed]:** The manager must admit at most one command or
  transfer per task at a time.
- **FR-054 [Confirmed]:** Admission must atomically observe a
  <code>READY</code> or <code>ACTIVE</code> record, durably record activation
  when a <code>READY</code> task is admitted for the first time, and acquire
  the current process fence before any relay side effect.
- **FR-055 [Confirmed]:** A durable transition to <code>TERMINATING</code> must
  close relay admission before any cleanup step begins.
- **FR-056 [Confirmed]:** Every command must carry a unique command ID and
  receive exactly one terminal outcome or <code>UNKNOWN</code>.
- **FR-057 [Confirmed]:** Stream loss, unconfirmed cancellation, timeout
  without confirmed process exit, and manager restart must produce
  <code>UNKNOWN</code>, never <code>SUCCESS</code> or a confirmed
  <code>CANCELLED</code>.
- **FR-058 [Confirmed]:** The manager must never automatically re-issue a
  command whose outcome is <code>UNKNOWN</code>. A caller may submit a new
  command ID after reconnection, accepting application-level duplicate risk.
- **FR-059 [Confirmed]:** A versioned command policy must be evaluated before
  every relay side effect, including <code>exec</code>, PTY creation, transfer
  setup, resize, and cancellation where applicable.
- **FR-060 [Confirmed]:** A missing or invalid policy configuration must deny
  admission.
- **FR-061 [Open]:** The allowlist language, filtering methods, normalization
  rules, and matching strategy of the command policy remain undecided.
- **FR-062 [Confirmed]:** The transport must supply an authenticated principal
  independently of message fields; the manager must bind it to the durable
  agent session, and a caller-provided session ID alone must never be accepted
  as proof of ownership.
- **FR-063 [Confirmed]:** PTY is an opaque interactive capability granted after
  admission; the manager does not claim subsequent PTY input is inspectable or
  filterable, and profiles keep PTY disabled unless an owner explicitly accepts
  the capability.
- **FR-064 [Confirmed]:** Built-in operations, including <code>upload</code>,
  <code>download</code>, and workspace setup, must pass through the same policy
  evaluation as <code>exec</code>.
- **FR-065 [Derived]:** The policy schema must reserve normalized evaluation
  fields for operation, executable, argv, working directory, environment names,
  path, size, profile, and authenticated principal.
- **FR-066 [Derived]:** Every admission decision must be auditable with
  identifiers, operation, policy version and decision reference, sizes,
  outcome, and latency.

## 9. Lifecycle Requirements

### 9.1 Baseline States

~~~text
QUEUED -> PROVISIONING -> READY -> ACTIVE -> TERMINATING -> COMPLETED
                         |          |             |
                         +----------+-------------+-> FAILED
                         +----------+-------------+-> TIMED_OUT
                         +----------+-------------+-> CANCELLED (Open)
~~~

| State | Meaning |
|---|---|
| <code>QUEUED</code> | Request is durably accepted and waiting for capacity |
| <code>PROVISIONING</code> | Sandbox resources are being prepared |
| <code>READY</code> | Readiness is durable and relay admission is available |
| <code>ACTIVE</code> | At least one command has been admitted; the agent is using the sandbox through the relay |
| <code>TERMINATING</code> | Relay admission is closed and cleanup is in progress |
| <code>COMPLETED</code> | Agent-reported work completed and cleanup finished |
| <code>FAILED</code> | An unrecoverable non-agent failure occurred |
| <code>TIMED_OUT</code> | A deadline or required agent health check failed |
| <code>CANCELLED</code> | Explicit agent cancellation (Open baseline) |

Whether <code>READY</code> and <code>ACTIVE</code> remain separate is Open;
the working baseline keeps them separate, with the first admitted command as
the activation trigger.

### 9.2 Required Transitions

| Current state | Trigger | Required behavior | Outcome |
|---|---|---|---|
| None | Valid spawn | Persist task | <code>QUEUED</code> |
| <code>QUEUED</code> | Capacity available | Reserve capacity and provision | <code>PROVISIONING</code> |
| <code>PROVISIONING</code> | Readiness succeeds | Make relay admission available | <code>READY</code> |
| <code>READY</code> | First command admission | Durably record activation and admit | <code>ACTIVE</code> |
| <code>READY</code> / <code>ACTIVE</code> | Valid health response | Update last successful check | No change |
| <code>READY</code> / <code>ACTIVE</code> | No valid response in 5 seconds | Close admission and begin immediate cleanup | <code>TIMED_OUT / AGENT_UNRESPONSIVE</code> |
| Non-terminal | Agent completion | Begin cleanup | <code>COMPLETED</code> after cleanup |
| Non-terminal | Agent cancellation | Begin cleanup | Open |
| Provisioning/running | Unrecoverable platform error | Begin cleanup | <code>FAILED</code> |
| Non-terminal | Configured deadline | Begin cleanup | <code>TIMED_OUT</code> |

### 9.3 State Invariants

- **SR-001 [Confirmed]:** A task must have exactly one current lifecycle state.
- **SR-002 [Confirmed]:** A terminal record must retain its reason and
  completion timestamp.
- **SR-003 [Derived]:** A terminal task must not return to a non-terminal state.
- **SR-004 [Derived]:** A task must not publish more than one logically
  distinct relay route.
- **SR-005 [Derived]:** Capacity release must not allow the configured maximum
  to be exceeded during concurrent reconciliation.
- **SR-006 [Open]:** Atomic capacity reservation remains undecided.
- **SR-007 [Confirmed]:** A task must have at most one in-flight command or
  transfer at any time.
- **SR-008 [Confirmed]:** No command may be admitted after the durable
  transition to <code>TERMINATING</code>.

## 10. Data and Message Requirements

### 10.1 Spawn Request

The serialization format remains Open. A logical spawn request must carry:

| Field | Requirement |
|---|---|
| Schema version | Derived; required for compatible evolution |
| Request ID | Confirmed unique request and deduplication key |
| Task ID | Confirmed durable task identity |
| Agent-session ID | Confirmed by ownership and health-check requirements |
| Sandbox type | Confirmed |
| Correlation/reply information | Derived; exact form Open |
| Requested deadline | Optional until deadline policy is approved |
| Image override | Open; must not be accepted without an approved policy |

### 10.2 Health-Check Exchange

A health-check request must carry:

- a unique nonce;
- task ID;
- current agent-session ID; and
- issuance time or equivalent manager-controlled deadline context.

A response must allow validation of the nonce, task, session, and receipt before
the 5-second deadline.

### 10.3 Ready Response

A ready response must carry request/task correlation, lifecycle status, the
relay route, the owning agent-session reference, the relay protocol version,
and any transport-credential expiry required by the authentication model. It
must not carry a sandbox endpoint or a sandbox credential.

### 10.4 Terminal Record

A terminal record must carry task ID, terminal state and reason, result-volume
identity or result location, completion time, confirmation that relay admission
is closed, and cleanup status sufficient for reconciliation.

### 10.5 Message Semantics

- Queue acknowledgement must follow durable state mutation.
- Duplicate delivery must be safe.
- Stale health responses must never extend sandbox lifetime.
- The authenticated principal comes from the transport, never from payload
  fields, and must match the owning agent session.
- Sensitive credentials, command arguments, environment values, stream
  payloads, and file contents must not appear in unprotected metadata or logs.
- Completion and cancellation inputs must be idempotent.
- Lifecycle retry, dead-letter, expiration, and ordering remain Open.

### 10.6 Command Messages

A command begins with a versioned control envelope that must carry:

- unique command ID;
- task ID;
- operation, one of <code>exec</code>, <code>pty</code>, <code>upload</code>,
  or <code>download</code>;
- protocol version;
- declared input and output sizes; and
- correlation information.

Data frames must:

- identify the command ID, channel, and sequence number;
- not exceed the fixed maximum frame size;
- define bounded buffering, backpressure, and missing or duplicate frame
  handling;
- carry explicit cancellation requests and terminal frames; and
- never represent opaque interactive PTY content as policy-inspectable.

Command outcomes are <code>SUCCESS</code>, <code>FAILED</code>,
<code>REJECTED</code>, and confirmed <code>CANCELLED</code> as terminal
classifications. Stream loss, unconfirmed cancellation, timeout without
confirmed process exit, and manager restart produce <code>UNKNOWN</code>.

## 11. Kubernetes and gVisor Requirements

- **KR-001 [Confirmed]:** Every sandbox workload must explicitly select the
  configured gVisor Kubernetes <code>RuntimeClass</code>.
- **KR-002 [Confirmed]:** Sandboxes must not fall back to the default OCI
  runtime when gVisor is unavailable.
- **KR-003 [Confirmed]:** gVisor scheduling or startup failure must be reported
  as a provisioning/runtime failure.
- **KR-004 [Confirmed]:** All Kubernetes resources must be traceable to the
  owning task.
- **KR-005 [Confirmed]:** Kubernetes cleanup must be safe to repeat.
- **KR-006 [Confirmed]:** A sandbox consists of a direct Pod, an ephemeral
  <code>/workspace</code>, a retained results PVC, an optional profile-required
  workload Secret, an ingress-deny NetworkPolicy, and approved per-profile
  egress rules. No Service or ingress exception is created.
- **KR-007 [Open]:** Node selection, tolerations, gVisor overhead declaration,
  namespace layout, owner references, and finalizers remain design decisions.
- **KR-008 [Confirmed]:** Immediately before every <code>pods/exec</code>, the
  manager must read the Pod and verify its UID, task annotation, labels,
  namespace, and configured container against the durable task record; a
  deterministic name alone is insufficient.
- **KR-009 [Confirmed]:** Every <code>pods/exec</code> request must invoke one
  fixed helper path with a fixed bootstrap argv from the qualified profile. The
  operation and user command must travel through the bounded stdin protocol,
  never as Kubernetes exec query arguments, so user argv does not enter
  Kubernetes request URIs or API audit events.
- **KR-010 [Confirmed]:** Manager RBAC must be narrowly scoped to
  <code>pods/get</code> and <code>pods/exec</code> for the sandbox namespace,
  and the manager identity must be covered by Kubernetes API audit.

## 12. Resource Requirements

The aggregate budget for all concurrently running workload containers in one
sandbox is:

~~~yaml
resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: "1"
    memory: 512Mi
~~~

- **RR-001 [Confirmed]:** Aggregate CPU request must be <code>250m</code>.
- **RR-002 [Confirmed]:** Aggregate memory request must be <code>256Mi</code>.
- **RR-003 [Confirmed]:** Aggregate CPU limits must not exceed <code>1</code>.
- **RR-004 [Confirmed]:** Aggregate memory limits must not exceed
  <code>512Mi</code>.
- **RR-005 [Confirmed]:** Spawn requests must not override these values.
- **RR-006 [Confirmed]:** The resulting Burstable QoS tradeoff is accepted to
  reduce reserved resources while retaining hard limits.
- **RR-007 [Derived]:** Any sidecar, including a future in-sandbox command
  supervisor, must consume part of this workload budget, not add another full
  sandbox budget.
- **RR-008 [Derived]:** gVisor runtime overhead must be capacity-planned
  separately.
- **RR-009 [Open]:** Ephemeral-storage and PID limits remain undecided.

## 13. Storage Requirements

- **ST-001 [Confirmed]:** Every sandbox must mount persistent result storage.
- **ST-002 [Confirmed]:** The storage identity must be recorded against the
  task.
- **ST-003 [Confirmed]:** Sandbox deletion must not delete results before the
  configured retention policy permits.
- **ST-004 [Confirmed]:** The terminal record must report the result volume
  identity or result location.
- **ST-005 [Derived]:** Storage provisioning failure must prevent readiness and
  must be durably reported.
- **ST-006 [Open]:** StorageClass, size, access mode, mount path, ownership,
  encryption, backup, publication, retention duration, and deletion ownership
  remain undecided.
- **ST-007 [Confirmed]:** Every sandbox must have an ephemeral
  <code>/workspace</code> that is lost with the Pod and is separate from the
  retained results volume.
- **ST-008 [Confirmed]:** Upload targets must be confined beneath the configured
  workspace root. Absolute paths, <code>..</code>, symlink escapes, device
  files, and writes through existing symlinks must be rejected.
- **ST-009 [Confirmed]:** Uploads must write through a bounded temporary file,
  verify declared size and checksum, and rename atomically.
- **ST-010 [Confirmed]:** Downloads must resolve beneath an explicitly readable
  profile root, reject symlink escapes and special files, and stream no more
  than the approved per-command and per-task limits. Archive extraction must
  not be added implicitly.
- **ST-011 [Confirmed]:** Partial or failed transfers must remove temporary
  workspace files and must never modify a retained result; publishing workspace
  content into results is a separate deferred operation.
- **ST-012 [Open]:** Profiles must declare workspace and ephemeral-storage
  quotas, readable roots, and transfer capability; the numeric limits must be
  owner-approved before a profile is enabled.

## 14. Security Requirements

- **SEC-001 [Confirmed]:** Sandboxes must be non-privileged.
- **SEC-002 [Confirmed]:** Sandboxes must not use host namespaces.
- **SEC-003 [Confirmed]:** Sandboxes must not use <code>hostPath</code> storage
  by default.
- **SEC-004 [Confirmed]:** The manager must not mint per-task sandbox connection
  credentials. Agent-transport credentials must be issued, stored, rotated,
  revoked, and bound to agent sessions under an owner-approved lifecycle;
  remaining details are Open.
- **SEC-005 [Confirmed]:** Failure to obtain gVisor isolation must fail sandbox
  creation.
- **SEC-006 [Confirmed]:** Every operation must be authenticated by the
  transport principal and authorized against the owning agent session; one
  agent must not control another agent's task, and stale sessions must be
  rejected.
- **SEC-007 [Open]:** Tenant model, extended RBAC, Pod security context,
  service accounts, capabilities, root filesystem policy, secret delivery,
  credential rotation mechanics, and image trust remain undecided.
- **SEC-008 [Confirmed]:** The manager service account is a remote-execution
  authority; its RBAC must be limited to the scoped operations of KR-010 and
  must be audited.
- **SEC-009 [Confirmed]:** Command arguments, environment values, file contents,
  PTY input, and stream data must never appear in manager logs, metrics, or
  Kubernetes request metadata. Audit records must contain identifiers,
  operation, policy version and decision reference, sizes, outcome, latency,
  and the Kubernetes audit correlation identifier when available.
- **SEC-010 [Confirmed]:** Sandbox ingress must always be denied. Sandbox egress
  must be denied unless a profile explicitly permits DNS or approved
  destinations; relay traffic itself must not require sandbox network ingress
  or egress.

## 15. Reliability, Efficiency, and Operations

- **NFR-001 [Confirmed]:** The manager must expose health and readiness signals.
- **NFR-002 [Confirmed]:** Lifecycle logs must be structured and contain request
  ID, task ID, and command ID where applicable.
- **NFR-003 [Confirmed]:** Health-check attempts, latency, invalid responses,
  missed responses, command admissions, policy decisions and denials, relay
  frames, backpressure events, command outcomes, and resulting terminations
  must be observable as metrics and structured logs.
- **NFR-004 [Derived]:** Capacity use and queued-task count must be observable.
- **NFR-005 [Derived]:** Reconciliation and cleanup failures must be observable.
- **NFR-006 [Derived]:** Polling must not be used where the selected queue or
  Kubernetes API provides a blocking delivery or watch mechanism.
- **NFR-007 [Derived]:** Common images should be cached on gVisor nodes to reduce
  provisioning latency without reserving warm-sandbox compute.
- **NFR-008 [Open]:** Manager CPU, memory, and dependency budgets remain
  undecided pending the Stage 4c measurements.
- **NFR-009 [Open]:** Provisioning latency, recovery, availability, and cleanup
  SLOs remain undecided.
- **NFR-010 [Open]:** Metrics/log backends, alerts, dashboards, audit retention,
  and disaster recovery remain undecided.
- **NFR-011 [Derived]:** Manager and API-server CPU, memory, connection, and
  throughput use under relay load with ten active sandboxes must be measured
  against owner-approved budgets.
- **NFR-012 [Derived]:** Framing, sequencing, backpressure, and per-command and
  per-task caps must remain bounded under slow consumers and oversized input.

## 16. Failure Handling

| Failure | Required behavior | Open policy |
|---|---|---|
| Malformed request | Reject without provisioning | Response/dead-letter mechanism |
| Unknown sandbox type | Reject without provisioning | Error schema |
| Duplicate delivery | Reconcile existing task; create no duplicate | Response replay |
| Identifier reused with different content | Create no second sandbox | Conflict response |
| Capacity full | Keep request queued/pending | Ordering and fairness |
| gVisor unavailable | Fail; never fall back | Retry count and timing |
| Storage unavailable | Do not expose readiness | Retry and timeout |
| Readiness failure | Do not open relay admission | Timeout and retry |
| Manager restart | Reconcile durable and Kubernetes state | HA timing |
| Manager restart mid-command | Record <code>UNKNOWN</code>; never auto re-issue | Caller retry contract |
| Command stream lost | Record <code>UNKNOWN</code>; never assume process exit | Later supervisor option |
| Pod replaced before exec | Refuse relay on UID mismatch | Recreate policy |
| Termination races admission | Durable fence wins; no new command starts | Cleanup deadline |
| Policy denies or is missing | Reject, audit, execute nothing | Allowlist grammar |
| Oversized frame or slow consumer | Bound buffering; apply backpressure and caps | Cap values |
| Exec timeout without confirmed exit | Record <code>UNKNOWN</code> | Timeout value |
| Pod disappears | Terminate/reconcile as failure | Recreate policy |
| Agent fails 5-second check | Begin termination; record <code>TIMED_OUT / AGENT_UNRESPONSIVE</code> | Physical cleanup SLA |
| Completion | Terminate and retain results | Result finalization |
| Cancellation | Terminate and retain a record | Terminal state name |
| Partial cleanup | Retry idempotently | Retry ceiling and escalation |
| Ready-response publication fails | Preserve durable readiness state | Retrieval/republish contract |
| Queue unavailable | Preserve existing work and state | Buffering and degraded mode |

## 17. Acceptance Criteria

- **AC-001:** One valid request with capacity persists exactly one task and
  provisions exactly one sandbox.
- **AC-002:** Redelivery creates no additional sandbox or result volume.
- **AC-003:** A message is not acknowledged if its state mutation is not
  durable.
- **AC-004:** A crash after durable mutation but before acknowledgement is
  recovered through idempotent redelivery.
- **AC-005:** Manager restart loses no accepted task and creates no duplicate
  Kubernetes resources.
- **AC-006:** Every sandbox explicitly uses the configured gVisor
  <code>RuntimeClass</code>.
- **AC-007:** If gVisor is unavailable, creation fails and no default-runtime
  sandbox starts.
- **AC-008:** Summing all concurrently running workload containers for a
  sandbox gives requests of <code>250m</code> CPU and <code>256Mi</code> memory
  and limits no greater than <code>1</code> CPU and <code>512Mi</code> memory.
- **AC-009:** Request attempts to override resources do not change the required
  values.
- **AC-010:** With 10 capacity-consuming sandboxes, an eleventh remains pending
  and creates no active sandbox until capacity is released.
- **AC-011:** Unknown sandbox types create no Kubernetes resources and receive a
  durable rejection outcome.
- **AC-012:** Every successful spawn resolves an image from its sandbox type.
- **AC-013:** No command is admitted before readiness is durable; first
  admission durably activates the task while <code>READY</code> and
  <code>ACTIVE</code> remain separate states.
- **AC-014:** A ready response identifies the task, the session reference, the
  relay protocol version, and the route; it contains no sandbox endpoint or
  sandbox credential.
- **AC-015:** Result storage is mounted, recorded, and remains available after
  sandbox termination for the configured retention period.
- **AC-016:** Every <code>READY</code> or <code>ACTIVE</code> task receives
  non-overlapping health checks at the 15-second cadence.
- **AC-017:** A valid matching response before the 5-second deadline keeps the
  sandbox eligible for use and updates the last-success timestamp.
- **AC-018:** No valid response by the deadline causes a durable transition to
  termination and ultimately <code>TIMED_OUT / AGENT_UNRESPONSIVE</code>.
- **AC-019:** Late, stale, malformed, nonce-mismatched, task-mismatched, and
  session-mismatched responses have the same outcome as no response.
- **AC-020:** A pre-restart health-check response is rejected and a fresh check
  is issued after restart.
- **AC-021:** Completion cleans ephemeral resources, preserves the result
  reference, and records <code>COMPLETED</code>.
- **AC-022:** Cancellation, deadline, Pod loss, and unrecoverable failure each
  initiate idempotent cleanup and leave a durable terminal record.
- **AC-023:** Repeated cleanup after partial failure neither affects another
  task nor creates new resources.
- **AC-024:** Logs and metrics expose correlation, capacity, lifecycle
  transitions, health-check latency/failure, and cleanup failure.
- **AC-025:** No operational or test artifact contains credentials, command
  arguments, environment values, stream payloads, or file contents.
- **AC-026:** All agent operations transit the authenticated manager relay;
  sandboxes expose no inbound Service or permitted ingress path.
- **AC-027:** Every operation reaches the policy port before side effects; a
  denial or missing policy is audited and never executed, independent of the
  future filtering method.
- **AC-028:** Upload and download remain within approved roots and caps, reject
  traversal and symlink escapes, handle partial failure atomically, and do not
  disturb retained results.
- **AC-029:** Stream loss or manager restart yields <code>UNKNOWN</code> for any
  command without a recorded terminal outcome; it never implies termination,
  success, or automatic retry.
- **AC-030:** The authenticated transport principal, not a caller-provided
  session field, must own the task; cross-task and stale-session operations are
  rejected.
- **AC-031:** Once termination wins the durable admission race, no new command
  starts; cleanup revokes relay admission before stopping or deleting the
  workload.
- **AC-032:** Relay execution occurs only after the observed Pod UID, task
  annotation, labels, namespace, and container match the durable task record.
- **AC-033:** Framing, sequencing, backpressure, cancellation signaling, and
  per-command and per-task caps remain bounded under slow consumers and
  oversized input.

Acceptance tests depending on Open decisions cannot be finalized until those
decisions are approved.

## 18. Traceability

| Source | SRS coverage | Verification |
|---|---|---|
| IR-1: Minimal, efficient implementation | RR-001–RR-009, NFR-006–NFR-012 | Architecture/resource review; quantitative budgets remain Open |
| IR-2: Queue input and preserved state | FR-001–FR-012, AC-001–AC-005 | Redelivery and restart tests |
| IR-3: gVisor sandboxing | KR-001–KR-003, AC-006–AC-007 | Runtime inspection and unavailable-runtime test |
| IR-4: 1 CPU / 512 MiB maximum | RR-001–RR-006, AC-008–AC-009 | Manifest inspection and load test |
| IR-5: Image for every spawn | FR-013–FR-018, AC-011–AC-012 | Profile resolution tests |
| IR-6: Multiple sandbox types | FR-013–FR-018 | Supported/unsupported profile tests |
| IR-7: Volumes preserve results | ST-001–ST-006, AC-015 | Termination and retention test |
| UC-1: Agent requests and waits | FR-001–FR-007 | Spawn workflow |
| UC-2: Manager returns the relay route | FR-019–FR-032, AC-013–AC-014 | Provision/readiness/response workflow |
| UC-3: Multiple interaction rounds through the relay | FR-033–FR-035, FR-053–FR-058, AC-029, AC-031 | Session continuity and outcome tests |
| UC-4: Agent reports completion | FR-045, FR-050, AC-021 | Completion workflow |
| UC-5: Timeout/error closes sandbox | FR-036–FR-044, FR-046–FR-052, AC-016–AC-020 | Health timeout and fault injection |
| Owner decision: 15-second/5-second check | FR-036–FR-044, AC-016–AC-020 | Deterministic timing tests |
| Owner decision: resource-saving requests | RR-001–RR-006, AC-008–AC-010 | Manifest and capacity tests |
| Owner direction 2026-08-28: manager-mediated relay | FR-025, FR-029–FR-035, FR-053–FR-066, KR-006, KR-008–KR-010, ST-007–ST-011, SEC-004, SEC-006, SEC-008–SEC-010, AC-013–AC-014, AC-025–AC-033 | Relay contract tests, RBAC/audit inspection, staging fault campaign |
| Owner direction 2026-08-28: command control and workspace operations | FR-059–FR-066, ST-008–ST-011, AC-027–AC-028 | Policy denial and transfer safety tests |

## 19. Risks

| Risk | Impact | Required mitigation or decision |
|---|---|---|
| One missed 5-second response terminates healthy work | Lost session and interrupted work | Owner accepts strict policy; preserve results; define transport SLO |
| Health-check transport cannot meet the deadline | Primary liveness requirement cannot be enforced | Approve a low-latency authenticated transport |
| <code>pods/exec</code> cannot provide durable command outcomes | Lost commands reported as <code>UNKNOWN</code> | Explicit <code>UNKNOWN</code>; no automatic retry; later supervisor option |
| API server or manager becomes a data-plane bottleneck | Relay latency and stability degrade | Bounded frames, one command per task, measured budgets (NFR-011) |
| Deterministic Pod name resolves to the wrong object | Command executes in a replaced Pod | Verify stored UID and ownership before every exec |
| Termination races command admission | Work starts after termination decided | Durable admission fence and one-command serialization |
| Policy is bypassed or prematurely frozen | Unsafe commands execute, or unusable policy ships | Mandatory versioned port, default deny, placeholder grammar |
| PTY undermines command inspection | Uninspectable interactive channels | Treat PTY as an opaque capability; disable by default |
| File transfer escapes the workspace or exhausts resources | Data corruption or resource starvation | Root confinement, atomic writes, quotas, caps |
| Egress policy breaks workloads or weakens isolation | Blocked tasks or broad network access | Per-profile rules and image qualification |
| Burstable QoS under node pressure | Running sandboxes may be evicted | Capacity planning and workload testing |
| gVisor incompatibility | Provisioning/runtime failure | Qualify every sandbox profile |
| Multi-replica concurrency is undefined | More than the intended sandboxes may start | Reserved contracts in Section 20; approve before multi-worker |
| Storage retention is undefined | Result loss or unbounded cost | Approve storage and retention policy |
| Efficiency has no manager budget | Cannot objectively accept "minimal" | Define manager resource and latency budgets |
| Cleanup has no completion SLA | "Immediate" can be interpreted differently | Define transition and deletion bounds |

## 20. Open Decisions Required Before Engineering Baseline

Decisions closed by owner direction on 2026-08-28:

1. Response channel (was Decision 4): the ready response carries task ID,
   session reference, relay protocol version, and route; no sandbox endpoint or
   credential.
2. Kubernetes topology (was Decision 7): direct Pod, ephemeral
   <code>/workspace</code>, retained results PVC, optional workload Secret,
   ingress-deny NetworkPolicy, approved per-profile egress, and the
   <code>pods/exec</code> relay.
3. Security model ingress/egress (was Decision 13): single-tenant v1; sandbox
   ingress always denied; egress denied unless a profile explicitly permits;
   agent-to-manager policy enforced at the transport.
4. Relay mechanism (new, Decision 17): <code>pods/exec</code> invoking one
   fixed, qualified relay helper so user argv never enters Kubernetes request
   or audit data; durable in-sandbox execution requires a later supervisor or
   reverse channel.
5. Command protocol (new, Decision 18): versioned structured control envelopes
   plus bounded, sequenced data frames for <code>exec</code>, optional
   <code>pty</code>, <code>upload</code>, and <code>download</code>; opaque
   interactive content is never represented as policy-inspectable.
6. Command outcomes (new, Decision 20): every command has a unique command ID
   and a terminal outcome or <code>UNKNOWN</code>; transport loss and manager
   restart never imply remote execution stopped, and the manager never
   automatically re-issues an unknown command.
7. Workspace separation (new, Decision 21): <code>/workspace</code> is
   ephemeral and separate from retained <code>/results</code>; transfer roots,
   path handling, quotas, and persistence behavior are part of the profile
   contract.
8. Principal binding (new, Decision 22): the transport supplies an
   authenticated principal independently of message fields, bound to the
   durable agent session; a caller-provided session ID is not proof of
   identity.

Decisions partially closed:

9. Command policy (new, Decision 19): the versioned
   <code>CommandPolicy</code> port and profile policy reference are mandatory,
   with default-deny missing-policy behavior and reserved evaluation fields;
   the allowlist language, filtering methods, and matching rules remain
   **Open** pending a separate security decision.
10. Command transport: authenticated Core NATS with request/reply admission and
    task-scoped ephemeral frame subjects is the baseline, subject to Stage 4a
    spike validation before schema freeze.

Still open:

11. JetStream delivery, retry, dead-letter, ordering, schema publication, and
    retention.
12. Durable source of truth and consistency/idempotency model.
13. Health-check transport authentication details and outage behavior.
14. Transport-credential issuance, rotation, revocation, and expiry.
15. Whether <code>READY</code> and <code>ACTIVE</code> remain separate, and the
    explicit-cancellation terminal state.
16. Provisioning timeout, readiness, maximum lifetime, retries, and failure
    classification.
17. Numeric workspace quotas, readable roots, PTY enablement per profile, and
    egress destinations.
18. Terminal record, audit, log, and metric retention.
19. Quantitative manager efficiency and latency targets, and the physical
    cleanup SLA.

Reserved for multi-worker operation (out of v1 scope): the durable repository
records capacity reservation, task claim, worker ID, lease expiry, and fencing
generation; a worker subscribes to a task route only after winning its durable
claim; command frames are not queued while no owner subscribes; future takeover
requires atomic claim acquisition, subscription handoff, supervision
reconstruction, and explicit handling of commands left <code>UNKNOWN</code>.
Per-worker capacity means total capacity is the worker count multiplied by ten.

Previously recommended choices such as Go, NATS JetStream, a
<code>SandboxTask</code> custom resource, leader election, WebSocket/TLS,
digest-pinned images, one PVC per task, NetworkPolicy, and specific retention
durations are recommendations, not approved requirements, except where closed
above.

## 21. Engineering Handoff Gate

Sprints 1-3 delivered the pre-relay application slice; Stage 4a of the Phase 4
plan is the active gate. The implementation baseline is complete when:

1. The Stage 4a contract and feasibility gates close: decisions above are
   approved, the authenticated transport and <code>pods/exec</code> spikes pass
   in a representative cluster, and the relay schemas freeze.
2. The state-transition table, message schemas, profile schema, policy schema,
   and failure/reconciliation matrix are published in versioned form.
3. AC-001 through AC-033 convert to executable unit, integration,
   fault-injection, and end-to-end tests.
4. The document owner and approver are assigned and the SRS moves from Draft to
   Approved.

## 22. References

- [Initial Sandbox Manager Requirements](./init_req.md)
- [Phase 4 Relay Update and Completion Plan](../planning/phase4_relay_update_and_release_plan.md)
- [Four-Sprint Delivery Plan](../planning/four_sprint_delivery_plan.md)
- [Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/)
- [Kubernetes Resource Management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Kubernetes Pod Quality of Service](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/)
- [Kubernetes Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
- [Kubernetes Pod Exec](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#exec)
- [gVisor Kubernetes Quick Start](https://gvisor.dev/docs/user_guide/quick_start/kubernetes/)
