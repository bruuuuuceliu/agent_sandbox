# Sandbox Manager: Initial Requirements

Status: Draft

Last updated: 2026-08-28

## 1. Purpose

Build a small sandbox manager for an existing Kubernetes cluster. The manager
creates isolated, task-specific sandboxes, operates them on the agent's behalf
through a manager-mediated command relay, and removes them when work completes,
times out, or fails.

The agent never connects to a sandbox. It issues commands and transfers to the
manager over one authenticated transport; the manager authorizes each
operation, relays it into the sandbox through the Kubernetes API, and returns
framed output with a terminal or unknown outcome.

The cluster and a working gVisor `RuntimeClass` are deployment prerequisites.

## 2. Goals

- Keep the implementation and operational footprint minimal.
- Accept sandbox lifecycle requests through a durable queue.
- Preserve task and sandbox state across manager restarts.
- Run every sandbox with gVisor isolation.
- Enforce a hard per-sandbox limit of 1 CPU and 512 MiB of memory.
- Support different task types with different sandbox configurations.
- Select an image for every sandbox creation.
- Attach persistent storage so task results survive sandbox deletion.
- Mediate every agent operation through one authenticated manager relay.
- Control whether a command may run by evaluating a command policy before
  execution.
- Offer built-in workspace operations such as upload and download.
- Run no more than 10 sandboxes per manager worker at the same time by default.

## 3. Functional Requirements

### FR-1: Queue input

The manager must consume spawn and lifecycle events from a durable queue.
Processing must be safe when a message is delivered more than once. Each
request therefore needs a unique request ID and task ID.

### FR-2: Durable state

The manager must durably record, at minimum:

- request ID and task ID;
- sandbox type and selected image;
- current lifecycle state;
- Kubernetes resource identity, including the Pod UID used by the relay;
- volume identity;
- relay route and owning agent-session reference, when available;
- admission state, including activation and the in-flight command ID;
- owning agent session and last successful health-check timestamp;
- creation, update, deadline, and completion timestamps;
- terminal result or error reason.

After a restart, the manager must reconcile stored state with Kubernetes and
continue, repair, or terminate incomplete work as appropriate.

### FR-3: Sandbox types and images

Each spawn request must name a supported sandbox type. A sandbox type maps to
an approved configuration, including its container image, command, relay
capability set, command-policy reference, workspace rules, container identity,
egress declaration, and volume mounts. The manager must reject unknown types.

An image must be resolved for every spawn. Image selection should normally be
controlled by the sandbox type rather than accepting an unrestricted image
from a requester.

### FR-4: gVisor isolation

Every sandbox Pod must use the configured gVisor Kubernetes `RuntimeClass`
(typically backed by `runsc`). A sandbox must not fall back to the default OCI
runtime if the gVisor runtime is unavailable.

### FR-5: Resource requests and limits

Every sandbox must use lower scheduling requests while preserving the hard
upper limits of 1 CPU and 512 MiB of memory:

```yaml
resources:
  requests:
    cpu: 250m
    memory: 256Mi
  limits:
    cpu: "1"
    memory: 512Mi
```

This deliberately gives sandboxes Burstable QoS so idle sandboxes reserve fewer
cluster resources. The limits remain mandatory and cannot be overridden by a
spawn request.

The initial concurrency limit is 10 active sandboxes per manager worker. When
capacity is full, new requests remain queued or pending until capacity becomes
available.

### FR-6: Persistent results and ephemeral workspace

Each sandbox must mount storage for task outputs. The storage identity must be
associated with the task record, and deleting the sandbox must not delete its
results before the configured retention policy allows it.

Each sandbox must also have an ephemeral `/workspace` that is lost with the
Pod. Uploads and downloads are confined to approved roots: absolute paths,
`..`, symlink escapes, and device files are rejected; uploads verify declared
size and checksum and rename atomically; failed transfers leave no partial
files and never touch retained results.

The manager must report the result volume identity or result location in the
terminal task record.

### FR-7: Command relay

The manager must mediate every agent operation. After readiness is durable, the
agent sends versioned command envelopes over one authenticated transport;
admission uses Core NATS request/reply and bounded data frames use task-scoped
ephemeral subjects.

The transport supplies an authenticated principal that the manager binds to
the owning agent session; a session ID inside a message is never proof of
identity. Every command carries a unique command ID. Commands are admitted at
most one per task at a time; the first admitted command durably activates a
ready task; a durable transition to termination closes admission before
cleanup.

A command policy must be evaluated before every side effect. Missing or
invalid policy denies admission. `exec`, optional `pty`, `upload`,
`download`, and workspace setup all pass through the same evaluation.

The manager relays admitted operations through Kubernetes: immediately before
execution it reads the Pod and verifies UID, task annotation, labels,
namespace, and container against the task record, then invokes one fixed,
qualified relay helper through `pods/exec`, with the operation and user command
carried in the bounded stdin protocol rather than Kubernetes query arguments.

Command outcomes are `SUCCESS`, `FAILED`, `REJECTED`, confirmed `CANCELLED`,
or `UNKNOWN`. Stream loss, unconfirmed cancellation, timeout without confirmed
exit, and manager restart produce `UNKNOWN`. The manager never automatically
re-issues an unknown command; the caller may submit a new command ID after
reconnecting.

Command arguments, environment values, stream payloads, PTY input, and file
contents must never appear in logs or unprotected metadata.

### FR-8: Lifecycle management

The minimum lifecycle is:

```text
QUEUED -> PROVISIONING -> READY -> ACTIVE -> TERMINATING -> COMPLETED
                         |          |             |
                         +----------+-------------+-> FAILED, TIMED_OUT, or CANCELLED
```

- `QUEUED`: accepted but waiting for manager capacity.
- `PROVISIONING`: Kubernetes resources are being created.
- `READY`: readiness is durable and relay admission is available.
- `ACTIVE`: at least one command has been admitted and the agent is using the
  sandbox through the relay.
- `TERMINATING`: relay admission is closed and cleanup is in progress.
- `COMPLETED`, `FAILED`, `TIMED_OUT`, and `CANCELLED`: terminal states retained
  for audit and recovery.

An agent completion event must move the sandbox toward termination. A failed
agent health check, deadline, unrecoverable provisioning/runtime error, deleted
Pod, or explicit cancellation must also trigger termination and a terminal
record. Exact provisioning, maximum-lifetime, and retry rules remain design
decisions.

### FR-9: Active agent health checking

While a sandbox is `READY` or `ACTIVE`, the manager must actively check the
owning agent every 15 seconds. Each check must contain a unique nonce and the
current task and agent-session IDs.

The agent has 5 seconds to return a valid response. A late, stale, mismatched,
or missing response does not count. If no valid response is received in that
window, the manager must immediately move the sandbox to `TERMINATING`, close
relay admission, close the sandbox, and record `TIMED_OUT` with reason
`AGENT_UNRESPONSIVE`.

Checks must not overlap for the same task. A manager restart must issue a new
check rather than accepting a response to a check created by the previous
manager process.

### FR-10: Kubernetes cleanup

Termination must remove ephemeral Kubernetes resources owned by the sandbox.
Cleanup must be idempotent. Persistent result storage follows its separate
retention policy. No sandbox Service or ingress exception exists to remove; a
sandbox never receives inbound network exposure.

## 4. Agent Sandbox Use Case

1. An agent submits a spawn request to the task queue and waits.
2. The manager validates the request and records it as `QUEUED`.
3. When capacity is available, the manager creates the task's storage and
   gVisor-backed sandbox, then records `PROVISIONING`.
4. After readiness succeeds, the manager records `READY` and returns the relay
   route, session reference, and protocol version through the configured
   response channel.
5. The agent issues commands, uploads, and downloads through the manager
   relay. The first command durably activates the sandbox; the same sandbox
   and workspace serve multiple rounds until termination starts.
6. While the sandbox is in use, the manager checks the owning agent every 15
   seconds and requires a valid response within 5 seconds.
7. The agent publishes completion or cancellation, or the manager detects an
   unanswered health check, deadline, or unrecoverable error.
8. The manager closes relay admission, terminates the sandbox, preserves the
   configured results, and records a terminal state.

## 5. Operational and Security Requirements

- The manager must expose health/readiness signals and structured lifecycle
  logs containing request ID, task ID, and command ID.
- Queue acknowledgement must occur only after the corresponding state change
  is durable.
- Kubernetes names and labels must make all resources traceable to a task.
- Sandboxes must be non-privileged and must not use host namespaces or
  `hostPath` volumes by default.
- Sandboxes must have no inbound network exposure; egress is denied unless a
  sandbox type explicitly permits it.
- The manager must not mint per-task sandbox connection credentials. Agent
  transport credentials follow an owner-approved issuance, rotation, and
  revocation lifecycle.
- The manager's Kubernetes identity must be limited to `pods/get` and
  `pods/exec` in the sandbox namespace and must be audited.
- The concurrency limit must be configurable, with a default and initial
  maximum of 10 per manager worker.
- Agent health-check attempts, response latency, command admissions, policy
  decisions and denials, command outcomes, and relay backpressure must be
  exposed as metrics and structured logs without payload content.

## 6. Initial Acceptance Criteria

- A valid queued request creates one gVisor-backed sandbox and returns the
  relay route after readiness; no sandbox endpoint is ever exposed.
- Duplicate delivery of the same request does not create another sandbox.
- A sandbox cannot exceed 1 CPU or 512 MiB of memory.
- A sandbox initially requests only 250 millicores and 256 MiB of memory.
- An eleventh request waits while 10 sandboxes are active on a worker.
- All agent operations transit the authenticated manager relay, and the
  authenticated principal must own the task.
- A command is admitted only after readiness is durable, at most one command
  per task runs at a time, and termination closes admission before cleanup.
- Every command is policy-checked before execution; a missing or denied policy
  results in an audited `REJECTED` outcome, never execution.
- Stream loss or manager restart yields `UNKNOWN` and never an automatic retry.
- Upload and download stay within approved roots and caps and reject traversal
  and symlink escapes.
- Each `READY` or `ACTIVE` sandbox receives an agent health check every 15
  seconds.
- A matching response received within 5 seconds keeps the sandbox active.
- A missing or invalid response after 5 seconds terminates the sandbox as
  `TIMED_OUT` with reason `AGENT_UNRESPONSIVE`.
- Completion, cancellation, timeout, and unrecoverable error paths remove the
  sandbox and retain a terminal record.
- The result volume remains available after sandbox termination according to
  its retention policy.
- Restarting the manager does not lose accepted tasks or create duplicate
  sandboxes.
- No log or artifact contains credentials, command arguments, environment
  values, stream payloads, or file contents.

## 7. Open Design Decisions

- JetStream delivery, retry, dead-letter, ordering, and retention rules.
- Durable-state technology and its consistency and idempotency model.
- Health-check transport authentication details and outage behavior.
- Agent-transport credential issuance, rotation, revocation, and expiry.
- Sandbox type schema, approved images, and image version/digest policy.
- Command-policy allowlist language, filtering methods, and matching rules;
  the policy port, profile reference, and default-deny behavior are already
  required.
- Numeric workspace quotas, readable roots, PTY enablement per sandbox type,
  and approved egress destinations.
- Volume backend, access mode, size, ownership, and retention duration.
- Provisioning readiness, maximum-lifetime, retry, and non-agent failure rules.
- Whether `READY` and `ACTIVE` need to remain separate observable states, and
  the explicit-cancellation terminal state.
- Terminal-state and audit-record retention duration.
- Multi-worker operation: reserved contracts exist (task claims, leases,
  fencing generations), but takeover behavior is out of scope for v1.
- Durable or exactly-once command execution via an in-sandbox supervisor or
  reverse channel; deferred until required.
