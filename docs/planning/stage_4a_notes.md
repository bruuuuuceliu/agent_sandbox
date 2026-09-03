# Stage 4a Engineering Notes

## Status

Stage 4a (contract and feasibility gate) is implemented and locally verified,
with two exceptions that require a staging cluster and are listed under Staging
Checklist below. Live-cluster `pods/exec` streaming and the gVisor image track
remain unverified because this workspace has no configured Kubernetes context
— the same constraint recorded in Sprint 2 and Sprint 3 notes.

The relay wire schemas, admission fencing, command policy port, workspace path
safety, and audit contracts are exercised by 117 new tests plus a live
authenticated Core NATS spike against the local container (nats:2.11). The
SRS v0.2 and initial-requirements rewrites (plan Section 2.4) are complete,
including the Stage 4a decision amendments (Decisions 17-22) and AC-026
through AC-033.

Decision 19's filtering grammar, numeric workspace quotas, and PTY enablement
remain explicitly Open, as the plan requires; the shipped code reserves the
evaluation fields and default-denies everything else.

## Delivered Scope

- Versioned relay wire schemas (`relay_protocol.py`): JSON control envelope
  with strict duplicate-key and unknown-field rejection; bounded binary data,
  cancel, and terminal frames (64 KiB fixed frame maximum, 256 MiB declared
  maximum); per-channel gap-free and duplicate-free frame sequencing; a
  payload-free terminal outcome model in which `UNKNOWN` is a first-class
  terminal outcome.
- Versioned command policy extension point (`command_policy.py`): reserved
  normalized evaluation fields (operation, profile, transport-authenticated
  principal, executable, argv, working directory, environment names, path,
  declared size), a `CommandPolicy` port, and default-deny resolution when a
  profile references a missing or invalid policy. No filtering method ships.
- Transport-neutral command admission (`commands.py`): authentication against
  the durable agent session before policy, policy before any side effect,
  atomic `READY -> ACTIVE` activation, one in-flight command per task, process
  fence generations with `STALE_FENCE` refusal, `TERMINATING` closing
  admission, repository persistence errors surfaced as `CommandPersistenceError`,
  and every relay exception mapped to `UNKNOWN` with the slot always released.
- Payload-free command audit entries that cannot carry argv, environment
  values, stream data, or file contents by construction (asserted by a field
  enumeration test).
- Workspace path safety (`workspace.py`): upload destinations confined beneath
  the workspace root with traversal, absolute, backslash, and control-text
  rejection; download sources confined beneath approved readable roots with
  prefix-escape rejection; symlink-component and special-file refusal;
  streaming SHA-256 verification; the 4b `CommandWorkspace` port.
- Thin Kubernetes client spike (`kubernetes_exec.py`): service-account-style
  configuration (no kubeconfig), `pods/get` snapshot parsing, KR-008
  verification (UID, namespace, phase, task annotation, labels, container)
  immediately before exec, a minimal RFC 6455 WebSocket upgrade with the
  `v4.channel.k8s.io` subprotocol, KR-009 exec request construction that only
  ever names the fixed qualified helper in the URL with argv traveling in a
  length-prefixed stdin frame, and fail-closed termination classification
  where only a received error-channel status yields `SUCCESS` or `FAILED`.
- Authenticated Core NATS spike (`examples/spikes/nats_relay_spike.py`): a
  minimal stdlib NATS client proving credential-refused connects, transport
  principal binding, admission request/reply, ephemeral task-scoped sequenced
  frames, server payload cap fit, bounded slow-consumer drops, abrupt stream
  loss mapping to `UNKNOWN`, and no redelivery after reconnect.
- Requirements rewrite: SRS v0.2 (FR-053-066, KR-008-010, ST-007-011,
  SEC-008-010, NFR-011-012, AC-026-033) and the initial-requirements document
  restated for the manager-mediated relay, with owner-direction traceability
  rows dated 2026-08-28.

## Evidence

Local verification, 2026-08-30 (Python 3.12.13, stdlib only, `PYTHONPATH=src`):

- `python3 -m pytest -q` — 229 tests and 64 subtests passed (112 pre-existing
  plus 117 new across relay protocol, command policy, admission, workspace,
  and the Kubernetes exec client).
- `python3 -m ruff check src tests examples/spikes` — all checks passed.
- `python3 -m mypy --strict src tests` — no issues in 39 source files.
- `PYTHONPATH=src python3 examples/spikes/nats_relay_spike.py` — 9/9 scenarios
  passed against the live local container (127.0.0.1:15181, nats:2.11-alpine
  with single-user basic auth). Five consecutive runs were clean after one
  transient failure in the reconnect scenario exposed a scenario race (the
  disconnect had not settled server-side before the outage publish); the
  scenario now waits for the disconnect to settle and asserts the gap.

NATS spike evidence lines (payload-free; identifiers, sizes, counts, outcomes):

- Wrong credentials are refused by the server (`-ERR 'Authorization
  Violation'`) before any application decision; the transport principal is
  the authenticated user, not a payload field.
- Admission request/reply admitted a task only for its owning principal; a
  well-formed envelope naming another principal's task was refused with
  `wrong_principal`.
- A 65,495-byte frame at the relay bound was published and delivered; the
  server cap (1,048,576 bytes) exceeds the 64 KiB relay frame bound.
- Three sequenced frames plus a terminal frame crossed an ephemeral
  task-scoped subject gap-free; `FrameSequencer` accepted all frames and the
  subject disappeared with the subscription.
- A consumer holding four pending slots received 4 of 16 published frames and
  dropped 12 — buffering stayed bounded; overflow drops rather than grows.
- An abrupt publisher loss left two data frames and no terminal frame; the
  outcome is `UNKNOWN`, and no component replayed the missing terminal state.
- After a reconnect the frame published during the outage was not
  redelivered (Core NATS is at-most-once); the post-reconnect frame showed a
  permanent `FrameSequenceError` gap, which maps to `UNKNOWN` — correctness
  requires a fresh command, never an automatic re-issue.

Kubernetes exec contract tests (in-process fake API server, no cluster):

- `pods/exec` request paths contain only the fixed helper command, container,
  and channel flags; a distinctive argv sentinel appears nowhere in the
  request path or query, only inside the masked stdin WebSocket frame.
- A Pod replaced under the task (UID mismatch) is refused with
  `uid_mismatch` before any exec request is issued (zero exec records).
- A confirmed exit code 0 is `SUCCESS`, a confirmed non-zero exit is
  `FAILED`, and an unconfirmed or unrecognised status is `UNKNOWN`.
- An abrupt mid-stream drop yields `UNKNOWN / stream_lost`.
- The PTY operation sets `tty=true` in the exec request; EXEC does not.
- `pods/get` failures and exec upgrade refusals surface as
  `KubernetesExecError` rather than silent success.

## Staging Checklist

Blocked on a staging cluster; none of these are claimed as verified:

- Live `pods/exec` streaming against a real API server, including the
  WebSocket upgrade over TLS with a service-account token, RBAC enforcement
  for the scoped `pods/get` + `pods/exec` role, and API audit-log confirmation
  that argv never appears in request metadata (KR-009).
- Live Pod-replacement race: delete the Pod mid-command and confirm the UID
  check refuses the replacement before exec.
- Live cancellation and stream-loss behaviour against kubelet timing, not the
  in-process fake server.
- One qualified sandbox image containing the fixed
  `/usr/local/bin/sandbox-relay-helper`, with gVisor `RuntimeClass`,
  StorageClass, and CNI as tracked in Stage 4c's environment inventory.
- Resource-load measurements under the Stage 4c budgets; the spike applies a
  provisional 256 MiB per-direction stream budget that staging must validate.

## Notes for Stage 4b Estimation

Observed Stage 4a throughput suggests the 39-point Stage 4b estimate is
plausible but front-loaded: the relay domain layer (admission gate, policy
port, workspace safety, audit) is already in place and tested, so 4b work is
concentrated in transport adapters (NATS adapter against the spike client's
proven protocol subset, Kubernetes relay wiring against
`KubernetesCommandRelay`), the durable command repository beyond the
single-worker in-memory implementation, and the fixed helper binary itself.
The NATS spike showed Core NATS delivers at-most-once with no session
resumption, so the 4b adapter must classify post-reconnect streams as
`UNKNOWN` and never replay — the domain layer already enforces this. The
`v4.channel.k8s.io` WebSocket client in `kubernetes_exec.py` is spike-grade:
it lacks ping/pong keepalives beyond responses, continuation-frame
reassembly, and close-code handling, which 4b must complete or replace with a
maintained client.
