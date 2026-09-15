# Stage 4b Engineering Notes

## Status

Stage 4b (relay implementation and cutover) is implemented and locally
verified. All six planned backlog stories (plan Section 5.2) are delivered:
the production `pods/exec` relay adapter was completed in Stage 4a's client
and is now wired end-to-end; the authenticated Core NATS command API, durable
admission lifecycle integration, and workspace transfer operations are new;
the direct agent-to-sandbox path is deleted. Every Section 5.5 exit criterion
is mapped to verification below.

The full-chain flow suite composed the shipped components against in-process
NATS and Kubernetes protocol fakes and found one genuine transport defect —
an at-most-once delivery race that silently lost the terminal frame of any
command failing fast on the manager side. Both sides of the subscription
ordering are now structural and regression-tested (see Transport Race).

Staging-dependent verification remains exactly as recorded in Stage 4a notes:
no cluster context exists in this workspace, so live `pods/exec`, RBAC, and
API-audit evidence stay on the Stage 4c checklist. Decision 19's filtering
grammar, numeric workspace quotas, and production PTY enablement remain Open.

## Delivered Scope

- Authenticated Core NATS command API (`nats_client.py`, `nats_relay.py`):
  a stdlib asyncio client (CONNECT auth, PING/PONG, PUB/SUB/UNSUB, bounded
  request/reply inbox, a 64 KiB publish bound, and per-subscription pending
  queues that drop with a counter instead of growing); the manager-side
  `NatsCommandServer` serving `sandbox.agent.<principal>.admit` under a
  wildcard, recovering the transport principal from the subject alone and
  translating gate refusals to stable reply codes; per-command sessions on
  task-scoped ephemeral subjects with a merged input stream that honours
  per-publisher ordering ahead of the `.closed` signal; and the agent-side
  `NatsCommandClient` that fails closed to `UNKNOWN` at every stage
  (unanswered admission, malformed reply or frame, unpublished input, missing
  terminal frame) and never re-issues.
- Durable admission integration (`commands.py`, `admission_lifecycle.py`):
  `InMemoryCommandRepository` gained `seed_ready` (seed-before-publish via
  `AdmissionSeedingPublisher`, idempotent re-seeding that refreshes the fence
  while refusing terminating tasks and owner or profile changes), an
  idempotent `begin_termination`, and `recover(fence)` which adopts a new
  process fence, classifies prior-fence in-flight commands `UNKNOWN`, releases
  their slots, and never re-issues them (Decision 20).
- Lifecycle wiring (`lifecycle.py`): the manager takes an `AdmissionCloser`
  port and closes relay admission durably before the first cleanup step; a
  close failure raises `LifecyclePersistenceError` and prevents cleanup
  entirely. `CleanupStep.REVOKE_ACCESS` remains as a durable ordering marker
  only — relay admission is already closed and there is no inbound network
  path left to revoke — so cleaner implementations drop `revoke_access`
  without changing the recorded step order.
- Workspace transfers over the relay (`relay_workspace.py`): uploads stream
  as one argv frame (destination, declared size, SHA-256) plus chunked STDIN
  frames with declared-size accounting that cuts over-long streams; downloads
  stream STDOUT through the output sink under the caller's byte limit. Both
  inherit the gate's admission ordering; the manager principal is a
  constructor input, never a payload field.
- Direct-path removal (plan Section 5.4 step 4): `ConnectionReference` is
  replaced by `RelayReference(protocol_version, route)` where the route is
  built from the task's owning agent session via
  `admission_subject(principal)` inside the readiness failure boundary;
  `SandboxProfile` loses its connection protocol and port fields; the
  provisioner builds only PVC, default-deny NetworkPolicy, and gVisor Pod
  resources (no Service, no credential Secret, no ingress exception, no
  listening port or readiness probe); cleanup references shrink to the Pod
  and deny policy with UID-checked deletion and retained results.
- Flow verification (`tests/test_relay_flow.py`): the full chain — real NATS
  wire protocol into `FakeNatsServer`, gate, `KubernetesCommandRelay` over
  real HTTP `pods/get` and WebSocket `pods/exec` into `FakeKubeServer` —
  with no `BridgeRelay` stand-in between the gate and Kubernetes.

## Transport Race

The flow suite's Pod-replacement scenario hung deterministically: the relay
fails Pod verification within milliseconds of the grant, the session
published its `UNKNOWN` terminal frame immediately, and the agent — which
subscribed to the output subject only after receiving the grant — missed it
under Core NATS at-most-once delivery, blocking until its terminal timeout.
The symmetric hazard existed on the input side, where the session subscribed
to `.in`/`.closed` after sending the grant.

Both windows are closed structurally in `nats_relay.py`, with the wire format
unchanged:

- The manager subscribes to a granted command's input subjects before sending
  the grant reply. Both travel the manager's connection in order, so by the
  time the agent can publish any frame its subscriber already exists.
- The agent subscribes to the deterministic output subject before sending the
  admission request (task subjects are a pure function of principal, task,
  and command), and a granted reply naming any other subjects is refused as
  `UNKNOWN / subject_mismatch` before any input is published.

Two regression tests pin the behaviour: a relay failing immediately must
deliver its terminal frame (not fall into the timeout), and a rogue grant
naming foreign subjects must fail closed with nothing published to them.

## Evidence

Local verification, 2026-09-02 (Python 3.12.13, stdlib only):

- `python3 -m pytest -q` — 296 tests and 59 subtests passed (229/64 at Stage
  4a close; the net 67 include the NATS API, admission lifecycle, workspace
  transfer, and flow suites, less consolidated Kubernetes resource subtests).
- `python3 -m ruff check sandbox_manager tests examples/spikes` — all checks passed.
- `python3 -m mypy --strict sandbox_manager tests` — no issues in 47 source files.
- `python3 -m examples.spikes.nats_command_api_spike` — 6/6
  scenarios passed against the live local container (127.0.0.1:15181,
  nats:2.11 with single-user auth): authenticated principal binding, wrong
  credentials refused, command round trip, wrong-principal refusal,
  single in-flight command, and manager transport loss leaving a durable
  `UNKNOWN` with the slot released.

Flow-suite evidence lines (payload-free; identifiers, sizes, counts, outcomes):

- Exec: one `pods/get` then one `pods/exec`; the request query names only the
  fixed helper, the container, channel flags, and `tty=false`; argv arrives
  on the masked stdin WebSocket frame and appears nowhere in the request path
  or query; stdout and stderr frames stream to the agent in order; the
  durable outcome is `SUCCESS` and a `READY` task durably activates on first
  admission; the audit entry carries identifiers, sizes, policy version, and
  decision reference only.
- Transfer: an upload's helper argv vector (destination, declared size,
  SHA-256) and reassembled payload reach the fake helper exactly once; a
  download streams multiple STDOUT frames to the agent before the terminal;
  the manager-side `RelayWorkspaceService` produces the same wire traffic
  through its own principal.
- Policy denial: the refusal arrives before any Kubernetes request (zero
  records), the task stays `READY`, no durable outcome is written, and the
  audit records `REJECTED` with the deny decision reference.
- Ownership: a command for another session's task is refused `wrong_principal`
  with zero Kubernetes records; the same principal then executes its own task.
- Termination race: with a command in flight, durable termination refuses new
  admission `not_admissible` while the in-flight command finishes and records
  its `SUCCESS` outcome under `TERMINATING`; exactly one exec ever ran.
- Pod replacement: the UID mismatch is caught by the verification read — one
  `pods/get`, zero `pods/exec` — and classified `UNKNOWN` durably.
- Stream loss and restart: an abrupt mid-stream drop is `UNKNOWN /
  stream_lost` on the agent side and durably; after a restart adopts a new
  fence the prior in-flight command is `UNKNOWN` with its slot released, the
  next command succeeds under the new fence, and the crashed command is never
  re-executed.
- Abuse caps: an admission envelope declaring over-bound input is refused
  `invalid_envelope` undecoded with zero Kubernetes records; a malformed
  input frame yields a durable `UNKNOWN` with no user argv delivered to the
  helper; input over the transport bound fails closed client-side as
  `input_unpublished`, and stopping the manager then abandons the session
  durably with the slot released.

## Section 5.5 Exit Criteria

| Criterion | Status and verification |
|---|---|
| Ready task accepts `exec`, `upload`, `download` through the manager; PTY framing works for a test profile | Flow suite (exec, agent-driven upload/download, `RelayWorkspaceService`, PTY with `tty=true`); production profiles keep PTY disabled — none enables it |
| No agent-to-sandbox endpoint, Service, or ingress exception remains | `test_sandbox_exposes_no_inbound_network_surface` plus the cutover deletions; live spike passes post-cutover |
| Every operation authenticated, ownership- and state-checked, policy-gated, tied to the current Pod UID | Gate ordering tests plus flow scenarios for wrong principal, policy denial, `not_admissible`, and UID mismatch |
| No command starts after termination wins the durable admission race | Lifecycle `close_command_admission` ordering tests plus the in-flight termination-race flow |
| Manager restart or stream loss reports `UNKNOWN`, never success or confirmed cancellation, never auto-reissued | Restart recovery unit and flow tests; stream-loss flow; `NatsCommandServer.stop` abandonment; live spike manager-loss scenario |
| Workspace path, symlink, checksum, partial-write, quota, and payload abuse tests pass without touching retained results | `test_workspace.py`, `test_relay_workspace.py`, and the transfer/abuse flows; results PVC untouched by any transfer path |
| Full pre-existing suite and new relay suite pass | 296 passed, 59 subtests; ruff and strict mypy clean |

## Notes for Stage 4c

- The durable command repository is still the single-worker in-memory
  implementation; 4c's compare-and-set repository must preserve the
  `seed_ready` / `begin_termination` / `recover` contract, which is now
  pinned by tests on all three operations.
- The `v4.channel.k8s.io` WebSocket client limitations carried from Stage 4a
  (no proactive keepalive, no continuation-frame reassembly, no close-code
  handling) remain; staging must either qualify it or replace it with a
  maintained client.
- The subscription-ordering contract in `nats_relay.py`'s module docstring
  is now load-bearing; any transport replacement must preserve both
  directions or reintroduce the race the flow suite caught.
- The Stage 4c fault campaign should re-run the fast-failure and restart
  flows against a real cluster: both exercise paths where the manager's
  answer must be a prompt `UNKNOWN`, not a silent timeout.
