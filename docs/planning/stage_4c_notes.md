# Stage 4c Engineering Notes

## Status

Stage 4c is implemented and locally verified on four backlogs: task #15 (exec
client hardening), #16 (JetStream lifecycle transport adapters), #17 (durable
command repository replay), and #18 (redaction and fault-injection suites).
Every story is green under the local gates and the live NATS spike passes
against the real server. Staging-dependent verification — a live cluster,
`pods/exec`, and API-audit evidence — remains on the Stage 4c checklist and is
recorded under Staging-Blocked below, in the same spirit as Stage 4a/4b.

Each feature here is the observable behaviour, not a plan idiom. The security
constraints governing the stage are fixed: payload (command arguments,
environment values, file contents, PTY input, stream data) never reaches audit
or telemetry, and credentials/`config/*.env` stay local-only and git-ignored.
The transport-supplied principal is the only ownership evidence; payload
session IDs never prove ownership.

## Delivered Scope

- **#15 Exec client hardened** (`kubernetes_exec.py`): the `stream` subprotocol
  reads a bounded initial `argv`/`env` frame then the terminal and data frames,
  with a length-prefixed frame decoder and a single-frame max. Two transport
  findings came out of the hardening spike and shaped the implementation:
  - *RST-on-unread-data* — a server returning a small payload may close the
    stream before the client has read it all; the reader must not treat EOF as
    a clean drain. The client now drains the declared frame bytes before
    reading the terminal frame, and a short stream is reported as lost.
  - *Buffered-reader poisoning* — reading past a frame boundary leaves stray
    bytes in the transport buffer that corrupt the next frame. The decoder is
    strict: each frame's declared length is consumed exactly, and any mismatch
    is a protocol error rather than a misparse.
- **#16 JetStream lifecycle transport adapters** (`nats_lifecycle.py`): an
  `ensure_stream`/`ensure_consumer` layer with explicit ack, ack-wait, and
  bounded redelivery. The probe that drove the shape found the server silently
  ignored the consumer create request and created a consumer with
  `ack_policy: none` and no redelivery. Root cause and fix are the key insight
  of the stage:
  - *Consumer create config must nest under `"config"`* — the Go
    `JSApiConsumerCreateRequest` shape is `{"stream_name": …, "config": {…}}`;
    flat top-level fields are ignored. Correct shape:
    `{"stream_name": s, "config": {durable_name, filter_subject,
    ack_policy: "explicit", ack_wait (ns), max_deliver, max_ack_pending,
    deliver_policy: "all"}}`.
  - Server 2.11 echoes the durable as `"name"` (not `durable_name`); verification
    accepts either spelling.
  - Pull requests use integer **nanoseconds** for `expires` on the wire; a
    seconds float is silently truncated to a zero wait. `fetch` treats a
    closed connection as a hard failure (never an empty window), so the
    consumer loop cannot hot-spin.
  - `CONNECT` header negotiation is used for JetStream publish acks and
    status replies; the client already sends `headers` and `no_responders`.
- **#17 Durable command repository replay** (`commands.py`,
  `nats_lifecycle.py`): every repository mutation is journaled to JetStream
  *before* its in-memory application, under the repository's lock, so the log
  never trails the live view; a failed append aborts the mutation (fail-closed
  admission). Replay folds the journal back through the public repository
  methods, which keeps it idempotent under lost-ack duplicates. Startup seeds
  a journal-less repository, attaches the journal, then adopts a new fence and
  classifies prior-process in-flight commands `UNKNOWN` without re-issuing
  them (Decision 20). Both `load_repository(fence)` and the live spike prove
  the full crash cycle end to end.
- **#18 Redaction and fault-injection suites**:
  - Redaction (`test_redaction.py`, AC-025): sentinels are planted only in
    payload positions (argv, stream bytes, transfer file content, connection
    credentials) and every in-process artifact is scanned — audit entries,
    durable journal entries, ready documents, protocol error strings, and the
    Kubernetes request-metadata subset an API server records
    (`kind`/`path`/`query`/`subprotocol`). Data-plane channels the fakes model
    (`argv`/`stdin` of the in-sandbox helper, the bearer header) are excluded
    by design. Identifiers may appear; payload may not.
  - Fault injection (`test_fault_injection.py`): three composed scenarios the
    unit suites never run together — intake rides out the loss of its NATS
    connection without crashing or misclassifying transport loss as poison and
    a replacement process drains the durable backlog; a journal whose
    transport dies mid-mutation fails admission closed with state unchanged
    and succeeds once a replacement journal is attached; and a full manager
    restart under traffic replays commands, adopts a new fence, classifies the
    interrupted command `UNKNOWN`, and drains the spawn backlog alongside fresh
    admissions.

## Fault Finding From the Injection Suite

`IntakeConnectionLossTests` hung at shutdown and exposed a genuine production
defect in `AsyncNatsConnection.request`:

1. `request`'s `finally` calls `unsubscribe`, which raises `NatsClosedError`
   on a dead connection.
2. An exception raised in a `finally` block **replaces** the exception already
   unwinding — including a `CancelledError`.
3. The intake loop's `except Exception` then swallows what was actually a
   cancellation, so `stop()` awaits a task that can never finish: a manager
   whose NATS connection died could never shut down cleanly.

Fix: the `finally` cleanup now suppresses `NatsClosedError`/`NatsTransportError`
so it never clobbers the propagating cancellation (currently in-progress
command or shutdown). Regression-tested by the injection suite.

## Verification

- Full pytest local gate: 355 passed, 89 subtests (includes the new
  redaction and fault-injection suites).
- `ruff` clean across `sandbox_manager`, `tests`, `examples/spikes`.
- `mypy --strict` clean across 51 source files.
- Live NATS spike (`nats_lifecycle_spike.py`) against the real server
  (nats:2.11-alpine, single-user auth, git-ignored `config/local.env`):
  **7/7 scenarios pass**, including `command_events_survive_restart`.

## Staging-Blocked

The following remain on the Stage 4c checklist and are **not** locally
verifiable — no cluster context exists in this workspace. They are enumerated
here as evidence-collection tasks for staging, not as work to be guessed at
locally:

- **Live `pods/exec` evidence** on a real cluster (the exec client hardening is
  unit- and fake-verified; live behaviour against kubelet is unverified).
- **RBAC and API-audit evidence**: the narrowed `pods/get` + `pods` + `pods/exec`
  RBAC (task #19) and the resulting audit records, verified against a staging
  API server with audit logging enabled.
- **Deployment manifests applied** (task #19): the manager Deployment and the
  JetStream-backed config, exercised on staging rather than assembled locally.
- **gVisor `RuntimeClass` / storage `StorageClass`**: referenced by the
  manager's Pod target but available only with a qualifying sandbox runtime and
  storage provisioner installed.
- **Qualified relay-helper image**: the in-sandbox helper the exec adapter
  drives lives behind a container registry reference; no build is produced
  locally.
- **Dashboards / alerts / metrics / traces**: telemetry plumbing exists but the
  Sprint 2–3 staging checklists that validate the charts and alarms are staging
  tasks.

## Open Decisions Pending Staging

- Decision 19's filtering grammar, numeric workspace quotas, and production
  PTY enablement remain Open, carried from Stage 4a/4b.
