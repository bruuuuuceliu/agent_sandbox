# Stage 4a Spikes

Feasibility spikes for the manager-mediated command relay. Both are
dependency-free and intentionally separate from the Stage 4b adapters that
will follow them.

## `nats_relay_spike.py`

A minimal Core NATS client (INFO/CONNECT basic auth, PING/PONG, SUB/PUB/UNSUB)
that exercises the frozen `relay_protocol` schemas against the real local
server from `examples/local/compose.yaml` (nats:2.11, separate manager/agent
ACLs).

Run with the local container up:

    python3 -m examples.spikes.nats_relay_spike

The separate manager and local-agent passwords are read from
`config/local.env` (git-ignored, local only).
Evidence lines are payload-free. The scenarios cover authenticated connects
and refused credentials, admission request/reply with transport-principal
ownership, ephemeral task-scoped sequenced frames, the server payload cap
versus the 64 KiB relay frame bound, bounded slow-consumer drops, abrupt
stream loss mapping to `UNKNOWN`, and no redelivery after a reconnect.

## Kubernetes exec spike

The Kubernetes side of Stage 4a lives in
`sandbox_manager/kubernetes_exec.py` with contract tests in
`tests/test_kubernetes_exec.py` against an in-process fake API server: pods/get
verification (KR-008), the fixed-helper exec request with argv confined to the
stdin channel (KR-009), and fail-closed `UNKNOWN` termination classification.
Live streaming verification requires a staging cluster; see
`docs/planning/stage_4a_notes.md` for the staging checklist.
