# Sandbox Manager Runbook

Operational procedures for the sandbox-manager Deployment delivered with this
stage. All commands assume the release namespace `sandbox-manager` and a
kubeconfig with permission to touch it. Evidence lines are payload-free:
identifiers, sizes, counts, and outcomes only.

## Single-Node Constraint

The release is single-worker (plan Section 8). Exactly one manager replica runs
at a time; total configured capacity is `MAX_SANDBOXES` = 10. Do not scale the
Deployment above 1 without the multi-worker claim/fencing/handoff stories.

## Deploy

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/serviceaccount.yaml
kubectl apply -f deploy/kubernetes/rbac.yaml
kubectl apply -f deploy/kubernetes/serviceaccount.yaml
kubectl apply -f deploy/kubernetes/configmap.yaml
# Create the NATS credential Secret out-of-band; never commit it.
kubectl -n sandbox-manager create secret generic sandbox-manager-nats \
  --from-literal=NATS_USERNAME=<user> --from-literal=NATS_PASSWORD=<pass>
kubectl apply -f deploy/kubernetes/manager-deployment.yaml
```

## Health

The HTTP server exposes `GET /healthz` and `GET /readyz` on port 15180.

- `healthz`: the server process is alive (liveness probe).
- `readyz`: the server is serving (readiness probe). It flips false on
  SIGTERM before shutdown, so a rolling update drains before the next pod
  admits.

```bash
kubectl -n sandbox-manager get pods -l app.kubernetes.io/name=sandbox-manager
kubectl -n sandbox-manager logs deploy/sandbox-manager --tail=50
```

Logs are JSON and payload-free: they carry identifiers, sizes, counts, and
outcomes, never command argv, environment values, stream payloads, file
contents, or credentials.

## Restart Under Traffic

The durable command-event journal (Stage 4c, task #17) makes restart safe:

1. Every repository mutation is journaled to JetStream before it is applied
   in memory; a failed append aborts the mutation (fail-closed admission).
2. A restarted manager replays the journal into a fresh repository, attaches
   the journal, and adopts a new fence. Commands left in flight by the prior
   process are classified `UNKNOWN` and never re-issued (Decision 20).
3. A command admitted under a prior fence is refused with `stale_fence`; a
   `TERMINATING` task refuses `not_admissible`.

No task, Pod, PVC, or NetworkPolicy is lost across the restart, and no
unfinished command is ever reported success or silently cancelled. To restart
the manager image: `kubectl -n sandbox-manager set image deploy/sandbox-manager
sandbox-manager=<new-image>`.

## Rollback

The release gate requires: rollback disables **new relay admission** before
reverting deployment components, and it never restores the deleted direct
sandbox exposure (plan Section 6.3).

1. Quiesce admissions first. Either scale the Deployment to zero so no new
   spawn is admitted, or terminate the affected tasks durably via the
   lifecycle's `begin_termination` (which refuses any further command). Read
   the ready state down before the revert:

   ```bash
   kubectl -n sandbox-manager scale deploy/sandbox-manager --replicas=0
   kubectl -n sandbox-manager wait --for=condition=available \
     deploy/sandbox-manager --timeout=60s   # satisfies "nothing serving"
   ```

2. Confirm no admission is in flight. In-flight commands from the quiesced
   process are classified `UNKNOWN` on the next start; they are never believed
   to have succeeded.

3. Revert only the manager component:

   ```bash
   kubectl -n sandbox-manager set image deploy/sandbox-manager \
     sandbox-manager=<prior-image>
   kubectl -n sandbox-manager scale deploy/sandbox-manager --replicas=1
   kubectl -n sandbox-manager rollout status deploy/sandbox-manager
   ```

4. Do **not** restore the direct sandbox exposure. The direct agent-to-sandbox
   path was deleted in Stage 4b; sandboxes expose no inbound Service or
   ingress and always transit the authenticated manager relay. A rollback that
   re-opens an inbound Service to a sandbox Pod is out of scope and contrary to
   AC-026. Re-apply the default-deny NetworkPolicy and confirm no
   `ClusterIP`/`NodePort`/`LoadBalancer` Service selects a sandbox Pod.

5. Re-enable admission only after the manager reports ready and the pods to
   serve are observed.

```bash
kubectl -n sandbox-manager scale deploy/sandbox-manager --replicas=1
```

## Verification After a Rollback/Restart

```bash
kubectl -n sandbox-manager logs deploy/sandbox-manager --tail=20
kubectl -n sandbox-manager get pods -l app.kubernetes.io/name=sandbox-manager
```

Confirm:

- the journal replayed cleanly (no undecodable entry; replay would fail closed
  and refuse admission);
- every interrupted command is `UNKNOWN`, never success;
- admission under a prior fence is refused `stale_fence`.

## Notes on the NATS Secret

The NATS credentials are injected as environment variables from the
`sandbox-manager-nats` Secret; `NATS_URL` in the ConfigMap expands them. A
manager that cannot read the Secret fails at startup (missing NATS_URL) rather
than starting unauthenticated.
