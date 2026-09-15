# Sprint 2 Engineering Notes

## Status

The Python application slice is implemented and locally verified. Live-cluster
acceptance remains pending because this workspace has no configured Kubernetes
context, gVisor `RuntimeClass`, StorageClass, CNI test environment, or qualified
sandbox image.

The SRS is still a draft with unassigned owners and approvers. These notes are
engineering evidence, not approval of the open product, storage, network,
transport, or retention decisions.

## Delivered Scope

- Versioned immutable sandbox profiles with allowlisted lookup, digest-pinned
  images, commands, connection settings, and a writable results mount.
- An async capacity limiter that atomically admits at most the configured
  number of task identities and can be initialized from recovered active tasks.
- Async provisioning orchestration with durable `PROVISIONING` before resource
  creation and durable `READY` before connection publication.
- Durable unknown-profile rejection before any capacity reservation or
  Kubernetes call.
- Failure-stage recording, idempotent rollback, and capacity release ordering.
  Resource and readiness cancellation follows the same recovery path, while an
  ambiguous persistence interruption retains state for reconciliation.
- Deterministic Kubernetes names and task labels for a PVC, immutable Secret,
  private Service, default-deny NetworkPolicy, ingress allow policy, and Pod.
- A restricted non-root Pod with no host namespaces, no service-account token,
  dropped capabilities, a read-only root filesystem, fixed resource budgets,
  and an explicit configured gVisor `RuntimeClass`.
- An async profile-to-Kubernetes adapter that applies dependencies before the
  Pod, waits for observed Pod readiness, returns task-bound connection and
  credential references, and deletes partial resources in a repeatable order.

## Runtime Inputs

`KubernetesProvisionerConfig` requires the namespace, gVisor RuntimeClass,
StorageClass, result-volume size, and allowed ingress peer. The profile provides
the digest-pinned image, command, connection protocol and port, and results mount
path. A credential provider supplies stable per-task secret bytes without
placing them in task metadata or connection responses.

The Kubernetes client, credential provider, durable repository, and ready
publisher are async protocols. A deployment adapter must implement
server-side create-or-converge behavior, a non-polling readiness watch, and
idempotent deletion. The ready publisher remains transport-neutral while the
response-channel decision is pending.

## Local Evidence

| Acceptance area | Evidence | Remaining gate |
|---|---|---|
| One request, one resource set | End-to-end orchestrator test reaches one durable ready task and six deterministic manifests | Durable-state and Kubernetes adapters in staging |
| gVisor selection | Every generated Pod contains the configured `runtimeClassName`; there is no fallback path | RuntimeClass-unavailable failure test on a real cluster |
| Resource budget | Manifest tests assert `250m`/`256Mi` requests and `1`/`512Mi` limits | Admission and runtime inspection |
| Profile and image policy | Unknown profiles fail before reservation; accepted images require a lowercase SHA-256 digest | Approved image qualification under gVisor |
| Capacity | Concurrent test admits ten distinct tasks, leaves the eleventh pending, and admits it after release | Recovery seeding from the durable repository |
| Readiness and response | The adapter waits for Pod readiness; state becomes durable before the publisher is called | Real watch behavior and approved response adapter |
| Storage and credentials | PVC and Secret are mounted separately; the PVC has no Pod owner reference; credential values are hidden from representations | PVC binding, file access, encryption, and retention validation |
| Network isolation | Private ClusterIP Service plus default-deny and explicit ingress manifests | CNI policy enforcement and approved egress behavior |
| Retry and cancellation | Partial apply converges against existing objects; resource/readiness cancellation records failure and completes rollback before propagating | Fault injection against the selected Kubernetes client |

The local verification run completed with:

- 56 passing unit and integration tests;
- Ruff lint and format checks passing;
- strict MyPy checks passing for `sandbox_manager` and `tests`;
- bytecode compilation passing;
- a successful wheel build; and
- a successful non-root Docker image build tagged `agent-sandbox:sprint2`.

## Staging Checklist

Before closing Sprint 2 in the delivery plan, the platform and security owners
must record evidence for:

1. The configured RuntimeClass exists and the qualified image runs through
   gVisor; an unavailable RuntimeClass never starts the Pod with another runtime.
2. Server-side apply converges identical manifests and reports ownership
   conflicts without mutating another task's resources.
3. The PVC binds through the selected StorageClass, mounts at the profile path,
   and is readable and writable by UID/GID 10000.
4. The mounted Secret is readable by the sandbox process and credential bytes do
   not appear in logs, events, errors, or test reports.
5. The CNI enforces default-deny behavior and permits only the approved agent
   peer to reach the sandbox port.
6. Readiness observation blocks connection publication until the Pod and
   connection path are usable, and failure records a durable outcome.
7. Restart recovery seeds active capacity from durable task state and repeated
   reconciliation creates no duplicate Pod or PVC.

Result retention and eventual deletion belong to the cleanup lifecycle and
remain part of Sprint 3. The Sprint 2 rollback path preserves the task PVC while
removing ephemeral resources; it does not define terminal result-retention
duration or deletion policy.
