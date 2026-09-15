# agent_sandbox

Minimal Python service foundation for the Kubernetes and gVisor sandbox
manager. It validates environment configuration, exposes operational health
endpoints, and provides asynchronous intake and secure sandbox-provisioning
boundaries.

The provisioning path resolves immutable, digest-pinned profiles, reserves
bounded capacity, persists lifecycle transitions around side effects, and
builds deterministic PVC, default-deny NetworkPolicy, and gVisor Pod
resources with no inbound network surface: every agent command transits the
authenticated manager relay over Core NATS. Async health supervision,
deadlines, terminal cleanup, and restart reconciliation preserve durable
ordering and retained result storage. Kubernetes API, durable-state, and
transport adapters remain explicit ports.

The installed CLI is currently an operational shell: it validates
configuration and serves liveness, but it does not yet compose those ports
into a running manager. Its readiness endpoint therefore remains unavailable.
The included deployment manifests are integration templates, not a production
release, until concrete persistence, Kubernetes, health-transport, and runtime
supervision adapters are wired at startup.

## Local Start

Start the manager operational shell and its project-scoped NATS JetStream
dependency:

~~~bash
./examples/local/start_all.sh --init
~~~

The manager is available at:

~~~text
http://127.0.0.1:15180
~~~

Stop while retaining local queue data:

~~~bash
./examples/local/stop_all.sh
~~~

Remove only this project's local containers, images, network, volumes, and
runtime files:

~~~bash
./examples/local/stop_all.sh --purge
~~~

## Project Layout

| Path | Purpose |
|---|---|
| `sandbox_manager/` | Service, configuration, and HTTP endpoint package |
| `tests/` | Standard-library unit and HTTP integration tests |
| `pyproject.toml` | Python package metadata and CLI entry point |
| `config/` | Local, development, and production configuration pairs |
| `deploy/kubernetes/` | Deployment manifests, narrowly-scoped RBAC, and runbook |
| `examples/local/` | Project-scoped local lifecycle tooling |
| `examples/showcases/` | Runnable capability demonstrations |
| `docs/requirements/` | Initial requirements and formal SRS |
| `docs/planning/` | Delivery plans, decision gates, and sprint notes |

Real `config/*.env` files are local-only and ignored by Git. Their matching
`*.env.example` templates are tracked. Non-secret settings belong in the
tracked `*.config` files.

## Deployment

The manager deployment template lives in `deploy/kubernetes/`: the `sandbox-manager`
namespace, a `sandbox-manager` ServiceAccount, a Role/RoleBinding scoped to
the narrow resource and subresource set the manager needs (`pods`, `pods/exec`,
`persistentvolumeclaims`, `networkpolicies`), a non-secret ConfigMap, and a
single-replica Deployment with `/healthz` and `/readyz` probes. NATS
credentials are injected from a Secret that is never committed. Sandbox Pod
manifests produced by the library carry no service-account token and expose no
inbound Service, so the intended command path is the authenticated manager
relay.

See `deploy/kubernetes/runbook.md` for the remaining runtime-composition gate,
deployment checks, and rollback procedure.

## Development

~~~bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --editable '.[dev]'
python -m unittest discover -s tests -v
ruff check .
ruff format --check .
mypy
~~~

The service has no third-party runtime dependencies. After installation, and
with the secret variables referenced by the selected config in the process
environment, run it directly with
`sandbox-manager --config config/local.config`.

## Documentation

- [Four-sprint delivery plan](docs/planning/four_sprint_delivery_plan.md)
- [Phase 4 relay update and release plan](docs/planning/phase4_relay_update_and_release_plan.md)
- [Sprint 2 engineering notes](docs/planning/sprint_2_notes.md)
- [Sprint 3 engineering notes](docs/planning/sprint_3_notes.md)
- [Stage 4a engineering notes](docs/planning/stage_4a_notes.md)
- [Stage 4b engineering notes](docs/planning/stage_4b_notes.md)
- [Sandbox Manager Software Requirements Specification](docs/requirements/sandbox_manager_srs.md)
- [Initial requirements](docs/requirements/init_req.md)
