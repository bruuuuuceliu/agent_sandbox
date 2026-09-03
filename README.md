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
transport adapters remain explicit ports so environment policy is supplied
at deployment.

## Local Start

Start the manager and its project-scoped NATS JetStream dependency:

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
| `src/sandbox_manager/` | Service, configuration, and HTTP endpoint package |
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

Manager deployment lives in `deploy/kubernetes/`: the `sandbox-manager`
namespace, a `sandbox-manager` ServiceAccount, a Role/RoleBinding scoped to
the narrow resource and subresource set the manager needs (`pods`, `pods/exec`,
`persistentvolumeclaims`, `networkpolicies`), a non-secret ConfigMap, and a
single-replica Deployment with `/healthz` and `/readyz` probes. NATS
credentials are injected from a Secret that is never committed; the sandbox
Pods the manager creates carry no service-account token and expose no inbound
Service, so every agent command transits the authenticated manager relay.

A rollback disables new relay admission before reverting the manager image and
never restores the deleted direct sandbox exposure (AC-026). See
`deploy/kubernetes/runbook.md` for deploy, health, restart-under-traffic, and
rollback procedures.

## Development

~~~bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --editable .
python -m unittest discover -s tests -v
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
