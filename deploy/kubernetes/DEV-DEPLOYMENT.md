# Rancher local development deployment

These manifests target the Rancher local cluster, Default project, namespace
agents-dev. GitHub dev is primary; the server mirror copies it to GitLab every
five minutes. GitLab builds immutable dev-PIPELINE-COMMIT images, moves the dev
alias, and updates every workload in .gitlab/ci/workloads.txt with the immutable
tag. The deployment job waits for readiness and fails if a rollout is unhealthy.

Runtime credentials live in the agents-runtime Kubernetes Secret, never in Git.
Each project has a read-only gitlab-registry-PROJECT image pull Secret and its own
protected KUBE_CONFIG file variable with access only to its existing workloads.
DEV_DEPLOY_ENABLED=true enables deployment. The runner is tagged agent-build.

The initial bootstrap is operator-managed: create the namespace and Secrets,
apply simple_agent/deploy/kubernetes/infrastructure.yaml, then each project's
dev.yaml. Later CI jobs update images only. Apply manifest configuration changes
as an operator; reapplying dev.yaml selects the current dev alias. Persistent
volumes are Retain volumes on local-node under /var/lib/rancher/agents-dev,
backed by the host /opt/rancher/agents-dev directory. PostgreSQL's directory must
be owned by UID/GID 999 before its first start. RAG uses UID/GID 10001.
Dependencies use IfNotPresent so the node can use preloaded public images.

The web service uses NodePort 30517. The server's agents-dev-web-edge container
forwards localhost:5174 and the Tailscale address:5174 to that service. Other
services and database/broker ports remain internal. The web proxy forwards API
requests to the gateway. Add a model provider credential through the application
before testing real model calls; deployment does not provision external API keys.

Eight versioned dev tags are retained per image component plus the moving dev
alias. simple_agent builds eleven components. Production and release tags are
not part of this cleanup. Registry garbage collection handles unused blob space.

To inspect a rollout, select local / Default / agents-dev in Rancher, or use:

    kubectl -n agents-dev get deployments,pods
    kubectl -n agents-dev rollout status deployment/WORKLOAD --timeout=300s
    kubectl -n agents-dev logs deployment/WORKLOAD --tail=100

Rollback an individual workload with kubectl rollout undo or set its image to
an earlier retained immutable tag. A newer successful dev pipeline will deploy
the current branch again. A failing readiness check is a failed deployment;
fix the application/configuration and retry or push another commit.

agent_sandbox currently exposes liveness but intentionally reports readiness
false: its CLI does not compose the runtime adapters. Its image build, registry
publication and Deployment update work; its rollout cannot pass until that
application implementation is finished. Keep its readiness gate in place.
