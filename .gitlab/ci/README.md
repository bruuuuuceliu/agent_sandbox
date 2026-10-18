# GitLab dev images and rollout

GitHub is primary. Push committed changes to GitHub dev; the five-minute
mirror copies them to GitLab and triggers this pipeline. Main remains the
release branch; this pipeline does not deploy main. Do not push divergent
commits directly to the GitLab copy.

The build job publishes each image listed in images.txt as
dev-PIPELINE_ID-COMMIT_SHORT_SHA in this project's GitLab container registry.
After every component builds successfully, publish_dev moves the dev alias
to those manifests, without rebuilding them. Concurrent publications are
serialized and superseded pipelines do not move dev backward.

The publish job uses the GitLab registry API to delete versioned dev tags
older than the newest eight for each image component (always preserving
the currently published dev image). A final cleanup job also runs after failed
builds so superseded and partial builds do not accumulate without a limit. The dev alias points
to one of those images, so it adds no additional image. Release tags and
unrelated image repositories are excluded. GitLab's preset cleanup values
do not include eight. Deleting tags removes registry references; reclaiming
unused blob storage depends on registry garbage collection configured by
the GitLab infrastructure owner.

## GitLab infrastructure contract

- Enable the project container registry and a runner tagged agent-build.
- The runner must support rootless BuildKit, including unprivileged user
  namespaces and the documented BuildKit seccomp/AppArmor configuration.
  Jobs do not mount the Docker socket or require Docker-in-Docker.
- GitLab and registry URLs must be reachable from runner jobs and Kubernetes
  nodes. localhost URLs must be corrected by the infrastructure setup.
- Install the internal CA in the runner/helper and container runtime trust
  stores. Supply REGISTRY_CA_FILE as a GitLab file variable if needed for
  BuildKit and Python. Supply CI_SERVER_TLS_CA_FILE for Python's GitLab API
  trust if the runner does not already provide it.
- DEV_REGISTRY_API_TOKEN is a masked, protected project access token with
  api scope. It permits listing pipelines/tags and deleting old registry tags.
  Its expiration is one year after provisioning; rotate before expiry.

## Rancher local rollout

The intended namespace is agents-dev in the local cluster. The deployment
job is enabled only when DEV_DEPLOY_ENABLED=true, KUBE_NAMESPACE, and the
KUBE_CONFIG file variable are present. DEV_DEPLOY_ENABLED remains false
until the target workloads and runtime dependencies have been deployed. KUBE_CONFIG must use a namespace-scoped service account, never
a cluster administrator account. Optional KUBE_CONTEXT selects the context.

workloads.txt lists the expected resource/container/image mapping. Adjust
this file to match actual Deployments or StatefulSets before setting those
variables. The pipeline only updates existing workloads; it does not create
application configuration, database migrations, secrets, dependencies, or
persistent storage. Each workload requires registry pull credentials, CA
trust, readiness checks, and runtime config supplied by the deployment setup.

Deployments use the immutable build tag rather than the mutable dev alias,
so Kubernetes creates a new rollout for each published build. The job waits
for rollout completion and fails if a workload does not become healthy.
Rancher displays the changes through its Kubernetes integration. It does
not poll the dev tag. Roll back by selecting one of the retained versioned
tags in Rancher or by rerunning a known-good revision as a new dev pipeline.
