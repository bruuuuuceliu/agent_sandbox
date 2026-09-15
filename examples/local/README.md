# Local Environment

The local environment runs the sandbox-manager service and NATS JetStream in
Docker. All Compose resources use the project name
<code>agent-sandbox-local</code>, so lifecycle commands cannot select unrelated
Compose projects.

The current executable is an operational shell: it validates configuration
and exposes liveness, while readiness stays unavailable until the runtime
adapters are composed. The local stack is suitable for transport spikes and
configuration checks, not end-to-end sandbox provisioning.

Start the dependency and manager:

~~~bash
./examples/local/start_all.sh --init
~~~

Start against an already-running local dependency:

~~~bash
./examples/local/start_all.sh
~~~

Stop the service and dependency while preserving the NATS volume:

~~~bash
./examples/local/stop_all.sh
~~~

Remove only this project's containers, images, network, volumes, and runtime files:

~~~bash
./examples/local/stop_all.sh --purge
~~~

The scripts load <code>config/local.env</code> and
<code>config/local.config</code> inside their own process. They do not modify the
caller's environment.

The local NATS server loads `nats-server.conf` with two separate users. The
manager uses the fixed `sandbox-manager` principal and the local agent uses
`sandbox-agent-local`; set distinct `NATS_MANAGER_PASSWORD` and
`NATS_AGENT_PASSWORD` values in `config/local.env`. The local agent connection
URL is `nats://sandbox-agent-local:<agent-password>@127.0.0.1:15181`.

The manager can consume this agent's spawn, admission, and command-input
subjects, publish replies and command output, and use the JetStream API. The
agent can publish only its own requests/input and subscribe only to its own
inbox, output, and ready subjects. Neither user has unrestricted `>` access.
