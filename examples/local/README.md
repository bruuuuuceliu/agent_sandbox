# Local Environment

The local environment runs the sandbox-manager service and NATS JetStream in
Docker. All Compose resources use the project name
<code>agent-sandbox-local</code>, so lifecycle commands cannot select unrelated
Compose projects.

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
