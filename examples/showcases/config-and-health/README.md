# Configuration and Health Showcase

This showcase starts the local dependencies and manager, then verifies the
public health and readiness endpoints.

~~~bash
./examples/showcases/config-and-health/run.sh
~~~

The showcase always stops what it starts. Pass <code>--purge</code> to also
remove the local NATS volume and generated runtime files after the check.
