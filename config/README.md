# Configuration

The manager has one committed configuration file and one ignored secret file
for each environment:

| Environment | Committed settings | Local secrets | Safe template |
| --- | --- | --- | --- |
| Local | `local.config` | `local.env` | `local.env.example` |
| Development | `dev.config` | `dev.env` | `dev.env.example` |
| Production | `production.config` | `production.env` | `production.env.example` |

The `.config` files contain non-secret application settings and may reference
secret environment variables with `${VARIABLE}`. The `.env` files provide
those secret variables for a local process and are ignored by Git. Never add
real credentials to a `.config` file or an `.env.example` file.

Both file types use data-only `KEY=VALUE` syntax. Do not put shell commands,
command substitutions, or other executable shell content in them.

## Loading order

Load the selected environment in this order:

1. Read `<environment>.env` without overwriting variables already present in
   the process environment.
2. Read `<environment>.config`, expanding `${VARIABLE}` references from the
   resulting environment, without overwriting process-level overrides.
3. Validate every required setting before starting the service.

The process environment therefore has the highest precedence, followed by the
ignored `.env` file, then the committed `.config` defaults. Loading must be
scoped to the service process; scripts must not export values into a user's
interactive shell.

To prepare an environment, copy its `.env.example` file to the matching `.env`
path and replace the placeholders using an approved secret source. Production
deployments should inject secrets directly through the runtime or Kubernetes
rather than distributing a populated file.

When the repository gains multiple independently deployed services, give each
service its own directory under `config/` with local, development, and
production config/env pairs. Do not share one service's secret file with
another service.

`local.config` also defines `NATS_CLIENT_PORT` and `NATS_MONITOR_PORT`. These
are non-secret host ports consumed by the local dependency scripts and are not
required manager settings in development or production.

The local NATS identities are fixed by `examples/local/nats-server.conf` so
their subject permissions cannot drift from their authenticated principals.
Set separate `NATS_MANAGER_PASSWORD` and `NATS_AGENT_PASSWORD` secrets in
`local.env`; the corresponding usernames are `sandbox-manager` and
`sandbox-agent-local`. Development and production still inject the manager's
`NATS_USERNAME` and `NATS_PASSWORD` through their environment-specific secret
stores.

`NATS_URL` accepts only the implemented plain Core NATS transport: `nats://`
with a subject-safe username, a non-empty password, a hostname, and an explicit
port. TLS and WebSocket schemes are rejected until those transports are
implemented rather than being accepted without their required handling.
