from __future__ import annotations

import io
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from sandbox_manager.config import (
    ConfigError,
    Environment,
    expand_environment,
    load_config,
    override_from_environment,
    parse_config,
    validate_config,
)


class ConfigTests(unittest.TestCase):
    def test_parse_config(self) -> None:
        values = parse_config(
            io.StringIO(
                """
# comment
APP_ENV = local
export MANAGER_PORT="15180"
TARGET_KUBE_CONTEXT='local-cluster'
NATS_URL=nats://localhost:15181
"""
            )
        )

        self.assertEqual(values["APP_ENV"], "local")
        self.assertEqual(values["MANAGER_PORT"], "15180")
        self.assertEqual(values["TARGET_KUBE_CONTEXT"], "local-cluster")
        self.assertEqual(values["NATS_URL"], "nats://localhost:15181")

    def test_parse_rejects_duplicate_key_without_printing_value(self) -> None:
        with self.assertRaises(ConfigError) as raised:
            parse_config(io.StringIO("TOKEN=first-secret\nTOKEN=second-secret\n"))
        self.assertNotIn("secret", str(raised.exception))

    def test_expand_environment(self) -> None:
        environment = {"NATS_USER": "worker", "NATS_PASSWORD": "s3cret"}
        values = expand_environment(
            {"NATS_URL": "nats://${NATS_USER}:${NATS_PASSWORD}@nats:15181"},
            environment.get,
        )
        self.assertEqual(values["NATS_URL"], "nats://worker:s3cret@nats:15181")

    def test_override_uses_only_declared_keys(self) -> None:
        environment = {
            "MANAGER_PORT": "15280",
            "UNKNOWN_KEY": "must-not-be-added",
        }
        values = override_from_environment(
            {"APP_ENV": "local", "MANAGER_PORT": "15180"}, environment.get
        )
        self.assertEqual(values["MANAGER_PORT"], "15280")
        self.assertNotIn("UNKNOWN_KEY", values)

    def test_expand_rejects_unresolved_variables(self) -> None:
        with self.assertRaisesRegex(
            ConfigError,
            "unresolved environment variables: MISSING_A, MISSING_B",
        ):
            expand_environment(
                {"A": "${MISSING_B}", "B": "${MISSING_A}"}, lambda _key: None
            )

    def test_validate_config(self) -> None:
        config = validate_config(valid_values())

        self.assertEqual(config.app_env, Environment.LOCAL)
        self.assertEqual(config.manager_port, 15180)
        self.assertEqual(config.max_sandboxes, 10)
        self.assertEqual(config.sandbox_cpu_request.millicores, 250)
        self.assertEqual(config.sandbox_cpu_limit.millicores, 1000)
        self.assertEqual(config.sandbox_memory_request.bytes, 256 * 1024 * 1024)
        self.assertEqual(config.sandbox_memory_limit.bytes, 512 * 1024 * 1024)
        self.assertEqual(config.agent_check_interval, timedelta(seconds=15))
        self.assertEqual(config.agent_check_timeout, timedelta(seconds=5))
        self.assertEqual(str(config.nats_url), "[redacted]")
        self.assertEqual(config.nats_url.value, "nats://user:secret@nats:15181")
        self.assertNotIn("secret", repr(config))

    def test_validate_rejects_invalid_configuration(self) -> None:
        cases = {
            "environment": ("APP_ENV", "staging", "APP_ENV"),
            "port": ("MANAGER_PORT", "70000", "MANAGER_PORT"),
            "capacity": ("MAX_SANDBOXES", "0", "MAX_SANDBOXES"),
            "runtime class": ("GVISOR_RUNTIME_CLASS", "RunSC", "GVISOR_RUNTIME_CLASS"),
            "CPU syntax": ("SANDBOX_CPU_REQUEST", "quarter", "SANDBOX_CPU_REQUEST"),
            "CPU ceiling": ("SANDBOX_CPU_LIMIT", "1001m", "SANDBOX_CPU_LIMIT"),
            "memory ceiling": (
                "SANDBOX_MEMORY_LIMIT",
                "1Gi",
                "SANDBOX_MEMORY_LIMIT",
            ),
            "duration syntax": (
                "AGENT_CHECK_TIMEOUT",
                "5 seconds",
                "AGENT_CHECK_TIMEOUT",
            ),
            "timeout ordering": (
                "AGENT_CHECK_TIMEOUT",
                "15s",
                "AGENT_CHECK_TIMEOUT",
            ),
            "NATS scheme": (
                "NATS_URL",
                "https://user:do-not-leak@nats:15181",
                "NATS_URL",
            ),
        }

        for name, (key, value, message) in cases.items():
            with self.subTest(name=name):
                values = valid_values()
                values[key] = value
                with self.assertRaises(ConfigError) as raised:
                    validate_config(values)
                self.assertIn(message, str(raised.exception))
                self.assertNotIn("do-not-leak", str(raised.exception))

    def test_validate_rejects_unusable_nats_urls_without_exposing_credentials(
        self,
    ) -> None:
        invalid_urls = {
            "TLS transport": "tls://agent:do-not-leak@nats:4222",
            "WebSocket transport": "ws://agent:do-not-leak@nats:4222",
            "secure WebSocket transport": "wss://agent:do-not-leak@nats:4222",
            "missing hostname": "nats://agent:do-not-leak@:4222",
            "invalid hostname": "nats://agent:do-not-leak@bad_host:4222",
            "missing port": "nats://agent:do-not-leak@nats",
            "zero port": "nats://agent:do-not-leak@nats:0",
            "invalid port": "nats://agent:do-not-leak@nats:not-a-port",
            "missing username": "nats://:do-not-leak@nats:4222",
            "unsafe username": "nats://agent.name:do-not-leak@nats:4222",
            "missing password": "nats://agent@nats:4222",
            "empty password": "nats://agent:@nats:4222",
            "path": "nats://agent:do-not-leak@nats:4222/account",
            "query": "nats://agent:do-not-leak@nats:4222?name=value",
            "fragment": "nats://agent:do-not-leak@nats:4222#fragment",
            "malformed IPv6": "nats://agent:do-not-leak@[::1:4222",
        }

        for name, nats_url in invalid_urls.items():
            with self.subTest(name=name):
                values = valid_values()
                values["NATS_URL"] = nats_url
                with self.assertRaises(ConfigError) as raised:
                    validate_config(values)
                message = str(raised.exception)
                self.assertIn("NATS_URL", message)
                self.assertNotIn("do-not-leak", message)

    def test_load_config_applies_environment_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "local.config")
            path.write_text(
                "\n".join(f"{key}={value}" for key, value in valid_values().items()),
                encoding="utf-8",
            )
            config = load_config(
                path,
                {
                    "MANAGER_PORT": "15280",
                    "UNDECLARED": "ignored",
                },
            )
        self.assertEqual(config.manager_port, 15280)


def valid_values() -> dict[str, str]:
    return {
        "APP_ENV": "local",
        "MANAGER_PORT": "15180",
        "TARGET_KUBE_CONTEXT": "local-cluster",
        "GVISOR_RUNTIME_CLASS": "runsc",
        "MAX_SANDBOXES": "10",
        "SANDBOX_CPU_REQUEST": "250m",
        "SANDBOX_MEMORY_REQUEST": "256Mi",
        "SANDBOX_CPU_LIMIT": "1",
        "SANDBOX_MEMORY_LIMIT": "512Mi",
        "AGENT_CHECK_INTERVAL": "15s",
        "AGENT_CHECK_TIMEOUT": "5s",
        "NATS_URL": "nats://user:secret@nats:15181",
    }


if __name__ == "__main__":
    unittest.main()
