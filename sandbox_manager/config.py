"""Configuration parsing and validation for the sandbox manager."""

from __future__ import annotations

import ast
import ipaddress
import os
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import TextIO
from urllib.parse import urlsplit

from .identifiers import is_identifier

MAX_CPU_MILLICORES = 1_000
MAX_MEMORY_BYTES = 512 * 1024 * 1024

_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_CPU_PATTERN = re.compile(r"^(?:[0-9]+(?:\.[0-9]{1,3})?|\.[0-9]{1,3})$")
_MILLI_CPU_PATTERN = re.compile(r"^[0-9]+m$")
_MEMORY_PATTERN = re.compile(r"^([0-9]+)(Ki|Mi|Gi)$")
_DNS_SUBDOMAIN_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
_INTEGER_PATTERN = re.compile(r"^[+-]?[0-9]+$")
_DURATION_PART_PATTERN = re.compile(
    r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:ns|us|\N{MICRO SIGN}s|\N{GREEK SMALL LETTER MU}s|ms|s|m|h)"
)
_DURATION_VALUE_PATTERN = re.compile(
    r"(?P<number>[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?P<unit>ns|us|\N{MICRO SIGN}s|\N{GREEK SMALL LETTER MU}s|ms|s|m|h)"
)
_HOST_LABEL_PATTERN = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$"
)
_DURATION_SECONDS = {
    "ns": Decimal("0.000000001"),
    "us": Decimal("0.000001"),
    "\N{MICRO SIGN}s": Decimal("0.000001"),
    "\N{GREEK SMALL LETTER MU}s": Decimal("0.000001"),
    "ms": Decimal("0.001"),
    "s": Decimal(1),
    "m": Decimal(60),
    "h": Decimal(3600),
}

EnvironmentLookup = Callable[[str], str | None]


class ConfigError(ValueError):
    """Raised when configuration cannot be parsed or validated."""


class Environment(StrEnum):
    LOCAL = "local"
    DEV = "dev"
    PRODUCTION = "production"


@dataclass(frozen=True, slots=True)
class CPUQuantity:
    raw: str
    millicores: int

    def __str__(self) -> str:
        return self.raw


@dataclass(frozen=True, slots=True)
class MemoryQuantity:
    raw: str
    bytes: int

    def __str__(self) -> str:
        return self.raw


@dataclass(frozen=True, slots=True)
class ConnectionURL:
    """A connection URL that redacts credentials when formatted."""

    _value: str = field(repr=False)

    @property
    def value(self) -> str:
        return self._value

    def __str__(self) -> str:
        return "[redacted]"

    def __repr__(self) -> str:
        return "ConnectionURL([redacted])"


@dataclass(frozen=True, slots=True)
class Config:
    app_env: Environment
    manager_port: int
    target_kube_context: str
    gvisor_runtime_class: str
    max_sandboxes: int
    sandbox_cpu_request: CPUQuantity
    sandbox_memory_request: MemoryQuantity
    sandbox_cpu_limit: CPUQuantity
    sandbox_memory_limit: MemoryQuantity
    agent_check_interval: timedelta
    agent_check_timeout: timedelta
    nats_url: ConnectionURL


def load_config(
    path: str | os.PathLike[str], environ: Mapping[str, str] | None = None
) -> Config:
    """Read and validate a non-secret ``.config`` file."""

    config_path = Path(path)
    if not str(path).strip():
        raise ConfigError("config path is required")
    if config_path.suffix != ".config":
        raise ConfigError("config path must name a .config file")

    environment = os.environ if environ is None else environ

    def lookup(key: str) -> str | None:
        return environment.get(key)

    try:
        with config_path.open(encoding="utf-8") as config_file:
            values = parse_config(config_file)
    except OSError as error:
        raise ConfigError(
            f'open config "{config_path}": {error.strerror or error}'
        ) from error

    values = override_from_environment(values, lookup)
    values = expand_environment(values, lookup)
    return validate_config(values)


def parse_config(reader: TextIO) -> dict[str, str]:
    """Parse data-only ``KEY=VALUE`` configuration syntax."""

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(reader, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            raise ConfigError(f"config line {line_number}: expected KEY=VALUE")

        raw_key, raw_value = line.split("=", maxsplit=1)
        key = raw_key.strip()
        if not _KEY_PATTERN.fullmatch(key):
            raise ConfigError(f"config line {line_number}: invalid key")
        if key in values:
            raise ConfigError(f"config line {line_number}: duplicate key {key}")

        try:
            values[key] = _decode_value(raw_value.strip())
        except (SyntaxError, ValueError) as error:
            raise ConfigError(
                f"config line {line_number}: invalid value for {key}"
            ) from error
    return values


def _decode_value(value: str) -> str:
    if len(value) < 2:
        return value
    if value.startswith('"'):
        if not value.endswith('"'):
            raise ValueError("unterminated quote")
        decoded = ast.literal_eval(value)
        if not isinstance(decoded, str):
            raise ValueError("value is not a string")
        return decoded
    if value.startswith("'"):
        if not value.endswith("'"):
            raise ValueError("unterminated quote")
        return value[1:-1]
    return value


def override_from_environment(
    values: Mapping[str, str], lookup: EnvironmentLookup
) -> dict[str, str]:
    """Override only keys explicitly declared by the configuration file."""

    overridden: dict[str, str] = {}
    for key, value in values.items():
        environment_value = lookup(key)
        overridden[key] = value if environment_value is None else environment_value
    return overridden


def expand_environment(
    values: Mapping[str, str], lookup: EnvironmentLookup
) -> dict[str, str]:
    """Expand ``${NAME}`` references without mutating the process environment."""

    missing: set[str] = set()

    def replace(reference: re.Match[str]) -> str:
        name = reference.group(1)
        resolved = lookup(name)
        if resolved is None:
            missing.add(name)
            return reference.group(0)
        return resolved

    expanded = {key: _ENV_PATTERN.sub(replace, value) for key, value in values.items()}
    if missing:
        raise ConfigError(
            f"unresolved environment variables: {', '.join(sorted(missing))}"
        )
    return expanded


def validate_config(values: Mapping[str, str]) -> Config:
    """Validate parsed values and return the typed service configuration."""

    def required(key: str) -> str:
        value = values.get(key, "").strip()
        if not value:
            raise ConfigError(f"{key} is required")
        return value

    app_env_value = required("APP_ENV")
    try:
        app_env = Environment(app_env_value)
    except ValueError as error:
        raise ConfigError("APP_ENV must be local, dev, or production") from error

    manager_port = _required_int(required, "MANAGER_PORT", 1, 65_535)
    target_kube_context = required("TARGET_KUBE_CONTEXT")
    gvisor_runtime_class = required("GVISOR_RUNTIME_CLASS")
    if len(gvisor_runtime_class) > 253 or not _DNS_SUBDOMAIN_PATTERN.fullmatch(
        gvisor_runtime_class
    ):
        raise ConfigError(
            "GVISOR_RUNTIME_CLASS must be a valid Kubernetes DNS subdomain name"
        )

    max_sandboxes = _required_int(required, "MAX_SANDBOXES", 1, sys.maxsize)
    sandbox_cpu_request = _required_cpu(required, "SANDBOX_CPU_REQUEST")
    sandbox_memory_request = _required_memory(required, "SANDBOX_MEMORY_REQUEST")
    sandbox_cpu_limit = _required_cpu(required, "SANDBOX_CPU_LIMIT")
    sandbox_memory_limit = _required_memory(required, "SANDBOX_MEMORY_LIMIT")

    if sandbox_cpu_request.millicores > sandbox_cpu_limit.millicores:
        raise ConfigError("SANDBOX_CPU_REQUEST must not exceed SANDBOX_CPU_LIMIT")
    if sandbox_memory_request.bytes > sandbox_memory_limit.bytes:
        raise ConfigError("SANDBOX_MEMORY_REQUEST must not exceed SANDBOX_MEMORY_LIMIT")
    if sandbox_cpu_limit.millicores > MAX_CPU_MILLICORES:
        raise ConfigError("SANDBOX_CPU_LIMIT must not exceed 1 CPU")
    if sandbox_memory_limit.bytes > MAX_MEMORY_BYTES:
        raise ConfigError("SANDBOX_MEMORY_LIMIT must not exceed 512Mi")

    agent_check_interval = _required_duration(required, "AGENT_CHECK_INTERVAL")
    agent_check_timeout = _required_duration(required, "AGENT_CHECK_TIMEOUT")
    if agent_check_timeout >= agent_check_interval:
        raise ConfigError(
            "AGENT_CHECK_TIMEOUT must be shorter than AGENT_CHECK_INTERVAL"
        )

    nats_url_value = required("NATS_URL")
    _validate_nats_url(nats_url_value)

    return Config(
        app_env=app_env,
        manager_port=manager_port,
        target_kube_context=target_kube_context,
        gvisor_runtime_class=gvisor_runtime_class,
        max_sandboxes=max_sandboxes,
        sandbox_cpu_request=sandbox_cpu_request,
        sandbox_memory_request=sandbox_memory_request,
        sandbox_cpu_limit=sandbox_cpu_limit,
        sandbox_memory_limit=sandbox_memory_limit,
        agent_check_interval=agent_check_interval,
        agent_check_timeout=agent_check_timeout,
        nats_url=ConnectionURL(nats_url_value),
    )


def _required_int(
    required: Callable[[str], str], key: str, minimum: int, maximum: int
) -> int:
    value = required(key)
    if not _INTEGER_PATTERN.fullmatch(value):
        raise ConfigError(f"{key} must be an integer between {minimum} and {maximum}")
    try:
        parsed = int(value)
    except ValueError as error:
        raise ConfigError(
            f"{key} must be an integer between {minimum} and {maximum}"
        ) from error
    if parsed < minimum or parsed > maximum:
        raise ConfigError(f"{key} must be an integer between {minimum} and {maximum}")
    return parsed


def _required_cpu(required: Callable[[str], str], key: str) -> CPUQuantity:
    value = required(key)
    try:
        return _parse_cpu(value)
    except (InvalidOperation, OverflowError, ValueError) as error:
        raise ConfigError(f"{key} must be a positive CPU quantity") from error


def _parse_cpu(value: str) -> CPUQuantity:
    if _MILLI_CPU_PATTERN.fullmatch(value):
        millicores = int(value[:-1])
    elif _CPU_PATTERN.fullmatch(value):
        quantity = Decimal(value) * 1_000
        if quantity != quantity.to_integral_value():
            raise ValueError("CPU quantity is too precise")
        millicores = int(quantity)
    else:
        raise ValueError("invalid CPU quantity")
    if millicores <= 0 or millicores > sys.maxsize:
        raise ValueError("CPU quantity must be positive")
    return CPUQuantity(raw=value, millicores=millicores)


def _required_memory(required: Callable[[str], str], key: str) -> MemoryQuantity:
    value = required(key)
    try:
        return _parse_memory(value)
    except ValueError as error:
        raise ConfigError(f"{key} must be a positive binary memory quantity") from error


def _parse_memory(value: str) -> MemoryQuantity:
    match = _MEMORY_PATTERN.fullmatch(value)
    if match is None:
        raise ValueError("invalid memory quantity")
    amount = int(match.group(1))
    multiplier = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}[match.group(2)]
    if amount <= 0 or amount > sys.maxsize // multiplier:
        raise ValueError("invalid memory quantity")
    return MemoryQuantity(raw=value, bytes=amount * multiplier)


def _required_duration(required: Callable[[str], str], key: str) -> timedelta:
    value = required(key)
    try:
        duration = _parse_duration(value)
    except (InvalidOperation, OverflowError, ValueError) as error:
        raise ConfigError(f"{key} must be a positive duration") from error
    if duration <= timedelta(0):
        raise ConfigError(f"{key} must be a positive duration")
    return duration


def _parse_duration(value: str) -> timedelta:
    sign = 1
    unsigned = value
    if unsigned.startswith(("+", "-")):
        if unsigned[0] == "-":
            sign = -1
        unsigned = unsigned[1:]
    if not unsigned or "".join(_DURATION_PART_PATTERN.findall(unsigned)) != unsigned:
        raise ValueError("invalid duration")

    seconds = Decimal(0)
    for match in _DURATION_VALUE_PATTERN.finditer(unsigned):
        seconds += (
            Decimal(match.group("number")) * _DURATION_SECONDS[match.group("unit")]
        )
    return timedelta(seconds=float(sign * seconds))


def _validate_nats_url(value: str) -> None:
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        raise ConfigError("NATS_URL must be a valid NATS connection URL") from None
    if parsed.scheme != "nats":
        raise ConfigError("NATS_URL must use nats://")
    if (
        not parsed.netloc
        or hostname is None
        or not _is_usable_hostname(hostname)
        or port is None
        or port < 1
        or parsed.username is None
        or not is_identifier(parsed.username)
        or parsed.password is None
        or not parsed.password
        or parsed.path
        or parsed.query
        or parsed.fragment
        or any(character.isspace() for character in value)
    ):
        raise ConfigError(
            "NATS_URL must include a valid hostname, port, username, and password"
        )


def _is_usable_hostname(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        hostname = value.removesuffix(".")
        return (
            0 < len(hostname) <= 253
            and all(_HOST_LABEL_PATTERN.fullmatch(label) for label in hostname.split("."))
        )
    return True
