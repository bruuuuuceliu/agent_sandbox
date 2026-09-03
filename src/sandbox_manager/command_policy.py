"""Versioned command policy extension point for relay admission.

Policy evaluation is mandatory before every relay side effect. The evaluation
schema below reserves the normalized fields a policy may eventually match on,
but the allowlist syntax, filters, normalization rules, and matching strategy
are deliberately unspecified: they remain an open decision (plan Decision 19)
and must not be embedded in implementation. Until a filtering method is
approved, the only shipped policy is the default-deny behaviour required when a
profile references a missing or invalid policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

from .relay_protocol import MAX_DECLARED_BYTES, RelayOperation

POLICY_SCHEMA_VERSION: Literal[1] = 1

_MISSING_POLICY = "policy_missing"


class CommandPolicyError(ValueError):
    """Raised when command policy data is invalid."""


class UnknownPolicyError(LookupError):
    """Raised when no policy exists for a profile's policy reference."""

    def __init__(self, policy_name: str) -> None:
        self.policy_name = policy_name
        super().__init__(f"unknown command policy: {policy_name}")


class PolicyDecision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"


@dataclass(frozen=True, slots=True)
class PolicyReference:
    """Names the policy a sandbox profile requires for relay admission."""

    schema_version: Literal[1]
    policy_name: str
    policy_version: int

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise CommandPolicyError(
                f"unsupported policy schema version: {self.schema_version}"
            )
        _require_text(self.policy_name, "policy name")
        if type(self.policy_version) is not int or self.policy_version < 1:
            raise CommandPolicyError("policy version must be a positive integer")


@dataclass(frozen=True, slots=True)
class PolicyEvaluation:
    """Reserved, normalized inputs a command policy may evaluate.

    The authenticated principal and profile come from the manager, never from
    caller-controlled message fields. Every field is optional because not every
    operation carries every input; a filtering method is not yet approved.
    """

    schema_version: Literal[1]
    operation: RelayOperation
    profile: str
    authenticated_principal: str
    executable: str | None = None
    argv: tuple[str, ...] | None = None
    working_directory: str | None = None
    environment_names: tuple[str, ...] | None = None
    path: str | None = None
    declared_size: int | None = None

    def __post_init__(self) -> None:
        if self.schema_version != POLICY_SCHEMA_VERSION:
            raise CommandPolicyError(
                f"unsupported policy schema version: {self.schema_version}"
            )
        if not isinstance(self.operation, RelayOperation):
            raise CommandPolicyError("operation must be a relay operation")
        _require_text(self.profile, "profile")
        _require_text(self.authenticated_principal, "authenticated principal")
        for value, field_name in (
            (self.executable, "executable"),
            (self.working_directory, "working directory"),
            (self.path, "path"),
        ):
            if value is not None:
                _require_text(value, field_name)
        for values, field_name in (
            (self.argv, "argv"),
            (self.environment_names, "environment names"),
        ):
            if values is None:
                continue
            if not isinstance(values, tuple) or not all(
                isinstance(item, str) and item for item in values
            ):
                raise CommandPolicyError(f"{field_name} must be a tuple of strings")
        if self.declared_size is not None and (
            type(self.declared_size) is not int
            or not 0 <= self.declared_size <= MAX_DECLARED_BYTES
        ):
            raise CommandPolicyError(
                f"declared size must be an integer between 0 and {MAX_DECLARED_BYTES}"
            )


@dataclass(frozen=True, slots=True)
class PolicyResult:
    """One policy decision with the audit reference for it."""

    decision: PolicyDecision
    policy_version: int
    decision_reference: str

    def __post_init__(self) -> None:
        if not isinstance(self.decision, PolicyDecision):
            raise CommandPolicyError("decision must be a policy decision")
        if type(self.policy_version) is not int or self.policy_version < 1:
            raise CommandPolicyError("policy version must be a positive integer")
        _require_text(self.decision_reference, "decision reference")


class CommandPolicy(Protocol):
    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        """Evaluate one normalized command admission against this policy.

        Implementations must be deterministic for equal inputs and must not
        perform side effects. The filtering method an implementation may use is
        not yet approved; shipping a filtering policy requires an owner decision
        recorded against plan Decision 19.
        """

        ...


class DenyAllPolicy:
    """Default-deny policy used when a referenced policy is missing or invalid."""

    def __init__(self, policy_version: int = 1, reason_code: str = _MISSING_POLICY) -> None:
        if type(policy_version) is not int or policy_version < 1:
            raise CommandPolicyError("policy version must be a positive integer")
        _require_text(reason_code, "reason code")
        self._policy_version = policy_version
        self._reason_code = reason_code

    @property
    def reason_code(self) -> str:
        return self._reason_code

    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        return PolicyResult(
            decision=PolicyDecision.DENY,
            policy_version=self._policy_version,
            decision_reference=self._reason_code,
        )


class CommandPolicyRegistry:
    """Resolves profile policy references and default-denies missing entries."""

    def __init__(self, policies: dict[str, CommandPolicy] | None = None) -> None:
        self._policies: dict[str, CommandPolicy] = dict(policies or {})

    def resolve_or_deny(self, reference: PolicyReference | None) -> CommandPolicy:
        """Return the referenced policy, or the default-deny policy.

        A missing, unreadable, or invalid referenced policy must never widen
        admission: it resolves to ``DenyAllPolicy`` instead of raising.
        """

        if not isinstance(reference, PolicyReference):
            return DenyAllPolicy()
        policy = self._policies.get(reference.policy_name)
        if policy is None:
            return DenyAllPolicy()
        return policy


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise CommandPolicyError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise CommandPolicyError(f"{field} contains invalid text")
