from __future__ import annotations

import unittest
from typing import Literal, cast

from sandbox_manager.command_policy import (
    POLICY_SCHEMA_VERSION,
    CommandPolicyError,
    CommandPolicyRegistry,
    DenyAllPolicy,
    PolicyDecision,
    PolicyEvaluation,
    PolicyReference,
    PolicyResult,
    UnknownPolicyError,
)
from sandbox_manager.relay_protocol import MAX_DECLARED_BYTES, RelayOperation


class _AllowPolicy:
    async def evaluate(self, request: PolicyEvaluation) -> PolicyResult:
        return PolicyResult(
            decision=PolicyDecision.ALLOW,
            policy_version=1,
            decision_reference="test-policy-v1",
        )


class PolicyReferenceTests(unittest.TestCase):
    def test_accepts_valid_reference(self) -> None:
        reference = PolicyReference(
            schema_version=POLICY_SCHEMA_VERSION,
            policy_name="default-exec",
            policy_version=3,
        )

        self.assertEqual(reference.policy_name, "default-exec")
        self.assertEqual(reference.policy_version, 3)

    def test_rejects_unsupported_schema_version(self) -> None:
        bad_version = cast(Literal[1], 2)  # deliberately invalid at runtime
        with self.assertRaises(CommandPolicyError):
            PolicyReference(schema_version=bad_version, policy_name="p", policy_version=1)

    def test_rejects_blank_name(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyReference(
                schema_version=POLICY_SCHEMA_VERSION, policy_name=" ", policy_version=1
            )

    def test_rejects_non_positive_version(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyReference(
                schema_version=POLICY_SCHEMA_VERSION, policy_name="p", policy_version=0
            )

    def test_rejects_non_integer_version(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyReference(
                schema_version=POLICY_SCHEMA_VERSION,
                policy_name="p",
                policy_version="1",  # type: ignore[arg-type]
            )


class PolicyEvaluationTests(unittest.TestCase):
    def test_accepts_full_evaluation(self) -> None:
        evaluation = PolicyEvaluation(
            schema_version=POLICY_SCHEMA_VERSION,
            operation=RelayOperation.EXEC,
            profile="default",
            authenticated_principal="agent-session-1",
            executable="/usr/bin/python3",
            argv=("-c", "print(1)"),
            working_directory="/workspace",
            environment_names=("PATH",),
            path=None,
            declared_size=16,
        )

        self.assertEqual(evaluation.argv, ("-c", "print(1)"))

    def test_accepts_minimal_evaluation(self) -> None:
        evaluation = PolicyEvaluation(
            schema_version=POLICY_SCHEMA_VERSION,
            operation=RelayOperation.DOWNLOAD,
            profile="default",
            authenticated_principal="agent-session-1",
        )

        self.assertIsNone(evaluation.executable)
        self.assertIsNone(evaluation.declared_size)

    def test_rejects_non_operation(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyEvaluation(
                schema_version=POLICY_SCHEMA_VERSION,
                operation="shell",  # type: ignore[arg-type]
                profile="default",
                authenticated_principal="agent-session-1",
            )

    def test_rejects_blank_profile(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyEvaluation(
                schema_version=POLICY_SCHEMA_VERSION,
                operation=RelayOperation.EXEC,
                profile="",
                authenticated_principal="agent-session-1",
            )

    def test_rejects_list_argv(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyEvaluation(
                schema_version=POLICY_SCHEMA_VERSION,
                operation=RelayOperation.EXEC,
                profile="default",
                authenticated_principal="agent-session-1",
                argv=["-c"],  # type: ignore[arg-type]
            )

    def test_rejects_oversized_declared_size(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyEvaluation(
                schema_version=POLICY_SCHEMA_VERSION,
                operation=RelayOperation.UPLOAD,
                profile="default",
                authenticated_principal="agent-session-1",
                declared_size=MAX_DECLARED_BYTES + 1,
            )


class PolicyResultTests(unittest.TestCase):
    def test_accepts_valid_result(self) -> None:
        result = PolicyResult(
            decision=PolicyDecision.DENY, policy_version=2, decision_reference="r-2"
        )

        self.assertIs(result.decision, PolicyDecision.DENY)

    def test_rejects_non_decision(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyResult(
                decision="allow",  # type: ignore[arg-type]
                policy_version=1,
                decision_reference="r-1",
            )

    def test_rejects_blank_decision_reference(self) -> None:
        with self.assertRaises(CommandPolicyError):
            PolicyResult(
                decision=PolicyDecision.ALLOW, policy_version=1, decision_reference=""
            )


class DenyAllPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_denies_with_missing_policy_reference(self) -> None:
        policy = DenyAllPolicy()
        evaluation = PolicyEvaluation(
            schema_version=POLICY_SCHEMA_VERSION,
            operation=RelayOperation.EXEC,
            profile="default",
            authenticated_principal="agent-session-1",
        )

        result = await policy.evaluate(evaluation)

        self.assertIs(result.decision, PolicyDecision.DENY)
        self.assertEqual(result.decision_reference, "policy_missing")


class CommandPolicyRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_registered_policy(self) -> None:
        policy = _AllowPolicy()
        registry = CommandPolicyRegistry({"default-exec": policy})

        evaluation = PolicyEvaluation(
            schema_version=POLICY_SCHEMA_VERSION,
            operation=RelayOperation.EXEC,
            profile="default",
            authenticated_principal="agent-session-1",
        )

        resolved = registry.resolve_or_deny(
            PolicyReference(
                schema_version=POLICY_SCHEMA_VERSION,
                policy_name="default-exec",
                policy_version=1,
            )
        )

        self.assertIs(resolved, policy)
        self.assertIs((await resolved.evaluate(evaluation)).decision, PolicyDecision.ALLOW)

    async def test_missing_policy_default_denies(self) -> None:
        registry = CommandPolicyRegistry({})

        resolved = registry.resolve_or_deny(
            PolicyReference(
                schema_version=POLICY_SCHEMA_VERSION,
                policy_name="absent",
                policy_version=1,
            )
        )

        self.assertIsInstance(resolved, DenyAllPolicy)

    async def test_absent_reference_default_denies(self) -> None:
        registry = CommandPolicyRegistry({"default-exec": _AllowPolicy()})

        resolved = registry.resolve_or_deny(None)

        self.assertIsInstance(resolved, DenyAllPolicy)


class UnknownPolicyErrorTests(unittest.TestCase):
    def test_carries_policy_name(self) -> None:
        error = UnknownPolicyError("absent")

        self.assertEqual(error.policy_name, "absent")


if __name__ == "__main__":
    unittest.main()
