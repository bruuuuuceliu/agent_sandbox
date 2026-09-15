"""Shared identifier rules for transport and resource boundaries."""

from __future__ import annotations

import re
from typing import TypeGuard

MAX_IDENTIFIER_LENGTH = 64

_IDENTIFIER_PATTERN = re.compile(
    rf"^[A-Za-z0-9_-]{{1,{MAX_IDENTIFIER_LENGTH}}}$"
)


def is_identifier(value: object) -> TypeGuard[str]:
    """Return whether a value is safe in wire frames and NATS subjects."""

    return isinstance(value, str) and _IDENTIFIER_PATTERN.fullmatch(value) is not None
