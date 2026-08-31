"""Minimal RFC 8785 JSON Canonicalization Scheme (JCS) implementation.

Deterministic JSON serialization used by the fleet envelope provenance
signature scheme (see blueeconomy-contracts docs/envelope-signature.md):

- object members sorted by key in UTF-16 code-unit order, no whitespace;
- strings use minimal JSON escaping with non-ASCII emitted raw (UTF-8);
- numbers follow ECMAScript ``Number::toString`` semantics, so callers should
  parse JSON with ``parse_int=float``/``parse_float=float`` to match the
  ECMAScript ``JSON.parse`` numeric model exactly.

No external dependencies.
"""

from __future__ import annotations

import json
import math
from typing import Any

__all__ = ["canonicalize"]


def canonicalize(value: Any) -> str:
    """Serialize *value* to its RFC 8785 canonical form."""
    return _serialize(value)


def _serialize(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return _serialize_string(value)
    if isinstance(value, (int, float)):
        return _serialize_number(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_serialize(item) for item in value) + "]"
    if isinstance(value, dict):
        members = sorted(value.keys(), key=_utf16_sort_key)
        for key in members:
            if not isinstance(key, str):
                raise ValueError("JCS object keys must be strings")
        return (
            "{"
            + ",".join(_serialize_string(key) + ":" + _serialize(value[key]) for key in members)
            + "}"
        )
    raise ValueError(f"JCS cannot canonicalize value of type {type(value).__name__}")


def _utf16_sort_key(text: str) -> bytes:
    # RFC 8785 sorts object keys by UTF-16 code units; encoding to UTF-16-BE
    # and comparing bytes reproduces that order (Python's default str order is
    # by code point, which differs for astral-plane characters).
    return text.encode("utf-16-be", "surrogatepass")


def _serialize_string(text: str) -> str:
    # json.dumps with ensure_ascii=False produces exactly the minimal escaping
    # RFC 8785 requires: only '"', '\\' and control characters < 0x20 are
    # escaped, using the short forms \\b \\t \\n \\f \\r where defined and
    # lowercase \\u00xx otherwise; non-ASCII is emitted raw.
    return json.dumps(text, ensure_ascii=False)


def _serialize_number(value: int | float) -> str:
    if isinstance(value, int):
        # ECMAScript Number::toString for integral values below 1e21 is the
        # plain decimal form. Beyond the IEEE-754 safe range the value would
        # not survive an ECMAScript JSON.parse round-trip, so reject it rather
        # than canonicalize something no peer implementation can reproduce.
        if abs(value) >= 1e21:
            raise ValueError("JCS cannot canonicalize integers outside the IEEE-754 range")
        return str(value)
    if not math.isfinite(value):
        raise ValueError("JCS cannot canonicalize non-finite numbers")
    return _ecmascript_number_to_string(value)


def _ecmascript_number_to_string(value: float) -> str:
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    digits, exponent = _shortest_digits(abs(value))
    # value == digits * 10**exponent; n positions the decimal point such that
    # value == 0.digits * 10**n, matching the ECMAScript spec variables.
    k = len(digits)
    n = exponent + k
    if k <= n <= 21:
        return sign + digits + "0" * (n - k)
    if 0 < n <= 21:
        return sign + digits[:n] + "." + digits[n:]
    if -6 < n <= 0:
        return sign + "0." + "0" * (-n) + digits
    exponent_text = f"e{'+' if n - 1 >= 0 else '-'}{abs(n - 1)}"
    if k == 1:
        return sign + digits + exponent_text
    return sign + digits[0] + "." + digits[1:] + exponent_text


def _shortest_digits(value: float) -> tuple[str, int]:
    """Return (digits, exponent) with value == int(digits) * 10**exponent.

    Python's repr() yields the same shortest round-trip decimal ECMAScript
    computes, so we re-format its digits per the ECMAScript layout rules.
    """
    text = repr(value)
    mantissa, _, exponent_text = text.partition("e")
    exponent = int(exponent_text) if exponent_text else 0
    integer, _, fraction = mantissa.partition(".")
    digits = (integer + fraction).lstrip("0")
    exponent -= len(fraction)
    stripped = digits.rstrip("0")
    exponent += len(digits) - len(stripped)
    if not stripped:
        return "0", 0
    return stripped, exponent
