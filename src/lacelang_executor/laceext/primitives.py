"""Built-in primitive functions available inside rule bodies and functions.

Implements lace-extensions.md §7 exactly — all implementations of the
`.laceext` processor share this surface.
"""

from __future__ import annotations

from typing import Any


def compare(a: Any, b: Any) -> str | None:
    """Return the op key describing the relationship between a and b."""
    if a is None or b is None:
        return None
    # Bool is a subclass of int in Python — only eq/neq meaningful for bool per spec.
    if isinstance(a, bool) or isinstance(b, bool):
        if type(a) is not type(b):
            return None
        return "eq" if a == b else "neq"
    if type(a) is not type(b):
        # int+float is OK (both numeric); everything else incomparable.
        if not (isinstance(a, (int, float)) and isinstance(b, (int, float))):
            if isinstance(a, str) != isinstance(b, str):
                return None
    try:
        if a < b:  return "lt"
        if a > b:  return "gt"
        if a == b: return "eq"
    except TypeError:
        return None
    return "neq"


def map_get(m: Any, key: Any) -> Any:
    if not isinstance(m, dict):
        return None
    if key in m:
        return m[key]
    if "default" in m:
        return m["default"]
    return None


def map_match(m: Any, actual: Any, expected: Any, op: Any) -> Any:
    if not isinstance(m, dict):
        return None
    actual_key = _scalar_to_key(actual)
    if actual_key is not None and actual_key in m:
        return m[actual_key]
    rel = compare(actual, expected)
    if rel is not None and rel in m:
        return m[rel]
    if "default" in m:
        return m["default"]
    return None


def _scalar_to_key(v: Any) -> str | None:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return v
    return None


def is_null(v: Any) -> bool:
    return v is None


def type_of(v: Any) -> str:
    if v is None:              return "null"
    if isinstance(v, bool):    return "bool"
    if isinstance(v, int):     return "int"
    if isinstance(v, float):   return "float"
    if isinstance(v, str):     return "string"
    if isinstance(v, list):    return "array"
    if isinstance(v, dict):    return "object"
    return "any"


def to_string(v: Any) -> str:
    if v is None:              return "null"
    if isinstance(v, bool):    return "true" if v else "false"
    if isinstance(v, str):     return v
    return str(v)


def replace(s: Any, pattern: Any, replacement: Any) -> Any:
    if s is None or pattern is None:
        return s
    return str(s).replace(str(pattern), to_string(replacement))


PRIMITIVES: dict[str, Any] = {
    "compare":    compare,
    "map_get":    map_get,
    "map_match":  map_match,
    "is_null":    is_null,
    "type_of":    type_of,
    "to_string":  to_string,
    "replace":    replace,
}
