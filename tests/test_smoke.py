"""Executor smoke tests — pure runtime behaviour, no network.

Uses fabricated ASTs and internal helpers. The validator's own tests
live in lacelang-python-validator.
"""

from lacelang_executor.executor import _eval, _interp


# ── String interpolation ────────────────────────────────────────

def test_interp_script_var(env_with_vars):
    env = env_with_vars(name="Max")
    assert _interp("hello $name!", env) == "hello Max!"


def test_interp_run_var(env):
    env.run_vars["token"] = "abc"
    assert _interp("auth=$$token", env) == "auth=abc"


def test_interp_braced_script_var(env_with_vars):
    env = env_with_vars(host="example.com")
    assert _interp("${$host}:8080", env) == "example.com:8080"


def test_interp_braced_run_var(env):
    env.run_vars["id"] = "42"
    assert _interp("item-${$$id}-detail", env) == "item-42-detail"


def test_interp_literal_dollar_no_match(env_with_vars):
    """A $ not followed by an identifier is left as-is."""
    env = env_with_vars(foo="bar")
    assert _interp("price=$100", env) == "price=$100"
    # $ at end of string
    assert _interp("cost$", env) == "cost$"


def test_interp_missing_var_null(env):
    warnings: list[str] = []
    result = _interp("val=$missing", env, warnings)
    assert result == "val=null"
    assert len(warnings) == 1


# ── Expression evaluation ───────────────────────────────────────

def test_eval_binary_comparison(env):
    expr = {
        "kind": "binary", "op": "gt",
        "left":  {"kind": "literal", "valueType": "int", "value": 5},
        "right": {"kind": "literal", "valueType": "int", "value": 3},
    }
    assert _eval(expr, env) is True


def test_eval_this_ref(env):
    env.this = {"body": {"count": 42}}
    expr = {"kind": "thisRef", "path": ["body", "count"]}
    assert _eval(expr, env) == 42


def test_eval_null_arithmetic_returns_none(env):
    """Null operand in arithmetic → None (indeterminate upstream)."""
    expr = {
        "kind": "binary", "op": "add",
        "left":  {"kind": "literal", "valueType": "int", "value": 5},
        "right": {"kind": "scriptVar", "name": "missing"},
    }
    assert _eval(expr, env) is None


def test_eval_null_equality(env):
    expr = {
        "kind": "binary", "op": "eq",
        "left":  {"kind": "scriptVar", "name": "a"},
        "right": {"kind": "scriptVar", "name": "b"},
    }
    # null eq null → True
    assert _eval(expr, env) is True


def test_eval_and_short_circuit(env_with_vars):
    """and/or return the deciding operand, not bool."""
    env = env_with_vars()
    expr = {
        "kind": "binary", "op": "and",
        "left":  {"kind": "literal", "valueType": "int", "value": 0},
        "right": {"kind": "literal", "valueType": "int", "value": 42},
    }
    # 0 is falsy → returns 0 (the left operand), not False
    assert _eval(expr, env) == 0


def test_eval_or_short_circuit(env_with_vars):
    env = env_with_vars()
    expr = {
        "kind": "binary", "op": "or",
        "left":  {"kind": "literal", "valueType": "string", "value": "hello"},
        "right": {"kind": "literal", "valueType": "string", "value": "world"},
    }
    # "hello" is truthy → returns "hello", not True
    assert _eval(expr, env) == "hello"
