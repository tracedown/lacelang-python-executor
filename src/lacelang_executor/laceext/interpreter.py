"""Tree-walking interpreter for the .laceext rule body DSL.

Implements:
    - null-propagating field/index/filter access
    - for / when / let / emit / exit / return semantics
    - ternary, boolean, arithmetic, comparison, equality
    - function dispatch: primitives, extension-defined functions, and
      implicit type-tag constructors (from [types.T] one_of entries)
    - emit-target validation (only result.actions.* and result.runVars)
"""

from __future__ import annotations

from typing import Any, Callable

from lacelang_executor.laceext.primitives import PRIMITIVES, compare


class _ExitRule(Exception):
    """Raised by `exit` to unwind out of a rule body."""


class _ReturnValue(Exception):
    def __init__(self, value: Any) -> None:
        self.value = value


class Scope:
    """Nested-scope local bindings. `get` walks up; `put` is current-scope only."""
    def __init__(self, parent: "Scope | None" = None):
        self.vars: dict[str, Any] = {}
        self.parent = parent

    def get(self, name: str) -> Any:
        cur: Scope | None = self
        while cur is not None:
            if name in cur.vars:
                return cur.vars[name]
            cur = cur.parent
        return None

    def has(self, name: str) -> bool:
        cur: Scope | None = self
        while cur is not None:
            if name in cur.vars:
                return True
            cur = cur.parent
        return False

    def put(self, name: str, value: Any) -> None:
        self.vars[name] = value

    def set(self, name: str, value: Any) -> bool:
        """Find and update an existing binding in the scope chain. Returns True if found."""
        cur: Scope | None = self
        while cur is not None:
            if name in cur.vars:
                cur.vars[name] = value
                return True
            cur = cur.parent
        return False

    def child(self) -> "Scope":
        return Scope(self)


class Interpreter:
    """One instance per `.laceext` file. Stateless across rule invocations."""

    def __init__(
        self,
        extension_name: str,
        functions: dict[str, dict[str, Any]],
        tag_constructors: dict[str, Callable[[list[Any]], Any]],
        emit_callback: Callable[[list[str], dict[str, Any]], None],
        config: dict[str, Any] | None = None,
        require_view: dict[str, dict[str, Any]] | None = None,
        qualified_call: Callable[[str, str, list[Any]], Any] | None = None,
        requires: list[str] | None = None,
    ) -> None:
        self.ext_name = extension_name
        self.functions = functions
        self.tag_constructors = tag_constructors
        self.emit = emit_callback
        self.config = config or {}
        # Per-rule snapshot of required extensions' live run_vars, keyed by
        # dependency name. See lace-extensions.md §9.1. Empty dict when the
        # current extension has no `require`.
        self.require_view = require_view or {}
        # Callback into the registry for invoking an exposed function owned
        # by another loaded extension. None when no qualified calls are
        # possible (e.g. unit tests that drive Interpreter directly).
        self.qualified_call = qualified_call
        # `require` list from the owning extension — authoritative list of
        # extensions this rule's qualified_call is allowed to target.
        self.requires = set(requires or [])

    # ── public: run one rule body ────────────────────────────────────

    def run_rule(self, body: list[dict[str, Any]], context: dict[str, Any]) -> None:
        scope = Scope()
        for k, v in context.items():
            scope.put(k, v)
        try:
            self._run_stmts(body, scope)
        except _ExitRule:
            return

    # ── statements ──────────────────────────────────────────────────

    def _run_stmts(self, stmts: list[dict[str, Any]], scope: Scope) -> None:
        for st in stmts:
            self._run_stmt(st, scope)

    def _run_stmt(self, st: dict[str, Any], scope: Scope) -> None:
        k = st["kind"]
        if k == "when_inline":
            if not self._truthy(self._eval(st["cond"], scope)):
                raise _ExitRule()
            return
        if k == "when_block":
            if self._truthy(self._eval(st["cond"], scope)):
                self._run_stmts(st["body"], scope.child())
            return
        if k == "for":
            it = self._eval(st["iter"], scope)
            if it is None:
                return
            if not isinstance(it, list):
                return
            for v in it:
                inner = scope.child()
                inner.put(st["binding"], v)
                try:
                    self._run_stmts(st["body"], inner)
                except _ExitRule:
                    raise
            return
        if k == "let":
            if scope.has(st["name"]):
                raise RuntimeError(f"let: name ${st['name']!r} already bound in this scope")
            scope.put(st["name"], self._eval(st["expr"], scope))
            return
        if k == "set":
            name = st["name"]
            if not scope.set(name, self._eval(st["expr"], scope)):
                raise RuntimeError(f"set: name ${name!r} is not bound in any enclosing scope")
            return
        if k == "emit":
            self._run_emit(st, scope)
            return
        if k == "exit":
            raise _ExitRule()
        if k == "return":
            raise _ReturnValue(self._eval(st["expr"], scope))
        if k == "call_stmt":
            self._eval(st["call"], scope)
            return
        raise RuntimeError(f"unknown statement kind: {k}")

    def _run_emit(self, st: dict[str, Any], scope: Scope) -> None:
        target = st["target"]  # ["result", "actions", "notifications"] or ["result", "runVars"]
        if len(target) < 2 or target[0] != "result":
            raise RuntimeError(f"invalid emit target: {'.'.join(target)}")
        # Evaluate field values
        payload: dict[str, Any] = {}
        for f in st["fields"]:
            payload[f["key"]] = self._eval(f["value"], scope)
        # Namespace guard on runVars
        if target == ["result", "runVars"]:
            prefixed: dict[str, Any] = {}
            for k, v in payload.items():
                if not k.startswith(f"{self.ext_name}."):
                    raise RuntimeError(
                        f"extension {self.ext_name!r} emitted run_vars key {k!r} "
                        f"without required prefix"
                    )
                prefixed[k] = v
            payload = prefixed
        self.emit(target, payload)

    # ── expressions ─────────────────────────────────────────────────

    def _eval(self, node: dict[str, Any], scope: Scope) -> Any:
        k = node["kind"]
        if k == "literal":
            return node["value"]
        if k == "base":
            n = node["name"]
            if n == "this":    return scope.get("this")
            if n == "prev":    return scope.get("prev")
            if n == "result":  return scope.get("result")
            if n == "config":  return self.config
            if n == "require": return self.require_view
            return None
        if k == "binding":
            return scope.get(node["name"])
        if k == "ident":
            # Bare identifier in expressions — lookup scope; if unbound, null.
            return scope.get(node["name"])
        if k == "access_field":
            base = self._eval(node["base"], scope)
            if base is None:
                return None
            if isinstance(base, dict):
                return base.get(node["name"])
            return None
        if k == "access_index":
            base = self._eval(node["base"], scope)
            if base is None:
                return None
            idx = self._eval(node["index"], scope)
            if isinstance(base, list) and isinstance(idx, int) and 0 <= idx < len(base):
                return base[idx]
            if isinstance(base, dict) and isinstance(idx, str):
                return base.get(idx)
            return None
        if k == "access_filter":
            base = self._eval(node["base"], scope)
            if not isinstance(base, list):
                return None
            for item in base:
                inner = scope.child()
                inner.put("$", item)  # `$` refers to current element in filter predicate
                if self._truthy(self._eval(node["cond"], inner)):
                    return item
            return None
        if k == "ternary":
            return self._eval(node["then"], scope) if self._truthy(self._eval(node["cond"], scope)) \
                   else self._eval(node["else"], scope)
        if k == "binop":
            return self._eval_binop(node, scope)
        if k == "unop":
            return self._eval_unop(node, scope)
        if k == "call" or k == "qualified_call":
            return self._eval_call(node, scope)
        if k == "object_lit":
            return {f["key"]: self._eval(f["value"], scope)
                    for f in node.get("fields", [])}
        raise RuntimeError(f"unknown expression kind: {k}")

    def _eval_binop(self, node: dict[str, Any], scope: Scope) -> Any:
        op = node["op"]
        if op == "and":
            left = self._eval(node["left"], scope)
            if not self._truthy(left):
                return left if left is None else False
            return self._eval(node["right"], scope)
        if op == "or":
            left = self._eval(node["left"], scope)
            if self._truthy(left):
                return left
            return self._eval(node["right"], scope)
        a = self._eval(node["left"], scope)
        b = self._eval(node["right"], scope)
        if op == "eq":
            return a == b
        if op == "neq":
            return a != b
        # arithmetic + ordered compare: null propagates
        if a is None or b is None:
            return None
        try:
            if op == "lt":  return a <  b
            if op == "lte": return a <= b
            if op == "gt":  return a >  b
            if op == "gte": return a >= b
            if op == "+":
                if isinstance(a, str) and isinstance(b, str):
                    return a + b
                if isinstance(a, (int, float)) and isinstance(b, (int, float)) \
                        and not isinstance(a, bool) and not isinstance(b, bool):
                    return a + b
                return None
            if op == "-":
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    return a - b
                return None
            if op == "*":
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    return a * b
                return None
            if op == "/":
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    return None if b == 0 else (a / b if isinstance(a, float) or isinstance(b, float) else a // b)
                return None
        except TypeError:
            return None
        return None

    def _eval_unop(self, node: dict[str, Any], scope: Scope) -> Any:
        op = node["op"]
        v = self._eval(node["operand"], scope)
        if op == "not":
            return not self._truthy(v)
        if op == "-":
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return -v
            return None
        return None

    def _eval_call(self, node: dict[str, Any], scope: Scope) -> Any:
        kind = node.get("kind")
        if kind == "qualified_call":
            ext = node["ext"]
            name = node["name"]
            args = [self._eval(a, scope) for a in node.get("args", [])]
            if ext not in self.requires:
                raise RuntimeError(
                    f"extension {self.ext_name!r} called "
                    f"{ext}.{name}(...) but does not require {ext!r} "
                    f"(add it to [extension].require)"
                )
            if self.qualified_call is None:
                raise RuntimeError(
                    "qualified function call unavailable in this context"
                )
            return self.qualified_call(ext, name, args)
        name = node["name"]
        args = [self._eval(a, scope) for a in node.get("args", [])]
        if name in PRIMITIVES:
            return PRIMITIVES[name](*args)
        if name in self.tag_constructors:
            return self.tag_constructors[name](args)
        if name in self.functions:
            return self._call_function(name, args)
        raise RuntimeError(f"unknown function in .laceext rule: {name!r}")

    def _call_function(self, name: str, args: list[Any]) -> Any:
        spec = self.functions[name]
        body = spec["body"]
        params: list[str] = spec.get("params", [])
        if len(args) != len(params):
            raise RuntimeError(f"function {name!r} expected {len(params)} args, got {len(args)}")
        scope = Scope()
        for p, v in zip(params, args, strict=False):
            scope.put(p, v)
        try:
            self._run_stmts(body, scope)
        except _ReturnValue as r:
            return r.value
        return None

    # ── helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _truthy(v: Any) -> bool:
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        return True
