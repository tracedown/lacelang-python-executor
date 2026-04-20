"""Extension registry — central coordinator for loaded `.laceext` files.

Instantiated per `run_script()` call. Responsibilities:
    - Hold the collection of loaded Extensions.
    - Validate each extension's `require` list resolves to a loaded peer.
    - Aggregate emit results: `result.actions.*` arrays and namespaced
      `result.runVars` entries.
    - Fire hooks in the topologically-sorted order specified by per-rule
      `after` / `before` qualifiers, with silent-drop semantics when an
      ordering constraint references an extension that contributed no rules
      to the hook (see lace-extensions.md §8.1.1).
    - Expose each dependent extension's required-extension variable view
      via the DSL `require[...]` base.
"""

from __future__ import annotations

from typing import Any, Callable

from lacelang_executor.laceext.interpreter import Interpreter
from lacelang_executor.laceext.loader import Extension, HookRegistration, RuleDef, load_extension


class ExtensionRegistry:
    def __init__(self, config: dict | None = None) -> None:
        self.extensions: list[Extension] = []
        self.actions: dict[str, list[Any]] = {}
        self.ext_run_vars: dict[str, Any] = {}          # flat {prefixed-key: value}
        # Per-extension live view, keyed by extension name. Each entry is
        # the same flat prefixed-key map restricted to that extension — what
        # a dependent sees through `require["<name>"]`.
        self.per_ext_run_vars: dict[str, dict[str, Any]] = {}
        self.warnings: list[str] = []
        # Per-extension config subtree. Keyed by extension name — each value
        # is the dict the interpreter exposes via the `config` base. Per
        # spec §11 + lace-extensions.md §11, this is the `[extensions.<name>]`
        # subtree of `lace.config`. Defaults to empty when unset.
        self.extension_config: dict = config or {}

    # ── loading ─────────────────────────────────────────────────────

    def load(self, path: str) -> Extension:
        ext = load_extension(path)
        self.extensions.append(ext)
        self.per_ext_run_vars.setdefault(ext.name, {})
        return ext

    def finalize(self) -> None:
        """Post-load checks. Safe to call once, after all `load()`s.

        Enforces three startup invariants:
          1. Every `require` target is loaded (spec §2.2 presence check).
          2. Every `after X` / `before X` qualifier on any rule names a
             loaded extension (spec §8.1.1 step 3 — name-resolve startup
             error). Conflated with "no rules at hook" before; now raises.
          3. The cross-extension function call graph (local +
             `qualified_call`) is acyclic (spec §6.1).
        """
        loaded = {e.name for e in self.extensions}

        # 1. require presence
        for ext in self.extensions:
            for dep in ext.requires:
                if dep not in loaded:
                    raise RuntimeError(
                        f"extension {ext.name!r} requires {dep!r}, "
                        f"but {dep!r} is not loaded"
                    )

        # 2. after/before name resolution
        for ext in self.extensions:
            for rule in ext.rules:
                for reg in rule.hooks:
                    for target in list(reg.after) + list(reg.before):
                        if target not in loaded:
                            raise RuntimeError(
                                f"extension {ext.name!r} rule {rule.name!r} "
                                f"on hook {reg.hook!r}: unknown extension "
                                f"{target!r} in 'after'/'before' qualifier"
                            )

        # 3. cross-extension function call graph cycle check
        _check_cross_ext_recursion(self.extensions)

    def is_active(self, name: str) -> bool:
        return any(e.name == name for e in self.extensions)

    def tag_constructors(self) -> dict[str, Callable[[list[Any]], Any]]:
        out: dict[str, Callable[[list[Any]], Any]] = {}
        for e in self.extensions:
            out.update(e.tag_constructors())
        return out

    # ── hook dispatch (topo-sorted) ─────────────────────────────────

    def fire_hook(self, hook: str, context: dict[str, Any]) -> None:
        """Resolve per-hook ordering and invoke every surviving rule.

        Algorithm (lace-extensions.md §8.1.1):
            1. Gather all (extension, rule, hook-registration) triples that
               register `hook`.
            2. Add implicit `after dep` edges from each rule's containing
               extension's `require` list, for deps that also contribute
               rules to this hook.
            3. Silent-drop rules whose explicit `after`/`before` targets
               contribute zero rules to this hook. Cascade until stable.
            4. Topo-sort. Cycle → raise.
            5. Execute in sorted order.
        """
        triples = self._gather_rules_for_hook(hook)
        if not triples:
            return

        # --- step 3: silent drop -------------------------------------------------
        while True:
            survivors: list[tuple[str, Extension, RuleDef, HookRegistration]] = []
            dropped_ext_some_rule = False
            # Build the set of extension names currently contributing to this hook.
            ext_has_rules_here = {name for name, _, _, _ in _enumerate(triples)}
            for ext_name, ext_obj, rule, reg in _enumerate(triples):
                ok = True
                for target in reg.after:
                    if target not in ext_has_rules_here:
                        ok = False; break
                if ok:
                    for target in reg.before:
                        if target not in ext_has_rules_here:
                            ok = False; break
                if ok:
                    survivors.append((ext_name, ext_obj, rule, reg))
                else:
                    dropped_ext_some_rule = True
            if not dropped_ext_some_rule:
                break
            triples = survivors
            if not triples:
                return

        # --- step 2: add implicit after-edges from `require` --------------------
        ext_has_rules_here = {name for name, _, _, _ in _enumerate(triples)}
        edges: list[tuple[int, int]] = []   # from i to j means rule[i] before rule[j]
        triple_list = list(_enumerate(triples))
        index_by_ext: dict[str, list[int]] = {}
        for idx, (name, _e, _r, _reg) in enumerate(triple_list):
            index_by_ext.setdefault(name, []).append(idx)

        for idx, (name, ext_obj, rule, reg) in enumerate(triple_list):
            # Explicit after: every dep-ext-rule at this hook → this rule.
            for target in reg.after:
                for src in index_by_ext.get(target, []):
                    edges.append((src, idx))
            # Explicit before: this rule → every target-ext-rule at this hook.
            for target in reg.before:
                for dst in index_by_ext.get(target, []):
                    edges.append((idx, dst))
            # Implicit after from require (only when dep has rules here and
            # there's no explicit constraint overriding it).
            for dep in ext_obj.requires:
                if dep not in ext_has_rules_here:
                    continue
                if dep in reg.before:
                    # author has flipped the default; don't add after edge.
                    continue
                for src in index_by_ext.get(dep, []):
                    if (src, idx) not in edges:
                        edges.append((src, idx))

        order = _topo_sort(len(triple_list), edges, triple_list)

        # --- step 5: execute ----------------------------------------------------
        for i in order:
            name, ext_obj, rule, reg = triple_list[i]
            interp = self._build_interpreter(ext_obj)
            try:
                interp.run_rule(rule.body, dict(context))
            except Exception as e:
                self.warnings.append(
                    f"extension {name!r} rule {rule.name!r} on {hook!r}: {e}"
                )

    def _gather_rules_for_hook(
        self, hook: str
    ) -> list[tuple[str, Extension, RuleDef, HookRegistration]]:
        out: list[tuple[str, Extension, RuleDef, HookRegistration]] = []
        for ext in self.extensions:
            for rule in ext.rules:
                for reg in rule.hooks:
                    if reg.hook == hook:
                        out.append((ext.name, ext, rule, reg))
        return out

    def _build_interpreter(self, ext: Extension) -> Interpreter:
        # Scope the dep view to this extension's declared requires only.
        dep_view: dict[str, dict[str, Any]] = {}
        for dep in ext.requires:
            dep_view[dep] = dict(self.per_ext_run_vars.get(dep, {}))
        # Per-extension config: merge .config defaults with lace.config
        # overrides (§2.3 + §11.1). Extension defaults are the base;
        # user overrides win for any key present in both.
        user_cfg = self.extension_config.get(ext.name, {}) \
            if isinstance(self.extension_config, dict) else {}
        if not isinstance(user_cfg, dict):
            user_cfg = {}
        user_cfg = {k: v for k, v in user_cfg.items() if k != "laceext"}
        ext_cfg = {**ext.config_defaults, **user_cfg}
        return Interpreter(
            extension_name=ext.name,
            functions=ext.function_specs(),
            tag_constructors=self.tag_constructors(),
            emit_callback=self._emit,
            config=ext_cfg,
            require_view=dep_view,
            qualified_call=self._invoke_exposed,
            requires=list(ext.requires),
        )

    # ── exposed function dispatch ────────────────────────────────────

    def _invoke_exposed(self, ext_name: str, fn_name: str, args: list[Any]) -> Any:
        """Dispatch `<ext>.<fn>(args)` from a calling rule.

        Per lace-extensions.md §6.1, exposed functions execute inside the
        OWNING extension's interpreter — emits, primitives, and its own
        internal functions are all resolved against the owner. This keeps
        `result.actions.*` and `result.runVars` entries attributed to the
        owner (so namespacing rules still hold).
        """
        owner = next((e for e in self.extensions if e.name == ext_name), None)
        if owner is None:
            raise RuntimeError(f"qualified call to unknown extension {ext_name!r}")
        fn = owner.functions.get(fn_name)
        if fn is None or not fn.exposed:
            raise RuntimeError(
                f"{ext_name}.{fn_name} is not an exposed function "
                f"(declare [functions.{fn_name}].exposed = true)"
            )
        owner_interp = self._build_interpreter(owner)
        return owner_interp._call_function(fn_name, list(args))

    # ── emit target routing ─────────────────────────────────────────

    def _emit(self, target: list[str], payload: dict[str, Any]) -> None:
        # target: ["result", "actions", "<key>"] or ["result", "runVars"]
        if target[:2] == ["result", "actions"] and len(target) == 3:
            key = target[2]
            self.actions.setdefault(key, []).append(payload)
            return
        if target == ["result", "runVars"]:
            self.ext_run_vars.update(payload)
            # Update the per-extension live view so dependents can read.
            for key, value in payload.items():
                owner = key.split(".", 1)[0]
                self.per_ext_run_vars.setdefault(owner, {})[key] = value
            return
        self.warnings.append(f"emit to disallowed target: {'.'.join(target)}")


def _enumerate(
    items: list[tuple[str, Extension, RuleDef, HookRegistration]]
) -> list[tuple[str, Extension, RuleDef, HookRegistration]]:
    return list(items)


def _topo_sort(
    n: int,
    edges: list[tuple[int, int]],
    nodes: list[tuple[str, Extension, RuleDef, HookRegistration]],
) -> list[int]:
    """Kahn's algorithm. Ties break per spec §8.1.1 step 6: declaration
    order within a file first, then extension name alphabetically."""
    indeg = [0] * n
    adj: list[list[int]] = [[] for _ in range(n)]
    for a, b in edges:
        adj[a].append(b)
        indeg[b] += 1

    # Initial roots — sorted for determinism.
    def sort_key(i: int) -> tuple[int, str]:
        # nodes[i] = (extension_name, ext_obj, rule, hook_reg)
        return (nodes[i][2].declaration_index, nodes[i][0])

    ready = sorted([i for i in range(n) if indeg[i] == 0], key=sort_key)
    out: list[int] = []
    while ready:
        i = ready.pop(0)
        out.append(i)
        for j in adj[i]:
            indeg[j] -= 1
            if indeg[j] == 0:
                # Insert preserving the sort_key order.
                ready.append(j)
                ready.sort(key=sort_key)
    if len(out) != n:
        stuck = [nodes[i] for i in range(n) if indeg[i] > 0]
        desc = ", ".join(f"{name}:{rule.name}" for name, _, rule, _ in stuck)
        raise RuntimeError(f"extension hook cycle among rules: {desc}")
    return out


# ── cross-extension function recursion check (spec §6.1) ───────────────

def _walk_call_targets(node: Any, owner: str, edges: set[tuple[tuple[str, str], tuple[str, str]]],
                       caller: tuple[str, str]) -> None:
    """Walk a function-body AST node, recording every (caller → callee)
    edge for both local `call` and `qualified_call` targets."""
    if isinstance(node, dict):
        kind = node.get("kind")
        if kind == "call":
            edges.add((caller, (owner, node["name"])))
        elif kind == "qualified_call":
            edges.add((caller, (node["ext"], node["name"])))
        for v in node.values():
            _walk_call_targets(v, owner, edges, caller)
    elif isinstance(node, list):
        for it in node:
            _walk_call_targets(it, owner, edges, caller)


def _check_cross_ext_recursion(extensions: list[Extension]) -> None:
    """Build the global function call graph across all loaded extensions
    (local + qualified_call edges) and reject any cycle."""
    edges: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    nodes: set[tuple[str, str]] = set()
    for ext in extensions:
        for fname, fdef in ext.functions.items():
            caller = (ext.name, fname)
            nodes.add(caller)
            _walk_call_targets(fdef.body, ext.name, edges, caller)
    # Adjacency list keyed by node.
    adj: dict[tuple[str, str], list[tuple[str, str]]] = {n: [] for n in nodes}
    for src, dst in edges:
        adj.setdefault(src, []).append(dst)
        adj.setdefault(dst, [])
    # DFS three-coloring.
    WHITE, GREY, BLACK = 0, 1, 2
    color = {n: WHITE for n in adj}

    def visit(n: tuple[str, str], stack: list[tuple[str, str]]) -> None:
        color[n] = GREY
        stack.append(n)
        for m in adj[n]:
            if color[m] == GREY:
                cycle = stack[stack.index(m):] + [m]
                path = " → ".join(f"{e}.{f}" for e, f in cycle)
                raise RuntimeError(f"function call cycle: {path}")
            if color[m] == WHITE:
                visit(m, stack)
        stack.pop()
        color[n] = BLACK

    for n in list(adj.keys()):
        if color[n] == WHITE:
            visit(n, [])
