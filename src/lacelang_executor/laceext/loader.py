"""Loader for `.laceext` TOML files (lace-extensions.md §2).

Extracts:
    - [extension] metadata (name, version)
    - [schema.*] field registrations (passed to validator)
    - [types.*] custom type declarations — used to derive tag-constructor
      functions for one_of types
    - [result.*] result-shape additions
    - [functions.*] DSL function bodies (parsed at load time)
    - [[rules.rule]] rule definitions with `on` hook list and body

Parses all DSL bodies eagerly so errors surface at load, not at dispatch.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]

from lacelang_executor.laceext.dsl_parser import parse_function_body, parse_rule_body


# Valid hook names per lace-extensions.md §8
HOOK_NAMES: frozenset[str] = frozenset({
    "before script", "script",
    "before call", "call",
    "before expect", "expect",
    "before check", "check",
    "before assert", "assert",
    "before store", "store",
})


@dataclass
class HookRegistration:
    """One `on` entry on a rule, after parsing ordering qualifiers."""
    hook: str
    after: list[str]   # extension names this rule must fire after on this hook
    before: list[str]  # extension names this rule must fire before on this hook


@dataclass
class RuleDef:
    name: str
    hooks: list[HookRegistration]
    body: list[dict[str, Any]]
    # Position within the source `[[rules.rule]]` array; used by the
    # registry's topo-sort tie-break (spec §8.1.1 step 6: declaration
    # order within a file, then extension name alphabetically).
    declaration_index: int = 0

    def hook_names(self) -> list[str]:
        return [h.hook for h in self.hooks]


@dataclass
class FunctionDef:
    name: str
    params: list[str]
    body: list[dict[str, Any]]
    # When True, the function is callable from rule bodies in OTHER
    # extensions that `require` this one via `{ext_name}.{fn_name}(...)`
    # syntax. See lace-extensions.md §6.1 (exposed functions).
    exposed: bool = False


@dataclass
class OneOfType:
    """A custom type declared via `one_of` — produces N tag constructors."""
    name: str
    variants: list[dict[str, Any]]   # [{tag, fields}, ...]


@dataclass
class Extension:
    name: str
    version: str
    path: str | None = None
    requires: list[str] = field(default_factory=list)
    schema: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] = field(default_factory=dict)
    functions: dict[str, FunctionDef] = field(default_factory=dict)
    one_of_types: dict[str, OneOfType] = field(default_factory=dict)
    rules: list[RuleDef] = field(default_factory=list)
    config_defaults: dict[str, Any] = field(default_factory=dict)

    def tag_constructors(self) -> dict[str, Callable[[list[Any]], Any]]:
        """Build `text(...)` / `template(...)` / `op_map(...)` style
        constructors from declared one_of types. Each tag becomes a function
        that accepts positional args matching the tag's declared field order
        and returns a tagged dict suitable for notification_val etc."""
        out: dict[str, Callable[[list[Any]], Any]] = {}
        for t in self.one_of_types.values():
            for variant in t.variants:
                tag = variant["tag"]
                fields = variant.get("fields") or {}
                field_names = list(fields.keys())
                out[tag] = _make_tag_ctor(tag, field_names)
        return out

    def function_specs(self) -> dict[str, dict[str, Any]]:
        return {n: {"params": f.params, "body": f.body, "exposed": f.exposed}
                for n, f in self.functions.items()}

    def exposed_function_specs(self) -> dict[str, dict[str, Any]]:
        """Subset of function_specs marked `exposed = true`. Extensions that
        `require` this one can call these via qualified name (§6.1)."""
        return {n: {"params": f.params, "body": f.body, "exposed": True}
                for n, f in self.functions.items() if f.exposed}


def _make_tag_ctor(tag: str, field_names: list[str]) -> Callable[[list[Any]], Any]:
    def ctor(args: list[Any]) -> dict[str, Any]:
        out: dict[str, Any] = {"tag": tag}
        for i, fname in enumerate(field_names):
            out[fname] = args[i] if i < len(args) else None
        return out
    ctor.__name__ = f"tag_ctor_{tag}"
    return ctor


def load_extension(path: str | Path) -> Extension:
    text = Path(path).read_bytes()
    doc = tomllib.loads(text.decode("utf-8"))

    # Spec §13.1 item 5: warn (don't fail) on unrecognised top-level sections
    # in `.laceext` files. Helps catch typos like `[function.foo]` early
    # without breaking forward-compatibility for future spec additions.
    _known_top_level = {"extension", "schema", "result", "types", "functions", "rules"}
    for key in doc.keys():
        if key not in _known_top_level:
            print(
                f"warning: {path}: unknown top-level section [{key}] "
                f"(known: {', '.join(sorted(_known_top_level))})",
                file=sys.stderr,
            )

    meta = doc.get("extension") or {}
    name = meta.get("name")
    version = meta.get("version", "0.0.0")
    if not name:
        raise ValueError(f"{path}: [extension].name is required")
    # Extension names must be camelCase — `[a-z][A-Za-z0-9]*`. No
    # hyphens, underscores, or other punctuation. Makes `extName.fnName(...)`
    # qualified calls parseable as a single IDENT on each side of the dot.
    import re as _re
    if not _re.fullmatch(r"[a-z][A-Za-z0-9]*", name):
        raise ValueError(
            f"{path}: [extension].name must match [a-z][A-Za-z0-9]* "
            f"(camelCase), got {name!r}"
        )

    ext = Extension(name=name, version=version, path=str(path))

    # Optional [extension].require = [...] — see lace-extensions.md §2.2.
    req = meta.get("require")
    if req is not None:
        if not isinstance(req, list) or not all(isinstance(r, str) for r in req):
            raise ValueError(f"{path}: [extension].require must be an array of strings")
        ext.requires = list(req)

    # Schema additions are opaque to the executor — passed through to validator.
    if isinstance(doc.get("schema"), dict):
        ext.schema = doc["schema"]

    # Result additions (also opaque here; mainly used for shape validation).
    if isinstance(doc.get("result"), dict):
        ext.result = doc["result"]

    # Custom types — harvest one_of entries into tag-constructor sources.
    types_section = doc.get("types") or {}
    for tname, tdef in types_section.items():
        if isinstance(tdef, dict) and isinstance(tdef.get("one_of"), list):
            variants = []
            for v in tdef["one_of"]:
                if not isinstance(v, dict) or "tag" not in v:
                    continue
                variants.append({"tag": v["tag"], "fields": v.get("fields") or {}})
            ext.one_of_types[tname] = OneOfType(name=tname, variants=variants)

    # Functions: parse body text now so errors are reported at load.
    funcs_section = doc.get("functions") or {}
    for fname, fdef in funcs_section.items():
        if not isinstance(fdef, dict):
            continue
        params = list(fdef.get("params") or [])
        body_text = fdef.get("body") or ""
        exposed = bool(fdef.get("exposed", False))
        try:
            body_ast = parse_function_body(body_text)
        except Exception as e:
            raise ValueError(f"{path}: error parsing function {fname!r}: {e}") from e
        # Spec §6 + §13.6 safety checks on function bodies.
        # 1. `exit` is a rule-only construct — functions return null for
        #    early termination. Reject any reachable exit node.
        if _body_contains_kind(body_ast, "exit"):
            raise ValueError(
                f"{path}: function {fname!r} contains an 'exit' statement; "
                f"exit is only valid in rule bodies (use 'return null' to "
                f"early-exit a function)"
            )
        # 2. `emit` is only valid inside rule bodies — OR inside an
        #    exposed function body (the function emits on behalf of the
        #    owning extension when invoked from a dependent — §6.1).
        if not exposed and _body_contains_kind(body_ast, "emit"):
            raise ValueError(
                f"{path}: function {fname!r} contains an 'emit' statement "
                f"but is not exposed; declare [functions.{fname}].exposed = "
                f"true to allow emit on behalf of this extension"
            )
        ext.functions[fname] = FunctionDef(name=fname, params=params,
                                           body=body_ast, exposed=exposed)

    # Spec §6 safety check: detect direct/indirect recursion in the function
    # call graph for THIS extension. Only `kind == "call"` (local calls) form
    # the graph — `qualified_call` nodes are cross-extension and cannot
    # produce a cycle within a single load.
    _check_no_recursion(path, ext.functions)

    # Rules: TOML array-of-tables at [[rules.rule]]
    rules_section = doc.get("rules") or {}
    rule_list = rules_section.get("rule") or []
    if not isinstance(rule_list, list):
        rule_list = [rule_list]
    for decl_idx, rdef in enumerate(rule_list):
        if not isinstance(rdef, dict):
            continue
        rname = rdef.get("name", "<unnamed>")
        raw_hooks = rdef.get("on") or []
        if isinstance(raw_hooks, str):
            raw_hooks = [raw_hooks]
        parsed_hooks: list[HookRegistration] = []
        for entry in raw_hooks:
            try:
                parsed_hooks.append(_parse_on_entry(entry))
            except ValueError as e:
                raise ValueError(f"{path}: rule {rname!r}: {e}") from e
        body_text = rdef.get("body") or ""
        try:
            body_ast = parse_rule_body(body_text)
        except Exception as e:
            raise ValueError(f"{path}: error parsing rule {rname!r}: {e}") from e
        ext.rules.append(RuleDef(name=rname, hooks=parsed_hooks, body=body_ast,
                                 declaration_index=decl_idx))

    # Sibling config file: {extName}.config provides default values for
    # config fields accessible via `config.*` in rule bodies (§2.3).
    ext.config_defaults = _load_config_defaults(path, ext.name, ext.version)

    return ext


def _load_config_defaults(
    laceext_path: str | Path, ext_name: str, ext_version: str,
) -> dict[str, Any]:
    """Load the sibling `{extName}.config` file if it exists.

    Returns the `[config]` section as a dict, or {} if the file is absent.
    Warns on name/version mismatch but does not fail.
    """
    p = Path(laceext_path)
    config_path = p.parent / f"{ext_name}.config"
    if not config_path.is_file():
        return {}
    try:
        doc = tomllib.loads(config_path.read_bytes().decode("utf-8"))
    except Exception as e:
        raise ValueError(
            f"{config_path}: failed to parse extension config: {e}"
        ) from e
    meta = doc.get("extension") or {}
    cfg_name = meta.get("name")
    cfg_version = meta.get("version")
    if cfg_name and cfg_name != ext_name:
        print(
            f"warning: {config_path}: config name {cfg_name!r} does not match "
            f"extension name {ext_name!r}",
            file=sys.stderr,
        )
    if cfg_version and cfg_version != ext_version:
        print(
            f"warning: {config_path}: config version {cfg_version!r} does not "
            f"match extension version {ext_version!r}",
            file=sys.stderr,
        )
    return dict(doc.get("config") or {})


def _walk_nodes(node: Any) -> Any:
    """Yield every dict node in the AST (statements + expressions)."""
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk_nodes(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_nodes(item)


def _body_contains_kind(body: list[dict[str, Any]], kind: str) -> bool:
    for n in _walk_nodes(body):
        if isinstance(n, dict) and n.get("kind") == kind:
            return True
    return False


def _check_no_recursion(path: str | Path, functions: dict[str, FunctionDef]) -> None:
    """Walk each function's AST collecting local call targets, then DFS for
    cycles. Raises ValueError naming the cycle path on detection."""
    graph: dict[str, set[str]] = {}
    for fname, fdef in functions.items():
        targets: set[str] = set()
        for n in _walk_nodes(fdef.body):
            if isinstance(n, dict) and n.get("kind") == "call":
                target = n.get("name")
                if isinstance(target, str) and target in functions:
                    targets.add(target)
        graph[fname] = targets

    WHITE, GREY, BLACK = 0, 1, 2
    color: dict[str, int] = {f: WHITE for f in graph}
    stack: list[str] = []

    def dfs(u: str) -> None:
        color[u] = GREY
        stack.append(u)
        for v in graph.get(u, ()):
            if color.get(v) == GREY:
                # Cycle — extract path from first occurrence of v in stack.
                idx = stack.index(v)
                cycle = stack[idx:] + [v]
                raise ValueError(
                    f"{path}: function recursion detected: "
                    f"{' -> '.join(cycle)} (recursion is forbidden per spec §6)"
                )
            if color.get(v) == WHITE:
                dfs(v)
        stack.pop()
        color[u] = BLACK

    for fname in graph:
        if color[fname] == WHITE:
            dfs(fname)


def _parse_on_entry(entry: str) -> HookRegistration:
    """Parse `<hook> (('after' | 'before') <ext>)*` into a HookRegistration.

    Hook names may contain a space (e.g. `before call`), so we greedy-match
    against the known hook set before consuming qualifier tokens.
    """
    if not isinstance(entry, str):
        raise ValueError(f"on-entry must be string, got {type(entry).__name__}")
    entry = entry.strip()
    # Split on whitespace; recombine the hook prefix which may span two tokens.
    tokens = entry.split()
    if not tokens:
        raise ValueError("empty on-entry")
    hook: str | None = None
    if len(tokens) >= 2 and " ".join(tokens[:2]) in HOOK_NAMES:
        hook = " ".join(tokens[:2])
        tokens = tokens[2:]
    elif tokens[0] in HOOK_NAMES:
        hook = tokens[0]
        tokens = tokens[1:]
    else:
        raise ValueError(f"unknown hook in on-entry {entry!r}")
    after: list[str] = []
    before: list[str] = []
    i = 0
    while i < len(tokens):
        kw = tokens[i]
        if kw not in ("after", "before"):
            raise ValueError(f"expected 'after' or 'before' in on-entry {entry!r}, got {kw!r}")
        if i + 1 >= len(tokens):
            raise ValueError(f"dangling qualifier in on-entry {entry!r}")
        ext_name = tokens[i + 1]
        if kw == "after":
            after.append(ext_name)
        else:
            before.append(ext_name)
        i += 2
    return HookRegistration(hook=hook, after=after, before=before)
