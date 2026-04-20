"""Parser for the .laceext rule body DSL (lace-extensions.md §5.1).

Produces a tree of typed dict nodes. Precedence (lowest to highest):

    ternary (? :) < or < and < eq/neq < lt/lte/gt/gte < add/sub < mul/div
    < unary (not, -) < primary
"""

from __future__ import annotations

from typing import Any

from lacelang_executor.laceext.dsl_lexer import Token, tokenize


class DSLParseError(Exception):
    def __init__(self, msg: str, line: int):
        super().__init__(f"line {line}: {msg}")
        self.line = line


class DSLParser:
    def __init__(self, tokens: list[Token], in_function: bool = False):
        self.toks = tokens
        self.pos = 0
        self.in_function = in_function

    @property
    def tok(self) -> Token:
        return self.toks[self.pos]

    def _peek(self, off: int = 0) -> Token:
        return self.toks[self.pos + off]

    def _advance(self) -> Token:
        t = self.toks[self.pos]
        if self.pos < len(self.toks) - 1:
            self.pos += 1
        return t

    def _check(self, *types: str) -> bool:
        return self.tok.type in types

    def _match(self, *types: str) -> Token | None:
        if self.tok.type in types:
            return self._advance()
        return None

    def _expect(self, ttype: str) -> Token:
        if self.tok.type != ttype:
            raise DSLParseError(f"expected {ttype}, got {self.tok.type!r} ({self.tok.value!r})",
                                self.tok.line)
        return self._advance()

    # ── program ──────────────────────────────────────────────────────

    def parse_body(self) -> list[dict[str, Any]]:
        """Parse a sequence of statements until EOF / DEDENT."""
        stmts: list[dict[str, Any]] = []
        while not self._check("EOF", "DEDENT"):
            if self._check("NEWLINE"):
                self._advance()
                continue
            stmts.append(self._parse_statement())
        return stmts

    def _parse_statement(self) -> dict[str, Any]:
        t = self.tok
        if t.type == "KW_FOR":    return self._parse_for()
        if t.type == "KW_WHEN":   return self._parse_when()
        if t.type == "KW_LET":    return self._parse_let()
        if t.type == "KW_SET":    return self._parse_set()
        if t.type == "KW_EMIT":   return self._parse_emit()
        if t.type == "KW_EXIT":
            self._advance()
            self._expect_stmt_end()
            return {"kind": "exit", "line": t.line}
        if t.type == "KW_RETURN":
            if not self.in_function:
                raise DSLParseError("return is only valid in function bodies", t.line)
            self._advance()
            e = self._parse_expr()
            self._expect_stmt_end()
            return {"kind": "return", "expr": e, "line": t.line}
        # fn_call_stmt: plain `IDENT "(" ... ")"` OR qualified
        # `ext-name.fn-name("(" ... ")"` for exposed inter-extension calls.
        if t.type == "IDENT":
            call = self._parse_func_call_expr()
            self._expect_stmt_end()
            return {"kind": "call_stmt", "call": call, "line": t.line}
        raise DSLParseError(f"unexpected token at statement start: {t.type} {t.value!r}", t.line)

    def _expect_stmt_end(self) -> None:
        if self._check("NEWLINE"):
            self._advance()
            return
        if self._check("EOF", "DEDENT"):
            return
        raise DSLParseError(f"expected end of statement, got {self.tok.type}", self.tok.line)

    # ── for / when / let / emit ─────────────────────────────────────

    def _parse_for(self) -> dict[str, Any]:
        start = self._advance()  # for
        binding = self._expect("BINDING").value
        self._expect("KW_IN")
        iter_expr = self._parse_expr()
        self._expect("COLON")
        self._expect("NEWLINE")
        self._expect("INDENT")
        body = self.parse_body()
        self._expect("DEDENT")
        return {"kind": "for", "binding": binding, "iter": iter_expr,
                "body": body, "line": start.line}

    def _parse_when(self) -> dict[str, Any]:
        start = self._advance()  # when
        cond = self._parse_expr()
        if self._match("COLON"):
            self._expect("NEWLINE")
            self._expect("INDENT")
            body = self.parse_body()
            self._expect("DEDENT")
            return {"kind": "when_block", "cond": cond, "body": body, "line": start.line}
        self._expect_stmt_end()
        return {"kind": "when_inline", "cond": cond, "line": start.line}

    def _parse_let(self) -> dict[str, Any]:
        start = self._advance()  # let
        name = self._expect("BINDING").value
        self._expect("EQ")
        expr = self._parse_expr()
        self._expect_stmt_end()
        return {"kind": "let", "name": name, "expr": expr, "line": start.line}

    def _parse_set(self) -> dict[str, Any]:
        start = self._advance()  # set
        if not self.in_function:
            raise DSLParseError("set is only valid in function bodies; rule bindings are immutable", start.line)
        name = self._expect("BINDING").value
        self._expect("EQ")
        expr = self._parse_expr()
        self._expect_stmt_end()
        return {"kind": "set", "name": name, "expr": expr, "line": start.line}

    def _parse_emit(self) -> dict[str, Any]:
        start = self._advance()  # emit
        # Target: result.IDENT(.IDENT)*
        self._expect("KW_RESULT")
        path: list[str] = ["result"]
        while self._match("DOT"):
            path.append(self._expect("IDENT").value)
        self._expect("ARROW")
        self._expect("LBRACE")
        fields: list[dict[str, Any]] = []
        if not self._check("RBRACE"):
            while True:
                key_tok = self.tok
                if key_tok.type == "STRING":
                    self._advance()
                    key = key_tok.value
                elif key_tok.type == "IDENT":
                    self._advance()
                    key = key_tok.value
                else:
                    raise DSLParseError(f"expected field key, got {key_tok.type}", key_tok.line)
                self._expect("COLON")
                val = self._parse_expr()
                fields.append({"key": key, "value": val})
                if not self._match("COMMA"):
                    break
                if self._check("RBRACE"):
                    break
        self._expect("RBRACE")
        self._expect_stmt_end()
        return {"kind": "emit", "target": path, "fields": fields, "line": start.line}

    # ── expressions (precedence climb) ───────────────────────────────

    def _parse_expr(self) -> dict[str, Any]:
        cond = self._parse_or()
        if self._match("QUESTION"):
            then = self._parse_expr()
            self._expect("COLON")
            else_ = self._parse_expr()
            return {"kind": "ternary", "cond": cond, "then": then, "else": else_}
        return cond

    def _parse_or(self) -> dict[str, Any]:
        left = self._parse_and()
        while self._check("KW_OR"):
            self._advance()
            right = self._parse_and()
            left = {"kind": "binop", "op": "or", "left": left, "right": right}
        return left

    def _parse_and(self) -> dict[str, Any]:
        left = self._parse_eq()
        while self._check("KW_AND"):
            self._advance()
            right = self._parse_eq()
            left = {"kind": "binop", "op": "and", "left": left, "right": right}
        return left

    def _parse_eq(self) -> dict[str, Any]:
        left = self._parse_ord()
        if self._check("KW_EQ", "KW_NEQ"):
            op_tok = self._advance()
            op = op_tok.value  # "eq" | "neq"
            right = self._parse_ord()
            left = {"kind": "binop", "op": op, "left": left, "right": right}
            # Spec §5.3: comparisons do not chain.
            if self._check("KW_EQ", "KW_NEQ"):
                raise DSLParseError(
                    "chained comparison: comparisons do not associate; "
                    "use 'and'/'or' with parentheses to combine",
                    self.tok.line,
                )
        return left

    def _parse_ord(self) -> dict[str, Any]:
        left = self._parse_addsub()
        if self._check("KW_LT", "KW_LTE", "KW_GT", "KW_GTE"):
            op_tok = self._advance()
            op = op_tok.value  # "lt" | "lte" | "gt" | "gte"
            right = self._parse_addsub()
            left = {"kind": "binop", "op": op, "left": left, "right": right}
            # Spec §5.3: comparisons do not chain.
            if self._check("KW_LT", "KW_LTE", "KW_GT", "KW_GTE"):
                raise DSLParseError(
                    "chained comparison: comparisons do not associate; "
                    "use 'and'/'or' with parentheses to combine",
                    self.tok.line,
                )
        return left

    def _parse_addsub(self) -> dict[str, Any]:
        left = self._parse_muldiv()
        while self._check("PLUS", "MINUS"):
            op = self._advance().value
            right = self._parse_muldiv()
            left = {"kind": "binop", "op": op, "left": left, "right": right}
        return left

    def _parse_muldiv(self) -> dict[str, Any]:
        left = self._parse_unary()
        while self._check("STAR", "SLASH"):
            op = self._advance().value
            right = self._parse_unary()
            left = {"kind": "binop", "op": op, "left": left, "right": right}
        return left

    def _parse_unary(self) -> dict[str, Any]:
        if self._check("KW_NOT"):
            self._advance()
            return {"kind": "unop", "op": "not", "operand": self._parse_unary()}
        if self._check("MINUS"):
            self._advance()
            return {"kind": "unop", "op": "-", "operand": self._parse_unary()}
        return self._parse_access()

    def _parse_access(self) -> dict[str, Any]:
        base = self._parse_primary()
        while True:
            if self._match("DOT") or self._match("QDOT"):
                # Field names may coincide with DSL keywords (e.g.
                # `call.config.timeout?.notification`). Accept any ident-like
                # token after `.` or `?.`.
                tok = self.tok
                if tok.type == "IDENT" or tok.type.startswith("KW_"):
                    self._advance()
                    name = tok.value
                else:
                    raise DSLParseError(f"expected field name, got {tok.type}", tok.line)
                base = {"kind": "access_field", "base": base, "name": name}
            elif self._match("LBRACK"):
                idx = self._parse_expr()
                self._expect("RBRACK")
                base = {"kind": "access_index", "base": base, "index": idx}
            elif self._match("QBRACK"):
                cond = self._parse_expr()
                self._expect("RBRACK")
                base = {"kind": "access_filter", "base": base, "cond": cond}
            else:
                break
        return base

    def _parse_primary(self) -> dict[str, Any]:
        t = self.tok
        if t.type == "LPAREN":
            self._advance()
            e = self._parse_expr()
            self._expect("RPAREN")
            return e
        if t.type == "LBRACE":
            # Object literal in expression position. Same shape as the
            # `{k: v, ...}` payload used by `emit` — needed so exposed
            # functions can accept a structured event argument.
            return self._parse_object_lit()
        if t.type == "STRING":
            self._advance()
            return {"kind": "literal", "valueType": "string", "value": t.value}
        if t.type == "INT":
            self._advance()
            return {"kind": "literal", "valueType": "int", "value": int(t.value)}
        if t.type == "FLOAT":
            self._advance()
            return {"kind": "literal", "valueType": "float", "value": float(t.value)}
        if t.type == "KW_TRUE":
            self._advance()
            return {"kind": "literal", "valueType": "bool", "value": True}
        if t.type == "KW_FALSE":
            self._advance()
            return {"kind": "literal", "valueType": "bool", "value": False}
        if t.type == "KW_NULL":
            self._advance()
            return {"kind": "literal", "valueType": "null", "value": None}
        if t.type == "KW_RESULT":
            self._advance()
            return {"kind": "base", "name": "result"}
        if t.type == "KW_PREV":
            self._advance()
            return {"kind": "base", "name": "prev"}
        if t.type == "KW_THIS":
            self._advance()
            return {"kind": "base", "name": "this"}
        if t.type == "KW_CONFIG":
            self._advance()
            return {"kind": "base", "name": "config"}
        if t.type == "KW_REQUIRE":
            self._advance()
            return {"kind": "base", "name": "require"}
        if t.type == "BINDING":
            self._advance()
            return {"kind": "binding", "name": t.value}
        if t.type == "IDENT":
            if self._peek(1).type == "LPAREN":
                return self._parse_func_call_expr()
            # Lookahead for qualified form `ext-name.fn-name(...)` — any
            # contiguous IDENT-MINUS-IDENT-... sequence followed by DOT
            # IDENT [MINUS IDENT]* LPAREN.
            if self._looks_like_qualified_call():
                return self._parse_func_call_expr()
            self._advance()
            return {"kind": "ident", "name": t.value}
        raise DSLParseError(f"unexpected token in expression: {t.type} {t.value!r}", t.line)

    def _parse_func_call_expr(self) -> dict[str, Any]:
        # Extension names are camelCase IDENT, so the qualified form
        # is simply `IDENT "." IDENT "(" ... ")"`. The leading IDENT is a
        # plain local function call when no DOT follows.
        head_tok = self._expect("IDENT")
        head = head_tok.value

        qualified: str | None = None
        if self._check("DOT") and self._peek(1).type == "IDENT" \
                and self._peek(2).type == "LPAREN":
            self._advance()  # DOT
            qualified = self._advance().value  # IDENT

        self._expect("LPAREN")
        args: list[dict[str, Any]] = []
        if not self._check("RPAREN"):
            while True:
                args.append(self._parse_expr())
                if not self._match("COMMA"):
                    break
                if self._check("RPAREN"):
                    break
        self._expect("RPAREN")
        if qualified is not None:
            return {"kind": "qualified_call", "ext": head, "name": qualified,
                    "args": args, "line": head_tok.line}
        return {"kind": "call", "name": head, "args": args, "line": head_tok.line}

    def _parse_object_lit(self) -> dict[str, Any]:
        start = self._expect("LBRACE")
        fields: list[dict[str, Any]] = []
        if not self._check("RBRACE"):
            while True:
                key_tok = self.tok
                if key_tok.type == "STRING":
                    self._advance()
                    key = key_tok.value
                elif key_tok.type == "IDENT":
                    self._advance()
                    key = key_tok.value
                else:
                    raise DSLParseError(
                        f"expected object-literal key, got {key_tok.type}",
                        key_tok.line,
                    )
                self._expect("COLON")
                val = self._parse_expr()
                fields.append({"key": key, "value": val})
                if not self._match("COMMA"):
                    break
                if self._check("RBRACE"):
                    break
        self._expect("RBRACE")
        return {"kind": "object_lit", "fields": fields, "line": start.line}

    def _looks_like_qualified_call(self) -> bool:
        """Lookahead to detect `ExtName.fnName(` at the current position.
        With camelCase extension names the form is simply
        IDENT DOT IDENT LPAREN."""
        i = self.pos
        return (i + 3 < len(self.toks)
                and self.toks[i].type == "IDENT"
                and self.toks[i + 1].type == "DOT"
                and self.toks[i + 2].type == "IDENT"
                and self.toks[i + 3].type == "LPAREN")


def _expand_inline_whens(src: str) -> str:
    """Rewrite inline `when X` guards to block form `when X: <indented>`.

    The reference extension (lace-extensions.md §12) relies on the pattern
    where a `when X` guards the statements that follow until a blank line.
    Under block form the parser's existing when-block semantics apply:
    skip the body when the guard is false, continue afterwards.

    Transformation:
        when X
        STMT_A
        STMT_B

        STMT_C

    becomes:
        when X:
            STMT_A
            STMT_B

        STMT_C

    Chained inline guards nest naturally via repeated application:
        when X
        when Y
        STMT
    →   when X:
            when Y:
                STMT
    """
    lines = src.split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()
        # Detect inline `when EXPR` (no trailing `:` means it's inline).
        if (stripped.startswith("when ") and not stripped.rstrip().endswith(":")
                and not stripped.startswith("when:")):
            indent = line[: len(line) - len(stripped)]
            # Peek ahead: gather following non-blank lines at >= same indent.
            body_lines: list[str] = []
            j = i + 1
            while j < len(lines):
                nxt = lines[j]
                if nxt.strip() == "":
                    break
                nxt_indent = len(nxt) - len(nxt.lstrip())
                if nxt_indent < len(indent):
                    break
                body_lines.append(nxt)
                j += 1
            if not body_lines:
                # Lonely `when` with no following body — emit the block form
                # with an explicit no-op so it parses cleanly.
                out.append(line.rstrip() + ":")
                out.append(indent + "    exit")
                i += 1
                continue
            # Emit block form.
            out.append(line.rstrip() + ":")
            extra = "    "
            # Recursively expand the body (to handle chained whens).
            body_src = "\n".join(body_lines)
            expanded = _expand_inline_whens(body_src)
            for bl in expanded.split("\n"):
                out.append(indent + extra + bl if bl.strip() else bl)
            i = j
            continue
        out.append(line)
        i += 1
    return "\n".join(out)


def parse_rule_body(src: str) -> list[dict[str, Any]]:
    src = _expand_inline_whens(src)
    tokens = tokenize(src)
    return DSLParser(tokens, in_function=False).parse_body()


def parse_function_body(src: str) -> list[dict[str, Any]]:
    src = _expand_inline_whens(src)
    tokens = tokenize(src)
    return DSLParser(tokens, in_function=True).parse_body()
