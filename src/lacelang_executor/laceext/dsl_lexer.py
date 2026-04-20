"""Lexer for the .laceext rule body DSL (lace-extensions.md §5.1).

Python-style indentation-sensitive. Emits INDENT / DEDENT tokens at
block boundaries. Comments start with `#` and run to end-of-line.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TokenType = Literal[
    "IDENT", "STRING", "INT", "FLOAT", "BINDING",
    "NEWLINE", "INDENT", "DEDENT",
    "LPAREN", "RPAREN", "LBRACK", "RBRACK", "LBRACE", "RBRACE",
    "COMMA", "COLON", "DOT", "QDOT", "QBRACK", "ARROW",
    "EQ",  # `==` — let-binding assignment only; comparisons use keywords (§5.1)
    "PLUS", "MINUS", "STAR", "SLASH",
    "QUESTION",
    # keywords
    "KW_FOR", "KW_IN", "KW_WHEN", "KW_LET", "KW_SET", "KW_EMIT",
    "KW_EXIT", "KW_RETURN",
    "KW_AND", "KW_OR", "KW_NOT",
    "KW_TRUE", "KW_FALSE", "KW_NULL",
    "KW_RESULT", "KW_PREV", "KW_THIS", "KW_CONFIG", "KW_REQUIRE",
    # comparison operator keywords
    "KW_EQ", "KW_NEQ", "KW_LT", "KW_LTE", "KW_GT", "KW_GTE",
    "EOF",
]

KEYWORDS: dict[str, TokenType] = {
    "for":    "KW_FOR",
    "in":     "KW_IN",
    "when":   "KW_WHEN",
    "let":    "KW_LET",
    "set":    "KW_SET",
    "emit":   "KW_EMIT",
    "exit":   "KW_EXIT",
    "return": "KW_RETURN",
    "and":    "KW_AND",
    "or":     "KW_OR",
    "not":    "KW_NOT",
    "true":   "KW_TRUE",
    "false":  "KW_FALSE",
    "null":   "KW_NULL",
    "result":  "KW_RESULT",
    "prev":    "KW_PREV",
    "this":    "KW_THIS",
    "config":  "KW_CONFIG",
    "require": "KW_REQUIRE",
    # comparison operators as keywords
    "eq":      "KW_EQ",
    "neq":     "KW_NEQ",
    "lt":      "KW_LT",
    "lte":     "KW_LTE",
    "gt":      "KW_GT",
    "gte":     "KW_GTE",
}


@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    col: int


class DSLLexError(Exception):
    def __init__(self, msg: str, line: int, col: int):
        super().__init__(f"{msg} (line {line}, col {col})")
        self.line = line
        self.col = col


class DSLLexer:
    def __init__(self, source: str):
        self.src = source
        self.pos = 0
        self.line = 1
        self.col = 1
        self.indent_stack: list[int] = [0]
        # Track paren depth so newlines inside (...) / [...] / {...} don't emit
        # NEWLINE / INDENT / DEDENT — DSL grammar is only line-sensitive at the
        # top level.
        self.paren_depth = 0
        self.tokens: list[Token] = []
        self.at_line_start = True

    def _peek(self, off: int = 0) -> str:
        p = self.pos + off
        return self.src[p] if p < len(self.src) else ""

    def _advance(self, n: int = 1) -> str:
        chunk = self.src[self.pos : self.pos + n]
        for ch in chunk:
            if ch == "\n":
                self.line += 1
                self.col = 1
            else:
                self.col += 1
        self.pos += n
        return chunk

    def _handle_line_start(self) -> None:
        """Emit INDENT/DEDENT for the next non-blank line."""
        # Skip blank lines / comment-only lines without emitting indent markers.
        while True:
            # Count leading spaces / tabs at current position.
            indent = 0
            scan = self.pos
            while scan < len(self.src) and self.src[scan] in " \t":
                indent += 1
                scan += 1
            if scan >= len(self.src):
                # EOF reached — let main loop handle.
                return
            ch = self.src[scan]
            if ch == "\n":
                # Blank line — consume and retry.
                self._advance(scan - self.pos + 1)
                continue
            if ch == "#":
                # Comment line — skip rest of the line and retry.
                while scan < len(self.src) and self.src[scan] != "\n":
                    scan += 1
                self._advance(scan - self.pos)
                if self.pos < len(self.src) and self._peek() == "\n":
                    self._advance()
                continue
            break
        # advance to the first real column
        self._advance(indent)
        cur_indent = self.indent_stack[-1]
        if indent > cur_indent:
            self.indent_stack.append(indent)
            self.tokens.append(Token("INDENT", "", self.line, 1))
        elif indent < cur_indent:
            while self.indent_stack and self.indent_stack[-1] > indent:
                self.indent_stack.pop()
                self.tokens.append(Token("DEDENT", "", self.line, 1))
            if self.indent_stack[-1] != indent:
                raise DSLLexError("inconsistent indentation", self.line, 1)
        self.at_line_start = False

    def _lex_ident(self) -> Token:
        start_line, start_col = self.line, self.col
        start = self.pos
        while self.pos < len(self.src) and (self._peek().isalnum() or self._peek() == "_"):
            self._advance()
        text = self.src[start : self.pos]
        kw = KEYWORDS.get(text)
        return Token(kw, text, start_line, start_col) if kw else Token("IDENT", text, start_line, start_col)

    def _lex_binding(self) -> Token:
        start_line, start_col = self.line, self.col
        self._advance()  # $
        # Bare `$` (not followed by an identifier char) is the array-filter
        # current-element reference used inside `[? cond]` expressions.
        if not (self._peek().isalpha() or self._peek() == "_"):
            return Token("BINDING", "$", start_line, start_col)
        start = self.pos
        while self.pos < len(self.src) and (self._peek().isalnum() or self._peek() == "_"):
            self._advance()
        return Token("BINDING", self.src[start : self.pos], start_line, start_col)

    def _lex_number(self) -> Token:
        start_line, start_col = self.line, self.col
        start = self.pos
        while self.pos < len(self.src) and self._peek().isdigit():
            self._advance()
        if self._peek() == "." and self._peek(1).isdigit():
            self._advance()
            while self.pos < len(self.src) and self._peek().isdigit():
                self._advance()
            return Token("FLOAT", self.src[start : self.pos], start_line, start_col)
        return Token("INT", self.src[start : self.pos], start_line, start_col)

    def _lex_string(self) -> Token:
        start_line, start_col = self.line, self.col
        quote = self._peek()
        self._advance()
        chars: list[str] = []
        while self.pos < len(self.src):
            ch = self._peek()
            if ch == quote:
                self._advance()
                return Token("STRING", "".join(chars), start_line, start_col)
            if ch == "\\":
                nxt = self._peek(1)
                mapping = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'"}
                if nxt in mapping:
                    chars.append(mapping[nxt])
                    self._advance(2)
                    continue
                raise DSLLexError(f"invalid escape \\{nxt}", self.line, self.col)
            if ch == "\n":
                raise DSLLexError("unterminated string literal", start_line, start_col)
            chars.append(ch)
            self._advance()
        raise DSLLexError("unterminated string literal", start_line, start_col)

    def tokenize(self) -> list[Token]:
        while self.pos < len(self.src):
            if self.at_line_start and self.paren_depth == 0:
                self._handle_line_start()
                if self.pos >= len(self.src):
                    break
            ch = self._peek()
            if ch == "\n":
                self._advance()
                if self.paren_depth == 0:
                    if not self.tokens or self.tokens[-1].type != "NEWLINE":
                        self.tokens.append(Token("NEWLINE", "", self.line, self.col))
                    self.at_line_start = True
                continue
            if ch in " \t":
                self._advance()
                continue
            if ch == "#":
                while self.pos < len(self.src) and self._peek() != "\n":
                    self._advance()
                continue
            if ch == "$":
                self.tokens.append(self._lex_binding())
                continue
            if ch.isalpha() or ch == "_":
                self.tokens.append(self._lex_ident())
                continue
            if ch.isdigit():
                self.tokens.append(self._lex_number())
                continue
            if ch == '"' or ch == "'":
                self.tokens.append(self._lex_string())
                continue
            # 2-char punct first
            two = self.src[self.pos : self.pos + 2]
            start_line, start_col = self.line, self.col
            if two == "<-":  self._advance(2); self.tokens.append(Token("ARROW","<-", start_line, start_col)); continue
            if two == "?.":  self._advance(2); self.tokens.append(Token("QDOT","?.", start_line, start_col)); continue
            if two == "[?":  self._advance(2); self.paren_depth += 1; self.tokens.append(Token("QBRACK","[?", start_line, start_col)); continue
            single = {
                "(": "LPAREN", ")": "RPAREN",
                "[": "LBRACK", "]": "RBRACK",
                "{": "LBRACE", "}": "RBRACE",
                ",": "COMMA", ":": "COLON", ".": "DOT",
                "+": "PLUS", "-": "MINUS",
                "*": "STAR", "/": "SLASH",
                "?": "QUESTION",
                # let-binding assignment.
                "=": "EQ",
            }
            if ch in single:
                if ch in "([{":
                    self.paren_depth += 1
                elif ch in ")]}":
                    self.paren_depth -= 1
                self._advance()
                self.tokens.append(Token(single[ch], ch, start_line, start_col))  # type: ignore[arg-type]
                continue
            raise DSLLexError(f"unexpected character {ch!r}", self.line, self.col)
        # Final newline + dedents to close open blocks.
        if self.tokens and self.tokens[-1].type != "NEWLINE":
            self.tokens.append(Token("NEWLINE", "", self.line, self.col))
        while len(self.indent_stack) > 1:
            self.indent_stack.pop()
            self.tokens.append(Token("DEDENT", "", self.line, self.col))
        self.tokens.append(Token("EOF", "", self.line, self.col))
        return self.tokens


def tokenize(source: str) -> list[Token]:
    return DSLLexer(source).tokenize()
