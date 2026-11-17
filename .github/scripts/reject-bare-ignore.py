#!/usr/bin/env python3
"""Fail when Rust source uses #[ignore] without a non-empty reason.

CI keeps long soak tests ignored. A reason string is required so a skipped
test documents why it is not part of the default suite:

    #[ignore = "longer local stress profile; pass --ignored"]

The same rule applies to `ignore` nested in `cfg_attr` (including nested
`cfg_attr`), because `cargo test` enables `cfg(test)`. Comments outside
literals are whitespace. Ordinary string escapes are decoded before the
whitespace-only check. Lifetimes are not character literals.
"""

from __future__ import annotations

import sys
from pathlib import Path

_HEX = frozenset("0123456789abcdefABCDEF")
_SIMPLE_ESCAPES = {
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "\\": "\\",
    "0": "\0",
    "'": "'",
    '"': '"',
}
# Rust string continuation skips Pattern_White_Space on the next line.
_CONTINUATION_WS = frozenset(" \t\u000b\u000c\r\u0085\u200e\u200f\u2028\u2029")


def _skip_line_comment(src: str, i: int) -> int:
    n = len(src)
    while i < n and src[i] != "\n":
        i += 1
    return i


def _skip_block_comment(src: str, i: int) -> int:
    """i points just after the opening /* ."""
    n = len(src)
    depth = 1
    while i < n and depth:
        if src.startswith("/*", i):
            depth += 1
            i += 2
            continue
        if src.startswith("*/", i):
            depth -= 1
            i += 2
            continue
        i += 1
    return i


def _skip_string(src: str, i: int) -> int | None:
    """If src[i] begins a string (not a char or lifetime), return the index after it."""
    n = len(src)
    j = i
    if j < n and src[j] == "b" and j + 1 < n and src[j + 1] in ('"', "r"):
        j += 1
    if j >= n:
        return None
    if src[j] == "r":
        k = j + 1
        hashes = 0
        while k < n and src[k] == "#":
            hashes += 1
            k += 1
        if k < n and src[k] == '"':
            end = '"' + ("#" * hashes)
            k += 1
            while k < n and not src.startswith(end, k):
                k += 1
            if k >= n:
                return n
            return k + len(end)
        return None
    if j >= n or src[j] != '"':
        return None
    k = j + 1
    while k < n:
        if src[k] == "\\":
            k += 2
            continue
        if src[k] == '"':
            return k + 1
        k += 1
    return n


def _skip_char_literal(src: str, i: int) -> int | None:
    """Consume a char literal only when the closing quote is immediate.

    Lifetimes such as `'static` are not char literals. A char is one scalar
    or one escape (`\\n`, `\\xHH`, `\\u{...}`) followed directly by `'`.
    """
    n = len(src)
    if i >= n or src[i] != "'":
        return None
    if i + 2 >= n:
        return None
    if src[i + 1] == "\\":
        j = i + 2
        if j >= n:
            return None
        if src[j] in _SIMPLE_ESCAPES and j + 1 < n and src[j + 1] == "'":
            return j + 2
        if src[j] == "x" and j + 3 < n and src[j + 1] in _HEX and src[j + 2] in _HEX and src[j + 3] == "'":
            return j + 4
        if src[j] == "u" and j + 1 < n and src[j + 1] == "{":
            k = j + 2
            if k >= n or src[k] not in _HEX:
                return None
            k += 1
            while k < n and src[k] in _HEX:
                k += 1
            digits = k - (j + 2)
            if 1 <= digits <= 6 and k + 1 < n and src[k] == "}" and src[k + 1] == "'":
                return k + 2
        return None
    if src[i + 1] in "'\\\n\r":
        return None
    if src[i + 2] == "'":
        return i + 3
    return None


def _skip_lifetime(src: str, i: int) -> int | None:
    n = len(src)
    if i + 1 >= n or src[i] != "'":
        return None
    if not (src[i + 1].isalpha() or src[i + 1] == "_"):
        return None
    j = i + 2
    while j < n and (src[j].isalnum() or src[j] == "_"):
        j += 1
    return j


def _advance_over_literal(src: str, i: int) -> int | None:
    skipped = _skip_string(src, i)
    if skipped is not None and skipped != i:
        return skipped
    if i < len(src) and src[i] == "'":
        skipped = _skip_char_literal(src, i)
        if skipped is not None:
            return skipped
        return _skip_lifetime(src, i)
    return None


def _blank_comments(src: str) -> str:
    """Replace comments outside literals with spaces, keeping newlines."""
    out: list[str] = []
    i = 0
    n = len(src)
    while i < n:
        if src.startswith("//", i):
            nxt = _skip_line_comment(src, i)
            out.append(" " * (nxt - i))
            i = nxt
            continue
        if src.startswith("/*", i):
            nxt = _skip_block_comment(src, i + 2)
            out.extend("\n" if ch == "\n" else " " for ch in src[i:nxt])
            i = nxt
            continue
        skipped = _advance_over_literal(src, i)
        if skipped is not None and skipped != i:
            out.append(src[i:skipped])
            i = skipped
            continue
        out.append(src[i])
        i += 1
    return "".join(out)


def _decode_rust_string(content: str) -> str | None:
    """Decode a Rust ordinary string body. None means the escapes are invalid."""
    out: list[str] = []
    i = 0
    n = len(content)
    while i < n:
        if content[i] != "\\":
            out.append(content[i])
            i += 1
            continue
        if i + 1 >= n:
            return None
        nxt = content[i + 1]
        if nxt in _SIMPLE_ESCAPES:
            out.append(_SIMPLE_ESCAPES[nxt])
            i += 2
            continue
        if nxt == "\n":
            i += 2
            while i < n and content[i] in _CONTINUATION_WS:
                i += 1
            continue
        if nxt == "x":
            if i + 3 >= n or content[i + 2] not in _HEX or content[i + 3] not in _HEX:
                return None
            value = int(content[i + 2 : i + 4], 16)
            if value > 0x7F:
                return None
            out.append(chr(value))
            i += 4
            continue
        if nxt == "u":
            if i + 2 >= n or content[i + 2] != "{":
                return None
            j = i + 3
            while j < n and content[j] in _HEX:
                j += 1
            if j >= n or content[j] != "}" or j == i + 3 or (j - (i + 3)) > 6:
                return None
            value = int(content[i + 3 : j], 16)
            if value > 0x10FFFF or 0xD800 <= value <= 0xDFFF:
                return None
            out.append(chr(value))
            i = j + 1
            continue
        return None
    return "".join(out)


def _parse_reason(rest: str) -> str | None:
    """Parse `= "reason"` / raw / byte strings. Comments must already be blanked."""
    text = rest.strip()
    if not text.startswith("="):
        return None
    body = text[1:].lstrip()
    if body.startswith("b") and len(body) > 1 and body[1] in ('"', "r"):
        body = body[1:]
    if body.startswith("r"):
        hashes = 0
        k = 1
        while k < len(body) and body[k] == "#":
            hashes += 1
            k += 1
        if k >= len(body) or body[k] != '"':
            return None
        end = '"' + ("#" * hashes)
        start = k + 1
        stop = body.find(end, start)
        if stop < 0:
            return None
        if body[stop + len(end) :].strip():
            return None
        return body[start:stop]
    if not body.startswith('"'):
        return None
    k = 1
    while k < len(body):
        if body[k] == "\\":
            k += 2
            continue
        if body[k] == '"':
            if body[k + 1 :].strip():
                return None
            return _decode_rust_string(body[1:k])
        k += 1
    return None


def _split_args(text: str) -> list[str]:
    args: list[str] = []
    start = 0
    paren = 0
    brack = 0
    i = 0
    n = len(text)
    while i < n:
        skipped = _advance_over_literal(text, i)
        if skipped is not None and skipped != i:
            i = skipped
            continue
        ch = text[i]
        if ch == "(":
            paren += 1
        elif ch == ")":
            paren = max(0, paren - 1)
        elif ch == "[":
            brack += 1
        elif ch == "]":
            brack = max(0, brack - 1)
        elif ch == "," and paren == 0 and brack == 0:
            args.append(text[start:i])
            start = i + 1
        i += 1
    args.append(text[start:])
    return args


def _matching_delim(text: str, open_at: int, open_ch: str, close_ch: str) -> int | None:
    if open_at >= len(text) or text[open_at] != open_ch:
        return None
    depth = 0
    i = open_at
    n = len(text)
    while i < n:
        if text.startswith("//", i):
            i = _skip_line_comment(text, i)
            continue
        if text.startswith("/*", i):
            i = _skip_block_comment(text, i + 2)
            continue
        skipped = _advance_over_literal(text, i)
        if skipped is not None and skipped != i:
            i = skipped
            continue
        if text[i] == open_ch:
            depth += 1
        elif text[i] == close_ch:
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _leading_ident(text: str) -> tuple[str, str] | None:
    stripped = text.lstrip()
    if not stripped or not (stripped[0].isalpha() or stripped[0] == "_"):
        return None
    i = 1
    while i < len(stripped) and (stripped[i].isalnum() or stripped[i] == "_"):
        i += 1
    return stripped[:i], stripped[i:]


def _attribute_problems(raw_inner: str) -> list[str]:
    text = _blank_comments(raw_inner)
    lead = _leading_ident(text)
    if lead is None:
        return []
    name, rest = lead
    if name == "ignore":
        reason = _parse_reason(rest)
        if reason is None:
            return ["missing reason"]
        if reason.strip() == "":
            return ["empty reason"]
        return []
    if name != "cfg_attr":
        return []
    args_src = rest.lstrip()
    if not args_src.startswith("("):
        return []
    end = _matching_delim(args_src, 0, "(", ")")
    if end is None:
        return []
    problems: list[str] = []
    for arg in _split_args(args_src[1:end])[1:]:
        problems.extend(_attribute_problems(arg))
    return problems


def bare_ignores(src: str) -> list[tuple[int, str]]:
    """Return (line, problem) for each ignore attribute without a real reason."""
    problems: list[tuple[int, str]] = []
    i = 0
    n = len(src)
    line = 1
    while i < n:
        if src[i] == "\n":
            line += 1
            i += 1
            continue
        if src.startswith("//", i):
            i = _skip_line_comment(src, i)
            continue
        if src.startswith("/*", i):
            nxt = _skip_block_comment(src, i + 2)
            line += src[i:nxt].count("\n")
            i = nxt
            continue
        skipped = _advance_over_literal(src, i)
        if skipped is not None and skipped != i:
            line += src[i:skipped].count("\n")
            i = skipped
            continue
        if src[i] != "#":
            i += 1
            continue
        attr_line = line
        j = i + 1
        if j < n and src[j] == "!":
            j += 1
        if j >= n or src[j] != "[":
            i += 1
            continue
        saved_line = line
        end = _matching_delim(src, j, "[", "]")
        if end is None:
            line = saved_line
            i += 1
            continue
        line += src[j : end + 1].count("\n")
        for problem in _attribute_problems(src[j + 1 : end]):
            problems.append((attr_line, problem))
        i = end + 1
    return problems


def scan(paths: list[Path]) -> list[str]:
    findings: list[str] = []
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(p for p in path.rglob("*.rs") if "target" not in p.parts))
        elif path.suffix == ".rs":
            files.append(path)
    for path in files:
        text = path.read_text(encoding="utf-8")
        for line_no, problem in bare_ignores(text):
            findings.append(
                f'{path}:{line_no}: #[ignore] {problem}; use #[ignore = "why CI skips this"]'
            )
    return findings


def _self_test() -> None:
    cases = {
        "//! mentioned `#[ignore]` in docs\n": [],
        "// #[ignore]\n": [],
        "/* #[ignore] */\n": [],
        'const S: &str = "#[ignore]";\n': [],
        "const S: &'static str = \"#[ignore]\";\n": [],
        '#[ignore = "longer local profile"]\nfn t() {}\n': [],
        '#[ignore="because"]\n': [],
        '#[ignore = r#"raw reason"#]\n': [],
        "#[ignore]\n": [(1, "missing reason")],
        '#[ignore = ""]\n': [(1, "empty reason")],
        '#[ignore = "   "]\n': [(1, "empty reason")],
        "#[cfg(test)]\n#[ignore]\nfn t() {}\n": [(2, "missing reason")],
        '#[ignore = "long soak" /* local only */]\n': [],
        '#[ignore /* local only */ = "long soak"]\n': [],
        '#[ignore = "long soak" // kept\n]\n': [],
        '#[ignore = r#"raw reason"# /* after */]\n': [],
        '#[ignore /* before */ = r##"raw reason"##]\n': [],
        '#[ignore = "\\n\\t\\u{20}"]\n': [(1, "empty reason")],
        '#[ignore = "\\nreason"]\n': [],
        '#[ignore = "\\u{000020}"]\n': [(1, "empty reason")],
        "const MESSAGE: &'static str = \"it's ready\";\n#[ignore]\n": [(2, "missing reason")],
        "let c = 'x';\n#[ignore]\n": [(2, "missing reason")],
        "let c = '\\n';\n#[ignore]\n": [(2, "missing reason")],
        "let c = '\\u{20}';\n#[ignore]\n": [(2, "missing reason")],
        "#[cfg_attr(test, ignore)]\n": [(1, "missing reason")],
        '#[cfg_attr(test, ignore = "because")]\n': [],
        '#[cfg_attr(all(test, feature = "x"), ignore)]\n': [(1, "missing reason")],
        "#[cfg_attr(test, cfg_attr(test, ignore))]\n": [(1, "missing reason")],
        '#[cfg_attr(test, cfg_attr(test, ignore = "nested"))]\n': [],
        "#[cfg_attr(test, allow(dead_code))]\n": [],
        "#[cfg_attr(test, allow(dead_code), ignore)]\n": [(1, "missing reason")],
        '#[cfg_attr(feature = "a,b", ignore)]\n': [(1, "missing reason")],
        '#[cfg_attr(test, ignore = "\\n\\t\\u{20}")]\n': [(1, "empty reason")],
        '#[ignore = "has ] bracket"]\n': [],
        '#[ignore = "long soak" /* ] */]\n': [],
        '#[cfg_attr(/* pred */ test, ignore = "ok")]\n': [],
    }
    failed = False
    for src, expected in cases.items():
        got = bare_ignores(src)
        if got != expected:
            failed = True
            print(
                f"self-test mismatch\n src={src!r}\n got={got}\n expected={expected}",
                file=sys.stderr,
            )
    repo_stress = Path("crates/metadb-server/tests/stress_harness.rs")
    if repo_stress.is_file() and bare_ignores(repo_stress.read_text(encoding="utf-8")):
        failed = True
        print(f"self-test: {repo_stress} should have no bare #[ignore]", file=sys.stderr)
    if failed:
        raise SystemExit(1)


def main(argv: list[str]) -> int:
    self_test = "--self-test" in argv
    roots = [Path(arg) for arg in argv if not arg.startswith("-")]
    if self_test:
        _self_test()
        print("reject-bare-ignore self-test passed")
    if not roots:
        if self_test:
            return 0
        roots = [Path("crates")]
    missing = [str(path) for path in roots if not path.exists()]
    if missing:
        print("missing path: " + ", ".join(missing), file=sys.stderr)
        return 1
    findings = scan(roots)
    if findings:
        print("bare #[ignore] is not allowed:")
        print("\n".join(findings))
        return 1
    print("no bare #[ignore] attributes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
