#!/usr/bin/env python3
"""Fail when Rust source uses #[ignore] without a non-empty reason.

CI keeps long soak tests ignored. A reason string is required so a skipped
test documents why it is not part of the default suite:

    #[ignore = "longer local stress profile; pass --ignored"]

Doc comments, // comments, block comments, and string literals are not
attributes. #[ignore = ""] and a whitespace-only reason fail closed.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _skip_line_comment(src: str, i: int) -> int:
    n = len(src)
    while i < n and src[i] != "\n":
        i += 1
    return i


def _skip_block_comment(src: str, i: int) -> int:
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


def _skip_char_or_string(src: str, i: int) -> int | None:
    """If src[i] begins a char or string literal, return the index after it."""
    n = len(src)
    j = i
    if j < n and src[j] == "b" and j + 1 < n and src[j + 1] in ("'", '"', "r"):
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
    if src[j] not in ("'", '"'):
        return None
    quote = src[j]
    k = j + 1
    while k < n:
        if src[k] == "\\":
            k += 2
            continue
        if src[k] == quote:
            return k + 1
        if quote == "'" and src[k] == "\n":
            return None
        k += 1
    return n


def _parse_reason(rest: str) -> str | None:
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
    chars: list[str] = []
    k = 1
    while k < len(body):
        ch = body[k]
        if ch == "\\":
            if k + 1 >= len(body):
                return None
            chars.append(body[k + 1])
            k += 2
            continue
        if ch == '"':
            if body[k + 1 :].strip():
                return None
            return "".join(chars)
        chars.append(ch)
        k += 1
    return None


def _ignore_problem(inner: str) -> str | None:
    text = inner.strip()
    if not text.startswith("ignore"):
        return None
    rest = text[len("ignore") :]
    if rest[:1].isalnum() or rest[:1] == "_":
        return None
    reason = _parse_reason(rest)
    if reason is None:
        return "missing reason"
    if reason.strip() == "":
        return "empty reason"
    return None


def bare_ignores(src: str) -> list[tuple[int, str]]:
    """Return (line, problem) for each #[ignore] / #![ignore] without a reason."""
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
            start_line = line
            nxt = _skip_block_comment(src, i + 2)
            line += src[i:nxt].count("\n")
            i = nxt
            if start_line > line:
                line = start_line
            continue
        skipped = _skip_char_or_string(src, i)
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
        depth = 0
        k = j
        end = None
        while k < n:
            if src[k] == "\n":
                line += 1
                k += 1
                continue
            if src.startswith("//", k):
                k = _skip_line_comment(src, k)
                continue
            if src.startswith("/*", k):
                nxt = _skip_block_comment(src, k + 2)
                line += src[k:nxt].count("\n")
                k = nxt
                continue
            skipped = _skip_char_or_string(src, k)
            if skipped is not None and skipped != k:
                line += src[k:skipped].count("\n")
                k = skipped
                continue
            if src[k] == "[":
                depth += 1
            elif src[k] == "]":
                depth -= 1
                if depth == 0:
                    end = k
                    break
            k += 1
        if end is None:
            line = saved_line
            i += 1
            continue
        problem = _ignore_problem(src[j + 1 : end])
        if problem is not None:
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
        for line, problem in bare_ignores(text):
            findings.append(f"{path}:{line}: #[ignore] {problem}; use #[ignore = \"why CI skips this\"]")
    return findings


def _self_test() -> None:
    cases = {
        '//! mentioned `#[ignore]` in docs\n': [],
        '// #[ignore]\n': [],
        '/* #[ignore] */\n': [],
        'const S: &str = "#[ignore]";\n': [],
        '#[ignore = "longer local profile"]\nfn t() {}\n': [],
        '#[ignore="because"]\n': [],
        '#[ignore = r#"raw reason"#]\n': [],
        '#[ignore]\n': [(1, "missing reason")],
        '#[ignore = ""]\n': [(1, "empty reason")],
        '#[ignore = "   "]\n': [(1, "empty reason")],
        '#[cfg(test)]\n#[ignore]\nfn t() {}\n': [(2, "missing reason")],
    }
    failed = False
    for src, expected in cases.items():
        got = bare_ignores(src)
        if got != expected:
            failed = True
            print(f"self-test mismatch\n src={src!r}\n got={got}\n expected={expected}", file=sys.stderr)
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
