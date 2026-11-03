"""Bounded DDGS Google HTML parser; this module never performs network I/O."""
from __future__ import annotations

import argparse
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path

_DDGS_VERSION = "9.16.0"
_LOCK_SHA256 = "2a851e2dbbf2f4207059efcb17f24c737c1a0dea3b8313e4c3b3276ea42bbb8d"
_MAX_INPUT = 1024 * 1024
_MAX_OUTPUT = 512 * 1024


def parse_google_html(body: bytes) -> list[dict[str, str]]:
    if metadata.version("ddgs") != _DDGS_VERSION:
        raise ValueError("parser_unavailable")
    if type(body) is not bytes or not body or len(body) > _MAX_INPUT:
        raise ValueError("parse_failed")
    from ddgs.engines.google import Google

    engine = object.__new__(Google)
    text = body.decode("utf-8", errors="replace")
    parsed = engine.post_extract_results(engine.extract_results(text))
    rows: list[dict[str, str]] = []
    for result in parsed[:8]:
        title = getattr(result, "title", None)
        href = getattr(result, "href", None)
        snippet = getattr(result, "body", "")
        if isinstance(title, str) and isinstance(href, str) and isinstance(snippet, str):
            rows.append({"title": title[:512], "href": href[:4096], "body": snippet[:1600]})
    return rows


def _verify_lock(path: Path) -> None:
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise ValueError("parser_unavailable") from exc
    if digest != _LOCK_SHA256:
        raise ValueError("parser_unavailable")


def _write_result(path: Path, result: dict) -> None:
    encoded = json.dumps(result, ensure_ascii=False, sort_keys=True).encode("utf-8")
    if len(encoded) > _MAX_OUTPUT:
        encoded = b'{"error":"response_too_large"}'
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--lock", required=True)
    args = parser.parse_args(argv)
    output = Path(args.output).resolve()
    try:
        source = Path(args.input).resolve()
        if source.stat().st_size > _MAX_INPUT:
            raise ValueError("response_too_large")
        _verify_lock(Path(args.lock).resolve())
        rows = parse_google_html(source.read_bytes())
        _write_result(output, {"results": rows})
        return 0
    except Exception as exc:
        known = {"parse_failed", "parser_unavailable", "response_too_large"}
        error = str(exc) if isinstance(exc, ValueError) and str(exc) in known else "parse_failed"
        try:
            _write_result(output, {"error": error})
        except OSError:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
