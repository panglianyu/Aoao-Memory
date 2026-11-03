"""Prepare the one approved R1b clone without starting a model or service."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "apps"):
    sys.path.insert(0, str(path))

from kernel.raw_message_clone import CLONE_ROOT, CONTROL_ROOT, prepare_clone  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Prepare the fixed private R1b challenge clone")
    parser.add_argument("--original-root", type=Path, required=True)
    parser.add_argument("--backup-root", type=Path, required=True)
    args = parser.parse_args(argv)
    report = prepare_clone(args.original_root, args.backup_root, CLONE_ROOT, CONTROL_ROOT)
    print(json.dumps(report, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print('{"state":"clone_rejected"}', flush=True)
        raise SystemExit(2) from None
