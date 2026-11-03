"""One finite R2 continuation in the existing, quiescent R1b data root."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
for path in (PROJECT_ROOT, PROJECT_ROOT / "apps"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from kernel.challenge_diagnostics import ensure_private_directory  # noqa: E402
from kernel.raw_message_recovery_r2 import (  # noqa: E402
    CONTROL, PRIOR_CONTROL, ROOT, R2CallBudget, R2Receipt, build_manifest, preflight, terminal,
)
from Tools import recover_memory_challenge_failed as shared  # noqa: E402
from Tools.serve_memory_challenge import _api_settings, _DataRootLease  # noqa: E402


def parser():
    value = argparse.ArgumentParser(description="Finite remaining-task R2 continuation")
    value.add_argument("--data-root", type=Path, required=True)
    value.add_argument("--prior-control", type=Path, required=True)
    value.add_argument("--manifest", type=Path, required=True)
    value.add_argument("--checkpoint-sha256")
    value.add_argument("--receipt", type=Path)
    value.add_argument("--api-config", type=Path)
    value.add_argument("--diagnostics-file", type=Path)
    mode = value.add_mutually_exclusive_group()
    mode.add_argument("--manifest-only", action="store_true")
    mode.add_argument("--inspect-only", action="store_true")
    return value


def main(argv=None):
    from system_memory_god.grouped_extractor import provider_request_options

    args = parser().parse_args(argv)
    if (args.data_root != ROOT or args.prior_control != PRIOR_CONTROL
            or args.manifest != CONTROL / "manifest.json"):
        raise ValueError("fixed R2 paths required")
    started = time.monotonic()
    lease = _DataRootLease.acquire(args.data_root)
    receipt = loop = None
    retained = False
    try:
        if args.manifest_only:
            manifest = build_manifest(args.data_root, args.prior_control, runtime_lease=lease)
            counts = preflight(args.data_root, manifest, prior_control=args.prior_control,
                checkpoint_sha256=manifest["checkpoint"]["sha256"], runtime_lease=lease)
            shared._private_write(args.manifest, manifest)
            print(json.dumps({"state": "manifest_frozen", **counts}, sort_keys=True), flush=True)
            return 0
        manifest = shared._load(args.manifest)
        counts = preflight(args.data_root, manifest, prior_control=args.prior_control,
                           checkpoint_sha256=args.checkpoint_sha256, runtime_lease=lease)
        if args.inspect_only:
            print(json.dumps({"state": "preflight_passed", **counts}, sort_keys=True), flush=True)
            return 0
        if (args.receipt != CONTROL / "receipt.jsonl" or args.api_config is None
                or args.diagnostics_file != ROOT / "diagnostics/recovery-r2.jsonl"):
            raise ValueError("fixed R2 execution outputs required")
        api = _api_settings(args.api_config)
        if api["model_name"] != manifest["model"] or not provider_request_options(api["base_url"]):
            raise ValueError("R2 provider identity differs")
        ensure_private_directory(args.receipt.parent)
        ensure_private_directory(args.diagnostics_file.parent)
        # Recheck the full checkpoint immediately before one-shot authorization.
        preflight(args.data_root, manifest, prior_control=args.prior_control,
                  checkpoint_sha256=args.checkpoint_sha256, runtime_lease=lease)
        receipt = R2Receipt(args.receipt, manifest)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            report = loop.run_until_complete(shared.execute(args, manifest, receipt, started,
                budget_factory=R2CallBudget, terminal=terminal, expected_additional_attempts=15))
        except BaseException:
            if not shared._RETAINED:
                raise
            report = {"state": "unknown", "closed": False}
        print(json.dumps(report, sort_keys=True), flush=True)
        if not report["closed"]:
            retained = True
            shared._RETAINED.extend((lease, receipt, loop))
            loop.run_forever()
        return 0 if report["state"] == "recovered" else 3
    finally:
        if not retained:
            if loop is not None:
                loop.close()
            if receipt is not None:
                receipt.close()
            lease.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Exception, KeyboardInterrupt):
        print('{"state":"rejected","closed":false}', flush=True)
        raise SystemExit(2) from None
