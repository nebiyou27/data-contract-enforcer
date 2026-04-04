#!/usr/bin/env python3
"""
contracts/batch_runner.py -- Concurrent Multi-Contract Validation

Reads a YAML job manifest and validates every contract in parallel using a
thread pool.  Each job calls contracts.runner.main() directly (no subprocess
overhead), so the process starts once and all jobs share the same Python
interpreter.

Job manifest format (batch.yaml)
---------------------------------
  jobs:
    - contract: generated_contracts/week3-document-refinery-extractions.yaml
      data:     outputs/week3/extractions.jsonl
      output:   validation_reports/week3_baseline.json
      mode:     ENFORCE           # optional, default AUDIT
      promote_baselines: false    # optional, default false

    - contract: generated_contracts/week4-lineage-graph.yaml
      data:     outputs/week4/lineage.jsonl
      output:   validation_reports/week4_baseline.json

  # Global defaults applied to every job (overridden per-job)
  defaults:
    mode: AUDIT
    promote_baselines: false

  # Maximum parallel workers (default: number of jobs, capped at 8)
  max_workers: 4

Exit codes
----------
  0  All jobs passed (exit code from each runner.main() was 0)
  1  One or more jobs failed or errored
  2  Manifest file not found or malformed

Usage
-----
  contracts-run-all --batch batch.yaml
  contracts-run-all --batch batch.yaml --max-workers 2
  contracts-run-all --batch batch.yaml --fail-fast
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import yaml

try:
    from contracts import runner as _runner
    from contracts.log_config import configure_logging
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from contracts import runner as _runner
    from contracts.log_config import configure_logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Manifest loading
# ---------------------------------------------------------------------------

_VALID_MODES = {"AUDIT", "WARN", "ENFORCE"}


def _load_manifest(path: Path) -> tuple[list[dict], int]:
    """Parse a batch YAML manifest.

    Returns (jobs, max_workers).
    Each job dict has: contract, data, output, mode, promote_baselines.
    """
    with open(path, "r", encoding="utf-8") as fh:
        manifest = yaml.safe_load(fh)

    if not isinstance(manifest, dict) or "jobs" not in manifest:
        raise ValueError("Manifest must be a YAML dict with a 'jobs' list")

    defaults = manifest.get("defaults", {})
    raw_jobs = manifest["jobs"]
    if not isinstance(raw_jobs, list) or not raw_jobs:
        raise ValueError("'jobs' must be a non-empty list")

    jobs: list[dict] = []
    for i, raw in enumerate(raw_jobs, 1):
        if not isinstance(raw, dict):
            raise ValueError(f"Job #{i} must be a dict, got {type(raw).__name__}")
        for required in ("contract", "data", "output"):
            if required not in raw and required not in defaults:
                raise ValueError(f"Job #{i} missing required field '{required}'")

        mode = raw.get("mode", defaults.get("mode", "AUDIT")).upper()
        if mode not in _VALID_MODES:
            raise ValueError(
                f"Job #{i}: invalid mode '{mode}'. Choose from {sorted(_VALID_MODES)}"
            )

        jobs.append(
            {
                "contract": raw.get("contract", defaults.get("contract")),
                "data": raw.get("data", defaults.get("data")),
                "output": raw.get("output", defaults.get("output")),
                "mode": mode,
                "promote_baselines": bool(
                    raw.get(
                        "promote_baselines",
                        defaults.get("promote_baselines", False),
                    )
                ),
            }
        )

    max_workers = int(manifest.get("max_workers", min(len(jobs), 8)))
    return jobs, max_workers


# ---------------------------------------------------------------------------
# Single-job runner (called from each thread)
# ---------------------------------------------------------------------------


def _run_job(job: dict) -> dict[str, Any]:
    """Run one validation job and return a result dict."""
    contract = job["contract"]
    data = job["data"]
    output = job["output"]
    mode = job["mode"]
    promote = job["promote_baselines"]

    argv = [
        "--contract", contract,
        "--data", data,
        "--output", output,
        "--mode", mode,
    ]
    if promote:
        argv.append("--promote-baselines")

    label = Path(contract).stem
    t0 = time.monotonic()
    exit_code: int
    captured_out = io.StringIO()
    captured_err = io.StringIO()

    try:
        # Redirect stdout/stderr so concurrent jobs don't interleave their output.
        # We collect them and flush after the job completes.
        with redirect_stdout(captured_out), redirect_stderr(captured_err):
            exit_code = _runner.main(argv)
    except SystemExit as exc:
        exit_code = int(exc.code) if exc.code is not None else 1
    except Exception as exc:  # noqa: BLE001
        exit_code = 2
        captured_err.write(f"Unhandled exception: {exc}\n")

    elapsed = time.monotonic() - t0
    return {
        "label": label,
        "contract": contract,
        "output": output,
        "exit_code": exit_code,
        "elapsed_s": round(elapsed, 2),
        "stdout": captured_out.getvalue(),
        "stderr": captured_err.getvalue(),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run multiple contract validations in parallel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--batch",
        required=True,
        metavar="PATH",
        help="Path to the batch job manifest YAML.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        metavar="N",
        help="Maximum parallel threads (overrides manifest max_workers).",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=False,
        help="Stop submitting new jobs as soon as any job fails.",
    )
    args = parser.parse_args(argv)

    configure_logging()

    manifest_path = Path(args.batch)
    if not manifest_path.exists():
        print(f"ERROR: batch manifest not found: {manifest_path}", file=sys.stderr)
        return 2

    try:
        jobs, manifest_workers = _load_manifest(manifest_path)
    except (ValueError, yaml.YAMLError) as exc:
        print(f"ERROR: invalid manifest: {exc}", file=sys.stderr)
        return 2

    workers = args.max_workers if args.max_workers is not None else manifest_workers
    workers = max(1, workers)

    print(
        f"Batch runner: {len(jobs)} job(s), {workers} worker(s)  [{manifest_path}]"
    )
    print("-" * 60)

    results: list[dict] = []
    failed_fast = False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_job = {pool.submit(_run_job, job): job for job in jobs}
        for future in as_completed(future_to_job):
            res = future.result()
            results.append(res)
            status = "PASS" if res["exit_code"] == 0 else "FAIL"
            print(
                f"  [{status}] {res['label']:<45}  "
                f"{res['elapsed_s']:>6.2f}s  exit={res['exit_code']}"
            )
            # Flush captured output under the job label
            if res["stdout"].strip():
                for line in res["stdout"].strip().splitlines():
                    print(f"        {line}")
            if res["stderr"].strip():
                for line in res["stderr"].strip().splitlines():
                    print(f"        STDERR: {line}", file=sys.stderr)

            if args.fail_fast and res["exit_code"] != 0:
                failed_fast = True
                # Cancel remaining futures
                for f in future_to_job:
                    f.cancel()
                break

    print("-" * 60)
    passed = sum(1 for r in results if r["exit_code"] == 0)
    failed = len(results) - passed
    total_time = sum(r["elapsed_s"] for r in results)
    skipped = len(jobs) - len(results)

    print(
        f"Summary: {passed} passed, {failed} failed"
        + (f", {skipped} skipped (--fail-fast)" if skipped else "")
        + f"  |  wall-clock ≈ {max(r['elapsed_s'] for r in results):.2f}s"
        f"  (serial would be ≈ {total_time:.2f}s)"
    )

    if failed_fast:
        print("Stopped early due to --fail-fast.", file=sys.stderr)

    # Write a machine-readable batch summary alongside the individual reports
    summary = {
        "batch_manifest": str(manifest_path),
        "workers": workers,
        "jobs_total": len(jobs),
        "jobs_run": len(results),
        "jobs_passed": passed,
        "jobs_failed": failed,
        "results": [
            {
                "label": r["label"],
                "contract": r["contract"],
                "output": r["output"],
                "exit_code": r["exit_code"],
                "elapsed_s": r["elapsed_s"],
            }
            for r in results
        ],
    }
    summary_path = manifest_path.with_suffix(".summary.json")
    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)
    print(f"Batch summary written: {summary_path}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
