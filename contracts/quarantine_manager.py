#!/usr/bin/env python3
"""
contracts/quarantine_manager.py -- Quarantine Review and Retry Workflow

Reads quarantined records written by contracts-ai-checks (prompt input schema
violations) and provides three operations:

  review   Print a human-readable summary grouped by error type, with record
           samples so an operator can decide what to fix.

  requeue  Write the original (unwrapped) records to a staging JSONL so they
           can be corrected and re-fed to contracts-ai-checks.  Only records
           whose schema errors all appear in --allow-errors are requeued;
           everything else stays in quarantine.

  clear    Archive the current quarantine file (moves it to
           quarantine/archive/ with a timestamp suffix) so the next run starts
           with a clean slate.  Pass --dry-run to preview without moving.

Quarantine record format (written by ai_extensions.check_prompt_input_schema):
  {
    "record":          { ...original document dict... },
    "schema_errors":   [ "Field 'doc_id' is required", ... ],
    "quarantined_at":  "2026-04-04T12:00:00+00:00"
  }

Usage
-----
  contracts-quarantine review
  contracts-quarantine review --quarantine quarantine/prompt_schema_violations.jsonl

  contracts-quarantine requeue --output staging/requeue.jsonl
  contracts-quarantine requeue --output staging/requeue.jsonl \\
      --allow-errors "Field 'extraction_model' is optional"

  contracts-quarantine clear
  contracts-quarantine clear --dry-run
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_QUARANTINE_PATH = Path("quarantine") / "prompt_schema_violations.jsonl"
ARCHIVE_DIR = Path("quarantine") / "archive"


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _load_quarantine(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Subcommand: review
# ---------------------------------------------------------------------------


def cmd_review(args: argparse.Namespace) -> int:
    q_path = Path(args.quarantine)
    records = _load_quarantine(q_path)

    if not records:
        print(f"No quarantined records found in {q_path}")
        return 0

    total = len(records)
    print(f"Quarantine: {q_path}  ({total} record(s))\n")

    # Group by error signature for a concise summary
    error_groups: dict[str, list[dict]] = defaultdict(list)
    error_counter: Counter = Counter()
    for qr in records:
        for err in qr.get("schema_errors", ["(no error message)"]):
            error_counter[err] += 1
            error_groups[err].append(qr)

    print("Error breakdown:")
    for err, count in error_counter.most_common():
        print(f"  {count:>5}x  {err}")
    print()

    # Print one sample record per distinct error (up to --samples)
    shown: set[int] = set()
    print(f"Sample records (up to {args.samples} per error type):")
    for err, group in error_groups.items():
        print(f"\n  [{err}]")
        for i, qr in enumerate(group[: args.samples]):
            rec_id = id(qr)
            if rec_id in shown:
                continue
            shown.add(rec_id)
            quarantined_at = qr.get("quarantined_at", "unknown")
            record_preview = json.dumps(qr.get("record", {}), ensure_ascii=False)
            if len(record_preview) > 200:
                record_preview = record_preview[:200] + "..."
            print(f"    [{i+1}] quarantined_at={quarantined_at}")
            print(f"         errors={qr.get('schema_errors', [])}")
            print(f"         record={record_preview}")

    print(
        f"\nTo requeue corrected records: "
        f"contracts-quarantine requeue --output staging/requeue.jsonl"
    )
    print(f"To archive after review:     contracts-quarantine clear")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: requeue
# ---------------------------------------------------------------------------


def cmd_requeue(args: argparse.Namespace) -> int:
    q_path = Path(args.quarantine)
    records = _load_quarantine(q_path)

    if not records:
        print(f"No quarantined records found in {q_path}")
        return 0

    allow_set: set[str] = set(args.allow_errors) if args.allow_errors else set()
    out_path = Path(args.output)

    requeued: list[dict] = []
    skipped: list[dict] = []

    for qr in records:
        errors = set(qr.get("schema_errors", []))
        if allow_set and not errors.issubset(allow_set):
            # At least one error is not in the allow list — leave in quarantine
            skipped.append(qr)
            continue
        # Strip quarantine metadata; emit the original record
        requeued.append(qr["record"])

    if not requeued:
        print(
            "No records matched the requeue criteria.\n"
            "  All errors must appear in --allow-errors to be requeued.\n"
            f"  Run `contracts-quarantine review` to see current error types."
        )
        return 1

    _write_jsonl(out_path, requeued)
    print(f"Requeued {len(requeued)} record(s) → {out_path}")

    if skipped:
        # Overwrite the quarantine file with only the skipped (still-bad) records
        if not args.dry_run:
            _write_jsonl(q_path, skipped)
            print(
                f"Quarantine updated: {len(skipped)} record(s) remain "
                f"(errors not in allow-list)"
            )
        else:
            print(
                f"[dry-run] Would leave {len(skipped)} record(s) in quarantine."
            )
    else:
        if not args.dry_run:
            # All records requeued — clear the quarantine file
            _write_jsonl(q_path, [])
            print("Quarantine emptied (all records requeued).")
        else:
            print("[dry-run] Would empty the quarantine file.")

    print(
        f"\nNext step: review {out_path}, fix any issues, then re-run:\n"
        f"  contracts-ai-checks --extractions {out_path} ..."
    )
    return 0


# ---------------------------------------------------------------------------
# Subcommand: clear
# ---------------------------------------------------------------------------


def cmd_clear(args: argparse.Namespace) -> int:
    q_path = Path(args.quarantine)

    if not q_path.exists():
        print(f"Nothing to clear — {q_path} does not exist.")
        return 0

    records = _load_quarantine(q_path)
    if not records:
        print(f"Quarantine file is already empty: {q_path}")
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_name = q_path.stem + f"_{ts}" + q_path.suffix
    archive_path = ARCHIVE_DIR / archive_name

    if args.dry_run:
        print(
            f"[dry-run] Would archive {len(records)} record(s):\n"
            f"  {q_path} → {archive_path}"
        )
        return 0

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.move(str(q_path), str(archive_path))
    print(
        f"Archived {len(records)} quarantined record(s):\n"
        f"  {q_path} → {archive_path}"
    )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Review and retry quarantined prompt-schema violations.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--quarantine",
        default=str(DEFAULT_QUARANTINE_PATH),
        metavar="PATH",
        help=f"Path to the quarantine JSONL file (default: {DEFAULT_QUARANTINE_PATH}).",
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # review
    p_review = sub.add_parser(
        "review",
        help="Print a summary of quarantined records grouped by error type.",
    )
    p_review.add_argument(
        "--samples",
        type=int,
        default=2,
        metavar="N",
        help="Number of sample records to show per error type (default: 2).",
    )

    # requeue
    p_requeue = sub.add_parser(
        "requeue",
        help=(
            "Write original records to a staging file for reprocessing. "
            "Use --allow-errors to control which error categories are requeued."
        ),
    )
    p_requeue.add_argument(
        "--output",
        required=True,
        metavar="PATH",
        help="Path for the requeue staging JSONL.",
    )
    p_requeue.add_argument(
        "--allow-errors",
        nargs="*",
        metavar="MSG",
        help=(
            "Only requeue records whose errors are all in this list. "
            "Omit to requeue every record regardless of error."
        ),
    )
    p_requeue.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would be requeued without writing files.",
    )

    # clear
    p_clear = sub.add_parser(
        "clear",
        help=(
            "Archive the quarantine file to quarantine/archive/ with a timestamp suffix."
        ),
    )
    p_clear.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would be archived without moving files.",
    )

    args = parser.parse_args(argv)

    dispatch = {
        "review": cmd_review,
        "requeue": cmd_requeue,
        "clear": cmd_clear,
    }
    return dispatch[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
