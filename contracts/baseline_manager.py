#!/usr/bin/env python3
"""
contracts/baseline_manager.py -- Baseline Promotion Manager

Manage statistical baselines used by drift checks in the validation runner.
Baselines are stored in schema_snapshots/baselines.json.

The runner NEVER overwrites baselines unless --promote-baselines is passed.
Use this tool to promote, inspect, or clear baselines independently of a
validation run.

Subcommands
-----------
  list     Show all baseline keys with per-column stats summaries.
  promote  Copy numeric stats from a saved validation report into baselines.json.
           This is equivalent to re-running with --promote-baselines but does not
           require re-executing validation against live data.
  clear    Remove baselines for one contract (or all contracts).

Examples
--------
  # See what baselines exist
  contracts-baseline list

  # Promote baselines from a previously-written validation report
  contracts-baseline promote --report validation_reports/week3_baseline.json

  # Remove baselines for one contract without touching others
  contracts-baseline clear --contract week3-document-refinery-extractions

  # Wipe everything (requires --yes to prevent accidents)
  contracts-baseline clear --all --yes
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASELINES_PATH = Path("schema_snapshots") / "baselines.json"


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _load_baselines() -> dict:
    if BASELINES_PATH.exists():
        with open(BASELINES_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def _save_baselines(baselines: dict) -> None:
    BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BASELINES_PATH, "w", encoding="utf-8") as fh:
        json.dump(baselines, fh, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Subcommand: list
# ---------------------------------------------------------------------------


def cmd_list(args: argparse.Namespace) -> int:  # noqa: ARG001
    baselines = _load_baselines()
    if not baselines:
        print(f"No baselines found in {BASELINES_PATH}")
        return 0

    print(f"Baselines in {BASELINES_PATH}  ({len(baselines)} table(s))\n")
    for table_key, cols in sorted(baselines.items()):
        print(f"  {table_key}  ({len(cols)} column(s))")
        for col, stats in sorted(cols.items()):
            mean = stats.get("mean", "n/a")
            stddev = stats.get("stddev", "n/a")
            count = stats.get("count", "n/a")
            print(f"    {col:35s}  mean={mean:<14}  stddev={stddev:<14}  n={count}")
        print()
    return 0


# ---------------------------------------------------------------------------
# Subcommand: promote
# ---------------------------------------------------------------------------


def _extract_stats_from_report(report: dict) -> dict[str, dict]:
    """Pull numeric column stats out of a validation report.

    The runner writes per-column stats into every result entry as `actual_value`
    strings when drift checks run, but the authoritative source is the full
    statistics block embedded in drift check results.  We reconstruct it by
    re-parsing the actual_value field from drift_mean results, which carry
    "mean=X (z=Y)" strings — but that's fragile.

    A cleaner path: the runner already computes stats and stores the new baselines
    in memory; if the report was produced by --promote-baselines or is a first-run
    report, we can trust its drift results to infer stats.  However, the most
    reliable source is to re-read the report's `results` list and pull out
    drift_mean entries, then reconstruct per-column stats from them.

    For reports where no drift was run (first-run or no numeric columns) this
    function returns an empty dict and the caller warns the user.
    """
    contract_id = report.get("contract_id", "unknown")
    results = report.get("results", [])

    # Gather all check_ids that look like drift checks to identify table+column pairs.
    # Also harvest min/max from range checks.
    table_col_stats: dict[str, dict[str, dict]] = {}

    for r in results:
        check_type = r.get("check_type", "")
        check_id = r.get("check_id", "")
        col = r.get("column_name", "")
        if not col or not check_id:
            continue

        # Infer table name from check_id: "<table>.<col>.<check_type>"
        parts = check_id.split(".")
        if len(parts) < 3:
            continue
        table = parts[0]
        table_key = f"{contract_id}/{table}"

        if table_key not in table_col_stats:
            table_col_stats[table_key] = {}
        if col not in table_col_stats[table_key]:
            table_col_stats[table_key][col] = {}

        entry = table_col_stats[table_key][col]

        # drift_mean carries "mean=X (z=Y)" in actual_value
        if check_type == "drift_mean":
            av = r.get("actual_value", "")
            try:
                mean_part = av.split("(")[0].strip()  # "mean=X"
                entry["mean"] = float(mean_part.split("=")[1])
            except (IndexError, ValueError):
                pass
            # expected carries "baseline mean=B ± S"
            ev = r.get("expected", "")
            try:
                after_pm = ev.split("±")
                if len(after_pm) == 2:
                    entry["stddev"] = float(after_pm[1].strip())
                    entry["mean_baseline"] = float(
                        after_pm[0].replace("baseline mean=", "").strip()
                    )
            except (IndexError, ValueError):
                pass

        # drift_null_fraction carries "null_frac=X" in actual_value
        if check_type == "drift_null_fraction":
            av = r.get("actual_value", "")
            try:
                frac = float(av.split("=")[1].split()[0])
                entry["null_fraction"] = frac
            except (IndexError, ValueError):
                pass

        # drift_cardinality carries "cardinality=X" in actual_value
        if check_type == "drift_cardinality":
            av = r.get("actual_value", "")
            try:
                card = int(av.split("=")[1].split()[0])
                entry["cardinality"] = card
            except (IndexError, ValueError):
                pass

    # Remove tables where no stats were harvested
    return {
        tk: {c: s for c, s in cols.items() if s}
        for tk, cols in table_col_stats.items()
        if any(s for s in cols.values())
    }


def cmd_promote(args: argparse.Namespace) -> int:
    report_path = Path(args.report)
    if not report_path.exists():
        print(f"ERROR: report file not found: {report_path}", file=sys.stderr)
        return 1

    with open(report_path, "r", encoding="utf-8") as fh:
        report = json.load(fh)

    contract_id = report.get("contract_id", "unknown")
    snapshot_id = report.get("snapshot_id", "unknown")[:12]

    new_stats = _extract_stats_from_report(report)
    if not new_stats:
        print(
            f"WARNING: no numeric column stats found in {report_path}.\n"
            "  This report may be a first-run report with no drift checks, or may\n"
            "  contain only structural checks.  Nothing was promoted.",
            file=sys.stderr,
        )
        return 1

    baselines = _load_baselines()
    promoted: list[str] = []
    for table_key, cols in new_stats.items():
        if table_key not in baselines:
            baselines[table_key] = {}
        baselines[table_key].update(cols)
        promoted.append(f"{table_key} ({len(cols)} column(s))")

    _save_baselines(baselines)
    print(
        f"Promoted baselines from {report_path}\n"
        f"  contract : {contract_id}\n"
        f"  snapshot : {snapshot_id}...\n"
        f"  tables   : {len(promoted)}"
    )
    for line in promoted:
        print(f"    {line}")
    print(f"\nBaselines written to {BASELINES_PATH}")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: clear
# ---------------------------------------------------------------------------


def cmd_clear(args: argparse.Namespace) -> int:
    baselines = _load_baselines()
    if not baselines:
        print(f"No baselines to clear in {BASELINES_PATH}")
        return 0

    if args.all:
        if not args.yes:
            print(
                "ERROR: --all removes every baseline. Pass --yes to confirm.",
                file=sys.stderr,
            )
            return 1
        count = len(baselines)
        baselines = {}
        _save_baselines(baselines)
        print(f"Cleared all {count} baseline table(s) from {BASELINES_PATH}")
        return 0

    if not args.contract:
        print(
            "ERROR: specify --contract <id> or use --all --yes to clear everything.",
            file=sys.stderr,
        )
        return 1

    prefix = args.contract.rstrip("/") + "/"
    keys_to_remove = [k for k in baselines if k.startswith(prefix) or k == args.contract]
    if not keys_to_remove:
        print(f"No baseline keys matching contract '{args.contract}' found.")
        return 0

    for k in keys_to_remove:
        del baselines[k]
    _save_baselines(baselines)
    print(
        f"Cleared {len(keys_to_remove)} baseline table(s) for contract '{args.contract}'"
    )
    for k in keys_to_remove:
        print(f"  removed: {k}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Manage statistical baselines for drift checks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # list
    sub.add_parser(
        "list",
        help="Show all baseline keys and per-column stats summaries.",
    )

    # promote
    p_promote = sub.add_parser(
        "promote",
        help=(
            "Promote stats from a saved validation report into baselines.json "
            "without re-running validation."
        ),
    )
    p_promote.add_argument(
        "--report",
        required=True,
        metavar="PATH",
        help="Path to a validation report JSON produced by contracts-run.",
    )

    # clear
    p_clear = sub.add_parser(
        "clear",
        help="Remove baselines for a specific contract or all contracts.",
    )
    p_clear.add_argument(
        "--contract",
        metavar="CONTRACT_ID",
        help="Remove baselines whose key starts with this contract id.",
    )
    p_clear.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Remove ALL baselines (requires --yes).",
    )
    p_clear.add_argument(
        "--yes",
        action="store_true",
        default=False,
        help="Confirm destructive --all operation.",
    )

    args = parser.parse_args(argv)

    dispatch = {
        "list": cmd_list,
        "promote": cmd_promote,
        "clear": cmd_clear,
    }
    return dispatch[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
