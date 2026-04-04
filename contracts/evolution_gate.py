#!/usr/bin/env python3
"""
contracts/evolution_gate.py -- Producer Evolution Gate

Compares a contract YAML at two git refs and blocks a push when removing a
field that downstream subscribers have declared as breaking in the registry.

Designed to be called from a pre-push git hook, but can also be run manually.

Exit codes
----------
  0  PASS  — no breaking removals, push may proceed
  1  BLOCK — one or more breaking fields would be removed; push is blocked
  2  ERROR — could not parse contract or reach git

Usage (manual)
--------------
  # Check working tree against the last published commit
  contracts-evolution-gate --contract generated_contracts/week3-...yaml

  # Check a specific proposed ref against a specific current ref
  contracts-evolution-gate \\
      --contract generated_contracts/week3-...yaml \\
      --proposed-ref abc1234 \\
      --current-ref origin/main

Usage (hook) -- called automatically by .git/hooks/pre-push
  See scripts/install_hooks.sh
"""

from __future__ import annotations

import argparse
import io
import subprocess
import sys
from pathlib import Path

import yaml

try:
    from contracts.attributor import DEFAULT_REGISTRY_PATH, load_registry
    from contracts.runner import check_producer_evolution_gate
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from contracts.attributor import DEFAULT_REGISTRY_PATH, load_registry
    from contracts.runner import check_producer_evolution_gate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _git_show(ref: str, path: str) -> str | None:
    """Return file content at a given git ref, or None if not found."""
    try:
        result = subprocess.run(
            ["git", "show", f"{ref}:{path}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    except subprocess.CalledProcessError:
        return None


def _read_contract(source: str) -> dict:
    """Load YAML from a file path or raw string content."""
    if source.startswith("---") or "\n" in source[:50]:
        return yaml.safe_load(io.StringIO(source))
    with open(source, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _field_names(contract: dict) -> list[str]:
    """Extract all field names across all tables from a Bitol contract."""
    names: list[str] = []
    for table in contract.get("schema", {}).get("tables", []):
        for field in table.get("fields", []):
            if name := field.get("name"):
                names.append(name)
    return names


def _repo_root() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Core check
# ---------------------------------------------------------------------------


def run_gate(
    contract_path: str,
    proposed_ref: str | None,
    current_ref: str,
    registry_path: str,
) -> int:
    """Compare proposed vs current schema and run the evolution gate.

    Returns 0 (PASS), 1 (BLOCK), or 2 (ERROR).
    """
    registry = load_registry(registry_path)

    # Resolve relative path to repo-root-relative for git show
    repo_root = _repo_root()
    abs_path = Path(contract_path).resolve()
    try:
        rel_path = str(abs_path.relative_to(repo_root)).replace("\\", "/")
    except ValueError:
        rel_path = contract_path.replace("\\", "/")

    # Load proposed (current working tree or specified ref)
    if proposed_ref:
        proposed_content = _git_show(proposed_ref, rel_path)
        if proposed_content is None:
            print(
                f"SKIP  {rel_path}: not found at ref '{proposed_ref}' "
                "(new file — no breaking removals possible)",
                file=sys.stderr,
            )
            return 0
        try:
            proposed_contract = yaml.safe_load(io.StringIO(proposed_content))
        except yaml.YAMLError as exc:
            print(f"ERROR parsing {rel_path} at {proposed_ref}: {exc}", file=sys.stderr)
            return 2
    else:
        try:
            with open(contract_path, "r", encoding="utf-8") as fh:
                proposed_contract = yaml.safe_load(fh)
        except (OSError, yaml.YAMLError) as exc:
            print(f"ERROR reading {contract_path}: {exc}", file=sys.stderr)
            return 2

    # Load current (published) schema
    current_content = _git_show(current_ref, rel_path)
    if current_content is None:
        print(
            f"SKIP  {rel_path}: not found at ref '{current_ref}' "
            "(new contract — no breaking removals possible)",
        )
        return 0

    try:
        current_contract = yaml.safe_load(io.StringIO(current_content))
    except yaml.YAMLError as exc:
        print(f"ERROR parsing {rel_path} at {current_ref}: {exc}", file=sys.stderr)
        return 2

    proposed_fields = _field_names(proposed_contract)
    current_fields = _field_names(current_contract)
    contract_id = proposed_contract.get("id") or current_contract.get("id") or rel_path

    result = check_producer_evolution_gate(
        proposed_fields, current_fields, contract_id, registry
    )

    action = result["action"]
    reason = result["reason"]

    if action == "PASS":
        print(f"PASS  {rel_path}: {reason}")
        return 0

    # BLOCK
    print(f"\n{'='*70}")
    print(f"BLOCK {rel_path}")
    print(f"  {reason}")
    print()
    for bf in result.get("breaking_fields_affected", []):
        print(
            f"  field      : {bf['field']}\n"
            f"  subscriber : {bf['subscriber']}\n"
            f"  reason     : {bf['reason']}\n"
        )
    print("Resolve the breaking change before pushing.")
    print(f"{'='*70}\n")
    return 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check a contract schema change against the subscriber registry.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--contract",
        required=True,
        metavar="PATH",
        help="Path to the contract YAML to check.",
    )
    parser.add_argument(
        "--proposed-ref",
        metavar="REF",
        default=None,
        help=(
            "Git ref for the proposed schema. "
            "Defaults to the working-tree file (HEAD + uncommitted changes)."
        ),
    )
    parser.add_argument(
        "--current-ref",
        metavar="REF",
        default="HEAD",
        help=(
            "Git ref for the currently published schema. "
            "Defaults to HEAD. "
            "In a pre-push hook use the remote sha passed by git."
        ),
    )
    parser.add_argument(
        "--registry",
        metavar="PATH",
        default=str(DEFAULT_REGISTRY_PATH),
        help=f"Path to subscriptions registry YAML (default: {DEFAULT_REGISTRY_PATH}).",
    )
    args = parser.parse_args(argv)

    return run_gate(
        contract_path=args.contract,
        proposed_ref=args.proposed_ref,
        current_ref=args.current_ref,
        registry_path=args.registry,
    )


if __name__ == "__main__":
    sys.exit(main())
