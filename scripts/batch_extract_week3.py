"""Batch extract Week 3 PDFs and rebuild the Week 7 Week 3 JSONL."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_WEEK3_REPO = Path(r"D:\TRP-1\Week-3\document-refinery")
DEFAULT_WEEK3_DATA_DIR = DEFAULT_WEEK3_REPO / "data"
DEFAULT_WEEK3_EXTRACTED_DIR = DEFAULT_WEEK3_REPO / ".refinery" / "extracted"
DEFAULT_WEEK3_PYTHON = DEFAULT_WEEK3_REPO / "venv" / "Scripts" / "python.exe"
DEFAULT_SKIP_NAMES = {
    "audit report - 2023.pdf",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch extract Week 3 PDFs.")
    parser.add_argument("--week3-repo", type=Path, default=DEFAULT_WEEK3_REPO)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_WEEK3_DATA_DIR)
    parser.add_argument("--extracted-dir", type=Path, default=DEFAULT_WEEK3_EXTRACTED_DIR)
    parser.add_argument("--week3-python", type=Path, default=DEFAULT_WEEK3_PYTHON)
    parser.add_argument(
        "--include-prefix",
        action="append",
        default=[],
        help="Only process PDFs whose filename starts with this prefix. Repeatable.",
    )
    parser.add_argument(
        "--skip-name",
        action="append",
        default=[],
        help="Skip a PDF by filename. Repeatable.",
    )
    parser.add_argument("--output", type=Path, default=Path("outputs/week3/extractions.jsonl"))
    parser.add_argument("--log-file", type=Path, default=Path("outputs/week3/batch_extract.log"))
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_path(path: str | Path) -> str:
    return Path(path).resolve().as_posix().lower()


def build_existing_index(extracted_dir: Path) -> dict[str, Path]:
    index: dict[str, Path] = {}
    if not extracted_dir.exists():
        return index
    for json_path in extracted_dir.glob("*.json"):
        if json_path.name.endswith(".routing.json"):
            continue
        try:
            payload = read_json(json_path)
        except Exception:
            continue
        source_path = payload.get("file_path")
        if source_path:
            index[normalize_path(source_path)] = json_path
        source_name = payload.get("file_name")
        if source_name:
            index[source_name.lower()] = json_path
    return index


def run_extractor(week3_python: Path, week3_repo: Path, pdf_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(week3_python), "scripts/run_extract.py", str(pdf_path)],
        cwd=str(week3_repo),
        text=True,
        capture_output=True,
    )


def rerun_migration(output_path: Path) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parents[1]
    return subprocess.run(
        [
            sys.executable,
            "scripts/migrate_week3.py",
            "--input-dir",
            r"D:\TRP-1\Week-3\document-refinery\.refinery\extracted",
            "--output",
            str(output_path),
        ],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
    )


def main() -> int:
    args = parse_args()
    week3_repo = args.week3_repo
    data_dir = args.data_dir
    extracted_dir = args.extracted_dir
    week3_python = args.week3_python
    include_prefixes = [prefix.lower() for prefix in args.include_prefix]
    skip_names = {name.lower() for name in args.skip_name} | DEFAULT_SKIP_NAMES
    output_path = args.output
    log_file = args.log_file

    log_file.parent.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}", file=sys.stderr)
        return 1
    if not week3_python.exists():
        print(f"Week 3 Python not found: {week3_python}", file=sys.stderr)
        return 1

    existing_index = build_existing_index(extracted_dir)
    pdfs = sorted(data_dir.glob("*.pdf"))
    if include_prefixes:
        pdfs = [pdf for pdf in pdfs if any(pdf.name.lower().startswith(prefix) for prefix in include_prefixes)]

    skipped = 0
    extracted = 0
    failed = 0

    with log_file.open("a", encoding="utf-8") as log:
        log.write(f"Batch run started for {len(pdfs)} PDFs\n")
        for pdf_path in pdfs:
            if pdf_path.name.lower() in skip_names:
                skipped += 1
                log.write(f"SKIP {pdf_path.name} skip-list\n")
                continue
            source_key = normalize_path(pdf_path)
            name_key = pdf_path.name.lower()
            existing = existing_index.get(source_key) or existing_index.get(name_key)
            if existing:
                skipped += 1
                log.write(f"SKIP {pdf_path.name} already extracted -> {existing}\n")
                continue

            result = run_extractor(week3_python, week3_repo, pdf_path)
            if result.returncode != 0:
                failed += 1
                log.write(f"ERROR {pdf_path.name}\n")
                if result.stdout.strip():
                    log.write(result.stdout.rstrip() + "\n")
                if result.stderr.strip():
                    log.write(result.stderr.rstrip() + "\n")
                log.write("\n")
                continue

            extracted += 1
            log.write(f"OK {pdf_path.name}\n")
            if result.stdout.strip():
                log.write(result.stdout.rstrip() + "\n")
            log.write("\n")

        migration = rerun_migration(output_path)
        if migration.returncode != 0:
            log.write("ERROR migration failed\n")
            if migration.stdout.strip():
                log.write(migration.stdout.rstrip() + "\n")
            if migration.stderr.strip():
                log.write(migration.stderr.rstrip() + "\n")
            print("Migration failed. See log for details.", file=sys.stderr)
            return 1

    try:
        line_count = sum(1 for line in output_path.read_text(encoding="utf-8").splitlines() if line.strip())
    except FileNotFoundError:
        line_count = 0

    print(f"PDFs scanned: {len(pdfs)}")
    print(f"Skipped: {skipped}")
    print(f"Extracted: {extracted}")
    print(f"Failed: {failed}")
    print(f"Migration output: {output_path}")
    print(f"Final record count: {line_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
