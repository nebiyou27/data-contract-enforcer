#!/usr/bin/env python3
"""Migrate Week 4 lineage graphs into the Week 7 canonical JSONL format."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


RELATIONSHIP_MAP: dict[str, str] = {
    "select": "READS",
    "join": "READS",
    "cte": "READS",
    "config": "READS",
    "python_read": "READS",
    "read": "READS",
    "reads": "READS",
    "python_write": "WRITES",
    "write": "WRITES",
    "writes": "WRITES",
    "import": "IMPORTS",
    "imports": "IMPORTS",
    "call": "CALLS",
    "calls": "CALLS",
    "macro_call": "CALLS",
    "produce": "PRODUCES",
    "produces": "PRODUCES",
    "consume": "CONSUMES",
    "consumes": "CONSUMES",
}

TYPE_PRIORITY = {
    "MODEL": 0,
    "PIPELINE": 1,
    "SERVICE": 2,
    "TABLE": 3,
    "FILE": 4,
    "EXTERNAL": 5,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate Week 4 lineage graphs to Week 7 canonical JSONL.")
    parser.add_argument("--source-repo", default=r"D:\TRP-1\Week-4\brownfield-cartographer")
    parser.add_argument("--lineage-graph", default=".cartography/lineage_graph.json")
    parser.add_argument("--module-graph", default=".cartography/module_graph.json")
    parser.add_argument("--output", default="outputs/week4/lineage_snapshots.jsonl")
    return parser.parse_args()


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().replace("\\", "/")


def is_path_like(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if not text:
        return False
    return (
        ":" in text[:4]
        or text.startswith("/")
        or text.startswith("\\")
        or text.endswith(".sql")
        or text.endswith(".yml")
        or text.endswith(".yaml")
    )


def safe_read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_graph(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return safe_read_json(path)


def infer_source_root(graphs: Iterable[dict[str, Any]]) -> Path | None:
    candidates: list[str] = []
    for graph in graphs:
        for node in graph.get("nodes") or []:
            for key in ("source_file", "id"):
                raw = node.get(key)
                if isinstance(raw, str) and is_path_like(raw):
                    candidates.append(os.path.normpath(raw))
        for edge in graph.get("edges") or graph.get("links") or []:
            for key in ("source_dataset", "target_dataset", "source_file"):
                raw = edge.get(key)
                if isinstance(raw, str) and is_path_like(raw):
                    candidates.append(os.path.normpath(raw))

    absolute_candidates = [candidate for candidate in candidates if Path(candidate).is_absolute()]
    if not absolute_candidates:
        return None

    try:
        common = os.path.commonpath(absolute_candidates)
    except ValueError:
        return None

    return Path(common)


def try_relativize(path_text: str, source_root: Path | None) -> str:
    normalized = normalize_text(path_text)
    if not normalized:
        return normalized

    candidate = Path(os.path.normpath(normalized))
    if source_root and candidate.is_absolute():
        try:
            return normalize_text(candidate.relative_to(source_root))
        except ValueError:
            return normalize_text(candidate)
    return normalized


def path_exists(path_text: str, source_root: Path | None, source_repo: Path) -> Path | None:
    candidate = Path(os.path.normpath(path_text))
    if candidate.exists():
        return candidate
    if source_root and not candidate.is_absolute():
        rooted = source_root / candidate
        if rooted.exists():
            return rooted
    if not candidate.is_absolute():
        repo_candidate = source_repo / candidate
        if repo_candidate.exists():
            return repo_candidate
    return None


def iso_from_mtime(path: Path | None, fallback: Path) -> str:
    timestamp_path = path if path and path.exists() else fallback
    return datetime.fromtimestamp(timestamp_path.stat().st_mtime, tz=timezone.utc).isoformat()


def infer_language(path_text: str) -> str:
    lower = normalize_text(path_text).lower()
    if lower.endswith(".sql"):
        return "sql"
    if lower.endswith(".yml") or lower.endswith(".yaml"):
        return "yaml"
    return "unknown"


def infer_node_type(node: dict[str, Any], canonical_path: str, source_graph: str) -> str:
    node_id = normalize_text(node.get("id")).lower()
    logical_name = normalize_text(node.get("logical_name")).lower()
    file_type = normalize_text(node.get("file_type")).lower()
    dataset_type = normalize_text(node.get("dataset_type")).lower()
    description = normalize_text(node.get("description")).lower()

    if "external" in dataset_type or "external" in description:
        return "EXTERNAL"
    if (
        node_id.startswith("macro:")
        or logical_name.startswith("default__")
        or logical_name.startswith("macro_")
        or "/macros/" in canonical_path
        or canonical_path.startswith("macros/")
    ):
        return "PIPELINE"
    if file_type in {"yaml", "yml"} or canonical_path.endswith(".yml") or canonical_path.endswith(".yaml"):
        return "FILE"
    if dataset_type == "sql_file" or canonical_path.endswith(".sql"):
        if source_graph == "module" and node_id.startswith("macro:"):
            return "PIPELINE"
        return "MODEL"
    if any(logical_name.startswith(prefix) for prefix in ("stg_", "int_", "fct_", "dim_", "base_")):
        return "MODEL"
    if any(token in logical_name or token in node_id for token in ("relationship", "source", "column", "metric", "exposure")):
        return "TABLE"
    if canonical_path:
        return "FILE"
    return "TABLE"


def canonical_label(node: dict[str, Any], canonical_path: str) -> str:
    for key in ("logical_name", "description"):
        value = normalize_text(node.get(key))
        if value:
            return value
    if canonical_path:
        stem = Path(canonical_path).stem
        if stem:
            return stem
    return normalize_text(node.get("id")) or "unknown"


def canonical_purpose(node: dict[str, Any]) -> str:
    for key in ("description", "logical_name"):
        value = normalize_text(node.get(key))
        if value:
            return value
    return ""


def type_rank(node_type: str) -> int:
    return TYPE_PRIORITY.get(node_type, 99)


def canonical_node_id(node_type: str, canonical_path: str) -> str:
    return f"{node_type.lower()}::{canonical_path}"


def build_aliases(node: dict[str, Any], canonical_path: str) -> list[str]:
    aliases: list[str] = []
    for key in ("id", "source_file", "logical_name"):
        value = normalize_text(node.get(key))
        if value:
            aliases.append(value)
    if canonical_path:
        aliases.append(canonical_path)
        aliases.append(Path(canonical_path).name)
        aliases.append(Path(canonical_path).stem)
    return [alias.replace("\\", "/") for alias in aliases if alias]


def choose_existing(preferred: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if preferred is None:
        return candidate
    preferred_type = preferred["type"]
    candidate_type = candidate["type"]
    if type_rank(candidate_type) < type_rank(preferred_type):
        return candidate
    if type_rank(candidate_type) > type_rank(preferred_type):
        return preferred
    preferred_path = preferred["metadata"].get("path", "")
    candidate_path = candidate["metadata"].get("path", "")
    if len(candidate_path) < len(preferred_path):
        return candidate
    return preferred


def collect_graph_nodes(
    graph: dict[str, Any],
    source_graph: str,
    source_repo: Path,
    source_root: Path | None,
    fallback_graph_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    alias_map: dict[str, str] = {}
    chosen: dict[str, dict[str, Any]] = {}

    for node in graph.get("nodes") or []:
        raw_path = normalize_text(node.get("source_file") or node.get("id"))
        canonical_path = try_relativize(raw_path, source_root)
        if not canonical_path and normalize_text(node.get("id")):
            canonical_path = normalize_text(node.get("id"))

        node_type = infer_node_type(node, canonical_path, source_graph)
        node_id = canonical_node_id(node_type, canonical_path or normalize_text(node.get("id")))
        metadata_path = canonical_path or normalize_text(node.get("source_file") or node.get("id"))
        source_file_path = normalize_text(node.get("source_file"))
        existing_path = path_exists(metadata_path or source_file_path, source_root, source_repo)

        canonical = {
            "node_id": node_id,
            "type": node_type,
            "label": canonical_label(node, canonical_path),
            "metadata": {
                "path": metadata_path,
                "language": infer_language(metadata_path or source_file_path),
                "purpose": canonical_purpose(node),
                "last_modified": iso_from_mtime(existing_path, fallback_graph_path),
            },
        }

        current = chosen.get(node_id)
        chosen[node_id] = choose_existing(current, canonical)

        for alias in build_aliases(node, canonical_path):
            alias_map[alias] = node_id

    return list(chosen.values()), alias_map


def build_edge_lookup_key(value: Any, source_root: Path | None) -> str:
    text = normalize_text(value)
    if not text:
        return text
    return try_relativize(text, source_root)


def resolve_node_id(value: Any, alias_map: dict[str, str], source_root: Path | None) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return normalized
    candidates = [
        normalized,
        normalized.replace("\\", "/"),
        build_edge_lookup_key(normalized, source_root),
        Path(normalized).name,
        Path(normalized).stem,
    ]
    for candidate in candidates:
        if candidate in alias_map:
            return alias_map[candidate]
    return canonical_node_id("EXTERNAL", build_edge_lookup_key(normalized, source_root) or normalized)


def relationship_for(edge: dict[str, Any]) -> str:
    raw = normalize_text(edge.get("transformation_type")).lower()
    return RELATIONSHIP_MAP.get(raw, "READS")


def build_edges(graph: dict[str, Any], alias_map: dict[str, str], source_root: Path | None) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    raw_edges = graph.get("edges") or graph.get("links") or []
    for edge in raw_edges:
        source_id = resolve_node_id(edge.get("source_dataset"), alias_map, source_root)
        target_id = resolve_node_id(edge.get("target_dataset"), alias_map, source_root)
        confidence = edge.get("confidence")
        if not isinstance(confidence, (int, float)):
            confidence = 0.0
        edges.append(
            {
                "source": source_id,
                "target": target_id,
                "relationship": relationship_for(edge),
                "confidence": round(max(0.0, min(1.0, float(confidence))), 3),
            }
        )
    return edges


def read_git_commit(repo_root: Path) -> str:
    result = subprocess.run(["git", "-C", str(repo_root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    commit = result.stdout.strip()
    if len(commit) != 40:
        raise ValueError(f"Unexpected git commit hash: {commit!r}")
    return commit


def stable_snapshot_id(repo_root: Path, git_commit: str, node_count: int, edge_count: int) -> str:
    seed = f"{repo_root.as_posix()}::{git_commit}::{node_count}::{edge_count}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def migrate(source_repo: Path, output_path: Path, lineage_rel: str, module_rel: str) -> dict[str, Any]:
    lineage_path = source_repo / lineage_rel
    module_path = source_repo / module_rel

    lineage_graph = load_graph(lineage_path)
    if lineage_graph is None:
        raise FileNotFoundError(f"Missing lineage graph: {lineage_path}")
    module_graph = load_graph(module_path)

    source_root = infer_source_root([graph for graph in (lineage_graph, module_graph) if graph])

    lineage_nodes, lineage_aliases = collect_graph_nodes(lineage_graph, "lineage", source_repo, source_root, lineage_path)
    module_nodes: list[dict[str, Any]] = []
    module_aliases: dict[str, str] = {}
    if module_graph:
        module_nodes, module_aliases = collect_graph_nodes(module_graph, "module", source_repo, source_root, module_path)

    node_map: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for node in lineage_nodes + module_nodes:
        current = node_map.get(node["node_id"])
        node_map[node["node_id"]] = choose_existing(current, node)

    alias_map = {**module_aliases, **lineage_aliases}
    edges = build_edges(lineage_graph, alias_map, source_root)

    git_commit = read_git_commit(source_repo)
    snapshot = {
        "snapshot_id": stable_snapshot_id(source_repo, git_commit, len(node_map), len(edges)),
        "codebase_root": normalize_text(source_repo),
        "git_commit": git_commit,
        "nodes": list(node_map.values()),
        "edges": edges,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(snapshot, ensure_ascii=False))
        handle.write("\n")
    return snapshot


def main() -> int:
    args = parse_args()
    source_repo = Path(args.source_repo)
    output_path = Path(args.output)

    try:
        snapshot = migrate(source_repo, output_path, args.lineage_graph, args.module_graph)
    except Exception as exc:  # pragma: no cover - CLI surface
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote 1 snapshot to {output_path} ({len(snapshot['nodes'])} nodes, {len(snapshot['edges'])} edges)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

