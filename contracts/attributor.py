#!/usr/bin/env python3
"""
contracts/attributor.py -- registry-first blast radius attribution helpers.

The subscription registry (contract_registry/subscriptions.yaml) is the
primary source of truth for:
  - which contracts are active vs out-of-scope (contracts catalog)
  - which downstream systems are direct subscribers (subscriptions)
  - which fields are breaking for each subscription

Lineage data is used only as enrichment when available.
Git log is called on the identified upstream producer file to build a ranked
blame chain.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

DEFAULT_REGISTRY_PATH = Path("contract_registry") / "subscriptions.yaml"


def load_registry(path: str | Path | None = None) -> dict[str, Any]:
    """Load the subscription registry and normalize the subscription list."""
    registry_path = Path(path or DEFAULT_REGISTRY_PATH)
    if not registry_path.exists():
        raise FileNotFoundError(f"Registry file not found: {registry_path}")

    with open(registry_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    subscriptions = raw.get("subscriptions") if isinstance(raw, dict) else raw
    if subscriptions is None:
        subscriptions = []
    if not isinstance(subscriptions, list):
        raise ValueError("subscriptions.yaml must contain a 'subscriptions' list")

    normalized: list[dict[str, Any]] = []
    for item in subscriptions:
        if not isinstance(item, dict):
            continue
        breaking_fields = item.get("breaking_fields") or []
        normalized.append(
            {
                **item,
                "breaking_fields": [
                    bf for bf in breaking_fields if isinstance(bf, dict) and bf.get("field")
                ],
            }
        )

    # Contracts catalog — lists every contract with status (active / out_of_scope)
    contracts: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        for entry in raw.get("contracts") or []:
            if isinstance(entry, dict):
                contracts.append(entry)

    # Schema-evolution policy declared in the registry header
    policy: dict[str, Any] = {}
    if isinstance(raw, dict):
        policy = raw.get("registry", {}).get("schema_evolution_policy") or {}

    return {
        "path": str(registry_path),
        "subscriptions": normalized,
        "contracts": contracts,
        "schema_evolution_policy": policy,
    }


def load_lineage_graph(path: str | Path | None) -> dict[str, Any]:
    """Load a lineage snapshot JSONL file, if present."""
    if not path:
        return {"path": None, "nodes": [], "edges": []}

    lineage_path = Path(path)
    if not lineage_path.exists():
        return {"path": str(lineage_path), "nodes": [], "edges": []}

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    with open(lineage_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            nodes.extend(record.get("nodes") or [])
            edges.extend(record.get("edges") or [])

    return {
        "path": str(lineage_path),
        "nodes": nodes,
        "edges": edges,
    }


def get_contract_status(contract_id: str, registry: dict[str, Any]) -> str:
    """Return the catalog status for a contract_id ('active', 'out_of_scope', or 'unknown')."""
    for entry in registry.get("contracts", []):
        if entry.get("id") == contract_id:
            return entry.get("status", "unknown")
    return "unknown"


def contract_source_label(contract_id: str) -> str:
    """Map a contract identifier to a human-readable source label."""
    lowered = contract_id.lower()
    if "week3" in lowered:
        return "Week 3"
    if "week4" in lowered:
        return "Week 4"
    if "week5" in lowered:
        return "Week 5"
    if "langsmith" in lowered:
        return "LangSmith"
    return contract_id


def _find_producer_file(contract_id: str, registry: dict[str, Any]) -> str | None:
    """Look up the source data path for a contract from the registry catalog."""
    for entry in registry.get("contracts", []):
        if entry.get("id") == contract_id:
            return entry.get("data_path")
    return None


def _run_git_log(file_path: str, n: int = 20) -> list[dict[str, str]]:
    """Run git log on a file and return parsed commit records.

    Each record contains: commit_hash, author, commit_timestamp, commit_message.
    Returns an empty list if git is unavailable or the file has no history.
    """
    try:
        result = subprocess.run(
            [
                "git", "log",
                "--follow",
                f"-n{n}",
                "--format=%H|%an|%aI|%s",
                "--",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        commits: list[dict[str, str]] = []
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|", 3)
            if len(parts) >= 4:
                commits.append(
                    {
                        "commit_hash": parts[0],
                        "author": parts[1],
                        "commit_timestamp": parts[2],
                        "commit_message": parts[3],
                    }
                )
        return commits
    except Exception:
        return []


def _build_blame_chain(
    commits: list[dict[str, str]],
    lineage_hops: int = 0,
    max_candidates: int = 5,
) -> list[dict[str, Any]]:
    """Rank git commits and build a blame chain.

    Confidence score formula:
        base  = 1.0 - (days_since_commit * 0.1)
        score = base - (lineage_hops * 0.2)
        score = clamp(score, 0.0, 1.0)

    Returns up to max_candidates entries sorted by confidence descending.
    """
    now = datetime.now(timezone.utc)
    candidates: list[dict[str, Any]] = []

    for commit in commits:
        try:
            ts_str = commit["commit_timestamp"]
            commit_dt = datetime.fromisoformat(ts_str)
            if commit_dt.tzinfo is None:
                commit_dt = commit_dt.replace(tzinfo=timezone.utc)
            days_since = max(0.0, (now - commit_dt).total_seconds() / 86400)
        except Exception:
            days_since = 30.0  # fallback when timestamp is unparseable

        base = 1.0 - (days_since * 0.1)
        score = base - (lineage_hops * 0.2)
        score = round(max(0.0, min(1.0, score)), 4)

        candidates.append(
            {
                "commit_hash": commit["commit_hash"],
                "author": commit["author"],
                "commit_timestamp": commit["commit_timestamp"],
                "commit_message": commit["commit_message"],
                "confidence_score": score,
            }
        )

    candidates.sort(key=lambda c: c["confidence_score"], reverse=True)
    return candidates[:max_candidates]


def _registry_subscriptions_for_source(registry: dict[str, Any], source_label: str) -> list[dict[str, Any]]:
    subscriptions = registry.get("subscriptions", [])
    return [
        sub
        for sub in subscriptions
        if sub.get("source") == source_label or sub.get("source_contract") == source_label
    ]


def _reachable_targets(subscriptions: list[dict[str, Any]], source_label: str) -> dict[str, int]:
    graph: dict[str, list[str]] = {}
    for sub in subscriptions:
        src = sub.get("source")
        tgt = sub.get("target")
        if not src or not tgt:
            continue
        graph.setdefault(src, []).append(tgt)

    depths: dict[str, int] = {}
    queue: deque[tuple[str, int]] = deque([(source_label, 0)])
    seen = {source_label}

    while queue:
        node, depth = queue.popleft()
        for nxt in graph.get(node, []):
            next_depth = depth + 1
            if nxt not in depths or next_depth < depths[nxt]:
                depths[nxt] = next_depth
            if nxt not in seen:
                seen.add(nxt)
                queue.append((nxt, next_depth))

    return depths


def _enrich_with_lineage(
    lineage_graph: dict[str, Any],
    target_names: list[str],
) -> list[dict[str, Any]]:
    if not lineage_graph.get("nodes") or not target_names:
        return []

    matches: list[dict[str, Any]] = []
    lowered_targets = [target.lower() for target in target_names]
    for node in lineage_graph.get("nodes", []):
        label = str(node.get("label", ""))
        path = str(node.get("path", ""))
        label_lower = label.lower()
        path_lower = path.lower()
        if any(target in label_lower or target in path_lower for target in lowered_targets):
            matches.append(
                {
                    "node_id": node.get("node_id"),
                    "label": label,
                    "type": node.get("type"),
                    "path": path,
                }
            )

    deduped: list[dict[str, Any]] = []
    seen = set()
    for item in matches:
        marker = (item.get("node_id"), item.get("path"))
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(item)
    return deduped


def attribute_violation(
    violation: dict[str, Any],
    contract_id: str,
    registry: dict[str, Any],
    lineage_graph: dict[str, Any] | None = None,
    snapshot_id: str = "",
) -> dict[str, Any]:
    """Attach blast-radius and blame-chain metadata to a validation result.

    Output fields:
        violation_id    — deterministic UUID5 derived from contract_id + snapshot_id + check_id,
                          stable across re-runs on the same data so downstream tooling can
                          deduplicate and correlate without producing phantom duplicates
        check_id        — from the original validation result
        detected_at     — ISO 8601 timestamp
        blame_chain     — up to 5 ranked git commits for the upstream producer file
        blast_radius    — affected subscribers and lineage nodes with contamination_depth
        (plus all original violation fields)
    """
    now = datetime.now(timezone.utc).isoformat()
    source_label = contract_source_label(contract_id)

    # --- Registry traversal ------------------------------------------------
    direct = _registry_subscriptions_for_source(registry, source_label)
    if not direct:
        direct = _registry_subscriptions_for_source(registry, contract_id)

    registry_subscriptions = registry.get("subscriptions", [])
    depth_map = _reachable_targets(registry_subscriptions, source_label)
    contamination_depth = max(depth_map.values(), default=0)

    direct_subscribers = [
        {
            "target": sub.get("target"),
            "target_contract": sub.get("target_contract"),
            "breaking_fields": sub.get("breaking_fields", []),
        }
        for sub in direct
    ]

    downstream_target_names = list(depth_map.keys())
    lineage_nodes = _enrich_with_lineage(lineage_graph or {}, downstream_target_names)

    # --- Git blame chain ---------------------------------------------------
    producer_file = _find_producer_file(contract_id, registry)
    lineage_hops = contamination_depth  # use registry depth as hop count proxy
    if producer_file:
        commits = _run_git_log(producer_file, n=20)
    else:
        commits = []

    blame_chain = _build_blame_chain(commits, lineage_hops=lineage_hops)

    # --- Assemble output ---------------------------------------------------
    enriched = dict(violation)
    enriched.update(
        {
            "violation_id": str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{contract_id}:{snapshot_id}:{violation.get('check_id', '')}",
            )),
            "check_id": violation.get("check_id", ""),
            "detected_at": now,
            "blame_chain": blame_chain,
            "blast_radius": {
                "direct_subscribers": direct_subscribers,
                "downstream_pipelines": downstream_target_names,
                "lineage_nodes": lineage_nodes,
                "contamination_depth": contamination_depth,
            },
            "producer_file": producer_file,
            "note": (
                f"Registry-first attribution for {contract_id}: "
                f"{len(direct_subscribers)} direct subscribers; "
                f"{len(blame_chain)} blame candidates from git log; "
                f"lineage enrichment returned {len(lineage_nodes)} downstream nodes."
            ),
        }
    )
    return enriched
