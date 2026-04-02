#!/usr/bin/env python3
"""
contracts/attributor.py -- registry-first blast radius attribution helpers.

The subscription registry is the primary source of truth for direct subscribers.
Lineage data is used only as enrichment when available.
"""

from __future__ import annotations

import json
from collections import deque
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

    return {
        "path": str(registry_path),
        "subscriptions": normalized,
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
) -> dict[str, Any]:
    """Attach blast-radius metadata to a validation result."""
    source_label = contract_source_label(contract_id)
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

    note = (
        f"Registry-first attribution for {contract_id}: "
        f"{len(direct_subscribers)} direct subscribers; "
        f"lineage enrichment returned {len(lineage_nodes)} downstream nodes."
    )

    enriched = dict(violation)
    enriched.update(
        {
            "direct_subscribers": direct_subscribers,
            "downstream_nodes_from_lineage": lineage_nodes,
            "contamination_depth": contamination_depth,
            "note": note,
        }
    )
    return enriched
