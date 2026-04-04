import unittest
from pathlib import Path

from contracts.attributor import attribute_violation, get_contract_status, load_registry


# ---------------------------------------------------------------------------
# Minimal in-memory registry fixture
# ---------------------------------------------------------------------------

REGISTRY: dict = {
    "path": "contract_registry/subscriptions.yaml",
    "subscriptions": [
        {
            "source": "Week 3",
            "source_contract": "week3-document-refinery-extractions",
            "target": "Week 4",
            "target_contract": "week4-lineage-graph",
            "breaking_fields": [
                {
                    "field": "extracted_facts.confidence",
                    "reason": "Week 4 lineage quality checks rely on confidence semantics.",
                },
                {
                    "field": "documents.fact_count",
                    "reason": "Week 4 depends on extraction completeness.",
                },
            ],
        },
        {
            "source": "Week 4",
            "source_contract": "week4-lineage-graph",
            "target": "Week 7",
            "target_contract": "week7-trust-boundary",
            "breaking_fields": [
                {
                    "field": "lineage_nodes.path",
                    "reason": "Week 7 needs stable node paths.",
                },
            ],
        },
    ],
}

VIOLATION_STUB: dict = {
    "check_id": "extracted_facts.confidence.range",
    "column_name": "confidence",
    "check_type": "range",
    "status": "FAIL",
    "actual_value": "44889 out of range",
    "expected": "[0.0, 1.0]",
    "severity": "HIGH",
    "records_failing": 44889,
    "sample_failing": [84.3, 81.2, 96.6],
    "message": "confidence: 44889 values outside [0.0, 1.0]",
}


class AttributeViolationTest(unittest.TestCase):
    def _attributed(self, contract_id: str = "week3-document-refinery-extractions") -> dict:
        return attribute_violation(VIOLATION_STUB, contract_id, REGISTRY)

    # ------------------------------------------------------------------
    # Required output fields are present
    # ------------------------------------------------------------------

    def test_required_top_level_fields_present(self) -> None:
        result = self._attributed()
        for field in ("blast_radius", "blame_chain", "violation_id", "note"):
            self.assertIn(field, result, f"Missing required field: {field}")

    def test_blast_radius_subfields_present(self) -> None:
        br = self._attributed()["blast_radius"]
        for field in ("direct_subscribers", "downstream_pipelines", "lineage_nodes", "contamination_depth"):
            self.assertIn(field, br, f"blast_radius missing: {field}")

    # ------------------------------------------------------------------
    # Registry is the primary blast-radius source
    # ------------------------------------------------------------------

    def test_direct_subscribers_from_registry(self) -> None:
        br = self._attributed()["blast_radius"]
        self.assertEqual(1, len(br["direct_subscribers"]))
        sub = br["direct_subscribers"][0]
        self.assertEqual("Week 4", sub["target"])
        self.assertEqual("week4-lineage-graph", sub["target_contract"])

    def test_direct_subscribers_carry_breaking_fields(self) -> None:
        br = self._attributed()["blast_radius"]
        fields = [bf["field"] for bf in br["direct_subscribers"][0]["breaking_fields"]]
        self.assertIn("extracted_facts.confidence", fields)
        self.assertIn("documents.fact_count", fields)

    # ------------------------------------------------------------------
    # Transitive contamination depth via registry graph
    # ------------------------------------------------------------------

    def test_contamination_depth_traverses_registry_graph(self) -> None:
        # Week 3 -> Week 4 (depth 1) -> Week 7 (depth 2)
        br = self._attributed()["blast_radius"]
        self.assertEqual(2, br["contamination_depth"])

    def test_contamination_depth_zero_for_leaf_source(self) -> None:
        # Week 7 has no outgoing subscriptions -- depth must be 0
        result = attribute_violation(VIOLATION_STUB, "week7-trust-boundary", REGISTRY)
        self.assertEqual(0, result["blast_radius"]["contamination_depth"])

    # ------------------------------------------------------------------
    # Lineage enrichment is additive (no lineage graph passed here)
    # ------------------------------------------------------------------

    def test_lineage_nodes_empty_without_lineage_graph(self) -> None:
        br = self._attributed()["blast_radius"]
        self.assertEqual([], br["lineage_nodes"])

    # ------------------------------------------------------------------
    # violation_id is a stable UUID5
    # ------------------------------------------------------------------

    def test_violation_id_is_stable_uuid5(self) -> None:
        r1 = self._attributed()
        r2 = self._attributed()
        self.assertEqual(r1["violation_id"], r2["violation_id"])
        # Must be a valid UUID string
        import uuid
        uuid.UUID(r1["violation_id"])

    # ------------------------------------------------------------------
    # Note field describes the attribution
    # ------------------------------------------------------------------

    def test_note_mentions_contract_id(self) -> None:
        result = self._attributed()
        self.assertIn("week3-document-refinery-extractions", result["note"])

    def test_note_mentions_subscriber_count(self) -> None:
        result = self._attributed()
        self.assertIn("1 direct subscriber", result["note"])

    # ------------------------------------------------------------------
    # Original violation fields are preserved
    # ------------------------------------------------------------------

    def test_original_violation_fields_preserved(self) -> None:
        result = self._attributed()
        self.assertEqual("range", result["check_type"])
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(44889, result["records_failing"])

    # ------------------------------------------------------------------
    # Unknown contract_id produces empty subscribers, depth 0
    # ------------------------------------------------------------------

    def test_unknown_contract_id_returns_empty_attribution(self) -> None:
        result = attribute_violation(VIOLATION_STUB, "nonexistent-contract", REGISTRY)
        br = result["blast_radius"]
        self.assertEqual([], br["direct_subscribers"])
        self.assertEqual(0, br["contamination_depth"])


class LoadRegistryNewSchemaTest(unittest.TestCase):
    """Verify load_registry() correctly surfaces the new catalog and policy fields."""

    _REGISTRY_PATH = Path("contract_registry") / "subscriptions.yaml"

    def setUp(self) -> None:
        if not self._REGISTRY_PATH.exists():
            self.skipTest("subscriptions.yaml not found; skipping file-based registry tests")
        self._registry = load_registry(self._REGISTRY_PATH)

    def test_contracts_catalog_present(self) -> None:
        self.assertIn("contracts", self._registry)
        self.assertIsInstance(self._registry["contracts"], list)

    def test_active_contracts_in_catalog(self) -> None:
        ids = {c["id"] for c in self._registry["contracts"]}
        self.assertIn("week3-document-refinery-extractions", ids)
        self.assertIn("week4-lineage-graph", ids)
        self.assertIn("week5-event-store", ids)

    def test_missing_source_contracts_in_catalog(self) -> None:
        ids = {c["id"] for c in self._registry["contracts"]}
        self.assertIn("week1-intent-correlator", ids)
        self.assertIn("week2-digital-courtroom", ids)
        self.assertIn("langsmith-traces", ids)

    def test_active_status_for_live_contracts(self) -> None:
        self.assertEqual("active", get_contract_status("week3-document-refinery-extractions", self._registry))
        self.assertEqual("active", get_contract_status("week4-lineage-graph", self._registry))
        self.assertEqual("active", get_contract_status("week5-event-store", self._registry))
        self.assertEqual("active", get_contract_status("langsmith-traces", self._registry))

    def test_out_of_scope_status_for_missing_sources(self) -> None:
        self.assertEqual("out_of_scope", get_contract_status("week1-intent-correlator", self._registry))
        self.assertEqual("out_of_scope", get_contract_status("week2-digital-courtroom", self._registry))

    def test_unknown_status_for_nonexistent_contract(self) -> None:
        self.assertEqual("unknown", get_contract_status("nonexistent-contract", self._registry))

    def test_schema_evolution_policy_present(self) -> None:
        policy = self._registry.get("schema_evolution_policy", {})
        self.assertEqual("producer-side", policy.get("gate"))
        self.assertEqual("block", policy.get("action_on_breaking_change"))

    def test_subscriptions_still_intact(self) -> None:
        subs = self._registry["subscriptions"]
        source_contracts = {s["source_contract"] for s in subs}
        self.assertIn("week3-document-refinery-extractions", source_contracts)
        self.assertIn("week4-lineage-graph", source_contracts)
        self.assertIn("week5-event-store", source_contracts)
        self.assertIn("langsmith-traces", source_contracts)


class BlameChainTest(unittest.TestCase):
    """_build_blame_chain confidence scoring and ranking."""

    from contracts.attributor import _build_blame_chain

    def _commit(self, days_ago: float, hash_: str = "abc") -> dict:
        from datetime import datetime, timedelta, timezone
        ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
        return {
            "commit_hash": hash_,
            "author": "Alice",
            "commit_timestamp": ts,
            "commit_message": "fix: something",
        }

    def test_empty_commits_returns_empty(self) -> None:
        from contracts.attributor import _build_blame_chain
        self.assertEqual([], _build_blame_chain([]))

    def test_recent_commit_has_higher_score_than_old(self) -> None:
        from contracts.attributor import _build_blame_chain
        recent = self._commit(0, "new")
        old = self._commit(8, "old")
        chain = _build_blame_chain([old, recent])
        self.assertEqual("new", chain[0]["commit_hash"])

    def test_lineage_hops_penalty_reduces_score(self) -> None:
        from contracts.attributor import _build_blame_chain
        commit = self._commit(0)
        no_hop = _build_blame_chain([commit], lineage_hops=0)[0]["confidence_score"]
        one_hop = _build_blame_chain([commit], lineage_hops=1)[0]["confidence_score"]
        self.assertGreater(no_hop, one_hop)

    def test_score_is_clamped_between_0_and_1(self) -> None:
        from contracts.attributor import _build_blame_chain
        stale = self._commit(100)  # 100 days → base = 1 - 10 = -9 → clamped 0
        chain = _build_blame_chain([stale])
        self.assertEqual(0.0, chain[0]["confidence_score"])

    def test_max_candidates_limits_output(self) -> None:
        from contracts.attributor import _build_blame_chain
        commits = [self._commit(i, f"h{i}") for i in range(10)]
        chain = _build_blame_chain(commits, max_candidates=3)
        self.assertEqual(3, len(chain))

    def test_required_output_fields_present(self) -> None:
        from contracts.attributor import _build_blame_chain
        chain = _build_blame_chain([self._commit(1)])
        for field in ("commit_hash", "author", "commit_timestamp", "commit_message", "confidence_score"):
            self.assertIn(field, chain[0])

    def test_unparseable_timestamp_uses_fallback_score(self) -> None:
        from contracts.attributor import _build_blame_chain
        bad = {
            "commit_hash": "bad",
            "author": "Bob",
            "commit_timestamp": "not-a-date",
            "commit_message": "oops",
        }
        chain = _build_blame_chain([bad])
        # 30-day fallback: base = 1 - 3 = -2 → clamped 0
        self.assertEqual(0.0, chain[0]["confidence_score"])


# ---------------------------------------------------------------------------
# _registry_subscriptions_for_source
# ---------------------------------------------------------------------------

class RegistrySubscriptionsForSourceTest(unittest.TestCase):
    from contracts.attributor import _registry_subscriptions_for_source

    def test_matches_by_source_field(self) -> None:
        from contracts.attributor import _registry_subscriptions_for_source
        result = _registry_subscriptions_for_source(REGISTRY, "Week 3")
        self.assertEqual(1, len(result))
        self.assertEqual("Week 4", result[0]["target"])

    def test_no_match_returns_empty(self) -> None:
        from contracts.attributor import _registry_subscriptions_for_source
        result = _registry_subscriptions_for_source(REGISTRY, "Week 99")
        self.assertEqual([], result)

    def test_leaf_node_returns_empty(self) -> None:
        from contracts.attributor import _registry_subscriptions_for_source
        # Week 7 has no outgoing subscriptions in REGISTRY
        result = _registry_subscriptions_for_source(REGISTRY, "Week 7")
        self.assertEqual([], result)

    def test_matches_by_source_contract_field(self) -> None:
        from contracts.attributor import _registry_subscriptions_for_source
        # source_contract match
        result = _registry_subscriptions_for_source(REGISTRY, "week3-document-refinery-extractions")
        self.assertEqual(1, len(result))


# ---------------------------------------------------------------------------
# _reachable_targets (BFS traversal)
# ---------------------------------------------------------------------------

class ReachableTargetsTest(unittest.TestCase):
    from contracts.attributor import _reachable_targets

    def test_direct_subscriber_at_depth_1(self) -> None:
        from contracts.attributor import _reachable_targets
        depths = _reachable_targets(REGISTRY["subscriptions"], "Week 3")
        self.assertEqual(1, depths["Week 4"])

    def test_transitive_subscriber_at_depth_2(self) -> None:
        from contracts.attributor import _reachable_targets
        depths = _reachable_targets(REGISTRY["subscriptions"], "Week 3")
        self.assertEqual(2, depths["Week 7"])

    def test_leaf_source_has_no_reachable_targets(self) -> None:
        from contracts.attributor import _reachable_targets
        depths = _reachable_targets(REGISTRY["subscriptions"], "Week 7")
        self.assertEqual({}, depths)

    def test_mid_chain_source_skips_upstream(self) -> None:
        from contracts.attributor import _reachable_targets
        depths = _reachable_targets(REGISTRY["subscriptions"], "Week 4")
        # Week 3 should NOT appear (it's upstream)
        self.assertNotIn("Week 3", depths)
        self.assertEqual(1, depths.get("Week 7"))

    def test_empty_subscriptions_returns_empty(self) -> None:
        from contracts.attributor import _reachable_targets
        self.assertEqual({}, _reachable_targets([], "Week 3"))


# ---------------------------------------------------------------------------
# _enrich_with_lineage
# ---------------------------------------------------------------------------

LINEAGE_GRAPH = {
    "nodes": [
        {"node_id": "n1", "label": "Week 4 Lineage Graph", "type": "dataset", "path": "outputs/week4/graph.jsonl"},
        {"node_id": "n2", "label": "Week 7 Trust Boundary", "type": "dataset", "path": "outputs/week7/trust.jsonl"},
        {"node_id": "n3", "label": "Unrelated Model", "type": "model", "path": "models/unrelated.py"},
    ],
    "edges": [],
}


class EnrichWithLineageTest(unittest.TestCase):
    from contracts.attributor import _enrich_with_lineage

    def test_matches_target_by_label_case_insensitive(self) -> None:
        from contracts.attributor import _enrich_with_lineage
        matches = _enrich_with_lineage(LINEAGE_GRAPH, ["week 4"])
        labels = [m["label"] for m in matches]
        self.assertIn("Week 4 Lineage Graph", labels)

    def test_unrelated_nodes_not_returned(self) -> None:
        from contracts.attributor import _enrich_with_lineage
        matches = _enrich_with_lineage(LINEAGE_GRAPH, ["week 4"])
        labels = [m["label"] for m in matches]
        self.assertNotIn("Unrelated Model", labels)

    def test_empty_target_names_returns_empty(self) -> None:
        from contracts.attributor import _enrich_with_lineage
        self.assertEqual([], _enrich_with_lineage(LINEAGE_GRAPH, []))

    def test_empty_lineage_nodes_returns_empty(self) -> None:
        from contracts.attributor import _enrich_with_lineage
        self.assertEqual([], _enrich_with_lineage({"nodes": [], "edges": []}, ["week 4"]))

    def test_deduplicates_on_node_id_and_path(self) -> None:
        from contracts.attributor import _enrich_with_lineage
        # Duplicate node
        dup_graph = {
            "nodes": [
                {"node_id": "n1", "label": "Week 4 Graph", "type": "dataset", "path": "a/b.jsonl"},
                {"node_id": "n1", "label": "Week 4 Graph", "type": "dataset", "path": "a/b.jsonl"},
            ],
            "edges": [],
        }
        matches = _enrich_with_lineage(dup_graph, ["week 4"])
        self.assertEqual(1, len(matches))

    def test_required_fields_in_match(self) -> None:
        from contracts.attributor import _enrich_with_lineage
        matches = _enrich_with_lineage(LINEAGE_GRAPH, ["week 4"])
        for field in ("node_id", "label", "type", "path"):
            self.assertIn(field, matches[0])


# ---------------------------------------------------------------------------
# load_lineage_graph
# ---------------------------------------------------------------------------

class LoadLineageGraphTest(unittest.TestCase):
    def test_missing_file_returns_empty_nodes_and_edges(self) -> None:
        from contracts.attributor import load_lineage_graph
        result = load_lineage_graph(Path("/no/such/lineage.jsonl"))
        self.assertEqual([], result["nodes"])
        self.assertEqual([], result["edges"])

    def test_loads_jsonl_records(self) -> None:
        from contracts.attributor import load_lineage_graph
        import tempfile
        record = {"nodes": [{"node_id": "n1"}], "edges": [{"from": "n1", "to": "n2"}]}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            import json
            fh.write(json.dumps(record) + "\n")
            path = Path(fh.name)
        try:
            result = load_lineage_graph(path)
            self.assertEqual(1, len(result["nodes"]))
            self.assertEqual("n1", result["nodes"][0]["node_id"])
        finally:
            path.unlink(missing_ok=True)

    def test_none_path_returns_empty(self) -> None:
        from contracts.attributor import load_lineage_graph
        result = load_lineage_graph(None)
        self.assertEqual([], result["nodes"])
        self.assertEqual([], result["edges"])


if __name__ == "__main__":
    unittest.main()


