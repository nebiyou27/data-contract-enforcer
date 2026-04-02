import unittest

from contracts.attributor import attribute_violation, load_registry


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

    def test_required_fields_present(self) -> None:
        result = self._attributed()
        for field in ("direct_subscribers", "downstream_nodes_from_lineage", "contamination_depth", "note"):
            self.assertIn(field, result, f"Missing required field: {field}")

    # ------------------------------------------------------------------
    # Registry is the primary blast-radius source
    # ------------------------------------------------------------------

    def test_direct_subscribers_from_registry(self) -> None:
        result = self._attributed()
        self.assertEqual(1, len(result["direct_subscribers"]))
        sub = result["direct_subscribers"][0]
        self.assertEqual("Week 4", sub["target"])
        self.assertEqual("week4-lineage-graph", sub["target_contract"])

    def test_direct_subscribers_carry_breaking_fields(self) -> None:
        result = self._attributed()
        fields = [bf["field"] for bf in result["direct_subscribers"][0]["breaking_fields"]]
        self.assertIn("extracted_facts.confidence", fields)
        self.assertIn("documents.fact_count", fields)

    # ------------------------------------------------------------------
    # Transitive contamination depth via registry graph
    # ------------------------------------------------------------------

    def test_contamination_depth_traverses_registry_graph(self) -> None:
        # Week 3 → Week 4 (depth 1) → Week 7 (depth 2)
        result = self._attributed()
        self.assertEqual(2, result["contamination_depth"])

    def test_contamination_depth_zero_for_leaf_source(self) -> None:
        # Week 7 has no outgoing subscriptions -- depth must be 0
        result = attribute_violation(VIOLATION_STUB, "week7-trust-boundary", REGISTRY)
        self.assertEqual(0, result["contamination_depth"])

    # ------------------------------------------------------------------
    # Lineage enrichment is additive (no lineage graph passed here)
    # ------------------------------------------------------------------

    def test_downstream_nodes_empty_without_lineage_graph(self) -> None:
        result = self._attributed()
        self.assertEqual([], result["downstream_nodes_from_lineage"])

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
        self.assertEqual([], result["direct_subscribers"])
        self.assertEqual(0, result["contamination_depth"])


if __name__ == "__main__":
    unittest.main()
