"""
tests/test_schema_evolution_gate.py

Tests for the producer-side schema-evolution gate
(contracts.runner.check_producer_evolution_gate).

The gate runs before a schema change is deployed.  It blocks the deploy if any
field declared as breaking in the registry would be removed by the proposed change.
"""

import unittest

from contracts.runner import check_producer_evolution_gate

# ---------------------------------------------------------------------------
# Shared registry fixture (mirrors the shape loaded from subscriptions.yaml)
# ---------------------------------------------------------------------------

REGISTRY: dict = {
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

CONTRACT_ID = "week3-document-refinery-extractions"


class ProducerEvolutionGateBlockTest(unittest.TestCase):
    """Gate must return BLOCK when a registered breaking field is removed."""

    def test_blocks_when_breaking_field_removed(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "fact_count"],   # confidence removed
            current_fields=["doc_id", "fact_count", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        self.assertEqual("BLOCK", result["action"])

    def test_block_result_names_the_affected_field(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "fact_count"],
            current_fields=["doc_id", "fact_count", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        fields = [bf["field"] for bf in result["breaking_fields_affected"]]
        self.assertIn("extracted_facts.confidence", fields)

    def test_block_result_names_the_subscriber(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "fact_count"],
            current_fields=["doc_id", "fact_count", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        subscribers = [bf["subscriber"] for bf in result["breaking_fields_affected"]]
        self.assertIn("Week 4", subscribers)

    def test_blocks_when_multiple_breaking_fields_removed(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id"],   # both confidence and fact_count removed
            current_fields=["doc_id", "fact_count", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        self.assertEqual("BLOCK", result["action"])
        self.assertEqual(2, len(result["breaking_fields_affected"]))

    def test_reason_mentions_contract_and_fields(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id"],
            current_fields=["doc_id", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        self.assertIn(CONTRACT_ID, result["reason"])
        self.assertIn("confidence", result["reason"])


class ProducerEvolutionGatePassTest(unittest.TestCase):
    """Gate must return PASS when no breaking fields are affected."""

    def test_passes_when_only_non_breaking_field_removed(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "confidence"],   # internal_field removed, not breaking
            current_fields=["doc_id", "confidence", "internal_field"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        self.assertEqual("PASS", result["action"])
        self.assertEqual([], result["breaking_fields_affected"])

    def test_passes_when_only_fields_added(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "confidence", "new_field"],
            current_fields=["doc_id", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        self.assertEqual("PASS", result["action"])

    def test_passes_when_schema_unchanged(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id", "confidence"],
            current_fields=["doc_id", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        self.assertEqual("PASS", result["action"])

    def test_passes_for_unknown_contract(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=[],
            current_fields=["some_field"],
            contract_id="nonexistent-contract",
            registry=REGISTRY,
        )
        self.assertEqual("PASS", result["action"])
        self.assertEqual([], result["breaking_fields_affected"])

    def test_passes_for_week4_removing_non_breaking_field(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["node_id", "label"],   # path kept; edge_type removed (not breaking)
            current_fields=["node_id", "label", "edge_type"],
            contract_id="week4-lineage-graph",
            registry=REGISTRY,
        )
        self.assertEqual("PASS", result["action"])

    def test_blocks_for_week4_removing_path(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["node_id", "label"],   # path removed — it IS breaking
            current_fields=["node_id", "label", "path"],
            contract_id="week4-lineage-graph",
            registry=REGISTRY,
        )
        self.assertEqual("BLOCK", result["action"])


class ProducerEvolutionGateResultShapeTest(unittest.TestCase):
    """Gate result always contains the required keys regardless of action."""

    def _required_keys(self) -> list[str]:
        return ["action", "breaking_fields_affected", "reason"]

    def test_pass_result_has_required_keys(self) -> None:
        result = check_producer_evolution_gate([], [], CONTRACT_ID, REGISTRY)
        for key in self._required_keys():
            self.assertIn(key, result)

    def test_block_result_has_required_keys(self) -> None:
        result = check_producer_evolution_gate(
            proposed_fields=["doc_id"],
            current_fields=["doc_id", "confidence"],
            contract_id=CONTRACT_ID,
            registry=REGISTRY,
        )
        for key in self._required_keys():
            self.assertIn(key, result)


if __name__ == "__main__":
    unittest.main()
