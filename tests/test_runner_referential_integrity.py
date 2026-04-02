import unittest

import pandas as pd

from contracts.runner import check_referential_integrity


class ReferentialIntegrityChecksTest(unittest.TestCase):
    def test_referential_integrity_flags_orphan_fact_doc_ids(self) -> None:
        child = pd.Series(["doc-1", "doc-2", "doc-2", "doc-3"])
        parent = pd.Series(["doc-1", "doc-3"])

        result = check_referential_integrity(
            "extracted_facts",
            "doc_id",
            child,
            "documents",
            "doc_id",
            parent,
        )

        self.assertEqual("FAIL", result["status"])
        self.assertEqual("referential_integrity", result["check_type"])
        self.assertEqual(2, result["records_failing"])
        self.assertEqual(["doc-2", "doc-2"], result["sample_failing"])

    def test_referential_integrity_passes_when_all_doc_ids_match(self) -> None:
        child = pd.Series(["doc-1", "doc-2"])
        parent = pd.Series(["doc-2", "doc-1"])

        result = check_referential_integrity(
            "extracted_facts",
            "doc_id",
            child,
            "documents",
            "doc_id",
            parent,
        )

        self.assertEqual("PASS", result["status"])
        self.assertEqual("all 2 values matched", result["actual_value"])


if __name__ == "__main__":
    unittest.main()
