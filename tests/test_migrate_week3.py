import json
import tempfile
import unittest
from pathlib import Path

from scripts.migrate_week3 import build_record, fact_confidence, validate_confidence_score


class ConfidenceValidationTest(unittest.TestCase):
    def test_validate_confidence_score_accepts_unit_interval(self) -> None:
        self.assertEqual(0.75, validate_confidence_score(0.75, 'test.confidence_score'))

    def test_fact_confidence_rejects_out_of_range_page_value(self) -> None:
        document = {}
        page = {"metadata": {"confidence_score": 1.25}}

        with self.assertRaises(ValueError):
            fact_confidence(document, page)

    def test_build_record_rejects_out_of_range_document_metadata_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'source.json'
            path.write_text(
                json.dumps(
                    {
                        "doc_id": "doc-1",
                        "file_path": "inputs/doc-1.pdf",
                        "metadata": {
                            "confidence_score": -0.1,
                            "strategy_used": "strategy_b",
                            "processing_time_sec": 1.2,
                        },
                        "pages": [],
                    }
                ),
                encoding='utf-8',
            )
            document = json.loads(path.read_text(encoding='utf-8'))

            with self.assertRaises(ValueError):
                build_record(path, document)


if __name__ == '__main__':
    unittest.main()
