import json
import tempfile
import unittest
from pathlib import Path

from contracts.ai_extensions import (
    _centroid,
    _cosine_distance,
    _scrub_record,
    _text_to_bow,
    _validate_against_schema,
    check_embedding_drift,
    check_llm_output_violation_rate,
    check_prompt_input_schema,
)


# ---------------------------------------------------------------------------
# _text_to_bow Tests
# ---------------------------------------------------------------------------


class TextToBowTest(unittest.TestCase):
    def test_single_word_gets_unit_frequency(self) -> None:
        bow = _text_to_bow("hello")
        self.assertEqual({"hello": 1.0}, bow)

    def test_multiple_words_normalized(self) -> None:
        bow = _text_to_bow("hello hello world")
        self.assertAlmostEqual(2.0 / 3.0, bow["hello"], places=5)
        self.assertAlmostEqual(1.0 / 3.0, bow["world"], places=5)

    def test_case_insensitive(self) -> None:
        bow = _text_to_bow("Hello HELLO hello")
        self.assertEqual(1.0, bow["hello"])

    def test_empty_string_returns_empty_dict(self) -> None:
        bow = _text_to_bow("")
        self.assertEqual({}, bow)

    def test_whitespace_only_returns_empty_dict(self) -> None:
        bow = _text_to_bow("   \t\n  ")
        self.assertEqual({}, bow)


# ---------------------------------------------------------------------------
# _centroid Tests
# ---------------------------------------------------------------------------


class CentroidTest(unittest.TestCase):
    def test_empty_list_returns_empty_dict(self) -> None:
        centroid = _centroid([])
        self.assertEqual({}, centroid)

    def test_single_vector_returned_as_is(self) -> None:
        vec = {"a": 0.5, "b": 0.5}
        centroid = _centroid([vec])
        self.assertEqual(vec, centroid)

    def test_two_identical_vectors_gives_same_result(self) -> None:
        vec = {"x": 0.3, "y": 0.7}
        centroid = _centroid([vec, vec])
        self.assertEqual(vec, centroid)

    def test_two_different_vectors_averaged(self) -> None:
        centroid = _centroid([{"a": 1.0}, {"b": 1.0}])
        self.assertAlmostEqual(0.5, centroid["a"], places=5)
        self.assertAlmostEqual(0.5, centroid["b"], places=5)


# ---------------------------------------------------------------------------
# _cosine_distance Tests
# ---------------------------------------------------------------------------


class CosineDistanceTest(unittest.TestCase):
    def test_identical_vectors_distance_is_zero(self) -> None:
        vec = {"a": 0.3, "b": 0.7}
        distance = _cosine_distance(vec, vec)
        self.assertEqual(0.0, distance)

    def test_orthogonal_vectors_distance_is_one(self) -> None:
        distance = _cosine_distance({"a": 1.0}, {"b": 1.0})
        self.assertAlmostEqual(1.0, distance, places=5)

    def test_empty_vector_distance_is_one(self) -> None:
        distance = _cosine_distance({}, {"a": 1.0})
        self.assertEqual(1.0, distance)

    def test_both_empty_distance_is_one(self) -> None:
        distance = _cosine_distance({}, {})
        self.assertEqual(1.0, distance)

    def test_distance_value_in_valid_range(self) -> None:
        distance = _cosine_distance({"a": 0.5, "b": 0.5}, {"b": 0.3, "c": 0.7})
        self.assertGreaterEqual(distance, 0.0)
        self.assertLessEqual(distance, 1.0)


# ---------------------------------------------------------------------------
# _scrub_record Tests
# ---------------------------------------------------------------------------


class ScrubRecordTest(unittest.TestCase):
    def test_no_sensitive_fields_unchanged(self) -> None:
        record = {"name": "Alice", "age": 30}
        scrubbed = _scrub_record(record)
        self.assertEqual(record, scrubbed)

    def test_api_key_redacted(self) -> None:
        record = {"api_key": "sk-123", "name": "test"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["api_key"])
        self.assertEqual("test", scrubbed["name"])

    def test_apikey_variant_redacted(self) -> None:
        record = {"apikey": "secret"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["apikey"])

    def test_auth_token_redacted(self) -> None:
        record = {"auth_token": "tok-123", "auth-token": "alt-tok"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["auth_token"])
        self.assertEqual("<redacted>", scrubbed["auth-token"])

    def test_password_variant_redacted(self) -> None:
        record = {"password": "pwd", "passwd": "alt-pwd"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["password"])
        self.assertEqual("<redacted>", scrubbed["passwd"])

    def test_authorization_header_redacted(self) -> None:
        record = {"authorization": "Bearer xyz"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["authorization"])

    def test_credential_redacted(self) -> None:
        record = {"credential": "cred123"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["credential"])

    def test_case_insensitive_matching(self) -> None:
        record = {"API_KEY": "secret", "ApiKey": "secret2"}
        scrubbed = _scrub_record(record)
        self.assertEqual("<redacted>", scrubbed["API_KEY"])
        self.assertEqual("<redacted>", scrubbed["ApiKey"])


# ---------------------------------------------------------------------------
# _validate_against_schema Tests
# ---------------------------------------------------------------------------


class ValidateAgainstSchemaTest(unittest.TestCase):
    def test_valid_record_returns_no_errors(self) -> None:
        schema = {
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}, "count": {"type": "integer"}},
        }
        record = {"id": "123", "count": 5}
        errors = _validate_against_schema(record, schema)
        self.assertEqual([], errors)

    def test_missing_required_field_produces_error(self) -> None:
        schema = {"type": "object", "required": ["id"]}
        record = {"name": "test"}
        errors = _validate_against_schema(record, schema)
        self.assertEqual(1, len(errors))
        self.assertIn("id", errors[0])

    def test_multiple_missing_required_fields(self) -> None:
        schema = {"type": "object", "required": ["id", "name"]}
        record = {}
        errors = _validate_against_schema(record, schema)
        self.assertEqual(2, len(errors))

    def test_type_mismatch_string_produces_error(self) -> None:
        schema = {"type": "object", "properties": {"id": {"type": "string"}}}
        record = {"id": 123}
        errors = _validate_against_schema(record, schema)
        self.assertEqual(1, len(errors))
        self.assertIn("string", errors[0])

    def test_type_mismatch_integer_produces_error(self) -> None:
        schema = {"type": "object", "properties": {"count": {"type": "integer"}}}
        record = {"count": "five"}
        errors = _validate_against_schema(record, schema)
        self.assertEqual(1, len(errors))
        self.assertIn("integer", errors[0])

    def test_type_mismatch_number_produces_error(self) -> None:
        schema = {"type": "object", "properties": {"score": {"type": "number"}}}
        record = {"score": "high"}
        errors = _validate_against_schema(record, schema)
        self.assertEqual(1, len(errors))
        self.assertIn("number", errors[0])

    def test_non_object_schema_returns_no_errors(self) -> None:
        schema = {"type": "array"}
        record = {"anything": "goes"}
        errors = _validate_against_schema(record, schema)
        self.assertEqual([], errors)

    def test_unspecified_properties_allowed(self) -> None:
        schema = {"type": "object", "properties": {"id": {"type": "string"}}}
        record = {"id": "123", "extra_field": "allowed"}
        errors = _validate_against_schema(record, schema)
        self.assertEqual([], errors)


# ---------------------------------------------------------------------------
# check_prompt_input_schema Tests
# ---------------------------------------------------------------------------


class CheckPromptInputSchemaTest(unittest.TestCase):
    def test_all_valid_records_pass(self) -> None:
        records = [
            {"doc_id": "d1", "source_path": "/path", "extracted_at": "2025-01-01T00:00:00Z"},
            {"doc_id": "d2", "source_path": "/path2", "extracted_at": "2025-01-02T00:00:00Z"},
        ]
        result = check_prompt_input_schema(records)
        self.assertEqual("PASS", result["status"])
        self.assertEqual(0, result["violations_found"])

    def test_missing_required_field_fails(self) -> None:
        records = [{"doc_id": "d1"}]  # missing source_path and extracted_at
        result = check_prompt_input_schema(records)
        self.assertNotEqual("PASS", result["status"])
        self.assertEqual(1, result["violations_found"])

    def test_violation_rate_computed_correctly(self) -> None:
        records = [
            {"doc_id": "d1", "source_path": "/p1", "extracted_at": "2025-01-01T00:00:00Z"},
            {"doc_id": "d2"},  # missing required fields
            {"doc_id": "d3"},  # missing required fields
        ]
        result = check_prompt_input_schema(records)
        self.assertEqual(2, result["violations_found"])
        self.assertAlmostEqual(66.67, result["violation_rate_pct"], places=1)

    def test_custom_schema_applied(self) -> None:
        schema = {
            "type": "object",
            "required": ["custom_id"],
            "properties": {"custom_id": {"type": "string"}},
        }
        records = [{"custom_id": "x1"}, {"no_id": "x2"}]
        result = check_prompt_input_schema(records, schema=schema)
        self.assertEqual(1, result["violations_found"])

    def test_empty_records_list_passes(self) -> None:
        result = check_prompt_input_schema([])
        self.assertEqual("PASS", result["status"])
        self.assertEqual(0, result["records_scanned"])


# ---------------------------------------------------------------------------
# check_embedding_drift Tests
# ---------------------------------------------------------------------------


class CheckEmbeddingDriftTest(unittest.TestCase):
    def setUp(self) -> None:
        # Use temp dir for baseline file to avoid polluting the project
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_result_has_required_fields(self) -> None:
        records = [{"text": "hello world"}]
        result = check_embedding_drift(records)
        for field in ("check_type", "field_name", "status", "message"):
            self.assertIn(field, result)

    def test_no_text_values_returns_error(self) -> None:
        records = [{"other_field": "value"}]
        result = check_embedding_drift(records, text_field="text")
        self.assertEqual("ERROR", result["status"])
        self.assertIn("No text values found", result["message"])

    def test_empty_text_values_skipped(self) -> None:
        records = [{"text": ""}, {"text": "   "}, {"text": "hello"}]
        result = check_embedding_drift(records, text_field="text")
        self.assertEqual("PASS", result["status"])
        self.assertEqual(1, result["current_record_count"])

    def test_status_pass_when_distance_below_threshold(self) -> None:
        # With no baseline, status should be PASS
        records = [{"text": "hello world"}]
        result = check_embedding_drift(records)
        self.assertEqual("PASS", result["status"])

    def test_cosine_distance_field_present(self) -> None:
        records = [{"text": "hello"}]
        result = check_embedding_drift(records)
        if "cosine_distance" in result:
            self.assertGreaterEqual(result["cosine_distance"], 0.0)
            self.assertLessEqual(result["cosine_distance"], 1.0)


# ---------------------------------------------------------------------------
# check_llm_output_violation_rate Tests
# ---------------------------------------------------------------------------


class CheckLlmOutputViolationRateTest(unittest.TestCase):
    def test_no_verdicts_returns_error(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            path = fh.name
        try:
            result = check_llm_output_violation_rate(path)
            self.assertEqual("ERROR", result["status"])
        finally:
            Path(path).unlink()

    def test_all_valid_verdicts_pass(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write(
                json.dumps({
                    "id": "v1",
                    "verdict": {
                        "decision": "approve",
                        "confidence": 0.95,
                        "reasoning": "looks good",
                    },
                })
                + "\n"
            )
            fh.write(
                json.dumps({
                    "id": "v2",
                    "verdict": {
                        "decision": "reject",
                        "confidence": 0.8,
                        "reasoning": "failed checks",
                    },
                })
                + "\n"
            )
            path = fh.name

        try:
            result = check_llm_output_violation_rate(path)
            self.assertEqual("PASS", result["status"])
            self.assertEqual(0, result["violation_count"])
        finally:
            Path(path).unlink()

    def test_missing_verdict_counts_as_violation(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write(json.dumps({"id": "v1"}) + "\n")
            fh.write(
                json.dumps({
                    "id": "v2",
                    "verdict": {
                        "decision": "ok",
                        "confidence": 0.5,
                        "reasoning": "good",
                    },
                })
                + "\n"
            )
            path = fh.name

        try:
            result = check_llm_output_violation_rate(path)
            self.assertEqual(1, result["violation_count"])
            self.assertIn("missing_verdict", result["violation_breakdown"])
        finally:
            Path(path).unlink()

    def test_missing_verdict_subfield_counts_as_violation(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write(
                json.dumps({
                    "id": "v1",
                    "verdict": {"decision": "ok", "confidence": 0.5}  # missing reasoning
                })
                + "\n"
            )
            path = fh.name

        try:
            result = check_llm_output_violation_rate(path)
            self.assertEqual(1, result["violation_count"])
            self.assertIn("missing_reasoning", result["violation_breakdown"])
        finally:
            Path(path).unlink()

    def test_confidence_out_of_range_counts_as_violation(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write(
                json.dumps({
                    "id": "v1",
                    "verdict": {
                        "decision": "ok",
                        "confidence": 1.5,  # out of range [0, 1]
                        "reasoning": "test",
                    },
                })
                + "\n"
            )
            path = fh.name

        try:
            result = check_llm_output_violation_rate(path)
            self.assertEqual(1, result["violation_count"])
            self.assertIn("confidence_out_of_range", result["violation_breakdown"])
        finally:
            Path(path).unlink()

    def test_trend_increasing_when_trend_worsens(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            # First half: all valid
            for i in range(5):
                fh.write(
                    json.dumps({
                        "id": f"v{i}",
                        "verdict": {
                            "decision": "ok",
                            "confidence": 0.9,
                            "reasoning": "good",
                        },
                    })
                    + "\n"
                )
            # Second half: mostly violations
            for i in range(5, 10):
                fh.write(json.dumps({"id": f"v{i}"}) + "\n")
            path = fh.name

        try:
            result = check_llm_output_violation_rate(path)
            self.assertEqual("increasing", result["trend"])
        finally:
            Path(path).unlink()

    def test_result_has_required_fields(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write(
                json.dumps({
                    "verdict": {"decision": "ok", "confidence": 0.5, "reasoning": "test"}
                })
                + "\n"
            )
            path = fh.name

        try:
            result = check_llm_output_violation_rate(path)
            for field in (
                "check_type",
                "status",
                "records_scanned",
                "violation_count",
                "trend",
                "violation_breakdown",
            ):
                self.assertIn(field, result)
        finally:
            Path(path).unlink()


# ---------------------------------------------------------------------------
# iter_jsonl + streaming path
# ---------------------------------------------------------------------------


class IterJsonlTest(unittest.TestCase):
    """iter_jsonl yields records without loading the full file into RAM."""

    from contracts.ai_extensions import iter_jsonl

    def _write_jsonl(self, records: list) -> Path:
        fh = tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
        )
        for r in records:
            fh.write(json.dumps(r) + "\n")
        fh.close()
        return Path(fh.name)

    def test_yields_all_records(self) -> None:
        from contracts.ai_extensions import iter_jsonl
        path = self._write_jsonl([{"a": 1}, {"a": 2}, {"a": 3}])
        try:
            result = list(iter_jsonl(path))
            self.assertEqual(3, len(result))
            self.assertEqual([1, 2, 3], [r["a"] for r in result])
        finally:
            path.unlink(missing_ok=True)

    def test_missing_file_yields_nothing(self) -> None:
        from contracts.ai_extensions import iter_jsonl
        result = list(iter_jsonl(Path("/no/such/file.jsonl")))
        self.assertEqual([], result)

    def test_skips_invalid_json_lines(self) -> None:
        from contracts.ai_extensions import iter_jsonl
        path = self._write_jsonl([{"ok": True}])
        # Append a bad line
        with open(path, "a") as fh:
            fh.write("{bad json\n")
        try:
            result = list(iter_jsonl(path))
            self.assertEqual(1, len(result))
        finally:
            path.unlink(missing_ok=True)

    def test_check_prompt_input_schema_accepts_generator(self) -> None:
        """Streaming generator (not a list) must be accepted by check_prompt_input_schema."""
        valid = {
            "doc_id": "d1",
            "source_path": "/x",
            "extracted_at": "2026-04-04T00:00:00Z",
        }

        def _gen():
            for _ in range(5):
                yield valid

        result = check_prompt_input_schema(_gen())
        self.assertEqual(5, result["records_scanned"])
        self.assertEqual("PASS", result["status"])

    def test_streaming_violation_rate_matches_list_result(self) -> None:
        """Streaming path produces identical violation_count to list-based path."""
        path = self._write_jsonl([
            {"verdict": {"decision": "allow", "confidence": 0.9, "reasoning": "ok"}},
            {"verdict": None},   # violation
            {"verdict": {"decision": "deny", "confidence": 0.8, "reasoning": "bad"}},
        ])
        try:
            result = check_llm_output_violation_rate(str(path))
            self.assertEqual(3, result["records_scanned"])
            self.assertEqual(1, result["violation_count"])
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
