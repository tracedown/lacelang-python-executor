"""Schema validation and size parsing tests."""

from lacelang_executor.executor import _validate_schema, _parse_size


# ── _parse_size (spec §4.3 pattern) ─────────────────────────────

class TestParseSize:
    def test_plain_int(self):
        assert _parse_size("500") == 500

    def test_k_suffix(self):
        assert _parse_size("10k") == 10 * 1024

    def test_kb_suffix(self):
        assert _parse_size("2kb") == 2 * 1024

    def test_mb_suffix(self):
        assert _parse_size("1MB") == 1024 ** 2

    def test_gb_suffix(self):
        assert _parse_size("1GB") == 1024 ** 3

    def test_m_suffix_case_insensitive(self):
        assert _parse_size("5m") == 5 * 1024 ** 2

    def test_rejects_b_suffix(self):
        """B is not in the spec pattern — should return the string."""
        assert _parse_size("500B") == "500B"

    def test_rejects_spaces(self):
        """Spaces are not in the spec pattern."""
        assert _parse_size("2 MB") == "2 MB"

    def test_rejects_float(self):
        assert _parse_size("1.5MB") == "1.5MB"

    def test_non_string_passthrough(self):
        assert _parse_size(1024) == 1024


# ── _validate_schema ────────────────────────────────────────────

class TestValidateSchema:
    def test_type_string(self):
        assert _validate_schema("hello", {"type": "string"}) == "passed"
        assert _validate_schema(42, {"type": "string"}) == "failed"

    def test_type_integer(self):
        assert _validate_schema(42, {"type": "integer"}) == "passed"
        assert _validate_schema(True, {"type": "integer"}) == "failed"

    def test_type_object(self):
        assert _validate_schema({"a": 1}, {"type": "object"}) == "passed"
        assert _validate_schema("str", {"type": "object"}) == "failed"

    def test_required(self):
        schema = {"type": "object", "required": ["name"]}
        assert _validate_schema({"name": "x"}, schema) == "passed"
        assert _validate_schema({}, schema) == "failed"

    def test_enum(self):
        schema = {"type": "string", "enum": ["a", "b", "c"]}
        assert _validate_schema("a", schema) == "passed"
        assert _validate_schema("z", schema) == "failed"

    def test_nested_properties(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
        }
        assert _validate_schema({"name": "x", "age": 1}, schema) == "passed"
        assert _validate_schema({"name": "x", "age": "old"}, schema) == "failed"

    def test_strict_mode_rejects_extra_keys(self):
        schema = {
            "type": "object",
            "properties": {"a": {"type": "string"}},
        }
        body = {"a": "x", "b": "extra"}
        assert _validate_schema(body, schema) == "passed"  # non-strict
        assert _validate_schema(body, schema, strict=True) == "failed"

    def test_strict_mode_passes_when_no_extras(self):
        schema = {
            "type": "object",
            "properties": {"a": {"type": "string"}},
        }
        assert _validate_schema({"a": "x"}, schema, strict=True) == "passed"

    def test_strict_mode_recursive(self):
        schema = {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "properties": {"x": {"type": "integer"}},
                },
            },
        }
        body = {"nested": {"x": 1, "y": 2}}
        assert _validate_schema(body, schema) == "passed"
        assert _validate_schema(body, schema, strict=True) == "failed"

    def test_array_items(self):
        schema = {"type": "array", "items": {"type": "integer"}}
        assert _validate_schema([1, 2, 3], schema) == "passed"
        assert _validate_schema([1, "two"], schema) == "failed"

    def test_null_body_fails(self):
        assert _validate_schema(None, {"type": "object"}) == "failed"

    def test_null_schema_indeterminate(self):
        assert _validate_schema({"a": 1}, None) == "indeterminate"
