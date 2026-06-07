from __future__ import annotations

from pathlib import Path

from tallyman_core import Manifest, read_manifest, write_manifest


def test_manifest_round_trip(tmp_path: Path):
    m = Manifest(content_hash="abc", project="p", prompt="hello", row_count=3, execute_seconds=0.05)
    out = write_manifest(tmp_path, m)
    assert out.exists()
    m2 = read_manifest(tmp_path)
    assert m2.content_hash == "abc"
    assert m2.project == "p"
    assert m2.prompt == "hello"
    assert m2.row_count == 3
    assert m2.execute_seconds == 0.05
    assert m2.created_at  # auto-set


def test_manifest_optional_fields_default():
    m = Manifest(content_hash="x", project="p")
    assert m.prompt is None
    assert m.row_count is None
    assert m.execute_seconds is None
    assert m.code_path == "expr.py"
    assert m.result_path == "result.parquet"
    assert m.schema_path == "schema.json"
