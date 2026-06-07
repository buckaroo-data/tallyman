from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field


class Manifest(BaseModel):
    content_hash: str
    project: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    prompt: str | None = None
    code_path: str = "expr.py"
    result_path: str = "result.parquet"
    schema_path: str = "schema.json"
    row_count: int | None = None
    execute_seconds: float | None = None


def write_manifest(entry_path: Path, manifest: Manifest) -> Path:
    out = entry_path / "manifest.json"
    out.write_text(json.dumps(manifest.model_dump(), indent=2))
    return out


def read_manifest(entry_path: Path) -> Manifest:
    raw = json.loads((entry_path / "manifest.json").read_text())
    return Manifest.model_validate(raw)
