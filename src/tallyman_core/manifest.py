from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

from tallyman_core.paths import (
    ENTRY_MANIFEST_FILENAME,
    ENTRY_RESULT_FILENAME,
    ENTRY_SCHEMA_FILENAME,
)


class Manifest(BaseModel):
    content_hash: str
    project: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    prompt: str | None = None
    code_path: str = "expr.py"
    result_path: str = ENTRY_RESULT_FILENAME
    schema_path: str = ENTRY_SCHEMA_FILENAME
    row_count: int | None = None
    execute_seconds: float | None = None
    # rel data path -> content md5, recorded when a source-identity mode is
    # active (tallyman_xorq.source_identity); absent under mode=off.
    sources: dict[str, str] | None = None


def write_manifest(entry_path: Path, manifest: Manifest) -> Path:
    out = entry_path / ENTRY_MANIFEST_FILENAME
    out.write_text(json.dumps(manifest.model_dump(), indent=2))
    return out


def read_manifest(entry_path: Path) -> Manifest:
    raw = json.loads((entry_path / ENTRY_MANIFEST_FILENAME).read_text())
    return Manifest.model_validate(raw)
