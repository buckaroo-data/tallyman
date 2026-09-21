from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

from tallyman_core.fsutil import atomic_write_text
from tallyman_core.paths import (
    ENTRY_MANIFEST_FILENAME,
    ENTRY_SCHEMA_FILENAME,
)


class ParentRef(BaseModel):
    """A resolved cross-entry parent edge recorded at build time (#84).

    ``hash`` is the build-time parent content hash (the DAG edge). ``ref`` is the
    original ``tracked_expr_from_alias`` argument and ``follow`` its read-intent: an alias
    argument (``follow=True``) follows the alias head and goes stale as it
    advances; a literal hash (``follow=False``) pins that exact revision.
    """

    hash: str
    ref: str
    follow: bool


class Manifest(BaseModel):
    content_hash: str
    project: str
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    prompt: str | None = None
    code_path: str = "expr.py"
    schema_path: str = ENTRY_SCHEMA_FILENAME
    row_count: int | None = None
    execute_seconds: float | None = None
    # Whether the entry is materialized (ADR-008 D4): ``cache_worthy`` and its ``cache_worthy_why`` are the verdict of
    # ``worthiness.classify_expr``, computed once on the live expression when the entry is built and read from here
    # ever after. ``compile_seconds`` (the author DAG's expr->backend-plan step) and ``cache_bytes`` (the snapshot's
    # size, None for a cheap entry that has no file) are the measured side of the cache-admission record (#87).
    compile_seconds: float | None = None
    cache_worthy: bool | None = None
    cache_worthy_why: str | None = None
    cache_bytes: int | None = None
    # Content digest of the materialized snapshot (ADR-009 D2), ``arrow-sha256:<hex>``: a SHA-256 over the file's
    # ordered Arrow data, read back, so it does not move with the row-group size, the codec or the writer's version.
    # content_hash keys on the expression *graph*, result_digest on the executed *result*. A rewrite of the snapshot
    # whose digest differs is execution nondeterminism the structural hash can't see (sample()/now()/an impure UDF)
    # or an engine change, caught when a healed snapshot is checked. None for a cheap entry, which has no file.
    result_digest: str | None = None
    # Whether two runs of the query at create time gave the same digest (ADR-009 D6). False pins the snapshot, since
    # it cannot be re-created faithfully, and ``nonreproducible_columns`` names the columns whose digests differed.
    # None for a cheap entry, which is not run twice.
    reproducible: bool | None = None
    nonreproducible_columns: list[str] | None = None
    # The version of the snapshot format (row-group size and materialization batch size, ADR-009 D3) this entry's
    # snapshot and ordered copies were written with, and the engine versions at build (ADR-009 D4), so a mismatch at a
    # heal can say the engine changed instead of blaming the recipe.
    snapshot_format: int | None = None
    engine_versions: dict[str, str] | None = None
    # Ordered copies of sources this entry's plan reads (ADR-007 D13, ADR-008 D2), keyed by the copy's file stem:
    # ``{"source": rel_or_abs_path, "digest": md5 of the source, "suffix": ".csv", "reader": {"kind": "parquet"|"csv",
    # ...}, "content_digest": "arrow-sha256:..."}``. What ``ensure_materialized`` needs to make a deleted copy again
    # from its clone, and the digest the new copy is checked against.
    ordered_copies: dict[str, dict] | None = None
    # rel data path -> content md5, recorded when a source-identity mode is
    # active (tallyman_xorq.source_identity); absent under mode=off.
    sources: dict[str, str] | None = None
    # Resolved tracked_expr_from_alias parent edges ({hash, ref, follow}), recorded at build
    # time so the inter-entry DAG survives #73/#74; absent for root entries (#84).
    parents: list[ParentRef] | None = None


def write_manifest(entry_path: Path, manifest: Manifest) -> Path:
    # Atomic: manifest.json is the build's completeness sentinel — zip_pending_entries
    # gates on (child / "manifest.json").is_file(), and the checkpoint runs from a
    # separate process. A plain write_text is .is_file()-true the instant it is
    # truncated, so a checkpoint firing in the write window could zip a partial
    # manifest member; tmp + replace makes the member appear whole or not at all.
    out = entry_path / ENTRY_MANIFEST_FILENAME
    return atomic_write_text(out, json.dumps(manifest.model_dump(), indent=2))


def read_manifest(entry_path: Path) -> Manifest:
    raw = json.loads((entry_path / ENTRY_MANIFEST_FILENAME).read_text())
    return Manifest.model_validate(raw)
