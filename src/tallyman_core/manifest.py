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
    # Cache-admission instrumentation (#87): the two verdicts recorded side by
    # side so the structural-vs-measured cache decision (#30) is decidable from
    # data. cache_worthy / cache_worthy_why are the structural classify_build
    # verdict (today computed and thrown away). compile_seconds (the author DAG's
    # expr->backend-plan step, the dominant per-view cost) and cache_bytes (the
    # baked snapshot size, the value-per-byte denominator) are the measured side;
    # with execute_seconds they give recompute_cost. cache_bytes is None for a
    # cheap entry that bakes no snapshot. All absent on entries built before #87.
    compile_seconds: float | None = None
    cache_worthy: bool | None = None
    cache_worthy_why: str | None = None
    cache_bytes: int | None = None
    # Order-sensitive content digest of the executed result bytes (#83), recorded
    # at build as a second identity axis: content_hash keys on the expression
    # *graph*, result_digest on the executed *bytes*. A cold recompute whose digest
    # differs is execution nondeterminism (sample()/now()/unordered limit/impure
    # UDF) the structural hash can't see — caught when a self-healed snapshot is
    # re-checked. Absent on entries built before #83.
    result_digest: str | None = None
    # Filename of the baked result-cache snapshot (the xorq cache key + .parquet),
    # recorded at build so the canonical read can assert its own derivation matches
    # (ADR D8, plans/adr-read-path-loads-builds.md). Build and read share one
    # derivation route; this is the tripwire that turns any future divergence (an
    # xorq tokenization change, a rewrite drift) into an immediate, attributable
    # failure instead of a silent wrong-file read. None for a cheap entry (bakes
    # no snapshot) and under salt identity mode.
    snapshot_key: str | None = None
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
