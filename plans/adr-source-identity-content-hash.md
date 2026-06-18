# ADR: Content-addressed source reads (CAS) so `content_hash` tracks source data

- **Status:** Accepted (2026-06-18 — default flipped `off` → `cas` in #86, with
  `cp --reflink=auto` on Linux and `.cas` GC wired into `reset_to`; supersedes
  the 2026-06-10 Proposed draft below. See the reconstruction caveat under
  Consequences.)
- **Context ticket:** buckaroo-data/tallyman#30 (precondition for the
  result-cache rubric's content-stable `content_hash` key)
- **Affected code:** `src/tallyman_xorq/source_identity.py` (new),
  `src/tallyman_xorq/io.py` (`from_project`), `src/tallyman_xorq/build.py`
  (`build_and_persist`), `src/tallyman_core/manifest.py` (`sources` map)
- **Related ADR:** `plans/adr-result-cache-cost-rubric.md` (relies on
  `content_hash` being a content-stable cache key)
- **Evidence:** `tests/test_cache_lab.py` (marker `cache_lab`), reports under
  `tests/cache_lab_reports/`

## What changed since the first draft

The original draft assumed xorq hashes a parquet source via
`normalize_read_path_stat` (`(mtime, size, inode)`) and proposed passing
`normalize_read_path_md5sum` in `from_project` / `from_catalog`. Measurement
disproved both halves:

- The build hash never consults `normalize_method`. `content_hash` comes
  from `get_expr_hash` (xorq `provenance_utils.py:18`), which tokenizes
  inside `SnapshotStrategy().normalization_context()`; its
  `snapshot_normalize_read` (xorq `caching/strategy.py:49`) keys a Read on
  its **path string only**. Neither file stat nor file content reaches the
  hash, in xorq 0.3.26 and on xorq main.
- So the md5 opt-in is a no-op for identity, and the failure mode is worse
  than stat-fragility: identity is *blind to data changes*. The cache lab's
  `test_append_invalidates` shows it — append rows to `trips.parquet`,
  rebuild the same code, get the same `content_hash`, and the build-time
  dedup (`build.py:286`) returns the stale entry's result.

Path-only identity means: an in-place content change is invisible (stale
results served under the old hash — the dangerous direction); a
byte-identical rewrite or reset is stable (accidentally correct); the
README's "same code + same inputs → same hash" contract is false exactly
when inputs change. Cross-machine, the absolute path prefix forks identity —
that part of the original draft stands, but it is a separate problem this
ADR does not solve.

xorq is a frozen upstream for this project, so the fix must be entirely
tallyman-side.

## Decision

Content-address the source read path itself. `from_project`:

1. digests the file (md5, memoized per `(mtime_ns, size, inode)` in
   `artifacts/source_digests.json`, so an unchanged file is hashed once,
   not once per build);
2. ensures a copy-on-write clone at `data/.cas/<digest><suffix>` (APFS
   `cp -c`; plain copy as fallback — never a hardlink, which would share
   the inode and let an in-place edit corrupt the snapshot);
3. reads through the clone.

Since xorq hashes reads by path string, the path **is** the content
identity: same content → same path → same `content_hash`; changed content →
new digest → new path → new hash. Every xorq-level key derived from the
expression — the build hash, `ParquetSnapshotCache` keys, anything future —
becomes content-honest with no further guards.

`from_catalog` is unchanged: it already reads
`entries/<content_hash>/result.parquet`, so the parent's identity is in the
path by construction.

Implemented behind `TALLYMAN_SOURCE_IDENTITY=off|cas|salt` (default `off`)
so the cache lab can benchmark strategies; accepting this ADR means
flipping the default to `cas` after the consequences below are addressed.

## Measured behavior (cache lab, 150k-row matrix)

| invariant / cost                          | off (today)        | cas         | salt        |
| ----------------------------------------- | ------------------ | ----------- | ----------- |
| in-place content change forks identity    | no — serves stale  | yes         | yes         |
| self-heal faithful after source edit      | no                 | **yes**     | no          |
| stat-preserving swap                      | stale, incoherent  | stale but coherent | stale, incoherent |
| byte-identical rewrite keeps identity     | yes                | yes         | yes         |
| `from_catalog` child stable; reset dedups | yes                | yes         | yes         |
| build overhead (memo hit / new content)   | —                  | +13ms / +110ms per 71MB | same |

"Self-heal faithful" is load-bearing for the result-cache ADR: its
budget/eviction loop assumes an evicted `result.parquet` recomputes to what
was evicted. Under `cas` the clone is a snapshot of the bytes the entry was
built from, so regeneration is faithful even after the user edits the
source in place; under `off`/`salt` regeneration silently absorbs the new
data under the old identity.

## Alternatives considered

- **`normalize_read_path_md5sum` via `deferred_read_parquet` (the original
  draft).** Ineffective: the snapshot hasher never consults
  `normalize_method` (see "What changed"). Rejected on measurement.
- **Salt: mix tallyman-computed source digests into `content_hash`**
  (`build_and_persist` collects digests recorded by `from_project` during
  user-code import). Passes the same identity invariants with the same
  overhead, and keeps human-readable `data/` paths in builds. Rejected
  because it fixes only the entry *name* while every xorq-level key stays
  path-only: the lab demonstrated two salted entries (same code and path,
  different data) colliding on one `ParquetSnapshotCache` key, with the new
  entry serving the old entry's rows. `result_cache.py` now routes salt-mode
  reads around that cache, but the collision class survives for any future
  xorq-key surface, and self-heal stays unfaithful. The implementation is
  kept in-tree behind the env switch so the lab can re-compare; delete it
  when `cas` is made the default.
- **Stat-based identity (mtime/size/inode).** Unreachable for the same
  snapshot-normalization reason, and undesirable anyway: forks on touch /
  reset / transport, misses stat-preserving swaps.
- **Fix the hash in xorq.** Out of scope: frozen upstream.

## Consequences

- The README idempotency contract becomes true in both directions: same
  code + same data dedups; changed data forks. `test_append_invalidates`
  flips from failing to passing and pins it.
- One md5 pass per *new content version* of a source (~110ms per 71MB,
  measured), +~13ms per build for the memo hit; the clone is free on APFS.
  Note the original draft claimed the digest read was free because schema
  inference already reads the file — wrong, parquet schema inference reads
  only the footer; the memo is what keeps the cost out of the per-build
  path.
- The stat memo reintroduces a stat-keyed shortcut: a swap preserving
  `(mtime_ns, size, inode)` serves the stale digest (the hole git's index
  accepts). Failure degrades safely — the entry stays coherent with its own
  snapshot rather than serving mixed state — and `TALLYMAN_SOURCE_REHASH=1`
  closes it at full-rehash cost.
- On filesystems without copy-on-write (ext4), the clone is a real copy:
  each content version of each source doubles its storage until GC. Before
  flipping the default: add `cp --reflink=auto` on Linux and document the
  copy fallback.
- `.cas/` accumulates one clone per content version. Manifests now record
  each entry's `{rel_path: digest}` map (`manifest.py`), so GC is "delete
  digests unreferenced by any live entry"; wire it into reset/bullpen
  maintenance.
- Builds embed `data/.cas/<digest>.parquet` paths; UI provenance should
  display the human name from the manifest `sources` map.
- Cross-machine identity (absolute path prefix in the hash) remains broken
  under every mode — `pack`/`serve` portability needs its own decision.
- **Caveat — the flip does NOT make cold reconstruction content-faithful.**
  `cached_result_expr` re-runs a cheap entry's recipe on every cold read
  (`result_cache.py` `_recipe_expr` → `_import_script`), and `from_project`
  re-digests the *live* source under `cas` (`io.py:55-60`), so an evicted or
  cheap entry still serves edited bytes under its old `content_hash`. The flip
  makes *build* identity content-aware (a rebuild forks the hash) — what
  `test_append_invalidates` and the fast-suite pins assert — but closing the
  cold-read hazard (#74) needs digest-pinned reconstruction, tracked in #115.
