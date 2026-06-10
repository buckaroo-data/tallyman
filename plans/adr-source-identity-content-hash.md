# ADR: Content-address source reads so entry identity survives reset and transport

- **Status:** Proposed (2026-06-10)
- **Context ticket:** buckaroo-data/tallyman#30 (precondition for the result-cache rubric's reset-stable `content_hash` key)
- **Affected code:** `src/tallyman_xorq/io.py` (`from_project`, `from_catalog`)
- **Related ADR:** `plans/adr-result-cache-cost-rubric.md` (relies on `content_hash` being a stable cache key)

## Problem

An entry's `content_hash` is xorq's build hash, and the README states the
contract "same code + same inputs → same hash → same entry dir (idempotent)".
That contract is weaker than it reads, because of how xorq hashes a parquet
source by default.

`xo.deferred_read_parquet` defaults to `normalize_method=normalize_read_path_stat`
(`xorq/python/xorq/common/utils/defer_utils.py:235`), which contributes
`(st_mtime, st_size, st_ino)` to the hash, not the file's content
(`defer_utils.py:84`). The build-hash tokenizer confirms this: for an absolute
path that exists it calls `read.normalize_method(p)`
(`xorq/python/xorq/common/utils/dasher/_relations.py:85`), and the compiler's
canonicalization defaults to the same stat method (`ibis_yaml/compiler.py:261`,
`compiler.py:400`). The content-hash variant `normalize_read_path_md5sum` exists
but is opt-in. (Memtables are separately forced to content-md5 at
`compiler.py:602`; only file reads use stat.)

tallyman lands in the stat branch: `from_project` resolves to an **absolute**
path and calls plain `deferred_read_parquet` (`io.py:38`), and its own docstring
notes the build embeds absolute paths. So **`content_hash` depends on the source
file's `(mtime, size, inode)`**, with these consequences:

1. **Reset-to / git checkout forks identity for identical content.** Checkout
   writes a new file — new inode, new mtime — so re-deriving an entry after a
   reset yields a *different* `content_hash` for the same logical input.
2. **Cross-machine transport forks identity.** `tallyman pack` / `serve` on
   another machine sees different mtime+inode, so the same project rebuilds to
   different hashes.
3. **In-place result regeneration forks downstream identity.** `ensure_result`
   regenerates a cheap entry's `result.parquet` in place
   (`result_cache.py:142`), changing its mtime+inode; any child built via
   `from_catalog` afterward gets a different hash.
4. **Conversely, a stat-preserving content swap goes stale.** A replacement that
   preserves size+mtime+inode keeps the hash, so a genuine content change is
   missed.

This does not corrupt an *existing* entry — `content_hash` is frozen into the
dir name at build (`build.py:271`) and never recomputed on load, so caches keyed
on it stay valid through reset and reload. The damage is on *re-derivation*: a
logically-identical rebuild produces a "new" duplicate entry, defeating the
build-time dedup (`build.py:275`) and orphaning the caches keyed on the old
hash.

## Decision

Pass `normalize_method=normalize_read_path_md5sum` in `from_project` and
`from_catalog`, so a source read contributes its content digest to the build
hash instead of filesystem stat.

## Alternatives considered

- **Keep stat hashing (status quo).** Fragile under exactly the operations
  tallyman wants to support — reset-to, pack/serve across machines, in-place
  regeneration. Rejected.
- **Build-relative `read_path` hashing.** xorq hashes a build-relative path as
  the path string alone (`dasher/_relations.py:81`), which is portable but
  content-*insensitive*: a content swap at the same relative path is never
  detected. Worse than stat for correctness. Rejected.
- **mtime-based cache invalidation layered on top.** Rejected on independent
  grounds: git checkout rewrites mtimes to "now" non-monotonically, so any
  mtime ordering check is fooled by reset in both directions (false-keep and
  false-invalidate). Content-addressing is the only reset-safe key.

## Consequences

- `content_hash` becomes stable across reset, machine, and in-place
  regeneration; the README idempotency claim becomes true, and `pack`/`serve`
  portability (currently flagged as not-yet-working in `io.py`) gets its
  identity precondition.
- This is the precondition the result-cache ADR relies on when it keys caches on
  `content_hash` and asserts reset-safety.
- One md5 pass over each source file per build. Cheap — the file is already read
  to infer schema — and one-time per content hash.
- This is a tallyman-side usage change only (which `normalize_method` we pass);
  **no modification to xorq**. Consistent with xorq's own choice to content-hash
  memtables.
- Deliberately recorded as its own ADR, separate from the cache-rubric ADR, so
  the source-identity decision can be revisited atomically without reopening the
  caching design.
