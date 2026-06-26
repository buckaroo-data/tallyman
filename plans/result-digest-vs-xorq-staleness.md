# result_digest vs xorq's content-aware cache staleness

- **Status:** Research note (2026-06-26)
- **Question:** xorq already ships a content-aware cache that knows when a cached
  result is stale. Why does tallyman maintain a separate `result_digest` /
  `verify_result_faithful`? Can we reuse xorq's staleness mechanism instead — or a
  portion of it?
- **Verdict:** The input-identity portion of xorq's mechanism is *already reused
  wholesale*. The output-faithfulness portion (`result_digest`) cannot be supplied
  by xorq at all — it answers a different question xorq's cache is structurally
  blind to — and is deliberately a thin, temporary crutch for LLM-authored recipes.
- **Related:** `plans/adr-result-digest-canonical-ordering.md`,
  `plans/89-determinism-prereqs-execution.md` (#83 digest / #88 lint / #89 epic),
  `plans/adr-source-identity-content-hash.md` (`content_hash` + source identity).
- **Method:** traced the installed `xorq` 0.3.26 / `xorq_datafusion` 0.2.7 source and
  tallyman `src/`; each load-bearing claim below was independently re-checked against
  the code.

## How xorq's content-aware staleness works

It is purely **input-addressed**, with no explicit invalidation step:

- The cache key is re-derived from the expression on every access (`Cache.calc_key`
  → `strategy.calc_key(expr)`, `xorq/caching/__init__.py:62-67`). The only "is it
  stale?" check is `storage.exists(key)` — a bare filename-presence test
  (`ParquetStorage.exists` = `get_path(key).exists()`, `xorq/caching/storage.py:120-121`)
  or a backend-table-presence test (`SourceStorage.exists` = `key in source.tables`,
  `storage.py:187-188`). **A cache miss *is* the staleness signal:** a changed input
  yields a different key, the new key isn't present, so it recomputes; the old
  artifact orphans under its old key.
- All "content-awareness" lives in **key derivation** (the strategy).
  `ModificationTimeStrategy.calc_key` returns `key_prefix + expr.ls.tokenized`
  (`strategy.py:88-90`); the default data-sensitive HASHER normalizes each source leaf
  via `normalize_read_path_stat`, folding `(st_mtime, st_size, st_ino)` into the leaf
  token (`xorq/common/utils/defer_utils.py:86-97`, reached through the Read-path
  normalization at `_paths.py:77` — the coupling is indirect, not a call inside
  `strategy.py`). There is also an opt-in content variant `normalize_read_path_md5sum`
  (`defer_utils.py:82-83`). `SnapshotStrategy` keys on path + structure only — "path
  identity only, not file modification stats" (`strategy.py:49-69`).
- **No strategy ever hashes or compares the executed OUTPUT bytes.** `put` writes the
  result parquet (`storage.py:131-138`); `get` reads it back (`123-129`); neither
  validates stored bytes against a recompute. The cache *assumes determinism*: same
  key ⇒ the stored bytes are by assumption the correct output.
- (`ParquetTTLStorage.exists`, `storage.py:160-168`, adds age-based eviction on the
  cache file's own mtime vs wall-clock — a TTL, not a source- or output-content check.
  So "the only check is `exists()`" is true, but `exists()` carries a TTL dimension in
  that one subclass.)

## Two non-overlapping questions

| | xorq cache staleness | tallyman `result_digest` |
| --- | --- | --- |
| Asks | "Have the **inputs** moved?" | "Does the **output** still reproduce?" |
| Addressed on | input identity (tokenized graph + source-leaf identity) | output bytes (`sha256` of the baked snapshot) |
| Signal | key changed ⇒ cache miss ⇒ recompute | recorded digest ≠ re-baked digest ⇒ unfaithful |
| Blind to | the same key yielding different bytes | anything about inputs |

Input-identity is necessary but not sufficient. A fixed graph over fixed declared
inputs can still execute to different bytes: `sample()`, `now()`, an unordered
`limit`, an impure UDF, or source drift under `off` identity. The plans say it
directly: the structural hash "is a `SnapshotStrategy` tokenization of the graph; it
cannot see this" (`89-determinism-prereqs-execution.md:40-42`); these failures "leave
the hash fixed and only move the *bytes*" (`:53-61`). That gap is exactly what
`result_digest` exists to police.

## What tallyman already reuses from xorq

tallyman takes xorq's entire input-identity stack and layers its own output audit on
top:

| xorq mechanism | tallyman analog | evidence |
| --- | --- | --- |
| graph tokenizer (`build_expr` → `xxh128` over the normalized graph) | `content_hash` is literally the build-dir basename | `build.py:402,407` (verbatim in default `cas` mode; `salt` mode post-wraps it, `build.py:408-411`) |
| `ParquetSnapshotCache` (SnapshotStrategy, path-identity keys) | used directly as the `.cache()` argument | `source_cache.py:118,160-161` (it is a Cache storage+strategy class; the graph node it yields is a `CachedNode`) |
| `ModificationTimeStrategy` mtime fold (source-leaf identity in the key) | `source_identity` content digest — the content-hash *upgrade* of the mtime fold | `cas` reroutes reads to a digest-named clone so the tokenized path embeds the digest (`io.py:71-74`); md5-over-bytes memoized on `(mtime_ns,size,inode)` (`source_identity.py:70-101`) |
| `storage.exists(key)` (per-expression "did the key change?") | `staleness.scan` / `entry_staleness` (catalog-level, two input axes) | followed-alias head vs recorded hash + source digest re-digest (`staleness.py:74-114,84-107`) |

So the "did an input move?" question is answered the xorq way, only stronger: a real
byte digest instead of `(st_mtime,st_size,st_ino)`, lifted onto git-versioned manifest
entries, with a forced rehash (`_force_source_rehash`) that catches same-stat in-place
edits xorq's mtime strategy would miss.

## Why xorq's cache cannot replace result_digest

- **Structurally blind to byte drift.** xorq never hashes or compares output bytes;
  `exists(key)` short-circuits on the input-derived key and assumes the stored bytes
  are correct. There is no xorq primitive that could detect output drift.
- **Frozen upstream.** Modifying xorq's hash to fold this in is explicitly rejected /
  out-of-scope (`adr-source-identity-content-hash.md:111`).
- **Abstraction mismatch.** tallyman reads results across a deliberate parquet boundary
  — `cached_result_expr` returns `deferred_read_parquet(snapshot)` rather than
  re-chaining the recipe (`result_cache.py`), to stay single-backend and skip
  re-running the DAG. Identity lives in a git-versioned manifest of catalog entries
  (`manifest.py:51-57`), not bound to one live in-process expression the way xorq's
  per-access key is.

tallyman's own code already draws this exact line: `result_digest` is "deliberately
*not* a staleness input: a recompute-differs entry is *nondeterministic*, not stale,
and recompute cannot make it fresh" (`staleness.py:20-22`; `recalc.py:33-36`).

## The reusable portion

Everything on the **input-identity** axis — and it is already taken. The tokenizer
(`build_expr` → `content_hash`) stays borrowed. The source-staleness idea is reused in
the right (stronger) form via `source_identity` + `staleness.scan`. The one
transferable *idea* still worth naming is xorq's `SnapshotStrategy` path-identity
discipline — pin identity to a stable path/structure token, decouple it from volatile
stat — which `cas` mode already operationalizes by making the path itself
content-addressed. Nothing on the **output-faithfulness** axis is reusable, because no
xorq strategy touches output bytes.

## result_digest is a crutch

`result_digest` exists because tallyman's recipes are **LLM-authored**, and an LLM can
inject nondeterminism (`now()`, `sample()`, unordered `limit`, an impure UDF) that a
human expert would not. xorq's cache has no equivalent precisely because it assumes a
**trusted author** — same key ⇒ blind reuse. So `result_digest` is the *tax of letting
an LLM write the cached computation*, and it should retract as the generator becomes
trustworthy.

The retraction path has a decidable part and a hard kernel:

- **Decidable nondeterminism** — `now()`, `sample()`, unordered `limit`, a baked random
  literal — is visible in the graph; the #88 structural lint (and
  `recipe_is_structurally_nondeterministic`) can reject or flag it at author time. For
  entries the lint proves clean, the digest can be skipped entirely (ADR open Q3). This
  shrinks the crutch as lint coverage grows, independent of model quality.
- **Undecidable nondeterminism** — an impure UDF (arbitrary Python) — cannot be proven
  pure statically. A *smarter* model doesn't close this: even a perfect author can
  write a UDF whose impurity isn't visible in source. So "reliable deterministic code
  gen" has to mean **the UDF surface is constrained or verified to a pure subset**
  (sandbox, allowlist, purity checker), not just a better LLM. Until that exists, a
  runtime byte-level audit is the only backstop for the UDF residual.

This argues for keeping `result_digest` exactly as the canonical-ordering branch
shapes it: thin, ≈free (hash the snapshot bytes already being baked), nothing recorded
for cheap entries, and pointedly *not* a staleness/recompute trigger. A crutch for an
untrusted generator is most valuable as a **steering signal back to the generator** —
surfacing "this recipe isn't reproducible" so the recipe gets rewritten — so the digest
should be loud and legible in the self-heal / diff / warning surface, with
cache-soundness as the side benefit.

The branch's contribution, in this light, is that it fixed a *broken* crutch: the old
order-sensitive digest flagged datafusion's benign parallel-scan reordering as if it
were LLM-introduced nondeterminism (false drift). The multiset / canonical-order
definition re-scopes the audit to fire only on *genuine* nondeterminism — the kind a
better generator (or completed lint) would actually prevent.

## Recommendation

1. Drive staleness/recompute entirely from input-addressing — `content_hash` +
   `source_identity` + `staleness.scan` — which already *is* xorq's mechanism. Keep
   `result_digest` out of that path (it already is).
2. Keep `result_digest` as a thin output-faithfulness / eviction-soundness audit,
   consumed by `verify_result_faithful` and self-heal, in its post-branch form
   (`sha256` of the canonical-order snapshot; nothing for cheap entries).
3. Do not try to merge the two or "use xorq's cache instead" — xorq cannot see the
   bytes. Treat `result_digest` as a removable crutch with the retraction path above;
   invest in #88 lint coverage and a constrained UDF surface, not in elaborating the
   digest.
