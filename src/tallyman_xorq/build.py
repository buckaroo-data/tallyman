from __future__ import annotations

import importlib.util
import json
import logging
import re
import shutil
import sys
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from tallyman_core import (
    ENTRY_MANIFEST_FILENAME,
    Manifest,
    atomic_write_text,
    ensure_project,
    entries_dir,
    entry_build_dir,
    entry_dir,
    entry_manifest_path,
    entry_schema_path,
    project_dir,
    write_manifest,
)
from tallyman_xorq.portable import make_portable_inplace

# Perf instrumentation rides a dedicated child namespace so it can be dialed up
# independently of the rest of tallyman's logging (#60), via TALLYMAN_LOG_LEVEL.
perf_log = logging.getLogger("tallyman.perf")


def _append_prompt(project: str, content_hash: str, prompt: str | None) -> None:
    """Append a prompt event to the entry's tracked ``prompts/<hash>.jsonl``.

    Relocated out of the (now gitignored) entry dir so the re-run history the
    UI disclosure shows survives a reset/clone — it is append-mutable provenance
    that the content-addressed recipe zip deliberately excludes.
    """
    if not prompt:
        return
    from tallyman_core.paths import prompts_path

    record = {
        "prompt": prompt,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    p = prompts_path(project, content_hash)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Atomic read-modify-write rather than an O_APPEND write: _append_prompt runs
    # outside the project lock (only the checkpoint holds it), so the checkpoint's
    # git add -A can fire mid-append from a separate process. tmp + replace makes
    # prompts/<hash>.jsonl always whole — never a torn trailing line (C2).
    existing = p.read_text() if p.exists() else ""
    atomic_write_text(p, existing + json.dumps(record) + "\n")


def read_prompts(project: str, content_hash: str) -> list[dict]:
    """Return every prompt seen for this entry, oldest first."""
    from tallyman_core.paths import prompts_path

    p = prompts_path(project, content_hash)
    if not p.exists():
        return []
    with p.open() as fh:
        return [json.loads(line) for line in fh if line.strip()]


@dataclass
class BuildResult:
    content_hash: str
    entry_path: Path
    row_count: int
    execute_seconds: float
    schema: dict
    # Whether the complete entry dir was persisted to disk (D2). None on the
    # re-run path (entry already on disk); True on a fresh build. Durability —
    # committing the recipe zip — is the next checkpoint's job, which always
    # succeeds because the dir is complete, so there is no False case anymore.
    catalog_registered: bool | None = None
    # Advisory build-time lints (#88): execution-nondeterministic ops whose result
    # varies run-to-run under one content_hash. Empty when the recipe is clean.
    lint_warnings: list[str] = field(default_factory=list)
    # Cache-admission instrumentation (#87), mirrored from the manifest for
    # in-process callers. See Manifest for the fields' meaning. None on entries
    # built before #87 (read back from a manifest that predates the keys).
    compile_seconds: float | None = None
    cache_worthy: bool | None = None
    cache_worthy_why: str | None = None
    cache_bytes: int | None = None


class BuildError(RuntimeError):
    pass


def _user_imports_bare_ibis(code: str) -> bool:
    """True if the user code imports the real `ibis` package directly.

    `import xorq.vendor.ibis as ibis` is fine and does not match.
    """
    for raw_line in code.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line == "import ibis" or line.startswith("import ibis "):
            return True
        if line.startswith("from ibis ") or line.startswith("from ibis."):
            return True
    return False


# Read helpers the model reaches for on the wrong namespace. The project-aware
# way is read_project_file/tracked_expr_from_alias; xo.deferred_read_parquet handles a raw path.
_READ_FNS = frozenset(
    {"read_parquet", "read_csv", "read_in_memory", "read_delta", "deferred_read_parquet", "deferred_read_csv"}
)
# Elementwise math the model reaches for as `ibis.fn(col)`; they are column methods.
_IBIS_MATH = frozenset(
    {
        "sin",
        "cos",
        "tan",
        "asin",
        "acos",
        "atan",
        "atan2",
        "cot",
        "exp",
        "ln",
        "log",
        "log2",
        "log10",
        "sqrt",
        "abs",
        "ceil",
        "floor",
        "round",
        "sign",
        "power",
        "pow",
        "degrees",
        "radians",
    }
)


def _ibis_import_hint(exc_msg: str, code: str = "") -> str:
    """Actionable hints for the namespace / column mistakes seen in the wild.

    Reactive backstop to the proactive guidance in the MCP tool docstrings:
    fired only after user code (or build_expr) raises, and shown to the model
    in the tool's error response. Each branch maps a recurring failure
    signature from the session scan to a one-line correction. Multiple signals
    can co-fire; their hints are concatenated.

    Signatures handled:
      - bare `import ibis` / `from ibis ...`, or the vendored-Expr class
        mismatch (expression built against the wrong ibis instance);
      - `module 'xorq' has no attribute 'X'` — xorq is not the API entrypoint
        (`import xorq.api as xo`); `_` lives on `xorq.vendor.ibis`;
      - `module 'ibis' has no attribute 'X'` — math is a column method, reads
        go through read_project_file/tracked_expr_from_alias, `ibis.case` is now `ibis.cases`;
      - `cannot import name 'X' from 'tallyman_xorq.io'` — invented loader;
      - any duckdb reach — there is no duckdb backend, only datafusion;
      - `'Table' object has no attribute 'X'` — guessed a column name.
    """
    hints: list[str] = []

    has_bad_import = _user_imports_bare_ibis(code)
    has_expr_mismatch = "xorq.vendor.ibis.expr.types.core.Expr" in exc_msg and "must be" in exc_msg
    if has_bad_import or has_expr_mismatch:
        hints.append(
            "this usually means the expression was built using `import ibis` "
            "instead of `import xorq.vendor.ibis as ibis`. Replace any "
            "`import ibis` (or `from ibis import ...`) with "
            "`import xorq.vendor.ibis as ibis` and rebuild."
        )

    m = re.search(r"module 'xorq' has no attribute '(\w+)'", exc_msg)
    if m:
        name = m.group(1)
        if name == "_":
            if not has_bad_import:
                hints.append(
                    "`xorq` has no `_` deferred accessor. Use `ibis._` where "
                    "`ibis` comes from `import xorq.vendor.ibis as ibis`."
                )
        elif name in _READ_FNS:
            hints.append(
                f"`xorq.{name}` does not exist — `xorq` is not the API entrypoint "
                "(`import xorq.api as xo`). Read data with `from tallyman_xorq.io "
                "import read_project_file, tracked_expr_from_alias, tallyman_read_csv`; "
                "use `tallyman_read_csv(abs_path, schema=...)` for CSVs (the only "
                "supported CSV ingest) and `xo.deferred_read_parquet(abs_path)` "
                "for a raw parquet path."
            )
        else:
            hints.append(
                f"`xorq.{name}` does not exist — `xorq` is not the API entrypoint. "
                f"`import xorq.api as xo` and call `xo.{name}` "
                "(e.g. xo.memtable, xo.deferred_read_parquet, xo.connect)."
            )

    m = re.search(r"module 'ibis' has no attribute '(\w+)'", exc_msg)
    if m:
        name = m.group(1)
        if name in _IBIS_MATH:
            hints.append(f"math/elementwise functions are column methods: `col.{name}()`, not `ibis.{name}(col)`.")
        elif name in {"read_parquet", "read_csv", "read_table"}:
            hints.append(
                f"`ibis.{name}` does not exist — read data with "
                "`from tallyman_xorq.io import read_project_file, tracked_expr_from_alias, tallyman_read_csv`; "
                "use `tallyman_read_csv(abs_path, schema=...)` for CSVs."
            )
        elif name == "case":
            hints.append("`ibis.case` is gone — use `ibis.cases((cond, val), ..., else_=default)`.")
        else:
            hints.append(
                f"`ibis.{name}` does not exist on `xorq.vendor.ibis`. Many operations are column methods "
                "(`col.method()`), and data is read via read_project_file/tracked_expr_from_alias."
            )

    m = re.search(r"cannot import name '(\w+)' from 'tallyman_xorq\.io'", exc_msg)
    if m:
        hints.append(
            f"`tallyman_xorq.io` has no `{m.group(1)}` — it exports `read_project_file` "
            "(raw files under <project>/data/), `tracked_expr_from_alias` (alias → records lineage), "
            "`pinned_expr_from_alias` (alias or hash, no lineage), and `tallyman_read_csv` "
            "(CSV ingest with original_row_order for stable digests)."
        )

    if "duckdb" in exc_msg.lower():
        hints.append(
            "there is no duckdb backend here — the only backend is xorq's built-in datafusion. "
            "Build with the ibis expression API over read_project_file/tracked_expr_from_alias sources; "
            "do not use duckdb, `.sql()`, or `con.register()`."
        )

    m = re.search(r"'Table' object has no attribute '(\w+)'", exc_msg)
    if m:
        name = m.group(1)
        hints.append(
            f"`{name}` is not a column on this table — attribute access on a missing column raises this "
            "opaque error. List the entry's columns first (catalog_list shows a compact columns summary "
            "per entry, or use the source step's returned `schema`) and reference exact names; "
            f"when unsure use `t['{name}']`, whose error lists the real columns."
        )

    if not hints:
        return ""
    return "\n\nHint: " + " ".join(hints)


# Execution-nondeterministic ops (#88): their result varies run-to-run for the
# *same* expression graph, so the structural content_hash can't see the change
# and #74's recompute-on-cold-read can serve different bytes than were built.
# Mapped to the friendly call that builds each. Deliberately NOT flagged: bare
# unordered LIMIT/head (deterministic-enough, and far too common to warn on
# without an ordering analysis) and impure UDFs (purity is undetectable from the
# graph). The durable, hash-invisible fix is tracked in #83.
_NONDETERMINISTIC_OPS = {
    "TimestampNow": "now()",
    "DateNow": "today()",
    "RandomScalar": "random()",
    "RandomUUID": "uuid()",
    "Sample": "sample()",
}


def _csv_direct_read_check(expr) -> None:
    """Raise BuildError if the recipe calls xo.deferred_read_csv directly.

    tallyman_read_csv is the only supported CSV ingest path — it bakes an
    order-stable polars intermediate so result_digest is byte-reproducible
    across builds. A raw deferred_read_csv bypasses that and produces a
    nondeterministic row order above datafusion's repartition threshold.
    """
    try:
        from xorq.expr.relations import Read

        csv_nodes = [
            node
            for node in expr.op().find(Read)
            if node.name.startswith("ibis_xorq-read_csv_")
        ]
    except Exception:
        return  # if the walk fails, don't block the build
    if csv_nodes:
        raise BuildError(
            "xo.deferred_read_csv is not allowed in tallyman recipes — "
            "use tallyman_read_csv(abs_path, schema=...) instead. "
            "tallyman_read_csv bakes an order-stable polars intermediate so "
            "result_digest is byte-reproducible; deferred_read_csv gives "
            "nondeterministic row order above datafusion's repartition threshold."
        )


def _nondeterminism_warnings(expr) -> list[str]:
    """Advisory lint: flag execution-nondeterministic ops in a recipe (#88).

    Returns at most one hint (or none). Best-effort by design — a walk failure
    must never break an otherwise-valid build, so any error yields no warning.
    """
    try:
        import xorq.vendor.ibis.expr.operations as ops

        types = []
        for name in _NONDETERMINISTIC_OPS:
            op_cls = getattr(ops, name, None)
            if op_cls is not None:
                types.append(op_cls)
        if not types:
            return []
        found = set()
        for node in expr.op().find(tuple(types)):
            # A seeded sample is reproducible run-to-run; only unseeded sampling
            # is nondeterministic, so don't flag a sample that carries a seed.
            if type(node).__name__ == "Sample" and getattr(node, "seed", None) is not None:
                continue
            found.add(type(node).__name__)
    except Exception:
        return []
    pretty = sorted({_NONDETERMINISTIC_OPS[n] for n in found if n in _NONDETERMINISTIC_OPS})
    if not pretty:
        return []
    return [
        "nondeterministic op(s) "
        + ", ".join(pretty)
        + " make this entry's result vary run-to-run under one content_hash; on a "
        "cold cache the viewer or a diff can show different bytes than were built "
        "(#88). Seed the sample or materialize the value at author time for a "
        "reproducible entry."
    ]


def _import_script(code: str) -> tuple[object, Path]:
    """Write code to a temp file, import it, return (module, temp_path).

    The temp file is kept alive for the caller to copy into the entry dir.
    """
    tmp = Path(tempfile.gettempdir()) / f"tallyman_expr_{uuid.uuid4().hex}.py"
    tmp.write_text(code)
    spec = importlib.util.spec_from_file_location(f"tallyman_expr_{tmp.stem}", tmp)
    if spec is None or spec.loader is None:
        raise BuildError(f"Could not load module spec from {tmp}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        hint = _ibis_import_hint(str(exc), code)
        raise BuildError(f"executing user code raised: {exc}{hint}\n{traceback.format_exc()}") from exc
    return module, tmp


def build_and_persist(
    project: str,
    code: str,
    expr_name: str = "expr",
    prompt: str | None = None,
) -> BuildResult:
    """Compile user code with xorq, materialize a parquet, write a catalog entry.

    The user code must bind a variable named `expr_name` (default "expr") to an
    ibis/xorq expression. Imports happen in a fresh module scope.
    """
    from xorq.ibis_yaml.compiler import build_expr, load_expr

    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq._git_state_guard import install_git_state_guard

    # git-provenance capture in xorq's compiler can crash (git SIGSEGV when forked
    # from the long-lived server) and abort the whole build. Make it best-effort.
    install_git_state_guard()

    ensure_project(project)

    # Collect source digests while user code imports (read_project_file notes each
    # file it reads); cas/salt identity modes consume them below.
    from tallyman_xorq import parent_capture as pc
    from tallyman_xorq import source_identity as si

    collect_token = si.begin_collect()
    parent_token = pc.begin_collect()
    try:
        module, tmp_script = _import_script(code)
    finally:
        sources = si.end_collect(collect_token)
        # Resolved tracked_expr_from_alias parent edges captured during import (#84).
        parents = pc.end_collect(parent_token)
    expr_obj = getattr(module, expr_name, None)
    if expr_obj is None:
        names = ", ".join(n for n in dir(module) if not n.startswith("_"))
        raise BuildError(f"variable {expr_name!r} not found in code. Available names: {names}")

    # Advisory nondeterminism lint (#88) on the author's expression, before the
    # rewrite wraps it in cache nodes. Surfaced on the result, never fatal.
    lint_warnings = _nondeterminism_warnings(expr_obj)

    # Fatal: raw deferred_read_csv is banned — tallyman_read_csv is the only
    # supported CSV ingest path (order-stable polars intermediate).
    _csv_direct_read_check(expr_obj)

    # Rewrite-then-build (#73): cache each non-parquet source read, bake a
    # top-level result cache when the expression is expensive (so every loader,
    # incl. the Buckaroo viewer, reads the cached result instead of re-running
    # the DAG), and reject in-memory reads. The content_hash below is computed
    # from this rewritten expression; xorq_build/ carries the cache nodes, while
    # expr.py keeps the author's literal source (tallyman does not depend on the
    # submitted form).
    from tallyman_xorq.source_cache import InMemoryReadError, rewrite_for_build

    # The author's DAG before cache injection — compile_seconds (#87) times its
    # expr->backend-plan step, the un-truncated "large expression DAG" recompile
    # that #30's profiling found dominates per-view cost. The rewritten expression
    # carries cache boundaries that would truncate that compile.
    author_expr = expr_obj
    try:
        expr_obj = rewrite_for_build(expr_obj, project)
    except InMemoryReadError as exc:
        raise BuildError(str(exc)) from exc

    created_target = False
    try:
        # Use a temp builds_dir so xorq's hash naming doesn't collide; we move
        # things into our catalog layout afterwards.
        with tempfile.TemporaryDirectory(prefix="tallyman_build_") as builds_str:
            builds_dir = Path(builds_str)
            try:
                build_path = Path(build_expr(expr_obj, builds_dir=builds_dir))
            except Exception as exc:
                hint = _ibis_import_hint(str(exc), code)
                raise BuildError(f"build_expr failed: {exc}{hint}\n{traceback.format_exc()}") from exc

            content_hash = build_path.name
            if si.mode() == "salt" and sources:
                # xorq's hash is path-identity only; mixing the source digests in
                # makes the entry hash content-sensitive (same shape, 12 hex).
                content_hash = si.salted_hash(content_hash, sources)
            target = entry_dir(project, content_hash)

            if target.exists():
                # Same content hash already on disk — nothing to do beyond return.
                # Record this invocation's prompt as a re-run event.
                manifest_path = entry_manifest_path(project, content_hash)
                if manifest_path.exists():
                    meta = json.loads(manifest_path.read_text())
                    _append_prompt(project, content_hash, prompt)
                    return BuildResult(
                        content_hash=content_hash,
                        entry_path=target,
                        row_count=meta.get("row_count", 0),
                        execute_seconds=meta.get("execute_seconds", 0.0),
                        schema=json.loads(entry_schema_path(project, content_hash).read_text()),
                        lint_warnings=lint_warnings,
                        compile_seconds=meta.get("compile_seconds"),
                        cache_worthy=meta.get("cache_worthy"),
                        cache_worthy_why=meta.get("cache_worthy_why"),
                        cache_bytes=meta.get("cache_bytes"),
                    )

            target.mkdir(parents=True, exist_ok=True)
            created_target = True  # past the complete-entry early-return; safe to rmtree on failure

            # Move xorq's build directory contents (expr.yaml + deps) under the entry.
            xorq_build_dir = entry_build_dir(project, content_hash)
            xorq_build_dir.mkdir(exist_ok=True)
            for item in build_path.rglob("*"):
                dest = xorq_build_dir / item.relative_to(build_path)
                if item.is_dir():
                    dest.mkdir(parents=True, exist_ok=True)
                else:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(item.read_bytes())

            # Rewrite project-root substrings to a portable placeholder so the
            # build is loadable on any machine with the same project laid out.
            make_portable_inplace(xorq_build_dir, project_dir(project))

            # Persist the user's source, also rewriting any project-root paths.
            code_persisted = code.replace(str(project_dir(project)), "${TALLYMAN_PROJECT_ROOT}")
            (target / "expr.py").write_text(code_persisted)

            # Load + execute.
            try:
                # Per-project compute cache (not the global ~/.cache/xorq), so a
                # reset can prune it to the revision's warm-set and a freshly added
                # expression computes cold. See plans/adr-reset-to-revision.md.
                cache_dir = compute_cache_dir(project)
                cache_dir.mkdir(parents=True, exist_ok=True)
                loaded = load_expr(build_path, cache_dir=cache_dir)
            except Exception as exc:
                hint = _ibis_import_hint(str(exc), code)
                raise BuildError(f"load_expr failed: {exc}{hint}\n{traceback.format_exc()}") from exc

            # Classify before executing (reads the serialized build, no eval): the
            # verdict selects the execution strategy below and is reused for the #87
            # admission instrumentation after the temp dir is gone.
            from tallyman_xorq.result_cache import _cached_node_path, classify_build  # noqa: PLC0415

            verdict = classify_build(xorq_build_dir)
            cache_worthy_v, cache_worthy_why = verdict["worthy"], verdict["why"]

            # Execute (#73). A worthy entry's build carries a baked result-cache
            # node (rewrite_for_build), so executing materialises it once into the
            # compute cache — and every later loader (the Buckaroo viewer, diffs,
            # tracked_expr_from_alias) reads that cached result instead of re-running the DAG;
            # baking evaluates every row, so count() there both validates and counts.
            # A cheap entry materialises nothing, so count() alone is satisfied from
            # source metadata and PRUNES the row projection — a failing cast /
            # arithmetic / UDF would never evaluate at build, committing + aliasing a
            # broken entry that throws only on the first materialising read. Stream
            # the full result and discard it to force row-level evaluation at author
            # time (constant memory; the one pass yields the exact row count).
            #
            # result_digest (ADR adr-result-digest-canonical-ordering): for worthy
            # entries the baked snapshot is sorted by original_row_order before
            # materialisation, so its bytes are deterministic run-to-run and we
            # hash the file directly (cheap, ~0.3s). Cheap entries record no digest
            # — their result has no snapshot to hash.
            from tallyman_xorq.result_cache import snapshot_file_digest, stream_row_count  # noqa: PLC0415

            t0 = time.monotonic()
            try:
                arrow_schema = loaded.schema().to_pyarrow()
                if cache_worthy_v:
                    # count() bakes the snapshot (the cache node forces full
                    # materialisation); then hash the baked file for the digest.
                    # The sort-before-bake is applied to author_expr before
                    # rewrite_for_build (see above), so the CachedNode already
                    # wraps the sorted expression and loaded bakes in canonical order.
                    row_count = int(loaded.count().execute())
                    snap_path = _cached_node_path(loaded)
                    result_digest_v: str | None = (
                        snapshot_file_digest(snap_path) if snap_path and snap_path.exists() else None
                    )
                else:
                    # Stream full result to force row-level evaluation (catch a
                    # failing cast / arithmetic at build time) and count rows.
                    # No digest for cheap entries — no snapshot to hash.
                    row_count = stream_row_count(loaded)
                    result_digest_v = None
            except Exception as exc:
                hint = _ibis_import_hint(str(exc), code)
                raise BuildError(f"build execution failed: {exc}{hint}\n{traceback.format_exc()}") from exc
            execute_seconds = round(time.monotonic() - t0, 3)

        # Schema + row count come from the expression directly (no result.parquet to
        # read back); a worthy entry's rows are already materialised in its cache.
        schema_doc = {
            "fields": [{"name": f.name, "type": str(f.type)} for f in arrow_schema],
            "row_count": row_count,
        }
        atomic_write_text(entry_schema_path(project, content_hash), json.dumps(schema_doc, indent=2))

        # Cache-admission instrumentation (#87): record the structural verdict
        # (computed above, before execute, since it now also selects the execution
        # strategy) alongside the measured value-per-byte inputs, so #30's
        # structural→measured flip is decidable from data. compile_seconds times the
        # author DAG's expr->backend-plan step (the dominant per-view cost per #30),
        # separate from execute; cache_bytes is the baked snapshot size (None for a
        # cheap entry that bakes nothing). loaded is the build_path expression whose
        # execute just baked the snapshot, so its CachedNode names that exact file.
        compile_seconds: float | None = None
        try:
            from xorq.expr.api import to_sql

            t_compile = time.monotonic()
            to_sql(author_expr)
            compile_seconds = round(time.monotonic() - t_compile, 3)
        except Exception:
            # Best-effort: a DAG xorq can't render to SQL must not break the build.
            pass

        cache_bytes: int | None = None
        snap = _cached_node_path(loaded)
        if snap is not None and snap.exists():
            try:
                cache_bytes = snap.stat().st_size
            except OSError:
                pass

        perf_log.info(
            "cache admission %s: structural worthy=%s (%s) | compile=%ss execute=%ss bytes=%s",
            content_hash,
            cache_worthy_v,
            cache_worthy_why,
            compile_seconds,
            execute_seconds,
            cache_bytes,
        )

        manifest = Manifest(
            content_hash=content_hash,
            project=project,
            prompt=prompt,
            row_count=row_count,
            execute_seconds=execute_seconds,
            compile_seconds=compile_seconds,
            cache_worthy=cache_worthy_v,
            cache_worthy_why=cache_worthy_why,
            cache_bytes=cache_bytes,
            result_digest=result_digest_v,
            sources=sources or None,
            parents=parents or None,
        )
        write_manifest(target, manifest)
    except Exception:
        # No partial entry dir survives a failed build: every population step is
        # covered, not just the load_expr/execute paths that had ad-hoc cleanup
        # before (D1/L4). created_target is set once we pass the complete-entry
        # (manifest-bearing) early-return, so the dir we remove is either one this
        # call made or a manifest-less mid-build leftover — never a complete,
        # durable entry, which always early-returns before created_target is set.
        if created_target:
            shutil.rmtree(target, ignore_errors=True)
        raise
    _append_prompt(project, content_hash, prompt)

    # Best-effort: drop the temp script.
    try:
        tmp_script.unlink()
    except OSError:
        pass

    # The complete entry dir (recipe + manifest + schema) is now persisted on
    # disk. catalog_registered reports that local fact — "entry dir persisted" —
    # not durability: committing the content-addressed recipe zip is the next
    # checkpoint's job, which always succeeds because the dir is complete (D2).
    # No subprocess, no out-of-lock writer, no silent #48 no-op.
    catalog_registered = True

    return BuildResult(
        content_hash=content_hash,
        entry_path=target,
        row_count=row_count,
        execute_seconds=execute_seconds,
        schema=schema_doc,
        catalog_registered=catalog_registered,
        lint_warnings=lint_warnings,
        compile_seconds=compile_seconds,
        cache_worthy=cache_worthy_v,
        cache_worthy_why=cache_worthy_why,
        cache_bytes=cache_bytes,
    )


# Bumped so an install that already ran the #73 marker re-runs the sweep once
# to clear any on-demand parquet / #71 viewer-read build dirs written since.
_MIGRATION_MARKER = ".migrated_no_ondemand_result_parquet"

# Per-entry paths the retired on-demand result.parquet layer (and the #71
# viewer-read build that fed off it) left behind. All dead now — every read
# resolves through cached_result_expr — so sweep them on upgrade.
_LEGACY_RESULT_NAMES = ("result.parquet", ".xorq_result_build", ".xorq_result_build_expanded")


def migrate_drop_result_parquet(project: str) -> int:
    """Upgrade migration: delete every per-entry on-demand ``result.parquet`` and
    the #71 viewer-read build dirs that fed off it.

    Safe — ``xorq_build/`` is the durable recipe and an expensive entry's rows
    live in its baked ``.cache()`` snapshot. After this every read resolves
    through ``cached_result_expr`` (cheap recompute / baked snapshot); nothing
    re-creates these files. Sweeps the pre-#73 build-time parquet and any
    on-demand parquet / result-read build written since. Runs once per project
    (guarded by a marker in the catalog dir so it doesn't churn regenerated
    caches on restart), is idempotent and best-effort, and returns the count
    of paths removed.
    """
    import shutil

    from tallyman_core.paths import catalog_dir

    marker = catalog_dir(project) / _MIGRATION_MARKER
    if marker.exists():
        return 0
    base = entries_dir(project)
    deleted = 0
    if base.is_dir():
        for child in base.iterdir():
            for name in _LEGACY_RESULT_NAMES:
                p = child / name
                try:
                    if p.is_file():
                        p.unlink()
                        deleted += 1
                    elif p.is_dir():
                        shutil.rmtree(p)
                        deleted += 1
                except OSError:
                    pass
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("")
    except OSError:
        pass  # best-effort; a missed marker just reruns a no-op next time
    return deleted


def list_entries(project: str) -> list[dict]:
    """Cheap listing: read every entry's manifest.json."""
    base = entries_dir(project)
    if not base.exists():
        return []
    out = []
    for child in sorted(base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        meta_path = child / ENTRY_MANIFEST_FILENAME
        if meta_path.exists():
            out.append(json.loads(meta_path.read_text()))
    return out
