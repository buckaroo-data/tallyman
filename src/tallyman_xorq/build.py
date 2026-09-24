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
    # Whether two runs of the query at create time gave the same digest (ADR-009 D6): None for a cheap entry, which is
    # not run twice, False when the recipe is not reproducible, with the columns whose digests differed.
    reproducible: bool | None = None
    nonreproducible_columns: list[str] = field(default_factory=list)


class BuildError(RuntimeError):
    pass


class NotAnEntryError(BuildError):
    """A read of a hash whose directory is not an entry: it has no manifest, or there is no directory (#204).

    The manifest is an entry's last write, so a directory without one is what a build or an import that did not
    finish leaves (ADR-007 D6). ``result_cache.entry_manifest`` raises it, and the message names the rebuild.
    """


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


# Read helpers the model reaches for on the wrong namespace. A recipe reads an alias
# (tracked_expr_from_alias / pinned_expr_from_alias), never a file.
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
        go through tracked_expr_from_alias, `ibis.case` is now `ibis.cases`;
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
                "(`import xorq.api as xo`), and a recipe does not open a file at all. Import the "
                "file once with `catalog_import_source('<abs path>', '<alias>')` and read it with "
                "`from tallyman_xorq.io import tracked_expr_from_alias`."
            )
        else:
            hints.append(
                f"`xorq.{name}` does not exist — `xorq` is not the API entrypoint. "
                f"`import xorq.api as xo` and call `xo.{name}` "
                "(e.g. xo.memtable, xo.connect)."
            )

    m = re.search(r"module 'ibis' has no attribute '(\w+)'", exc_msg)
    if m:
        name = m.group(1)
        if name in _IBIS_MATH:
            hints.append(f"math/elementwise functions are column methods: `col.{name}()`, not `ibis.{name}(col)`.")
        elif name in {"read_parquet", "read_csv", "read_table"}:
            hints.append(
                f"`ibis.{name}` does not exist — a recipe reads an alias, not a file. Import the file "
                "once with `catalog_import_source('<abs path>', '<alias>')`, then "
                "`from tallyman_xorq.io import tracked_expr_from_alias`."
            )
        elif name == "case":
            hints.append("`ibis.case` is gone — use `ibis.cases((cond, val), ..., else_=default)`.")
        else:
            hints.append(
                f"`ibis.{name}` does not exist on `xorq.vendor.ibis`. Many operations are column methods "
                "(`col.method()`), and data is read via tracked_expr_from_alias."
            )

    m = re.search(r"cannot import name '(\w+)' from 'tallyman_xorq\.io'", exc_msg)
    if m:
        hints.append(
            f"`tallyman_xorq.io` has no `{m.group(1)}` — it exports `tracked_expr_from_alias` "
            "(an alias, followed, recording the parent edge) and `pinned_expr_from_alias` "
            "('<alias>-v<N>', pinned). A file is not a recipe input: import it with "
            "`catalog_import_source('<abs path>', '<alias>')` first."
        )

    if "duckdb" in exc_msg.lower():
        hints.append(
            "there is no duckdb backend here — the only backend is xorq's built-in datafusion. "
            "Build with the ibis expression API over tracked_expr_from_alias sources; "
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

    A CSV enters the catalog by an import, which digests it, clones the bytes and writes one ordered snapshot of
    them (ADR-011 D1), so the entry's rows are fixed under its hash and paging is repeatable. A raw
    deferred_read_csv bypasses all of it and gives a nondeterministic row order above datafusion's repartition
    threshold.
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
            "xo.deferred_read_csv is not allowed in tallyman recipes — a recipe never opens a file. Import the "
            "CSV once, catalog_import_source('<abs path>', '<alias>', schema=...), and read it with "
            "tracked_expr_from_alias('<alias>'). The import writes one ordered snapshot with a stable "
            "__row_order, so the entry's rows are fixed under its hash; deferred_read_csv gives a "
            "nondeterministic row order above datafusion's repartition threshold."
        )


def _raw_parquet_read_check(expr, project: str) -> None:
    """Raise BuildError if the recipe reads a parquet file that tallyman did not write (ADR-008 D12).

    Such a read has no digest and no clone, so an entry built on it has no ``__row_order`` to page by and no record
    of which bytes it was built from. Tallyman's own files are the snapshots under the project's ``compute_cache/``;
    everything else enters by an import (ADR-011 D2).
    """
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import Read

    from tallyman_core.paths import compute_cache_dir

    root = compute_cache_dir(project).resolve()
    for node in walk_nodes(Read, expr):
        if node.method_name != "read_parquet":
            continue
        path = dict(node.read_kwargs).get("hash_path")
        if path is None or Path(str(path)).resolve().is_relative_to(root):
            continue
        raise BuildError(
            f"the recipe reads {path} with xo.deferred_read_parquet, which is not allowed: the file gets no "
            "content digest and no clone, so the entry would have no __row_order to page by and no record of "
            f"which bytes it was built from. Import the file first — catalog_import_source({str(path)!r}, "
            "'<alias>') — and read it, like any other parent, with tracked_expr_from_alias('<alias>')."
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
    """Compile user code with xorq, materialize a worthy entry's snapshot, write a catalog entry.

    The user code must bind a variable named `expr_name` (default "expr") to an
    ibis/xorq expression. Imports happen in a fresh module scope.

    The whole build holds the project's write lock (ADR-007 D11): one write at a time per project, so two builds of
    one entry cannot end with the failing one deleting the winner's directory, and a chained build waits for the
    materialization of its parent.
    """
    from tallyman_core.catalog_state import project_lock

    ensure_project(project)
    with project_lock(project):
        return _build_and_persist(project, code, expr_name, prompt)


def _reading(expr, project: str) -> str:
    """What a cheap entry reads, in words, for the message that says it must keep ``__row_order`` (ADR-008 D3)."""
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import Read

    from tallyman_xorq.ordered_copy import describe_read

    paths = [dict(r.read_kwargs).get("hash_path") for r in walk_nodes(Read, expr)]
    paths = [Path(str(p)) for p in paths if p]
    return describe_read(project, paths[0]) if len(paths) == 1 else "a file"


def _build_and_persist(project: str, code: str, expr_name: str, prompt: str | None) -> BuildResult:
    from xorq.ibis_yaml.compiler import build_expr, load_expr

    from tallyman_xorq._git_state_guard import install_git_state_guard
    from tallyman_xorq.materialize import (
        SNAPSHOT_FORMAT_VERSION,
        Materialized,
        engine_versions,
        materialize,
        publish_snapshot,
    )
    from tallyman_xorq.result_cache import stream_row_count
    from tallyman_xorq.row_order import RowOrderError
    from tallyman_xorq.worthiness import classify_expr

    # git-provenance capture in xorq's compiler can crash (git SIGSEGV when forked
    # from the long-lived server) and abort the whole build. Make it best-effort.
    install_git_state_guard()

    # Collect the resolved tracked_expr_from_alias parent edges while user code imports (#84). They are the whole
    # record of what the entry reads: a recipe names aliases and never a file (ADR-011 D2), so there are no source
    # digests to fold up and no ordered copies to record.
    from tallyman_xorq import parent_capture as pc

    parent_token = pc.begin_collect()
    try:
        module, tmp_script = _import_script(code)
    finally:
        parents = pc.end_collect(parent_token)
    expr_obj = getattr(module, expr_name, None)
    if expr_obj is None:
        names = ", ".join(n for n in dir(module) if not n.startswith("_"))
        raise BuildError(f"variable {expr_name!r} not found in code. Available names: {names}")

    # Advisory nondeterminism lint (#88) on the author's expression, surfaced on the result, never fatal.
    lint_warnings = _nondeterminism_warnings(expr_obj)

    # Fatal: raw reads are banned. A file enters the catalog by an import and a recipe reads the source alias
    # (ADR-011 D2); read_project_file and tallyman_read_csv refuse in io.py, and these two catch the xorq readers
    # that would otherwise go straight to a file with no digest, no clone and no __row_order.
    _csv_direct_read_check(expr_obj)
    _raw_parquet_read_check(expr_obj, project)

    # Whether the entry is materialized is decided ONCE, here, on the expression the author wrote (ADR-008 D4), and
    # recorded in the manifest. The rewrite below then adds the canonical sort to a worthy entry and checks that a
    # cheap one keeps __row_order; no cache node is created (ADR-007 D1).
    from tallyman_xorq.source_cache import CacheNodeError, InMemoryReadError, rewrite_for_build

    verdict = classify_expr(expr_obj)
    # The author's DAG before the canonical sort: compile_seconds (#87) times its expr->backend-plan step, the
    # un-truncated "large expression DAG" recompile that #30's profiling found dominates per-view cost.
    author_expr = expr_obj
    try:
        expr_obj = rewrite_for_build(
            expr_obj,
            project,
            verdict=verdict,
            reading=_reading(expr_obj, project) if not verdict.worthy else None,
        )
    except (InMemoryReadError, CacheNodeError, RowOrderError) as exc:
        raise BuildError(str(exc)) from exc
    except Exception as exc:
        from tallyman_xorq.row_order import translate_collision

        translated = translate_collision(exc)
        if translated is not None:
            raise BuildError(str(translated)) from exc
        raise

    created_target = False
    staged: Materialized | None = None  # a worthy entry's snapshot, complete at a temp name until it is published
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
                        reproducible=meta.get("reproducible"),
                        nonreproducible_columns=meta.get("nonreproducible_columns") or [],
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

            # Execute (ADR-007 D4). A worthy entry is materialized: the ONE writer runs the frozen build on a
            # single-partition connection and writes the snapshot, twice, so a recipe that is not reproducible is known
            # from birth (ADR-009 D6). The file stays at a temp name until the manifest is written (below). A cheap
            # entry writes nothing: one full streaming pass forces row-level evaluation at author time (a failing
            # cast / arithmetic / UDF surfaces here, in tallyman's process, and not later in a grid query), and the one
            # pass yields the exact row count.
            cache_worthy_v, cache_worthy_why = verdict.worthy, verdict.why
            reproducible: bool | None = None
            differing: list[str] = []
            t0 = time.monotonic()
            try:
                if cache_worthy_v:
                    staged = materialize(project, content_hash, check_reproducible=True, publish=False)
                    row_count = staged.row_count
                    result_digest_v: str | None = staged.digest
                    arrow_schema = staged.schema
                    reproducible, differing = staged.reproducible, staged.differing_columns
                else:
                    loaded = load_expr(build_path)
                    arrow_schema = loaded.schema().to_pyarrow()
                    row_count = stream_row_count(loaded)
                    result_digest_v = None
            except Exception as exc:
                hint = _ibis_import_hint(str(exc), code)
                from tallyman_xorq.row_order import translate_collision

                translated = translate_collision(exc)
                if translated is not None:
                    raise BuildError(str(translated)) from exc
                raise BuildError(f"build execution failed: {exc}{hint}\n{traceback.format_exc()}") from exc
            execute_seconds = round(time.monotonic() - t0, 3)

        # Schema + row count: a worthy entry's schema is read from the file it wrote (parquet changes some types, and
        # the writer adds __row_order), a cheap entry's from the expression, which by the D3 check already ends in it.
        schema_doc = {
            "fields": [{"name": f.name, "type": str(f.type)} for f in arrow_schema],
            "row_count": row_count,
        }
        atomic_write_text(entry_schema_path(project, content_hash), json.dumps(schema_doc, indent=2))

        # Cache-admission instrumentation (#87): record the structural verdict alongside the measured inputs.
        # compile_seconds times the author DAG's expr->backend-plan step (the dominant per-view cost per #30),
        # separate from execute; cache_bytes is the snapshot size (None for a cheap entry that writes nothing).
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
        if staged is not None:
            try:
                cache_bytes = staged.path.stat().st_size
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
            reproducible=reproducible,
            nonreproducible_columns=differing or None,
            snapshot_format=SNAPSHOT_FORMAT_VERSION,
            engine_versions=engine_versions(),
            parents=parents or None,
        )
        write_manifest(target, manifest)
        if staged is not None:
            # The last step: the snapshot's path changes only once the entry is complete. A build that failed before
            # here left the file already at the path as it was, such as the one a reset left on disk (ADR-007 D14),
            # which for an entry that is not reproducible is the only copy of its rows (#193).
            publish_snapshot(project, content_hash, staged)
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
    finally:
        # The staged temp file is gone once it is published. If the build failed before then, it is removed here.
        if staged is not None:
            staged.path.unlink(missing_ok=True)
    _append_prompt(project, content_hash, prompt)

    if reproducible is False:
        lint_warnings = [
            *lint_warnings,
            "this entry's query is not reproducible: two runs at create time gave different results in column(s) "
            + ", ".join(differing)
            + ". The entry was built, and its snapshot file is pinned (never deleted by the Cache page) because it "
            "cannot be re-created faithfully. Seed a sample, or replace now()/random()/an impure UDF with a value "
            "fixed at author time, for a reproducible entry.",
        ]

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
        reproducible=reproducible,
        nonreproducible_columns=differing,
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
