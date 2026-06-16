from __future__ import annotations

import importlib.util
import json
import logging
import re
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


def _append_prompt(entry_path: Path, prompt: str | None) -> None:
    """Append a prompt event to the entry's prompts.jsonl."""
    if not prompt:
        return
    record = {
        "prompt": prompt,
        "at": datetime.now(timezone.utc).isoformat(),
    }
    with (entry_path / "prompts.jsonl").open("a") as fh:
        fh.write(json.dumps(record) + "\n")


def read_prompts(entry_path: Path) -> list[dict]:
    """Return every prompt seen for this entry, oldest first."""
    p = entry_path / "prompts.jsonl"
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
    # Whether the entry's recipe landed in the xorq catalog git repo. None on the
    # re-run path (entry already on disk, registration not re-attempted); True/False
    # on a fresh build. False means the recipe is NOT durable — see #48.
    catalog_registered: bool | None = None
    # Advisory build-time lints (#88): execution-nondeterministic ops whose result
    # varies run-to-run under one content_hash. Empty when the recipe is clean.
    lint_warnings: list[str] = field(default_factory=list)


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
# way is from_project/from_catalog; xo.deferred_read_parquet handles a raw path.
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
        go through from_project/from_catalog, `ibis.case` is now `ibis.cases`;
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
                "import from_project, from_catalog`, or `xo.deferred_read_parquet"
                "(abs_path)` for a raw path."
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
                "`from tallyman_xorq.io import from_project, from_catalog`."
            )
        elif name == "case":
            hints.append("`ibis.case` is gone — use `ibis.cases((cond, val), ..., else_=default)`.")
        else:
            hints.append(
                f"`ibis.{name}` does not exist on `xorq.vendor.ibis`. Many operations are column methods "
                "(`col.method()`), and data is read via from_project/from_catalog."
            )

    m = re.search(r"cannot import name '(\w+)' from 'tallyman_xorq\.io'", exc_msg)
    if m:
        hints.append(
            f"`tallyman_xorq.io` has no `{m.group(1)}` — it exports only `from_project` "
            "(raw files under <project>/data/) and `from_catalog` (named entries / content hashes)."
        )

    if "duckdb" in exc_msg.lower():
        hints.append(
            "there is no duckdb backend here — the only backend is xorq's built-in datafusion. "
            "Build with the ibis expression API over from_project/from_catalog sources; "
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
        found = {type(node).__name__ for node in expr.op().find(tuple(types))}
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

    # Collect source digests while user code imports (from_project notes each
    # file it reads); cas/salt identity modes consume them below.
    from tallyman_xorq import source_identity as si

    collect_token = si.begin_collect()
    try:
        module, tmp_script = _import_script(code)
    finally:
        sources = si.end_collect(collect_token)
    expr_obj = getattr(module, expr_name, None)
    if expr_obj is None:
        names = ", ".join(n for n in dir(module) if not n.startswith("_"))
        raise BuildError(f"variable {expr_name!r} not found in code. Available names: {names}")

    # Advisory nondeterminism lint (#88) on the author's expression, before the
    # rewrite wraps it in cache nodes. Surfaced on the result, never fatal.
    lint_warnings = _nondeterminism_warnings(expr_obj)

    # Rewrite-then-build (#73): cache each non-parquet source read, bake a
    # top-level result cache when the expression is expensive (so every loader,
    # incl. the Buckaroo viewer, reads the cached result instead of re-running
    # the DAG), and reject in-memory reads. The content_hash below is computed
    # from this rewritten expression; xorq_build/ carries the cache nodes, while
    # expr.py keeps the author's literal source (tallyman does not depend on the
    # submitted form).
    from tallyman_xorq.source_cache import InMemoryReadError, rewrite_for_build

    try:
        expr_obj = rewrite_for_build(expr_obj, project)
    except InMemoryReadError as exc:
        raise BuildError(str(exc)) from exc

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
                _append_prompt(target, prompt)
                return BuildResult(
                    content_hash=content_hash,
                    entry_path=target,
                    row_count=meta.get("row_count", 0),
                    execute_seconds=meta.get("execute_seconds", 0.0),
                    schema=json.loads(entry_schema_path(project, content_hash).read_text()),
                    lint_warnings=lint_warnings,
                )

        target.mkdir(parents=True, exist_ok=True)

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

        # Execute (#73). A worthy entry's build carries a baked result-cache
        # node (rewrite_for_build), so executing materialises it once into the
        # compute cache — and every later loader (the Buckaroo viewer, diffs,
        # from_catalog) reads that cached result instead of re-running the DAG.
        # A cheap entry materialises nothing: this is a pushdown count() over a
        # columnar source plus a schema read, no parquet written.
        t0 = time.monotonic()
        try:
            arrow_schema = loaded.schema().to_pyarrow()
            row_count = int(loaded.count().execute())
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
    entry_schema_path(project, content_hash).write_text(json.dumps(schema_doc, indent=2))

    manifest = Manifest(
        content_hash=content_hash,
        project=project,
        prompt=prompt,
        row_count=row_count,
        execute_seconds=execute_seconds,
        sources=sources or None,
    )
    write_manifest(target, manifest)
    _append_prompt(target, prompt)

    # Best-effort: drop the temp script.
    try:
        tmp_script.unlink()
    except OSError:
        pass

    # Best-effort by design (a missing/misconfigured catalog repo must not break
    # the build), but the outcome is surfaced on the result instead of swallowed:
    # a failed add means the recipe never reached git and is not durable (#48).
    catalog_registered: bool | None = None
    cause = ""
    try:
        from tallyman_core.xorq_catalog import add_entry as _xcat_add

        catalog_registered = _xcat_add(project, xorq_build_dir, entry_name=content_hash)
    except Exception as exc:
        catalog_registered = False
        cause = f" ({exc})"
    if catalog_registered is False:
        logging.getLogger(__name__).error(
            "xorq catalog add failed for %s%s — entry recipe is not durable in the catalog repo (#48)",
            content_hash,
            cause,
        )

    return BuildResult(
        content_hash=content_hash,
        entry_path=target,
        row_count=row_count,
        execute_seconds=execute_seconds,
        schema=schema_doc,
        catalog_registered=catalog_registered,
        lint_warnings=lint_warnings,
    )


_MIGRATION_MARKER = ".migrated_no_build_result_parquet"


def migrate_drop_result_parquet(project: str) -> int:
    """#73 upgrade migration: delete every per-entry build-time result.parquet.

    Safe — ``xorq_build/`` is the durable recipe. After this, an expensive
    entry's result lives in its baked cache (rebuilt entries) or repopulates
    lazily on first view, and a cheap entry recomputes trivially via
    ``ensure_result`` on demand. Runs once per project (guarded by a marker in
    the catalog dir so it doesn't churn lazily-regenerated caches on restart),
    is idempotent and best-effort, and returns the count deleted.
    """
    from tallyman_core.paths import ENTRY_RESULT_FILENAME, catalog_dir

    marker = catalog_dir(project) / _MIGRATION_MARKER
    if marker.exists():
        return 0
    base = entries_dir(project)
    deleted = 0
    if base.is_dir():
        for child in base.iterdir():
            rp = child / ENTRY_RESULT_FILENAME
            if rp.is_file():
                try:
                    rp.unlink()
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


def load_entry(project: str, content_hash: str):
    """Reload an existing catalog entry's xorq expression on the current host.

    The persisted build is portable (paths use a `${TALLYMAN_PROJECT_ROOT}`
    placeholder). This expands placeholders against the current machine's
    project root before handing the build to xorq.

    Loads against the per-project compute cache, like the build-time load:
    #45 — caches warm lazily at view time, and view time comes through here,
    so the global ~/.cache/xorq would make the recorded warm-set vacuous and
    leak warm hits across projects and steps.
    """
    from tallyman_core.paths import compute_cache_dir
    from tallyman_xorq.portable import load_expr_portable

    build_dir = entry_build_dir(project, content_hash)
    if not build_dir.is_dir():
        raise FileNotFoundError(f"no xorq_build dir for {content_hash} in project {project}")
    cache_dir = compute_cache_dir(project)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return load_expr_portable(build_dir, project_dir(project), cache_dir=cache_dir)
