from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pydata_core import (
    Manifest,
    ensure_project,
    entries_dir,
    entry_dir,
    project_dir,
    write_manifest,
)
from pydata_xorq.portable import make_portable_inplace


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


def _ibis_import_hint(exc_msg: str, code: str = "") -> str:
    """Actionable hint for the three most common ibis/xorq import mistakes.

    Signals:
      1. User code contains a bare `import ibis` / `from ibis ...`.
      2. The exception message names xorq's vendored Expr class (expression
         built against the wrong ibis instance).
      3. `xorq._` used instead of `ibis._` (xorq has no `_` deferred proxy;
         it lives on `xorq.vendor.ibis`).
    """
    has_bad_import = _user_imports_bare_ibis(code)
    has_expr_mismatch = "xorq.vendor.ibis.expr.types.core.Expr" in exc_msg and "must be" in exc_msg
    has_xorq_underscore = "has no attribute '_'" in exc_msg and "xorq" in exc_msg
    if not (has_bad_import or has_expr_mismatch or has_xorq_underscore):
        return ""
    if has_xorq_underscore and not has_bad_import:
        return (
            "\n\nHint: `xorq` has no `_` deferred accessor. Use `ibis._` "
            "where `ibis` comes from `import xorq.vendor.ibis as ibis`."
        )
    return (
        "\n\nHint: this usually means the expression was built using "
        "`import ibis` instead of `import xorq.vendor.ibis as ibis`. "
        "Replace any `import ibis` (or `from ibis import ...`) with "
        "`import xorq.vendor.ibis as ibis` and rebuild."
    )


def _import_script(code: str) -> tuple[object, Path]:
    """Write code to a temp file, import it, return (module, temp_path).

    The temp file is kept alive for the caller to copy into the entry dir.
    """
    tmp = Path(tempfile.gettempdir()) / f"pydata_expr_{uuid.uuid4().hex}.py"
    tmp.write_text(code)
    spec = importlib.util.spec_from_file_location(f"pydata_expr_{tmp.stem}", tmp)
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

    from pydata_core.paths import compute_cache_dir
    from pydata_xorq._git_state_guard import install_git_state_guard

    # git-provenance capture in xorq's compiler can crash (git SIGSEGV when forked
    # from the long-lived server) and abort the whole build. Make it best-effort.
    install_git_state_guard()

    ensure_project(project)

    module, tmp_script = _import_script(code)
    expr_obj = getattr(module, expr_name, None)
    if expr_obj is None:
        names = ", ".join(n for n in dir(module) if not n.startswith("_"))
        raise BuildError(f"variable {expr_name!r} not found in code. Available names: {names}")

    # Use a temp builds_dir so xorq's hash naming doesn't collide; we move
    # things into our catalog layout afterwards.
    with tempfile.TemporaryDirectory(prefix="pydata_build_") as builds_str:
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
            manifest_path = target / "manifest.json"
            if manifest_path.exists():
                meta = json.loads(manifest_path.read_text())
                _append_prompt(target, prompt)
                return BuildResult(
                    content_hash=content_hash,
                    entry_path=target,
                    row_count=meta.get("row_count", 0),
                    execute_seconds=meta.get("execute_seconds", 0.0),
                    schema=json.loads((target / "schema.json").read_text()),
                )

        target.mkdir(parents=True, exist_ok=True)

        # Move xorq's build directory contents (expr.yaml + deps) under the entry.
        xorq_build_dir = target / "xorq_build"
        xorq_build_dir.mkdir(exist_ok=True)
        for item in build_path.iterdir():
            (xorq_build_dir / item.name).write_bytes(item.read_bytes())

        # Rewrite project-root substrings to a portable placeholder so the
        # build is loadable on any machine with the same project laid out.
        make_portable_inplace(xorq_build_dir, project_dir(project))

        # Persist the user's source, also rewriting any project-root paths.
        code_persisted = code.replace(str(project_dir(project)), "${PYDATA_PROJECT_ROOT}")
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

        result_path = target / "result.parquet"
        t0 = time.monotonic()
        try:
            loaded.to_parquet(str(result_path))
        except Exception as exc:
            hint = _ibis_import_hint(str(exc), code)
            raise BuildError(f"to_parquet failed: {exc}{hint}\n{traceback.format_exc()}") from exc
        execute_seconds = round(time.monotonic() - t0, 3)

    # Schema + row count via pyarrow (no pandas materialize needed).
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(result_path)
    arrow_schema = pf.schema_arrow
    row_count = pf.metadata.num_rows
    schema_doc = {
        "fields": [{"name": f.name, "type": str(f.type)} for f in arrow_schema],
        "row_count": row_count,
    }
    (target / "schema.json").write_text(json.dumps(schema_doc, indent=2))

    manifest = Manifest(
        content_hash=content_hash,
        project=project,
        prompt=prompt,
        row_count=row_count,
        execute_seconds=execute_seconds,
    )
    write_manifest(target, manifest)
    _append_prompt(target, prompt)

    # Best-effort: drop the temp script.
    try:
        tmp_script.unlink()
    except OSError:
        pass

    # Best-effort: register with the xorq catalog git repo.
    try:
        from pydata_core.xorq_catalog import add_entry as _xcat_add

        _xcat_add(project, xorq_build_dir, entry_name=content_hash)
    except Exception as exc:
        import logging

        logging.getLogger(__name__).warning("xorq catalog add skipped: %s", exc)

    return BuildResult(
        content_hash=content_hash,
        entry_path=target,
        row_count=row_count,
        execute_seconds=execute_seconds,
        schema=schema_doc,
    )


def list_entries(project: str) -> list[dict]:
    """Cheap listing: read every entry's manifest.json."""
    base = entries_dir(project)
    if not base.exists():
        return []
    out = []
    for child in sorted(base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        meta_path = child / "manifest.json"
        if meta_path.exists():
            out.append(json.loads(meta_path.read_text()))
    return out


def load_entry(project: str, content_hash: str):
    """Reload an existing catalog entry's xorq expression on the current host.

    The persisted build is portable (paths use a `${PYDATA_PROJECT_ROOT}`
    placeholder). This expands placeholders against the current machine's
    project root before handing the build to xorq.
    """
    from xorq.common.utils.caching_utils import get_xorq_cache_dir

    from pydata_xorq.portable import load_expr_portable

    target = entry_dir(project, content_hash)
    build_dir = target / "xorq_build"
    if not build_dir.is_dir():
        raise FileNotFoundError(f"no xorq_build dir for {content_hash} in project {project}")
    return load_expr_portable(build_dir, project_dir(project), cache_dir=get_xorq_cache_dir())
