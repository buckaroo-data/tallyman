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
    write_manifest,
)


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
        raise BuildError(f"executing user code raised: {exc}\n{traceback.format_exc()}") from exc
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
    from xorq.common.utils.caching_utils import get_xorq_cache_dir
    from xorq.ibis_yaml.compiler import build_expr, load_expr

    ensure_project(project)

    module, tmp_script = _import_script(code)
    expr_obj = getattr(module, expr_name, None)
    if expr_obj is None:
        names = ", ".join(n for n in dir(module) if not n.startswith("_"))
        raise BuildError(
            f"variable {expr_name!r} not found in code. Available names: {names}"
        )

    # Use a temp builds_dir so xorq's hash naming doesn't collide; we move
    # things into our catalog layout afterwards.
    with tempfile.TemporaryDirectory(prefix="pydata_build_") as builds_str:
        builds_dir = Path(builds_str)
        try:
            build_path = Path(build_expr(expr_obj, builds_dir=builds_dir))
        except Exception as exc:
            raise BuildError(f"build_expr failed: {exc}\n{traceback.format_exc()}") from exc

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

        # Persist the user's source.
        (target / "expr.py").write_text(code)

        # Load + execute.
        try:
            cache_dir = get_xorq_cache_dir()
            loaded = load_expr(build_path, cache_dir=cache_dir)
        except Exception as exc:
            raise BuildError(f"load_expr failed: {exc}\n{traceback.format_exc()}") from exc

        result_path = target / "result.parquet"
        t0 = time.monotonic()
        try:
            loaded.to_parquet(str(result_path))
        except Exception as exc:
            raise BuildError(f"to_parquet failed: {exc}\n{traceback.format_exc()}") from exc
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
