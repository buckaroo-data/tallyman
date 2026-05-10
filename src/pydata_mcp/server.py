from __future__ import annotations

import logging
import os
import sys

import httpx
from fastmcp import FastMCP

from pydata_core import resolve_project
from pydata_xorq import BuildError, build_and_persist, list_entries

log = logging.getLogger("pydata_mcp")
COMPANION_URL = os.environ.get("PYDATA_COMPANION_URL", "http://127.0.0.1:7860")

mcp = FastMCP("pydata")


def _notify(kind: str, content_hash: str | None = None, **extra) -> None:
    """Best-effort POST to the companion's /internal/notify. Never raise."""
    try:
        with httpx.Client(timeout=2.0) as client:
            client.post(
                f"{COMPANION_URL}/internal/notify",
                json={"kind": kind, "hash": content_hash, "extra": extra or None},
            )
    except Exception as exc:
        # Companion may not be up; that's allowed. Log and move on.
        print(f"[pydata_mcp] notify failed ({kind}): {exc}", file=sys.stderr)


@mcp.tool()
def catalog_run(code: str, prompt: str = "") -> dict:
    """Execute a xorq expression script and persist it as a catalog entry.

    The provided code MUST bind a top-level variable named `expr` to a xorq/ibis
    expression. Imports allowed; all imports happen in a fresh module scope.

    Use `xorq.api as xo` and the deferred readers:
        import xorq.api as xo
        t = xo.deferred_read_parquet("/path/to/file.parquet")
        expr = t.group_by("region").aggregate(total=t.price.sum())

    Use `xorq.vendor.ibis as ibis` if you need ibis directly. Do NOT `import ibis`.

    Args:
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description (the user's intent).

    Returns:
        dict with keys: hash, row_count, execute_seconds, schema, entry_path.
    """
    project = resolve_project()
    try:
        result = build_and_persist(project=project, code=code, prompt=prompt or None)
    except BuildError as exc:
        return {"error": str(exc)}
    _notify("new_entry", content_hash=result.content_hash)
    return {
        "hash": result.content_hash,
        "row_count": result.row_count,
        "execute_seconds": result.execute_seconds,
        "schema": result.schema,
        "entry_path": str(result.entry_path),
    }


@mcp.tool()
def catalog_list() -> list[dict]:
    """List every entry in the current project's catalog (most recent first)."""
    return list_entries(resolve_project())


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PYDATA_LOG_LEVEL", "INFO"),
        format="[%(name)s] %(message)s",
    )
    mcp.run()


if __name__ == "__main__":
    main()
