from __future__ import annotations

import logging
import os
import sys

import httpx
from fastmcp import FastMCP

from pydata_core import (
    AliasExists,
    AliasNotFound,
    alias_for_hash,
    entry_dir,
    get_alias,
    record_error,
    rename_alias,
    resolve_project,
    set_alias,
)
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
    out = _run_and_record(project, code, prompt)
    if "error" in out:
        return out
    out.pop("_build", None)
    _notify("new_entry", content_hash=out["hash"])
    return out


@mcp.tool()
def catalog_load_parquet(rel_path: str, prompt: str = "") -> dict:
    """Register a parquet file from the project's data/ directory as a catalog entry.

    Use this for simple "load this file" steps. The agent does not need to write
    any xorq code — pass a path relative to the project's `data/` directory.

    Args:
        rel_path: Path relative to `<project>/data/`. Example: "orders.parquet".
        prompt: Optional human-readable description (the user's intent).

    Returns:
        Same shape as catalog_run.
    """
    project = resolve_project()
    code = (
        "from pydata_xorq.io import from_project\n"
        f"expr = from_project({rel_path!r}, project={project!r})\n"
    )
    out = _run_and_record(project, code, prompt)
    if "error" in out:
        return out
    out.pop("_build", None)
    _notify("new_entry", content_hash=out["hash"])
    return out


def _run_and_record(project: str, code: str, prompt: str) -> dict:
    """Shared body for tools that compile-and-persist; returns the tool reply dict."""
    try:
        result = build_and_persist(project=project, code=code, prompt=prompt or None)
    except BuildError as exc:
        rec = record_error(project, code=code, message=str(exc), prompt=prompt or None)
        _notify("build_failed", error_id=rec["id"])
        return {"error": str(exc), "error_id": rec["id"]}
    return {
        "_build": result,
        "hash": result.content_hash,
        "row_count": result.row_count,
        "execute_seconds": result.execute_seconds,
        "schema": result.schema,
        "entry_path": str(result.entry_path),
    }


@mcp.tool()
def catalog_create(name: str, code: str, prompt: str = "") -> dict:
    """Execute and persist a NAMED catalog entry (alias).

    Use this when the user gives a concept a name ("name this `shoe_sales`").
    Errors if the alias already exists — use `catalog_revise` to update an
    existing alias instead.

    Args:
        name: The alias for this entry. Must not already exist in the project.
        code: A self-contained Python script that binds `expr`. Same conventions
            as `catalog_run`. Use `from pydata_xorq.io import from_project` for
            project-local data files.
        prompt: Optional human-readable description.

    Returns:
        Same shape as catalog_run, plus `alias` and `version`.
    """
    project = resolve_project()
    if get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists. Use catalog_revise to update it."}
    out = _run_and_record(project, code, prompt)
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=False)
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
    return out


@mcp.tool()
def catalog_revise(name: str, code: str, prompt: str = "") -> dict:
    """Persist a NEW VERSION of an existing alias.

    The alias is repointed to the new content hash. The previous hash remains
    in the catalog as a forensic artifact and is recorded in alias_history.

    Args:
        name: An existing alias.
        code: A self-contained Python script that binds `expr`.
        prompt: Optional human-readable description of what changed.
    """
    project = resolve_project()
    if get_alias(project, name) is None:
        return {"error": f"alias {name!r} does not exist. Use catalog_create to create it."}
    out = _run_and_record(project, code, prompt)
    if "error" in out:
        return out
    info = set_alias(project, name, out["hash"], expect_exists=True)
    out.pop("_build", None)
    out["alias"] = info["name"]
    out["version"] = info["version"]
    _notify("new_entry", content_hash=out["hash"], alias=name, version=info["version"])
    return out


@mcp.tool()
def catalog_alias(hash: str, name: str) -> dict:
    """Promote an existing scratch (unnamed) entry to a named entry.

    Use this when an entry was created via `catalog_run` and you decide
    post-hoc that it deserves a name. Errors if the alias already exists.
    """
    project = resolve_project()
    if not entry_dir(project, hash).exists():
        return {"error": f"no catalog entry for hash {hash!r}"}
    if get_alias(project, name) is not None:
        return {"error": f"alias {name!r} already exists"}
    info = set_alias(project, name, hash, expect_exists=False)
    _notify("alias_changed", content_hash=hash, alias=name, version=info["version"])
    return {"hash": hash, "alias": name, "version": info["version"]}


@mcp.tool()
def catalog_rename(old_name: str, new_name: str) -> dict:
    """Rename an existing alias. Preserves version history."""
    project = resolve_project()
    try:
        info = rename_alias(project, old_name, new_name)
    except AliasNotFound:
        return {"error": f"alias {old_name!r} does not exist"}
    except AliasExists:
        return {"error": f"alias {new_name!r} already exists"}
    _notify("alias_renamed", old=old_name, new=new_name, content_hash=info["hash"])
    return info


@mcp.tool()
def catalog_list() -> list[dict]:
    """List every entry in the current project's catalog (most recent first).

    Each entry is annotated with `alias` and `version` if it currently
    corresponds to a named version. Scratch entries (no alias) are also
    included.
    """
    project = resolve_project()
    entries = list_entries(project)
    for e in entries:
        a = alias_for_hash(project, e["content_hash"])
        if a is not None:
            from pydata_core import version_of_hash

            info = version_of_hash(project, e["content_hash"])
            e["alias"] = a
            e["version"] = info[1] if info else 1
        else:
            e["alias"] = None
            e["version"] = None
    return entries


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("PYDATA_LOG_LEVEL", "INFO"),
        format="[%(name)s] %(message)s",
    )
    mcp.run()


if __name__ == "__main__":
    main()
