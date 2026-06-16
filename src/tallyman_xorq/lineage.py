"""Two flavors of lineage:

- **Internal lineage**: the expression DAG inside one catalog entry. Comes
  pre-baked from xorq's `expr_metadata.json` — nodes are operations (Read,
  Aggregate, Field, ...), edges connect them.

- **Catalog lineage**: which *catalog entries* depend on which other catalog
  entries. Read from `manifest.parents` — the resolved `from_catalog` edges
  ({hash, ref, follow}) recorded at build time (#84).

V0 derived catalog edges by matching a parent `result.parquet` path in the
child's `expr.yaml`. As of #73, `from_catalog()` composes the parent's
*expression* into the child (the parent's own source reads, wrapped in a cache
node) instead of reading a `result.parquet` path, so no path survives to match
and the catalog DAG went dark. #84 restores it: `from_catalog` resolves the
parent's content hash at build time and records it in `manifest.parents`, which
`catalog_parents` now reads. The path-matching below is retained as a fallback
for pre-#74 builds that still reference a parent `result.parquet` directly.
"""

from __future__ import annotations

import json
import re

from tallyman_core import ENTRY_MANIFEST_FILENAME, entries_dir, entry_build_dir, entry_dir
from tallyman_xorq.portable import PLACEHOLDER

# Cache-machinery node types injected by the rewrite-then-build step (#73):
# source-read caches and the baked result cache wrap logical ops in a
# CachedNode (+ a RemoteTable for the cross-backend hop). They are an
# implementation detail of materialisation, not part of the user's logical
# pipeline, so the internal-lineage view contracts them out.
_LINEAGE_HIDDEN_TYPES = {"CachedNode", "RemoteTable"}


def _strip_cache_nodes(lineage: dict) -> dict:
    """Contract cache/remote nodes out of an internal-lineage graph.

    Drops every ``CachedNode`` / ``RemoteTable`` node and re-links across it
    (a parent of a dropped node connects to the dropped node's surviving
    descendants), so the displayed DAG shows logical ops only. Edges are
    parent→child.
    """
    nodes = lineage.get("nodes", [])
    drop = {n["id"] for n in nodes if n.get("type") in _LINEAGE_HIDDEN_TYPES}
    if not drop:
        return lineage

    children: dict[str, list[str]] = {}
    for a, b in lineage.get("edges", []):
        children.setdefault(a, []).append(b)

    def survivors_below(nid: str, seen: set[str]) -> list[str]:
        out: list[str] = []
        for child in children.get(nid, []):
            if child in seen:
                continue
            seen.add(child)
            out.extend(survivors_below(child, seen) if child in drop else [child])
        return out

    kept = [n for n in nodes if n["id"] not in drop]
    new_edges: list[list[str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for a, b in lineage.get("edges", []):
        if a in drop:
            continue  # a's surviving ancestors re-link to b's survivors
        targets = [b] if b not in drop else survivors_below(b, set())
        for target in targets:
            if (a, target) not in seen_pairs:
                seen_pairs.add((a, target))
                new_edges.append([a, target])

    root = lineage.get("root")
    if root in drop:
        below = survivors_below(root, set())
        root = below[0] if below else (kept[0]["id"] if kept else None)
    return {"nodes": kept, "edges": new_edges, "root": root}


def read_internal_lineage(project: str, content_hash: str) -> dict:
    """Return the per-entry expression DAG as recorded by xorq.

    The cache/remote nodes injected by the rewrite-then-build step (#73) are
    contracted out so the view shows the author's logical pipeline.
    """
    meta_path = entry_build_dir(project, content_hash) / "expr_metadata.json"
    if not meta_path.exists():
        return {"nodes": [], "edges": [], "root": None}
    meta = json.loads(meta_path.read_text())
    return _strip_cache_nodes(meta.get("lineage", {"nodes": [], "edges": [], "root": None}))


_HASH_PATH_RE = re.compile(r"hash_path\s*\n\s*-\s+([^\n]+)")


def read_data_sources(project: str, content_hash: str) -> list[str]:
    """Return the list of source paths an entry reads from.

    Paths are returned with the `${TALLYMAN_PROJECT_ROOT}` placeholder still in
    place (the persisted build is portable). Callers wanting absolute paths
    should expand against the current project_dir.
    """
    yaml_path = entry_build_dir(project, content_hash) / "expr.yaml"
    if not yaml_path.exists():
        return []
    text = yaml_path.read_text()
    return [match.strip() for match in _HASH_PATH_RE.findall(text)]


def catalog_parents(project: str, content_hash: str) -> list[str]:
    """Content hashes of the catalog entries this entry was built from.

    Reads ``manifest.parents`` — the resolved ``from_catalog`` edges recorded at
    build time (#84). Falls back to matching a parent ``result.parquet`` path in
    the entry's ``expr.yaml`` for pre-#74 builds, whose recipes read the parent's
    materialised result directly and never recorded a manifest parent.
    """
    parents: list[str] = []
    seen: set[str] = set()

    manifest_p = entry_dir(project, content_hash) / ENTRY_MANIFEST_FILENAME
    if manifest_p.exists():
        meta = json.loads(manifest_p.read_text())
        for parent in meta.get("parents") or []:
            h = parent.get("hash")
            if h and h != content_hash and h not in seen:
                seen.add(h)
                parents.append(h)

    # The portable form references PLACEHOLDER paths like
    # ${TALLYMAN_PROJECT_ROOT}/artifacts/catalog/entries/<hash>/result.parquet.
    rel_re = re.compile(rf"^{re.escape(PLACEHOLDER)}/artifacts/catalog/entries/([0-9a-f]+)/result\.parquet$")
    for src in read_data_sources(project, content_hash):
        m = rel_re.match(src)
        if m and m.group(1) != content_hash and m.group(1) not in seen:
            seen.add(m.group(1))
            parents.append(m.group(1))
    return parents


def column_lineage(project: str, content_hash: str, *, max_depth: int = 6) -> dict[str, str]:
    """Return per-output-column lineage trees as ASCII strings.

    Loads the entry's xorq expression via the portable load path, calls
    `xorq.common.utils.lineage_utils.build_column_trees`, and renders each
    via `build_tree(...).__str__`. Returns `{column_name: ascii_tree}`.

    Returns an empty dict when the entry has no build artifacts or the
    expression can't be column-traced (e.g. some opaque UDFs).
    """
    from tallyman_xorq.build import load_entry

    try:
        expr = load_entry(project, content_hash)
    except Exception:
        return {}

    try:
        from xorq.common.utils.lineage_utils import build_column_trees, build_tree
    except ImportError:
        return {}

    try:
        trees = build_column_trees(expr)
    except Exception:
        return {}

    out: dict[str, str] = {}
    for col, node in trees.items():
        try:
            out[col] = str(build_tree(node, max_depth=max_depth))
        except Exception:
            continue
    return out


def catalog_dag(project: str) -> dict:
    """Return the full inter-entry DAG for a project.

    Shape: `{nodes: [{hash, prompt, row_count}], edges: [{from, to}]}`,
    where edges flow from parent to child (the parent's `result.parquet` is
    a source to the child).
    """
    base = entries_dir(project)
    if not base.exists():
        return {"nodes": [], "edges": []}
    nodes = []
    edges = []
    for child in sorted(base.iterdir()):
        manifest_p = child / ENTRY_MANIFEST_FILENAME
        if not manifest_p.exists():
            continue
        meta = json.loads(manifest_p.read_text())
        h = meta["content_hash"]
        nodes.append(
            {
                "hash": h,
                "prompt": meta.get("prompt"),
                "row_count": meta.get("row_count"),
            }
        )
        for parent in catalog_parents(project, h):
            edges.append({"from": parent, "to": h})
    return {"nodes": nodes, "edges": edges}
