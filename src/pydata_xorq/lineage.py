"""Two flavors of lineage:

- **Internal lineage**: the expression DAG inside one catalog entry. Comes
  pre-baked from xorq's `expr_metadata.json` — nodes are operations (Read,
  Aggregate, Field, ...), edges connect them.

- **Catalog lineage**: which *catalog entries* depend on which other catalog
  entries. Derived from the absolute paths that an entry's `xorq_build/expr.yaml`
  references in its `read_kwargs.hash_path` values: if any of those paths points
  at another entry's `result.parquet`, that other entry is a parent.

V0 only emits catalog edges when user code uses `pydata_xorq.io.from_catalog()`
(or otherwise reads from another entry's `result.parquet`). Without those
cross-references the catalog DAG is a forest of single-node trees rooted at
each entry's data source.
"""

from __future__ import annotations

import json
import re

from pydata_core import entries_dir, entry_dir
from pydata_xorq.portable import PLACEHOLDER


def read_internal_lineage(project: str, content_hash: str) -> dict:
    """Return the per-entry expression DAG as recorded by xorq."""
    meta_path = entry_dir(project, content_hash) / "xorq_build" / "expr_metadata.json"
    if not meta_path.exists():
        return {"nodes": [], "edges": [], "root": None}
    meta = json.loads(meta_path.read_text())
    return meta.get("lineage", {"nodes": [], "edges": [], "root": None})


_HASH_PATH_RE = re.compile(r"hash_path\s*\n\s*-\s+([^\n]+)")


def read_data_sources(project: str, content_hash: str) -> list[str]:
    """Return the list of source paths an entry reads from.

    Paths are returned with the `${PYDATA_PROJECT_ROOT}` placeholder still in
    place (the persisted build is portable). Callers wanting absolute paths
    should expand against the current project_dir.
    """
    yaml_path = entry_dir(project, content_hash) / "xorq_build" / "expr.yaml"
    if not yaml_path.exists():
        return []
    text = yaml_path.read_text()
    return [match.strip() for match in _HASH_PATH_RE.findall(text)]


def catalog_parents(project: str, content_hash: str) -> list[str]:
    """Hashes of any catalog entries whose result.parquet is referenced as a source."""
    sources = read_data_sources(project, content_hash)
    parents: list[str] = []
    # The portable form references PLACEHOLDER paths like
    # ${PYDATA_PROJECT_ROOT}/artifacts/catalog/entries/<hash>/result.parquet.
    rel_re = re.compile(rf"^{re.escape(PLACEHOLDER)}/artifacts/catalog/entries/([0-9a-f]+)/result\.parquet$")
    for src in sources:
        m = rel_re.match(src)
        if m and m.group(1) != content_hash:
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
    from pydata_xorq.build import load_entry

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
        manifest_p = child / "manifest.json"
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
