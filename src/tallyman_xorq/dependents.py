"""Read the cross-entry dependency DAG recorded in each manifest (#84/#89).

``manifest.parents`` records every entry's resolved ``tracked_expr_from_alias`` edges
(``{hash, ref, follow}``) at build time, but nothing reads them: #76 deleted
``lineage.py`` (``catalog_parents`` / ``catalog_dag``) along with the column-
lineage views that were its only consumer. The reactive consumer needs the DAG
walk back — not the column lineage — to find the dependents of a changed entry.
This is that thin reader over the recorded manifests — the single seam every
consumer (recalc, staleness) goes through instead of touching manifest fields
directly: a child's parents, its source-leaf digests, the reverse index (a
parent's children), and the topologically-ordered descendant cone a recalc
rebuilds in dependency order.
"""

from __future__ import annotations

from collections import deque

from tallyman_core.manifest import ParentRef, read_manifest
from tallyman_core.paths import entry_dir
from tallyman_xorq.build import list_entries


class DependencyCycleError(ValueError):
    """The recorded parent edges form a cycle. Build guards against a recipe
    reading its own alias (io.py), but the reader must not assume acyclicity."""


def parents_of(project: str, content_hash: str) -> list[ParentRef]:
    """The recorded parent edges of one entry (``[]`` for a root)."""
    return list(read_manifest(entry_dir(project, content_hash)).parents or [])


def references_own_alias(project: str, content_hash: str, alias: str) -> bool:
    """True if the entry references its own ``alias`` *by name* (#135).

    A revision of alias X whose recipe calls ``tracked_expr_from_alias("X")`` /
    ``pinned_expr_from_alias("X")`` records a parent edge with ``ref == X``. That
    makes the entry an opaque self-reference — and a permanently-stale,
    follows-its-own-head entry — so ``catalog_revise`` rejects it. A reference to
    a prior version *by hash* records ``ref == <hash>`` (not the alias name) and
    is allowed.
    """
    return any(p.ref == alias for p in parents_of(project, content_hash))


def sources_of(project: str, content_hash: str) -> dict[str, str] | None:
    """The recorded source-leaf digests of one entry (``{rel_path: digest}``).

    ``None`` when the entry was built under source-identity ``off`` (no digests
    recorded) — kept distinct from ``{}`` (identity on, but the recipe read no
    raw files), because staleness treats ``None`` as an unevaluable axis rather
    than silently "fresh".
    """
    return read_manifest(entry_dir(project, content_hash)).sources


def build_dag(project: str) -> dict[str, list[ParentRef]]:
    """Forward edges for every live entry: ``{child_hash: [ParentRef, ...]}``.

    Enumerates via ``build.list_entries`` (every on-disk ``manifest.json``), not
    ``read_tallyman_state``, so a freshly built but not-yet-checkpointed entry is
    still seen.
    """
    dag: dict[str, list[ParentRef]] = {}
    for entry in list_entries(project):
        parents = entry.get("parents") or []
        dag[entry["content_hash"]] = [ParentRef.model_validate(p) for p in parents]
    return dag


def dependents_index(project: str) -> dict[str, set[str]]:
    """The reverse index: ``{parent_hash: {child_hash, ...}}``.

    The recalc walk needs this to go from a changed entry to everything that
    depends on it; it does not exist anywhere else.
    """
    return _children_map(build_dag(project))


def descendant_cone(project: str, root_hashes: list[str]) -> list[str]:
    """The roots and all their transitive dependents, parents before children.

    Topologically ordered so a recalc rebuilds in dependency order. Raises
    ``DependencyCycleError`` on a cycle rather than looping.
    """
    dag = build_dag(project)
    children = _children_map(dag)
    return _order_cone(root_hashes, dag, children)


def _children_map(dag: dict[str, list[ParentRef]]) -> dict[str, set[str]]:
    children: dict[str, set[str]] = {}
    for child, parents in dag.items():
        for parent in parents:
            children.setdefault(parent.hash, set()).add(child)
    return children


def _order_cone(
    roots: list[str],
    dag: dict[str, list[ParentRef]],
    children: dict[str, set[str]],
) -> list[str]:
    # 1. Collect the cone: every node reachable from a root via child edges,
    #    plus the roots themselves.
    cone: set[str] = set()
    stack = list(roots)
    while stack:
        node = stack.pop()
        if node in cone:
            continue
        cone.add(node)
        stack.extend(children.get(node, ()))

    # 2. Topologically sort the induced subgraph (Kahn). In-degree counts only
    #    intra-cone parents, so a node whose stable parents sit outside the cone
    #    is a source within it. Sorting keeps the order deterministic.
    indegree = {node: sum(1 for parent in dag.get(node, []) if parent.hash in cone) for node in cone}
    queue = deque(sorted(node for node in cone if indegree[node] == 0))
    order: list[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for child in sorted(children.get(node, ())):
            if child not in cone:
                continue
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(order) != len(cone):
        raise DependencyCycleError(f"dependency cycle among entries: {sorted(cone - set(order))}")
    return order
