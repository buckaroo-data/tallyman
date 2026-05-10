"""Lay out a DAG into 2-D positions for SVG rendering.

Server-computed positions keep the templates dumb and the layout testable.
No JS layout library needed for the small (3-12 node) DAGs the demo produces.
"""
from __future__ import annotations

from collections import defaultdict


def layered_positions(
    nodes: list[dict],
    edges: list,
    root: str | None = None,
    *,
    node_key: str = "id",
    edge_from: int = 0,
    edge_to: int = 1,
    x_step: int = 150,
    y_step: int = 60,
    margin: int = 30,
) -> dict:
    """Return `{node_id: (x, y)}` plus canvas dimensions.

    Layered by BFS depth from `root` (or the first node if root is None).
    Within each depth, nodes are sorted by id for determinism.

    edges can be `list[list[str]]` (xorq's `[[a, b], ...]`) or
    `list[dict]` (catalog DAG's `[{from, to}, ...]`); `edge_from` and
    `edge_to` select the keys for dict form (use 0/1 for the list form).
    """
    by_id = {str(n[node_key]): n for n in nodes}
    if not by_id:
        return {"positions": {}, "width": 0, "height": 0}

    # Build adjacency in the consumer→producer direction (xorq's convention).
    # We invert so producers (data sources) appear at the *left*: BFS from
    # the root following edges in their natural direction puts dependencies
    # progressively deeper, which gives the same layered structure.
    adj: dict[str, list[str]] = defaultdict(list)
    if edges and isinstance(edges[0], dict):
        get = lambda e, i: e["from"] if i == edge_from else e["to"]  # noqa: E731
        edge_iter = [(get(e, edge_from), get(e, edge_to)) for e in edges]
    else:
        edge_iter = [(str(e[edge_from]), str(e[edge_to])) for e in edges]
    for src, dst in edge_iter:
        adj[src].append(dst)

    root_id = str(root) if root is not None else next(iter(by_id))
    depth: dict[str, int] = {root_id: 0}
    queue = [root_id]
    while queue:
        cur = queue.pop(0)
        for nxt in adj.get(cur, []):
            if nxt not in depth:
                depth[nxt] = depth[cur] + 1
                queue.append(nxt)
    # Any node not reachable from root: append at depth max+1.
    if any(n not in depth for n in by_id):
        max_d = max(depth.values(), default=0)
        for n in by_id:
            depth.setdefault(n, max_d + 1)

    by_depth: dict[int, list[str]] = defaultdict(list)
    for n, d in depth.items():
        by_depth[d].append(n)
    for d in by_depth:
        by_depth[d].sort()

    positions: dict[str, tuple[int, int]] = {}
    for d, ids in by_depth.items():
        for i, nid in enumerate(ids):
            positions[nid] = (margin + d * x_step, margin + i * y_step)

    max_x = max((p[0] for p in positions.values()), default=0) + 120
    max_y = max((p[1] for p in positions.values()), default=0) + 40
    return {"positions": positions, "width": max_x, "height": max_y}
