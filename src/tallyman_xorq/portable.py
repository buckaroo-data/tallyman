"""Make xorq builds portable across machines/users.

xorq writes absolute filesystem paths into its build artifacts (see
`expr.yaml` → `read_kwargs.hash_path`). On a colleague's machine those paths
don't exist, and `load_expr` fails. The fix is symmetric:

- **On write**, replace every `<project_root>/...` substring with the
  placeholder `${TALLYMAN_PROJECT_ROOT}/...` before persisting the build under
  the catalog entry. The xorq-internal node names (which embed a hash of
  the original path) are left alone — xorq doesn't re-validate those at load
  time, they're just identifiers.
- **On load**, expand the placeholder back to the current machine's absolute
  project root into a stable, content-addressed `.xorq_build_expanded` dir
  alongside the entry, then call `xorq.ibis_yaml.compiler.load_expr` on it. The
  expanded dir must persist (not a throwaway tmp dir): `load_expr` returns a
  *lazy* expression, and an entry whose source is snapshotted into
  `xorq_build/database_tables/` reads that source from the expanded dir at
  execute time — which happens long after load. Tearing the dir down first
  makes the read silently yield zero rows.
"""

from __future__ import annotations

import shutil
import threading
from pathlib import Path

PLACEHOLDER = "${TALLYMAN_PROJECT_ROOT}"

# Per-expanded-path locks so two concurrent loads of the same entry don't race
# on the rmtree/mkdir/expand sequence below. Keyed by the expanded dir path; a
# bounded number of entries are touched per process, so this never grows large.
_expand_locks_guard = threading.Lock()
_expand_locks: dict[str, threading.Lock] = {}


def _expand_lock(key: str) -> threading.Lock:
    with _expand_locks_guard:
        return _expand_locks.setdefault(key, threading.Lock())


def make_portable_inplace(build_dir: Path, project_root: Path) -> int:
    """Replace `project_root` substrings in every file under `build_dir`.

    Returns the number of files modified.
    """
    project_root_str = str(project_root)
    modified = 0
    for f in build_dir.iterdir():
        if not f.is_file():
            continue
        try:
            text = f.read_text()
        except UnicodeDecodeError:
            continue  # binary file, skip
        if project_root_str not in text:
            continue
        f.write_text(text.replace(project_root_str, PLACEHOLDER))
        modified += 1
    return modified


def expand_into_dir(build_dir: Path, project_root: Path, target: Path) -> None:
    """Expand placeholders from `build_dir` into an existing `target` directory."""
    project_root_str = str(project_root)
    for item in build_dir.iterdir():
        if item.is_dir():
            shutil.copytree(item, target / item.name)
            continue
        try:
            text = item.read_text()
        except UnicodeDecodeError:
            shutil.copyfile(item, target / item.name)
            continue
        (target / item.name).write_text(text.replace(PLACEHOLDER, project_root_str))


def rewrite_cache_dirs(expr, cache_dir: Path):
    """Point every ``CachedNode``'s storage at *cache_dir* — nested nodes included.

    xorq's own ``load_expr(cache_dir=…)`` rewrite (``ExprLoader.replace_base_path``)
    walks the graph with vendored ibis's ``.replace``, which does not descend into
    opaque ``Expr``-typed fields — so a cache node nested inside another cache
    node's ``parent`` (a chained parent's cache traveling in a child's build, ADR
    D4) keeps whatever base path the build serialized. This deep variant runs the
    same storage rewrite through xorq's ``replace_nodes``, whose traversal does
    descend ``CachedNode.parent``, so every cache node in the closure lands in the
    project's compute cache regardless of nesting depth or where the build was
    written.
    """
    from attr import evolve
    from xorq.common.utils.graph_utils import replace_nodes
    from xorq.expr.relations import CachedNode

    cache_dir = Path(cache_dir)

    def replacer(node, kwargs):
        if isinstance(node, CachedNode):
            storage = getattr(node.cache, "storage", None)
            if storage is not None and Path(str(storage.base_path)) != cache_dir:
                evolved = evolve(node.cache, storage=evolve(storage, base_path=cache_dir))
                return node.__recreate__(dict(zip(node.__argnames__, node.__args__)) | {"cache": evolved})
            return node
        return node.__recreate__(kwargs) if kwargs else node

    return replace_nodes(replacer, expr).to_expr()


def ensure_expanded_build(build_dir: Path, project_root: Path, expanded: Path) -> Path:
    """Expand `${TALLYMAN_PROJECT_ROOT}` placeholders into a stable, persistent dir.

    Returns `expanded`, a content-addressed directory whose path is identical
    across calls and process restarts — never a throwaway tmp dir. It must
    persist for two reasons:

    * xorq's `load_expr` returns a *lazy* expression that reads its source from
      this dir (when the source is snapshotted into `database_tables/`). The
      read happens at execute time — diff/viewer time, long after load — so
      deleting the dir first yields a silent zero-row read.
    * xorq embeds the build dir path in the expression hash used by
      `ParquetSnapshotCache`, so a stable path keeps stat-cache lookups hitting
      rather than missing on every fresh tmp path.

    A sibling `.complete` marker, written last, gates reuse: an expansion
    interrupted by a crash/OOM leaves a partial dir with no marker and is
    redone, never served truncated. Entries are immutable and content-addressed,
    so the expansion is always correct once written.

    Concurrency caveat: `_expand_lock` is process-local. It serializes
    concurrent expansions of the same entry within one process, but two
    separate processes expanding the same entry could still have one `rmtree`
    the partial dir while the other reads it. That is safe for tallyman's
    single-process runtime; a cross-process expansion (a second process serving
    the viewer) would need an `O_CREAT|O_EXCL` lockfile here instead.
    """
    marker = expanded.with_name(expanded.name + ".complete")
    if marker.exists():
        return expanded
    with _expand_lock(str(expanded)):
        # Re-check under the lock: another thread may have finished expanding.
        if marker.exists():
            return expanded
        if expanded.exists():
            shutil.rmtree(expanded)
        expanded.mkdir(parents=True)
        expand_into_dir(build_dir, project_root, expanded)
        marker.write_text("")
    return expanded
