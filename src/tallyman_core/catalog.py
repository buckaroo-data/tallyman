"""The native content-addressed recipe store (replaces xorq's catalog package).

tallyman owns the catalog *model*; this module is the *store*. It writes one
immutable, deterministic ``entries/<hash>.zip`` per entry from an allowlist of
the entry dir, and enforces the tracked surface as a path allowlist
(deny-by-default) so a stray file ``git add -A`` would otherwise stage is
rejected instead of committed silently (#52).

Call direction is one-way: ``catalog_state`` (the versioning engine) calls into
here; this module imports only ``paths``/``git_util`` and never
``catalog_state`` — ``assert_catalog_consistent`` takes the pointer set as an
argument to keep it acyclic.

What we deliberately drop from xorq's catalog: ``assert_consistency`` (its tree
contract), the three-way merge, the per-expression wheel + requirements.txt, and
the zip internal format / ``REQUIRED_ARCHIVE_NAMES``. Nothing reads our zip back
(the only consumer is ``tracked_recipe_hashes`` over zip *names*), so the format
is entirely ours.
"""

from __future__ import annotations

import fnmatch
import zipfile
from pathlib import Path

from tallyman_core.git_util import run_git
from tallyman_core.paths import catalog_dir, entries_dir

# The immutable recipe is an ALLOWLIST of the entry dir, not "everything except":
# a future derived artifact dropped in the entry dir can't leak into the durable
# zip. Top-level files plus the xorq_build/ tree. Excludes .xorq_build_expanded/
# (machine-local), result.parquet (untracked heavy result), and prompts.jsonl
# (append-mutable provenance — a tracked decomposition file instead).
RECIPE_TOP_LEVEL = ("expr.py", "schema.json", "manifest.json")
RECIPE_TREE_DIRS = ("xorq_build",)

# Deterministic archive constants so an identical rebuild yields a byte-identical
# blob (no churn in git history). Single machine, so cross-zlib reproducibility
# is moot; pinning the level keeps bytes stable regardless of zlib defaults.
_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_ZIP_EXTERNAL_ATTR = 0o644 << 16
_ZIP_COMPRESSLEVEL = 6

# The git-tracked surface as fnmatch patterns (deny-by-default). Every
# ``git ls-files`` path must match one of these or assert_catalog_consistent
# raises. The heavy/derived artifacts are kept out by the repo .gitignore; this
# allowlist is the durable guard, since ``git add -A`` is allow-by-default.
TRACKED_SURFACE = (
    ".gitignore",
    "entries/*.zip",
    "chart_specs/*.vl.json",
    "display_configs/*.json",
    "aliases.jsonl",
    "notebook.jsonl",
    "entries.jsonl",
    "compute_cache.jsonl",
    "prompts/*.jsonl",
    "post_processing/*.py",
    "post_processing/_disabled/*.py",
    "stats/*.py",
    "stats/_disabled/*.py",
    "pyproject.toml",
    "uv.lock",
)

# The repo .gitignore: keep the heavy/derived artifacts out of ``git add -A``.
# Trailing-slash patterns match directories only, so ``entries/*/`` ignores the
# build dirs while ``entries/<hash>.zip`` files stay trackable, and
# ``compute_cache/`` ignores the cache dir while ``compute_cache.jsonl`` (the
# pointer file) stays trackable.
GITIGNORE_LINES = (
    "entries/*/",
    "bullpen/",
    "result_cache/",
    "compute_cache/",
    "diff_stat_cache/",
    ".checkpoint.lock",
    ".xorq_build_expanded/",
    ".migrated_*",  # machine-local upgrade-migration markers (e.g. #104), not catalog content
    "*.tmp",  # an interrupted atomic write's temp (atomic_write_text / write_recipe_zip), never committable
)


def write_gitignore(project: str) -> Path:
    """Write the catalog repo's .gitignore (idempotent). The native store is the
    first to introduce one — before, an explicit ``git add catalog.yaml`` kept
    staging honest; now ``git add -A`` needs the denylist (plus the allowlist
    consistency guard)."""
    p = catalog_dir(project) / ".gitignore"
    body = "\n".join(GITIGNORE_LINES) + "\n"
    if not p.exists() or p.read_text() != body:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
    return p


def _surface_match(path: str, pattern: str) -> bool:
    """fnmatch per path segment, so a ``*`` never spans ``/``.

    ``post_processing/*.py`` matches ``post_processing/foo.py`` but NOT
    ``post_processing/sub/leak.py`` — plain ``fnmatch`` lets ``*`` cross ``/`` and
    would wave the nested stray through (and ``entries/*.zip`` past
    ``entries/sub/x.zip``), defeating the deny-by-default allowlist.
    """
    segs, pats = path.split("/"), pattern.split("/")
    return len(segs) == len(pats) and all(fnmatch.fnmatchcase(s, p) for s, p in zip(segs, pats))


def _recipe_members(entry_dir: Path) -> list[tuple[Path, str]]:
    """(abs_path, relpath) for every allowlisted recipe file, sorted by relpath."""
    members: list[tuple[Path, str]] = []
    for name in RECIPE_TOP_LEVEL:
        f = entry_dir / name
        if f.is_file():
            members.append((f, name))
    for d in RECIPE_TREE_DIRS:
        root = entry_dir / d
        if root.is_dir():
            for f in root.rglob("*"):
                if f.is_file():
                    members.append((f, str(f.relative_to(entry_dir))))
    return sorted(members, key=lambda t: t[1])


def write_recipe_zip(entry_dir: Path, dest: Path, content_hash: str) -> Path:
    """Archive the recipe allowlist of *entry_dir* into *dest*, deterministically.

    Members stored under arcname ``<content_hash>/<relpath>`` (single top-level
    prefix so a future clone-restore can reuse it). Named by the (possibly
    salted) *content_hash*, not ``build_path.name`` — otherwise the zip names
    diverge from the entry pointers under source-identity salt mode (Risk #5).
    """
    members = _recipe_members(entry_dir)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=_ZIP_COMPRESSLEVEL) as zf:
        for path, rel in members:
            info = zipfile.ZipInfo(f"{content_hash}/{rel}", date_time=_ZIP_DATE_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = _ZIP_EXTERNAL_ATTR
            zf.writestr(info, path.read_bytes())
    tmp.replace(dest)
    return dest


def tracked_recipe_hashes(project: str) -> set[str]:
    """Hashes of the git-tracked ``entries/<hash>.zip`` recipes — the durable,
    content-addressed artifact set (what survives a clone or ``git reset``), as
    opposed to the untracked ``entries/<hash>/`` build dirs the bullpen
    reconciles. Sourced from ``git ls-files`` so it reflects the committed tree.
    """
    rc, out, _ = run_git(["ls-files", "entries"], cwd=catalog_dir(project))
    if rc != 0:
        return set()
    return {ln[len("entries/") : -len(".zip")] for ln in out.split() if ln.endswith(".zip")}


def zip_pending_entries(project: str) -> list[str]:
    """Zip every complete entry dir that lacks a tracked recipe zip.

    The sole zip writer, called by the checkpoint under the project lock. A
    "complete" dir has a manifest (the build's last write), so a partial dir
    from an interrupted build is skipped, not zipped. Deterministic + content-
    addressed, so re-zipping an unchanged entry is a byte-identical no-op.
    """
    tracked = tracked_recipe_hashes(project)
    ed = entries_dir(project)
    if not ed.exists():
        return []
    written: list[str] = []
    for child in sorted(ed.iterdir()):
        if not child.is_dir() or child.name in tracked:
            continue
        if not (child / "manifest.json").is_file():
            continue  # incomplete build dir — let the next checkpoint pick it up
        write_recipe_zip(child, ed / f"{child.name}.zip", child.name)
        written.append(child.name)
    return written


def _committed_sidecar_hashes(cd: Path, prefix: str, suffix: str) -> set[str]:
    """Content hashes of the git-tracked ``<prefix>/<hash><suffix>`` sidecars.

    Reads ``git ls-files`` (the committed tree, like ``tracked_recipe_hashes``),
    so a transient *untracked* sidecar on disk — set but not yet checkpointed —
    is never mistaken for a committed orphan.
    """
    rc, out, _ = run_git(["ls-files", prefix], cwd=cd)
    if rc != 0:
        return set()
    return {
        ln[len(prefix) + 1 : -len(suffix)] for ln in out.split() if ln.startswith(f"{prefix}/") and ln.endswith(suffix)
    }


def assert_catalog_consistent(project: str, pointers: set[str]) -> None:
    """Three guards over the committed tree (#52), raising on any violation.

    1. **Tracked-surface allowlist** (deny-by-default): every ``git ls-files``
       path must fnmatch ``TRACKED_SURFACE`` or it raises — so a stray dir that
       ``git add -A`` would otherwise stage is rejected, not silently committed.
    2. **Pointer/recipe agreement**: the git-tracked recipe set
       (``entries/<hash>.zip``) must equal the tallyman *pointers*
       (``entry_hashes``), so a pointer with no durable recipe — or a recipe
       with no pointer — fails loudly instead of returning a masked divergence.
    3. **Sidecar hash-reference integrity**: every decomposed file keyed by
       content hash (``aliases.jsonl`` latest+history, ``chart_specs/<hash>``,
       ``display_configs/<hash>``) must name a hash in the durable recipe set.
       Guard 2 only pairs recipes with pointers; without this a committed step
       whose alias/chart/display points at a hash with no recipe would reset
       clean and report OK — and a dangling alias head later blows up the
       self-chaining ``from_catalog`` build. (Notebook cells key on alias *name*,
       not hash, so they are deliberately out of scope here.)

    Takes *pointers* as an argument (rather than reading ``catalog_state``) to
    keep the call direction one-way and acyclic.
    """
    cd = catalog_dir(project)
    rc, out, _ = run_git(["ls-files"], cwd=cd)
    if rc == 0:
        unlisted = [p for p in out.split() if not any(_surface_match(p, pat) for pat in TRACKED_SURFACE)]
        if unlisted:
            raise RuntimeError(
                "catalog tracks paths outside the allowed surface (a stray file `git add -A` would commit): "
                f"{sorted(unlisted)}"
            )
    tracked = tracked_recipe_hashes(project)
    if tracked != pointers:
        missing = sorted(pointers - tracked)  # pointer present, no durable recipe
        orphan = sorted(tracked - pointers)  # durable recipe, no pointer
        raise RuntimeError(
            "catalog inconsistent: the git-tracked recipe set and the entry_hashes pointers disagree "
            f"(pointers with no durable recipe: {missing}; tracked recipes with no pointer: {orphan})"
        )

    # Guard 3 — sidecar hash-reference integrity. Function-level import keeps the
    # call direction one-way: aliases imports only paths/fsutil, never catalog or
    # catalog_state, so this stays acyclic. Aliases are read on-disk (== the
    # committed tree after reset_to's git reset --hard); charts/display from git.
    from tallyman_core import aliases as _aliases

    alias_refs: set[str] = set(_aliases.load_aliases(project).values())
    for history in _aliases.load_history(project).values():
        alias_refs.update(history)

    dangling: dict[str, list[str]] = {}
    bad_aliases = sorted(h for h in alias_refs if h not in tracked)
    if bad_aliases:
        dangling["aliases.jsonl"] = bad_aliases
    for prefix, suffix in (("chart_specs", ".vl.json"), ("display_configs", ".json")):
        bad = sorted(h for h in _committed_sidecar_hashes(cd, prefix, suffix) if h not in tracked)
        if bad:
            dangling[prefix] = bad
    if dangling:
        raise RuntimeError(
            "catalog inconsistent: decomposed sidecars name content hashes with no durable recipe "
            f"(orphans by surface: {dangling})"
        )
