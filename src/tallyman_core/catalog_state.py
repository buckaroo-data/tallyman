"""The native catalog's versioning engine (see plans/adr-reset-to-revision.md).

Every piece of mutable catalog state is now its own git-tracked file the
native store (#52) allows (charts under ``chart_specs/``, display configs under
``display_configs/``, post-processing/stats ``.py`` under the repo, aliases in
``aliases.jsonl``, the notebook in ``notebook.jsonl``, per-entry prompt history
in ``prompts/<hash>.jsonl``). Their own mutators write them; ``reset_to``'s
``git reset --hard`` restores them all at once. There is no longer a
``catalog.yaml`` round-trip (no ``capture``/``materialize`` of those sections).

What remains here is the *pointer* bookkeeping for the two **untracked**
artifact sets — the entry build dirs and the compute-cache warm set — in two
tracked JSONL files:

    entries.jsonl:       {"hash": content_hash}   # untracked entries/<hash>/ dirs
    compute_cache.jsonl: {"path": relpath}        # untracked compute-cache warm set

Those heavy artifacts are content-addressed, additive, and gitignored, so
``git reset`` can't roll them back; ``reset_to`` reconciles them to the recorded
pointers via the bullpen — evictions retire (not deleted), and anything a
restored step records but is missing comes back by copy. Live operations never
read the bullpen, so a re-added expression still computes cold.

A checkpoint captures the pointer lists, zips any pending recipe (catalog.py),
``git add -A``, and commits once — through the fork-safe git primitive
(git_util) under a per-project lock — keeping the four invariants #33 kept
re-breaking structural rather than reviewed.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import shutil
from pathlib import Path

from tallyman_core import catalog
from tallyman_core.git_util import run_git
from tallyman_core.paths import (
    ENTRIES_DIRNAME,
    bullpen_dir,
    catalog_dir,
    compute_cache_dir,
    entries_dir,
)

log = logging.getLogger(__name__)

# git-level identity flags, so commits work without a global git config.
_GIT_ID = ["-c", "user.email=tallyman@local", "-c", "user.name=tallyman"]
_STEP_RE = re.compile(r"^step-(\d+)$")
# Refs and labels are operator input that ends up as git arguments. A plain
# tag-shaped name only: no leading dash (option injection), no revision
# navigation (~ ^ : @), nothing git would resolve beyond a tag lookup.
_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


# ---------------------------------------------------------------------------
# pointer bookkeeping: the two tracked JSONL files that replace catalog.yaml
# ---------------------------------------------------------------------------


def _entries_file(project: str) -> Path:
    return catalog_dir(project) / "entries.jsonl"


def _compute_cache_file(project: str) -> Path:
    return catalog_dir(project) / "compute_cache.jsonl"


def _read_jsonl(path: Path, field: str) -> list[str] | None:
    """The *field* of each line, or None when the file is absent — so callers
    can tell "never recorded" (no-op) from a recorded-empty list (reconcile to
    nothing)."""
    if not path.exists():
        return None
    return [json.loads(line)[field] for line in path.read_text().splitlines() if line.strip()]


def _write_jsonl(path: Path, field: str, values: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps({field: v}) + "\n" for v in values))


def read_tallyman_state(project: str) -> dict:
    """The pointer lists for the *untracked* artifacts the bullpen reconciles —
    entry build dirs (``entry_hashes``) and the compute-cache warm set
    (``compute_cache``). Each defaults to [] so callers never KeyError.

    The decomposed mutable sections (charts, display, post-processing, stats,
    aliases, notebook) are tracked files now, restored by ``git reset`` directly,
    so they are no longer carried here.
    """
    return {
        "entry_hashes": _read_jsonl(_entries_file(project), "hash") or [],
        "compute_cache": _read_jsonl(_compute_cache_file(project), "path") or [],
    }


def write_tallyman_state(
    project: str, *, entry_hashes: list[str] | None = None, compute_cache: list[str] | None = None
) -> None:
    """Persist whichever pointer list is given to its tracked JSONL (the other
    is left untouched)."""
    if entry_hashes is not None:
        _write_jsonl(_entries_file(project), "hash", entry_hashes)
    if compute_cache is not None:
        _write_jsonl(_compute_cache_file(project), "path", compute_cache)


# ---------------------------------------------------------------------------
# capture: live untracked-artifact listings -> the two tracked pointer files
# ---------------------------------------------------------------------------


def _list_cache_files(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def capture_tallyman_state(project: str) -> dict:
    """Snapshot the pointer lists for the untracked artifacts into their tracked
    JSONL files. The decomposed sections write their own tracked files, so
    capture no longer touches charts/display/pp/stats/aliases/notebook."""
    ed = entries_dir(project)
    # Only COMPLETE entry dirs (a manifest is the build's last write) — the same
    # filter zip_pending_entries uses, so capture and the zip writer agree on the
    # set a checkpoint records. Otherwise a checkpoint racing a mid-build dir
    # records a pointer with no durable recipe zip, and the step is permanently
    # un-reset-able (the pointer/recipe consistency guard rejects it).
    entry_hashes = (
        sorted(c.name for c in ed.iterdir() if c.is_dir() and (c / "manifest.json").is_file()) if ed.exists() else []
    )
    compute_cache = _list_cache_files(compute_cache_dir(project))
    write_tallyman_state(project, entry_hashes=entry_hashes, compute_cache=compute_cache)
    return {"entry_hashes": entry_hashes, "compute_cache": compute_cache}


# ---------------------------------------------------------------------------
# prune/restore: reconcile untracked artifacts to the pointer lists, via the
# bullpen — evictions are retired (moved), not destroyed, and a forward reset
# copies the step's recorded set back instead of recomputing it.
# ---------------------------------------------------------------------------


def _retire(src: Path, dest: Path) -> None:
    """Move an evicted artifact into the bullpen.

    Content-addressed names mean an existing *dest* is the same content, so
    the source is simply dropped rather than re-moved.
    """
    if dest.exists():
        shutil.rmtree(src) if src.is_dir() else src.unlink()
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))


def prune_entries(project: str) -> int:
    """Retire entry dirs not named by ``entries.jsonl``. No-op when the pointer
    file is absent (never captured), so a build before the first checkpoint is
    not mistaken for an eviction."""
    valid = _read_jsonl(_entries_file(project), "hash")
    if valid is None:
        return 0
    valid = set(valid)
    ed = entries_dir(project)
    if not ed.exists():
        return 0
    removed = 0
    for child in ed.iterdir():
        if child.is_dir() and child.name not in valid:
            _retire(child, bullpen_dir(project) / ENTRIES_DIRNAME / child.name)
            removed += 1
    return removed


def prune_compute_cache(project: str) -> int:
    valid = _read_jsonl(_compute_cache_file(project), "path")
    if valid is None:
        return 0
    valid = set(valid)
    root = compute_cache_dir(project)
    if not root.exists():
        return 0
    removed = 0
    for p in root.rglob("*"):
        if p.is_file() and (rel := str(p.relative_to(root))) not in valid:
            _retire(p, bullpen_dir(project) / "compute_cache" / rel)
            removed += 1
    return removed


def restore_from_bullpen(project: str) -> int:
    """Copy recorded-but-missing artifacts back from the bullpen.

    The inverse of the prunes, for a reset that walks forward: anything the
    restored pointer files name that is absent from the live tree comes back by
    *copy*, so the bullpen keeps its set and the back/forward rehearsal loop can
    repeat. Only ``reset_to`` calls this — live operations never see the
    bullpen, which is what keeps a re-added expression honest (cold).
    """
    bp = bullpen_dir(project)
    restored = 0
    ed = entries_dir(project)
    for h in _read_jsonl(_entries_file(project), "hash") or []:
        live, parked = ed / h, bp / ENTRIES_DIRNAME / h
        if not live.exists() and parked.is_dir():
            shutil.copytree(parked, live)
            restored += 1
    root = compute_cache_dir(project)
    for rel in _read_jsonl(_compute_cache_file(project), "path") or []:
        live, parked = root / rel, bp / "compute_cache" / rel
        if not live.exists() and parked.is_file():
            live.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parked, live)
            restored += 1
    return restored


# ---------------------------------------------------------------------------
# the catalog git repo: init, checkpoint (one commit per op), reset
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _project_lock(project: str):
    """Cross-process file lock so the companion and MCP server can't race a
    checkpoint (the git index.lock race that swallowed #33 writes)."""
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(cd / ".checkpoint.lock"), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def ensure_catalog_repo(project: str) -> bool:
    """Idempotently git-init the catalog repo. Returns True if it exists after.

    The branch is pinned to ``main`` (git ≥ 2.28) rather than inheriting the
    host's ``init.defaultBranch`` (master on stock git), so the repo layout is
    deterministic and host-config-independent.
    """
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    if not (cd / ".git").exists():
        rc, _, err = run_git(["init", "-q", "-b", "main"], cwd=cd)
        if rc != 0:
            log.warning("catalog git init failed in %s: %s", cd, err)
            return False
    # The native store's .gitignore keeps the heavy/derived artifacts out of the
    # checkpoint's `git add -A`; the allowlist consistency guard is the durable
    # backstop. Idempotent, so existing repos pick it up too.
    catalog.write_gitignore(project)
    return True


def _step_tags(project: str) -> list[int]:
    rc, out, _ = run_git(["tag", "-l", "step-*"], cwd=catalog_dir(project))
    nums = []
    for t in out.split():
        m = _STEP_RE.match(t)
        if m:
            nums.append(int(m.group(1)))
    return sorted(nums)


def checkpoint_catalog(project: str, message: str, *, step: int | None = None, label: str | None = None) -> int | None:
    """Capture pointers, zip pending recipes, commit the tracked surface once, tag the step.

    One commit per operation (not per mutator), through the fork-safe primitive,
    under the per-project lock. Returns the step number, or None when the
    commit did not land (e.g. index.lock contention with xorq's own catalog
    add, which commits to this repo outside the flock). The step tag only
    moves with a landed commit: tagging anyway would stack step-N on
    step-(N-1)'s commit and hide the earlier step from ``list_revisions``,
    which keys rows by commit.
    """
    cd = catalog_dir(project)
    with _project_lock(project):
        ensure_catalog_repo(project)
        capture_tallyman_state(project)
        # The checkpoint is the sole zip writer and sole git transaction: zip any
        # complete entry dir lacking a tracked recipe, then stage the whole
        # tracked surface. A committed pointer always has its recipe in the same
        # commit; the .gitignore + allowlist keep `git add -A` honest.
        catalog.zip_pending_entries(project)
        run_git(["add", "-A"], cwd=cd)
        if step is None:
            tags = _step_tags(project)
            step = tags[-1] + 1 if tags else 0
        rc, _, err = run_git([*_GIT_ID, "commit", "-m", message, "--allow-empty"], cwd=cd)
        if rc != 0:
            log.warning("catalog checkpoint commit failed (step %s): %s", step, err)
            return None
        run_git(["tag", "-f", f"step-{step:03d}"], cwd=cd)
        if label:
            run_git(["tag", "-f", label], cwd=cd)
    return step


def _resolve_tag(project: str, tag: str) -> str:
    """The commit a step/label tag points at.

    Refs arrive from the HTTP payload and the CLI, so anything that is not a
    plain *existing* tag is rejected here — option-like strings, revision
    navigation (``step-001~1``, ``:/msg``), and bare refs like ``HEAD`` never
    reach ``git reset`` as arguments.
    """
    if not _TAG_RE.match(tag):
        raise RuntimeError(f"invalid revision {tag!r}: expected a step number or label")
    rc, out, _ = run_git(["rev-parse", "--verify", "-q", f"refs/tags/{tag}^{{commit}}"], cwd=catalog_dir(project))
    if rc != 0:
        raise RuntimeError(f"unknown revision {tag!r}")
    return out


def reset_to(project: str, ref: int | str) -> None:
    """Restore the catalog to a step/label: ``git reset --hard`` (which restores
    every tracked file — the recipe zips and all decomposed mutable state — at
    once), then reconcile the *untracked* artifacts to the recorded pointers:
    evictions retire to the bullpen, recorded-but-missing files come back from
    it. Finally re-validate the tracked recipe set against the pointers, so a
    step whose two views disagree fails loudly rather than returning a masked
    divergence (#52)."""
    cd = catalog_dir(project)
    tag = f"step-{ref:03d}" if isinstance(ref, int) else str(ref)
    with _project_lock(project):
        commit = _resolve_tag(project, tag)
        rc, _, err = run_git(["reset", "--hard", commit], cwd=cd)
        if rc != 0:
            raise RuntimeError(f"catalog reset to {tag!r} failed: {err}")
        prune_entries(project)
        prune_compute_cache(project)
        restore_from_bullpen(project)
        catalog.assert_catalog_consistent(project, set(read_tallyman_state(project)["entry_hashes"]))


def genesis(project: str) -> int | None:
    """Record step-000 from the freshly-created tree, once.

    Called at the project-creation surfaces (CLI ``init``, companion
    ``/api/projects/new``, MCP ``project_new``) so the baseline ``reset-to``
    targets is the true empty start — not a state with the first edit already
    folded in. Returns None when there is nothing to record — the repo already
    has commits (so a ``--force`` re-init preserves history) — or when the
    genesis commit itself failed to land. Existing projects that predate the
    feature get their step 0 lazily from the first checkpoint instead.
    """
    if not ensure_catalog_repo(project):
        return None
    rc, _, _ = run_git(["rev-parse", "--verify", "-q", "HEAD"], cwd=catalog_dir(project))
    if rc == 0:
        return None
    return checkpoint_catalog(project, "genesis", step=0)


def current_step(project: str) -> int | None:
    """The step tag at HEAD, or None when the repo has no steps yet."""
    rc, out, _ = run_git(["tag", "--points-at", "HEAD", "-l", "step-*"], cwd=catalog_dir(project))
    nums = [int(m.group(1)) for t in out.split() if (m := _STEP_RE.match(t))]
    return max(nums) if nums else None


def label_step(project: str, step: int, name: str) -> None:
    """Name a step so the operator can ``reset-to <name>``.

    The name becomes a git argument and lives in the tag namespace, so three
    shapes are refused: option-like names (``-d`` would *delete* tags),
    step-shaped names (``tag -f`` would clobber a real step), and all-digit
    names (unreachable — digit refs parse as step numbers).
    """
    if not _TAG_RE.match(name) or name.isdigit() or _STEP_RE.match(name):
        raise RuntimeError(f"invalid label {name!r}: use a name that is not a step tag or a bare number")
    rc, _, err = run_git(["tag", "-f", name, f"step-{step:03d}"], cwd=catalog_dir(project))
    if rc != 0:
        raise RuntimeError(f"labelling step {step} as {name!r} failed: {err}")


def list_revisions(project: str) -> list[dict]:
    """The timeline: every step tag with its commit, op message, labels, and
    a marker for the step ``current`` (HEAD) points at."""
    cd = catalog_dir(project)
    rc, out, _ = run_git(
        ["for-each-ref", "refs/tags", "--format=%(refname:short)%09%(objectname:short)%09%(subject)"],
        cwd=cd,
    )
    if rc != 0:
        return []
    steps: dict[str, dict] = {}  # commit -> revision row
    labels: dict[str, list[str]] = {}  # commit -> non-step tag names
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        ref, commit = parts[0], parts[1]
        subject = parts[2] if len(parts) > 2 else ""
        m = _STEP_RE.match(ref)
        if m:
            steps[commit] = {"step": int(m.group(1)), "commit": commit, "op": subject}
        else:
            labels.setdefault(commit, []).append(ref)
    cur = current_step(project)
    revs = sorted(steps.values(), key=lambda r: r["step"])
    for r in revs:
        r["labels"] = sorted(labels.get(r["commit"], []))
        r["current"] = r["step"] == cur
    return revs
