"""The native catalog's versioning engine (see plans/adr-reset-to-revision.md).

Every piece of mutable catalog state is now its own git-tracked file the
native store (#52) allows (charts under ``chart_specs/``, display configs under
``display_configs/``, post-processing/stats ``.py`` under the repo, aliases in
``aliases.jsonl``, the notebook in ``notebook.jsonl``, per-entry prompt history
in ``prompts/<hash>.jsonl``). Their own mutators write them; ``reset_to``'s
``git reset --hard`` restores them all at once. There is no longer a
``catalog.yaml`` round-trip (no ``capture``/``materialize`` of those sections).

What remains here is the *pointer* bookkeeping for the untracked entry build
dirs, in one tracked JSONL file:

    entries.jsonl:       {"hash": content_hash}   # untracked entries/<hash>/ dirs

Those heavy artifacts are content-addressed, additive, and gitignored, so
``git reset`` can't roll them back; ``reset_to`` reconciles them to the recorded
pointers via the bullpen: evictions retire (not deleted), and anything a
restored step records but is missing comes back by copy. An eviction replaces a
parked dir of the same name, since the live one matches the snapshot on disk
(#194). Live operations never read the bullpen.

``compute_cache/`` is not managed here (ADR-007 D14). Its files are named by
content hash and each one can be made again by ``ensure_materialized``, so a
reset leaves them alone and a snapshot that is missing afterwards is healed and
verified like any other. The source clones under ``data/.cas/`` are data, the
only frozen copy of the bytes an entry was built from, so a reset moves the ones
no surviving entry refers to into the bullpen (never deletes them) and a reset
forward copies them back.

A checkpoint captures the pointer list, zips any pending recipe (catalog.py),
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
import threading
from pathlib import Path

from tallyman_core import catalog
from tallyman_core.git_util import run_git
from tallyman_core.paths import (
    ENTRIES_DIRNAME,
    bullpen_dir,
    catalog_dir,
    data_dir,
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
# pointer bookkeeping: the tracked JSONL file that replaces catalog.yaml
# ---------------------------------------------------------------------------


def _entries_file(project: str) -> Path:
    return catalog_dir(project) / "entries.jsonl"


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
    """The pointer list for the *untracked* entry build dirs the bullpen
    reconciles (``entry_hashes``). Defaults to [] so callers never KeyError.

    The decomposed mutable sections (charts, display, post-processing, stats,
    aliases, notebook) are tracked files now, restored by ``git reset`` directly,
    so they are no longer carried here.
    """
    return {"entry_hashes": _read_jsonl(_entries_file(project), "hash") or []}


def write_tallyman_state(project: str, *, entry_hashes: list[str] | None = None) -> None:
    """Persist the entry pointer list to its tracked JSONL."""
    if entry_hashes is not None:
        _write_jsonl(_entries_file(project), "hash", entry_hashes)


# ---------------------------------------------------------------------------
# capture: the live entry listing -> the tracked pointer file
# ---------------------------------------------------------------------------


def capture_tallyman_state(project: str) -> dict:
    """Snapshot the entry pointer list into its tracked JSONL file. The
    decomposed sections write their own tracked files, so capture no longer
    touches charts/display/pp/stats/aliases/notebook, and it no longer lists
    ``compute_cache/`` (ADR-007 D14), whose cost grew with the cache (#22)."""
    ed = entries_dir(project)
    # Only COMPLETE entry dirs (a manifest is the build's last write) — the same
    # filter zip_pending_entries uses, so capture and the zip writer agree on the
    # set a checkpoint records. Otherwise a checkpoint racing a mid-build dir
    # records a pointer with no durable recipe zip, and the step is permanently
    # un-reset-able (the pointer/recipe consistency guard rejects it).
    entry_hashes = (
        sorted(c.name for c in ed.iterdir() if c.is_dir() and (c / "manifest.json").is_file()) if ed.exists() else []
    )
    write_tallyman_state(project, entry_hashes=entry_hashes)
    return {"entry_hashes": entry_hashes}


# ---------------------------------------------------------------------------
# prune/restore: reconcile untracked artifacts to the pointer list, via the
# bullpen — evictions are retired (moved), not destroyed, and a forward reset
# copies the step's recorded set back instead of recomputing it.
# ---------------------------------------------------------------------------


def _retire(src: Path, dest: Path) -> None:
    """Move an evicted entry dir into the bullpen, replacing any dir already parked under its name.

    The name is the content hash, but the dir's contents are not a function of it: the manifest records ``created_at``
    and ``prompt``, and a recipe that is not reproducible records another ``result_digest`` each time it is created.
    The live dir is the one that agrees with the snapshot on disk, since a create always rewrites the snapshot
    (ADR-007 D4), so it replaces the parked one (#194). A crash between the two steps loses only the older copy. A live
    dir with no manifest is what an interrupted build leaves (the manifest is the build's last write), so it never
    replaces a parked dir and is dropped instead. Source clones are content-addressed files: ``source_identity.gc_cas``
    retires them, and still drops one whose copy is already parked.
    """
    if dest.exists():
        if not (src / "manifest.json").is_file():
            shutil.rmtree(src)
            return
        shutil.rmtree(dest) if dest.is_dir() else dest.unlink()
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


def _cas_bullpen(project: str) -> Path:
    return bullpen_dir(project) / "cas"


def _live_source_digests(project: str) -> set[str] | None:
    """Every clone digest a surviving entry needs, or None when a manifest can't be read.

    One way an entry refers to a clone: ``manifest.provenance.digest``, the bytes an import copied in (ADR-011 D1).
    The clone is the only record of the file as imported and a re-import is the only way back, so the retention
    closure is simply which source versions survive — which is the DAG, and the reason ADR-011 D6 could delete
    ``manifest.sources``, the map a build used to fold up from everything it read.

    An unreadable manifest means the sweep would run on partial information, and a clone wrongly retired is the only
    frozen copy of somebody's bytes, so callers skip the sweep.
    """
    from tallyman_core.manifest import read_manifest
    from tallyman_core.paths import entry_dir

    live: set[str] = set()
    for h in read_tallyman_state(project)["entry_hashes"]:
        try:
            manifest = read_manifest(entry_dir(project, h))
        except Exception:
            return None
        if manifest.provenance is not None:
            live.add(manifest.provenance.digest)
    return live


def restore_from_bullpen(project: str) -> int:
    """Copy recorded-but-missing artifacts back from the bullpen.

    The inverse of the prunes, for a reset that walks forward: every entry dir the
    restored pointer file names that is absent from the live tree comes back by
    *copy*, and so does every source clone (``data/.cas/``) that a restored entry
    refers to, so the bullpen keeps its set and the back/forward rehearsal loop
    can repeat. Only ``reset_to`` calls this — live operations never see the
    bullpen.
    """
    bp = bullpen_dir(project)
    restored = 0
    ed = entries_dir(project)
    for h in _read_jsonl(_entries_file(project), "hash") or []:
        live, parked = ed / h, bp / ENTRIES_DIRNAME / h
        if not live.exists() and parked.is_dir():
            shutil.copytree(parked, live)
            restored += 1
    parked_clones = _cas_bullpen(project)
    live_digests = _live_source_digests(project)
    if live_digests and parked_clones.is_dir():
        cas = data_dir(project) / ".cas"
        for parked in parked_clones.iterdir():
            if parked.is_file() and parked.stem in live_digests and not (cas / parked.name).exists():
                cas.mkdir(parents=True, exist_ok=True)
                shutil.copy2(parked, cas / parked.name)
                restored += 1
    return restored


# ---------------------------------------------------------------------------
# the catalog git repo: init, checkpoint (one commit per op), reset
# ---------------------------------------------------------------------------


# The project locks this thread holds, and how deep. flock takes a lock per open file description, so a nested
# acquire on a fresh descriptor would block forever behind the outer one: re-entrancy has to be counted per thread.
_held = threading.local()


@contextlib.contextmanager
def project_lock(project: str):
    """One write at a time per project (ADR-007 D11): a build, a materialization, a promote, a recalc, a checkpoint.

    A cross-process file lock, so it holds between the two processes of a normal tallyman (the MCP server and the
    companion, which both build), and re-entrant within a thread, since a promote builds and then checkpoints and a
    build materializes. Another thread or process waits. It is blocking with no timeout (ADR-007 D11, #186), and it
    cannot be held across an ``await``: the companion moves work between threads with ``run_in_threadpool``.
    """
    depth = getattr(_held, "depth", None)
    if depth is None:
        depth = _held.depth = {}
    if depth.get(project, 0) > 0:
        depth[project] += 1
        try:
            yield
        finally:
            depth[project] -= 1
        return
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(cd / ".checkpoint.lock"), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        depth[project] = 1
        try:
            yield
        finally:
            depth.pop(project, None)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


_project_lock = project_lock  # the name the lock had before it became public


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
    with project_lock(project):
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
    evicted entry dirs retire to the bullpen, recorded-but-missing ones come back
    from it, and so do the source clones a restored entry refers to. Clones no
    surviving entry refers to are moved to the bullpen too, never deleted.
    ``compute_cache/`` is left alone (ADR-007 D14). Finally re-validate the
    tracked recipe set against the pointers, so a step whose two views disagree
    fails loudly rather than returning a masked divergence (#52)."""
    cd = catalog_dir(project)
    tag = f"step-{ref:03d}" if isinstance(ref, int) else str(ref)
    with project_lock(project):
        commit = _resolve_tag(project, tag)
        rc, _, err = run_git(["reset", "--hard", commit], cwd=cd)
        if rc != 0:
            raise RuntimeError(f"catalog reset to {tag!r} failed: {err}")
        prune_entries(project)
        restore_from_bullpen(project)
        _retire_cas_clones(project)
        catalog.assert_catalog_consistent(project, set(read_tallyman_state(project)["entry_hashes"]))
    # Clear the in-process result-plan memo: a reset changes which entries exist, and a memoised plan for a retired
    # entry would outlive it. Entries are content-addressed, so the next read rebuilds an identical plan. Lazy import
    # avoids a core->xorq import cycle.
    from tallyman_xorq.result_cache import cached_result_expr  # noqa: PLC0415

    cached_result_expr.cache_clear()


def _retire_cas_clones(project: str) -> int:
    """Move the source clones (``data/.cas``) no surviving entry references into the bullpen (ADR-007 D14).

    ``.cas`` lives under ``data/`` — outside the catalog git repo — so the ``git reset`` above cannot roll it back.
    A clone is data, the only frozen copy of the bytes an entry was built from once the live file is edited, so
    nothing deletes one: a reset forward brings it back (``restore_from_bullpen``). Liveness is every surviving
    source version's ``manifest.provenance.digest``. Conservative and best-effort: if any entry's manifest can't be
    read we skip the sweep, and a failure here never aborts the reset.
    """
    from tallyman_xorq import source_identity  # lazy: avoid a core->xorq import cycle

    live_digests = _live_source_digests(project)
    if live_digests is None:
        return 0
    return source_identity.gc_cas(project, live_digests, bullpen=_cas_bullpen(project))


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
