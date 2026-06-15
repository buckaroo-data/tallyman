"""Pydata-owned state in the xorq catalog's catalog.yaml + the reset engine.

catalog.yaml is the versioned state carrier (see plans/adr-reset-to-revision.md).
xorq owns ``entries`` and ``aliases`` in that file; tallyman owns:

    post_processing: [{name, source, disabled}]
    stats:           [{name, source, disabled}]
    charts:          [{content_hash, spec}]
    display_configs: [{content_hash, config}]
    notebook:        {cells: [{cell_id, alias, markdown}]}
    entry_hashes:    [content_hash, ...]      # pointer to untracked entries/<hash>/
    result_cache:    [relpath, ...]           # pointer to untracked result cache
    compute_cache:   [relpath, ...]           # pointer to untracked compute cache
    alias_map:       {alias: latest_hash}     # tallyman's alias bookkeeping
    alias_history:   {alias: [hash, ...]}     # tallyman's alias bookkeeping

The catalog git repo tracks only ``catalog.yaml``. tallyman's alias bookkeeping
lives *in* it, as the ``alias_map``/``alias_history`` keys above (owned by
``aliases.py``) — committing separate ``aliases.json``/``alias_history.json``
files inside the repo made xorq's ``assert_consistency`` reject it and every
``xorq catalog add`` after the first silently no-op (#48). Keeping the state as
catalog.yaml keys means there are no extra tracked files, and ``reset_to``'s
``git reset`` rolls alias state back with the rest of the file (no separate
reconcile). The heavy artifacts (entries/, the caches) are content-addressed,
additive, and
untracked — ``git reset`` can't roll them back, so ``reset_to`` reconciles them
to the recorded pointer lists: evictions retire to the bullpen (not deleted),
and anything the restored step records that is missing comes back from the
bullpen by copy. Live operations never read the bullpen, so a re-added
expression still computes cold. The derived files (.py scripts, chart specs,
the notebook) live outside the repo, so they are reconstructed by
``materialize``.

A checkpoint captures the live on-disk state into catalog.yaml and commits once,
through the fork-safe git primitive (git_util) under a per-project lock. This
keeps the four invariants #33 kept re-breaking structural rather than reviewed.
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

import yaml

from tallyman_core import charts, notebook
from tallyman_core import display_configs as dc
from tallyman_core import post_processing as pp
from tallyman_core import summary_stats as ss
from tallyman_core.git_util import run_git
from tallyman_core.paths import (
    ENTRIES_DIRNAME,
    bullpen_dir,
    catalog_dir,
    compute_cache_dir,
    entries_dir,
    notebooks_dir,
    post_processing_dir,
    result_cache_dir,
    stats_dir,
)

log = logging.getLogger(__name__)

# git-level identity flags, so commits work without a global git config.
_GIT_ID = ["-c", "user.email=tallyman@local", "-c", "user.name=tallyman"]
# Only catalog.yaml is git-tracked in the catalog repo. Alias bookkeeping is
# carried as catalog.yaml keys, not separate files — tracking those alongside
# xorq's managed set made its assert_consistency reject the repo (#48).
_TRACKED = ("catalog.yaml",)
_STEP_RE = re.compile(r"^step-(\d+)$")
# Refs and labels are operator input that ends up as git arguments. A plain
# tag-shaped name only: no leading dash (option injection), no revision
# navigation (~ ^ : @), nothing git would resolve beyond a tag lookup.
_TAG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


# ---------------------------------------------------------------------------
# catalog.yaml read / write (read-modify-write so xorq's keys survive)
# ---------------------------------------------------------------------------


def _catalog_yaml(project: str) -> Path:
    return catalog_dir(project) / "catalog.yaml"


def _read_raw(project: str) -> dict:
    p = _catalog_yaml(project)
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text()) or {}


def read_tallyman_state(project: str) -> dict:
    """The tallyman-owned keys, each defaulted so callers never KeyError."""
    raw = _read_raw(project)
    return {
        "post_processing": raw.get("post_processing", []),
        "stats": raw.get("stats", []),
        "charts": raw.get("charts", []),
        "display_configs": raw.get("display_configs", []),
        "notebook": raw.get("notebook", {"cells": []}),
        "entry_hashes": raw.get("entry_hashes", []),
        "result_cache": raw.get("result_cache", []),
        "compute_cache": raw.get("compute_cache", []),
        "alias_map": raw.get("alias_map", {}),
        "alias_history": raw.get("alias_history", {}),
    }


def write_tallyman_state(project: str, **keys) -> None:
    """Merge tallyman keys into catalog.yaml atomically, preserving xorq's keys.

    xorq's keys are also *seeded* when absent: its ``CatalogYaml.contents``
    indexes ``entries``/``aliases`` directly (no defaulting for dict-shaped
    files), so a catalog.yaml born from genesis/capture without them makes
    every best-effort ``xorq catalog add`` fail — silently. xorq round-trips
    unknown keys, so the two empty lists are the whole coexistence contract.
    """
    p = _catalog_yaml(project)
    p.parent.mkdir(parents=True, exist_ok=True)
    raw = _read_raw(project)
    raw.setdefault("entries", [])
    raw.setdefault("aliases", [])
    for k, v in keys.items():
        if v is not None:
            raw[k] = v
    tmp = p.with_name(p.name + ".tmp")
    # safe_dump, to mirror _read_raw's safe_load: the full Dumper would write a
    # stray non-plain value (a Path in a chart spec) as a !!python/object tag,
    # after which every read fails until the file is hand-repaired. Raising
    # here — before the tmp replace — keeps the previous file readable.
    tmp.write_text(yaml.safe_dump(raw, default_flow_style=False, sort_keys=False))
    tmp.replace(p)


# ---------------------------------------------------------------------------
# capture: live on-disk state -> catalog.yaml keys
# ---------------------------------------------------------------------------


def _scan_scripts(listing: list[dict]) -> list[dict]:
    return [{"name": e["name"], "source": e["source"], "disabled": e["disabled"]} for e in listing]


def _list_cache_files(root: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def capture_tallyman_state(project: str) -> dict:
    """Read the live authored state + artifact listings into catalog.yaml."""
    ed = entries_dir(project)
    state = {
        "post_processing": _scan_scripts(pp.list_post_processings(project)),
        "stats": _scan_scripts(ss.list_stats(project)),
        "charts": [{"content_hash": h, "spec": charts.get_chart(project, h)} for h in charts.list_charts(project)],
        "display_configs": [
            {"content_hash": h, "config": dc.get_display_config(project, h)} for h in dc.list_display_configs(project)
        ],
        "notebook": notebook.load(project),
        "entry_hashes": sorted(c.name for c in ed.iterdir() if c.is_dir()) if ed.exists() else [],
        "result_cache": _list_cache_files(result_cache_dir(project)),
        "compute_cache": _list_cache_files(compute_cache_dir(project)),
    }
    # alias_map/alias_history are written live by aliases.py and already in
    # catalog.yaml; write_tallyman_state preserves keys it isn't given, so
    # capture leaves them untouched rather than re-snapshotting.
    write_tallyman_state(project, **state)
    return state


# ---------------------------------------------------------------------------
# materialize: catalog.yaml keys -> on-disk derived files (non-destructive)
# ---------------------------------------------------------------------------


def _materialize_scripts(target: Path, entries: list[dict]) -> None:
    if target.exists():
        shutil.rmtree(target)  # removes _disabled/ with it
    target.mkdir(parents=True, exist_ok=True)
    for e in entries or []:
        dest = (target / "_disabled") if e.get("disabled") else target
        dest.mkdir(parents=True, exist_ok=True)
        src = e.get("source", "")
        (dest / f"{e['name']}.py").write_text(src if src.endswith("\n") else src + "\n")


def _materialize_charts(project: str, entries: list[dict]) -> None:
    recorded = {c["content_hash"]: c["spec"] for c in entries or []}
    for h in list(charts.list_charts(project)):
        if h not in recorded:
            charts.remove_chart(project, h)
    for h, spec in recorded.items():
        charts.set_chart(project, h, spec)


def _materialize_display_configs(project: str, entries: list[dict]) -> None:
    recorded = {c["content_hash"]: c["config"] for c in entries or []}
    for h in list(dc.list_display_configs(project)):
        if h not in recorded:
            dc.remove_display_config(project, h)
    for h, config in recorded.items():
        if config is not None:
            dc.set_display_config(project, h, config)


def _materialize_notebook(project: str, nb: dict) -> None:
    nbfile = notebooks_dir(project) / "default.json"
    cells = (nb or {}).get("cells") or []
    if cells:
        notebooks_dir(project).mkdir(parents=True, exist_ok=True)
        nbfile.write_text(json.dumps({"cells": cells}, indent=2))
    else:
        nbfile.unlink(missing_ok=True)


def materialize(project: str) -> None:
    """Reconstruct on-disk derived files from catalog.yaml.

    A section is rebuilt only when its key is *present*: an absent key means
    the project never recorded that state (e.g. predates these keys), so its
    on-disk files are left alone rather than wiped. A present-but-empty list is
    a deliberate "none at this step" and clears the section.
    """
    raw = _read_raw(project)
    if "post_processing" in raw:
        _materialize_scripts(post_processing_dir(project), raw["post_processing"])
    if "stats" in raw:
        _materialize_scripts(stats_dir(project), raw["stats"])
    if "charts" in raw:
        _materialize_charts(project, raw["charts"])
    if "display_configs" in raw:
        _materialize_display_configs(project, raw["display_configs"])
    if "notebook" in raw:
        _materialize_notebook(project, raw["notebook"] or {"cells": []})
    # alias_map/alias_history need no materialize step: aliases.py reads them
    # straight from catalog.yaml, which git reset has already rolled back.


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
    """Retire entry dirs not named by ``entry_hashes``. No-op when key absent."""
    raw = _read_raw(project)
    if "entry_hashes" not in raw:
        return 0
    valid = set(raw["entry_hashes"] or [])
    ed = entries_dir(project)
    if not ed.exists():
        return 0
    removed = 0
    for child in ed.iterdir():
        if child.is_dir() and child.name not in valid:
            _retire(child, bullpen_dir(project) / ENTRIES_DIRNAME / child.name)
            removed += 1
    return removed


def _prune_cache(project: str, root: Path, key: str) -> int:
    raw = _read_raw(project)
    if key not in raw:
        return 0
    valid = set(raw[key] or [])
    if not root.exists():
        return 0
    removed = 0
    for p in root.rglob("*"):
        if p.is_file() and (rel := str(p.relative_to(root))) not in valid:
            _retire(p, bullpen_dir(project) / key / rel)
            removed += 1
    return removed


def prune_result_cache(project: str) -> int:
    return _prune_cache(project, result_cache_dir(project), "result_cache")


def prune_compute_cache(project: str) -> int:
    return _prune_cache(project, compute_cache_dir(project), "compute_cache")


def restore_from_bullpen(project: str) -> int:
    """Copy recorded-but-missing artifacts back from the bullpen.

    The inverse of the prunes, for a reset that walks forward: anything the
    restored catalog.yaml's pointer lists name that is absent from the live
    tree comes back by *copy*, so the bullpen keeps its set and the
    back/forward rehearsal loop can repeat. Only ``reset_to`` calls this —
    live operations never see the bullpen, which is what keeps a re-added
    expression honest (cold).
    """
    raw = _read_raw(project)
    bp = bullpen_dir(project)
    restored = 0
    ed = entries_dir(project)
    for h in raw.get("entry_hashes") or []:
        live, parked = ed / h, bp / ENTRIES_DIRNAME / h
        if not live.exists() and parked.is_dir():
            shutil.copytree(parked, live)
            restored += 1
    for key, root in (("result_cache", result_cache_dir(project)), ("compute_cache", compute_cache_dir(project))):
        for rel in raw.get(key) or []:
            live, parked = root / rel, bp / key / rel
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

    The branch is pinned to ``main`` (git ≥ 2.28): xorq's catalog layer
    hardcodes it, and inheriting the host's ``init.defaultBranch`` (master on
    stock git) makes every ``xorq catalog add`` fail — silently, because
    registration is best-effort.
    """
    cd = catalog_dir(project)
    cd.mkdir(parents=True, exist_ok=True)
    if (cd / ".git").exists():
        return True
    rc, _, err = run_git(["init", "-q", "-b", "main"], cwd=cd)
    if rc != 0:
        log.warning("catalog git init failed in %s: %s", cd, err)
        return False
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
    """Capture state, commit catalog.yaml once, tag the step.

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
        existing = [f for f in _TRACKED if (cd / f).exists()]
        if existing:
            run_git(["add", *existing], cwd=cd)
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


def _tracked_recipe_hashes(project: str) -> set[str]:
    """Hashes of the git-tracked ``entries/<hash>.zip`` recipes.

    The durable, content-addressed xorq artifact set — what survives a clone or
    a ``git reset`` — as opposed to the untracked ``entries/<hash>/`` build dirs
    the bullpen reconciles. Sourced from ``git ls-files`` so it reflects the
    committed tree, not whatever the working dir happens to hold.
    """
    rc, out, _ = run_git(["ls-files", "entries"], cwd=catalog_dir(project))
    if rc != 0:
        return set()
    return {line[len("entries/") : -len(".zip")] for line in out.split() if line.endswith(".zip")}


def _assert_catalog_consistent(project: str) -> None:
    """The two views of "what entries exist" must agree after a reset.

    ``git reset --hard`` restores the durable xorq recipe set
    (``entries/<hash>.zip``); the bullpen restores the untracked build dirs
    against the ``entry_hashes`` pointers. They are reconciled by different
    mechanisms that never see each other, so a pointer whose recipe was never
    committed (any interrupted or failed best-effort ``xorq catalog add``) comes
    back from the bullpen as a build-dir copy while the recipe it should recompute
    from does not exist. Surface that divergence instead of returning a catalog
    whose two views silently disagree and reporting success (#52).
    """
    pointers = set(read_tallyman_state(project)["entry_hashes"])
    tracked = _tracked_recipe_hashes(project)
    if tracked != pointers:
        missing = sorted(pointers - tracked)  # pointer present, no durable recipe
        orphan = sorted(tracked - pointers)  # durable recipe, no pointer
        raise RuntimeError(
            "catalog inconsistent after reset: the git-tracked xorq recipe set and the "
            f"tallyman entry_hashes pointers disagree (pointers with no durable recipe: "
            f"{missing}; tracked recipes with no pointer: {orphan})"
        )


def reset_to(project: str, ref: int | str) -> None:
    """Restore the catalog to a step/label: git reset --hard, materialize the
    derived files, then reconcile the untracked artifacts to the recorded
    pointers — evictions retire to the bullpen, recorded-but-missing files
    come back from it. Finally re-validate the git-tracked xorq recipe set
    against the tallyman pointers, so a step whose two views disagree fails
    loudly rather than returning a masked divergence (#52)."""
    cd = catalog_dir(project)
    tag = f"step-{ref:03d}" if isinstance(ref, int) else str(ref)
    with _project_lock(project):
        commit = _resolve_tag(project, tag)
        rc, _, err = run_git(["reset", "--hard", commit], cwd=cd)
        if rc != 0:
            raise RuntimeError(f"catalog reset to {tag!r} failed: {err}")
        materialize(project)
        prune_entries(project)
        prune_result_cache(project)
        prune_compute_cache(project)
        restore_from_bullpen(project)
        _assert_catalog_consistent(project)


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
