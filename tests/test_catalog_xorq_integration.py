"""catalog ↔ xorq coexistence across multi-entry adds and reset round-trips.

Companion to ``test_reset_to_revision.py``. That suite drives synthetic
``entries/aaaa`` dirs and never runs xorq; its one real-xorq integration test
(``test_xorq_catalog_add_registers_after_genesis``) adds exactly *one* entry, so
it never crosses the checkpoint that commits the bookkeeping files into the
xorq catalog repo. That blind spot is how #48 shipped through all of V0.

These tests reproduce the real demo flow (genesis → catalog_create →
catalog_create → reset_to) through the production MCP surface, so the
dispatch-boundary checkpoint wrapper fires exactly as it does in production —
the actual #48 trigger. Two distinct bugs were surfaced here:

1. **#48** — the checkpoint commits tallyman's ``aliases.json``/
   ``alias_history.json`` files into the xorq catalog repo, ``assert_consistency``
   then fails, and every ``xorq catalog add`` after the first silently no-ops
   (Group A, D). The fix carries that state as ``catalog.yaml`` keys instead, so
   no extra files are tracked.
2. **reset ↔ xorq-catalog divergence** — a reset round-trips the tallyman
   ``entries/<hash>/`` dir through the bullpen, but the durable xorq ``.zip``
   recipe is reconciled by ``git reset`` and never regained. The two views of
   "what entries exist" drift apart and ``reset_to`` never re-validates the
   xorq catalog (Group B).

Groups A–E originally landed as the failing-test half of a TDD split (the #48
fix was a separate PR, now merged); they pass today and stand as the regression
guard. Group F characterizes a remaining follow-up (#56): the entry/alias
registration outcome is computed but not yet surfaced to the MCP caller —
``test_set_alias_swallows_failed_xorq_registration`` pins the current swallow
and flips when that signal lands. See ``plans/catalog-xorq-integration-tests.md``.

Like the reset suite, these shell out to git directly to assert on the catalog
repo — the sanctioned exception to the no-bare-subprocess-git guard.
"""

from __future__ import annotations

import subprocess

import pytest

from tallyman_core import aliases as al
from tallyman_core import catalog_state as cs
from tallyman_core import paths
from tallyman_core import xorq_catalog as xc
from tallyman_xorq import build_and_persist

# Distinct aggregations → distinct exprs → distinct xorq build hashes, so each
# entry is a genuinely separate catalog add (not a content-hash re-run no-op).
_AGGS = [
    "total=t.price.sum(), n=t.count()",
    "avg=t.price.mean()",
    "mx=t.price.max()",
    "mn=t.price.min()",
]


def _agg_code(parquet_path, agg: str) -> str:
    return (
        "import xorq.api as xo\n"
        f"t = xo.deferred_read_parquet({str(parquet_path)!r})\n"
        f"expr = t.group_by('region').aggregate({agg})\n"
    )


def _git(project: str, *args: str) -> str:
    cd = paths.catalog_dir(project)
    return subprocess.run(["git", "-C", str(cd), *args], capture_output=True, text=True).stdout.strip()


def _tracked_entry_zips(project: str) -> set[str]:
    """Bare hashes of the git-tracked ``entries/<hash>.zip`` recipes.

    The durable, content-addressed xorq artifact set — what survives a clone or
    a ``git reset``, as opposed to the untracked ``entries/<hash>/`` working
    dirs the tallyman bullpen reconciles.
    """
    out = _git(project, "ls-files", "entries")
    return {line[len("entries/") : -len(".zip")] for line in out.split() if line.endswith(".zip")}


def _tracked_alias_links(project: str) -> set[str]:
    """Git-tracked alias symlinks under ``aliases/`` in the xorq catalog repo.

    The durable record of an alias: what survives a clone or a ``git reset``.
    tallyman's own bookkeeping lives in ``catalog.yaml`` (#48) and is *not*
    this — these are xorq's symlinks, written only by a successful
    ``xorq catalog add-alias``."""
    return set(_git(project, "ls-files", "aliases").split())


def _open_xorq_catalog(project: str):
    """Open the project's xorq catalog with consistency checking on.

    ``from_kwargs`` defaults ``check_consistency=True``, so this runs
    ``assert_consistency`` — the exact check #48 breaks. Returns the catalog or
    re-raises, letting a test assert it opens.
    """
    from xorq.catalog.catalog import Catalog

    return Catalog.from_kwargs(path=paths.catalog_dir(project), init=False)


def _make_entries(project: str, orders_parquet, n: int) -> list[str]:
    """Drive ``catalog_create`` n times through the real MCP tool (so the
    checkpoint wrapper fires), returning the content hashes in order."""
    from tallyman_mcp.server import catalog_create

    cs.genesis(project)  # branch-pinned, xorq-keyed catalog.yaml baseline
    hashes: list[str] = []
    for i in range(n):
        res = catalog_create(name=f"agg{i}", code=_agg_code(orders_parquet, _AGGS[i]))
        assert "error" not in res, f"catalog_create #{i} errored: {res}"
        hashes.append(res["hash"])
    return hashes


# ---------------------------------------------------------------------------
# Group A — multi-entry catalog add (#48 core)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_every_entry_registers_in_xorq_catalog(project, orders_parquet):
    """Every entry's recipe must land in the xorq catalog, not just the first.

    Pre-fix only entry 1's ``.zip`` was tracked: entry 1's checkpoint committed
    the bookkeeping files, ``assert_consistency`` then failed, and entries 2+
    add silently no-op'd. Guards against a regression."""
    hashes = _make_entries(project, orders_parquet, 3)
    tracked = _tracked_entry_zips(project)
    missing = [h for h in hashes if h not in tracked]
    assert not missing, f"entries never registered in xorq catalog: {missing} (tracked: {tracked})"


@pytest.mark.integration
def test_catalog_openable_after_checkpoint(project, orders_parquet):
    """The xorq catalog must stay openable after a checkpoint. Pins the
    ``assert_consistency`` root cause: genesis + one ``catalog_create`` (which
    checkpoints) left it unopenable pre-fix."""
    _make_entries(project, orders_parquet, 1)
    try:
        _open_xorq_catalog(project)
    except AssertionError as exc:
        pytest.fail(f"xorq catalog unopenable after checkpoint (#48 assert_consistency): {exc!r}")


@pytest.mark.integration
def test_bookkeeping_files_not_tracked_in_xorq_repo(project, orders_parquet):
    """The precise #48 invariant: tallyman's alias bookkeeping must not be
    git-tracked as separate files inside the xorq catalog repo. The checkpoint's
    ``_TRACKED`` set committed both pre-fix, which is what broke
    ``assert_consistency``. The fix (carry alias state as ``catalog.yaml`` keys
    instead of ``aliases.json``/``alias_history.json`` files) makes this pass."""
    _make_entries(project, orders_parquet, 1)
    tracked = set(_git(project, "ls-files").split())
    offenders = tracked & {"aliases.json", "alias_history.json"}
    assert not offenders, f"tallyman bookkeeping tracked in xorq catalog repo: {sorted(offenders)}"


# ---------------------------------------------------------------------------
# Group B — reset round-trips keep the xorq catalog consistent (second bug)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_reset_keeps_xorq_catalog_consistent(project, orders_parquet):
    """A reset must leave the xorq catalog openable. Pre-fix the reset target
    (step-1) was itself checkpointed with the bookkeeping files tracked, so
    ``git reset --hard`` restored an inconsistent tree and ``reset_to`` never
    re-validated it."""
    _make_entries(project, orders_parquet, 2)
    cs.reset_to(project, 1)
    try:
        _open_xorq_catalog(project)
    except AssertionError as exc:
        pytest.fail(f"xorq catalog unopenable after reset_to(step-1): {exc!r}")


@pytest.mark.integration
def test_xorq_entry_set_matches_tallyman_pointers(project, orders_parquet):
    """The durable xorq recipe set must match tallyman's ``entry_hashes``
    pointers. Pre-fix they diverged — 1 tracked ``.zip`` against 2 pointers —
    the visible signature of "git-backed, recompute from history" being false
    for every entry after the first."""
    _make_entries(project, orders_parquet, 2)
    pointers = set(cs.read_tallyman_state(project)["entry_hashes"])
    tracked = _tracked_entry_zips(project)
    assert tracked == pointers, f"xorq tracked zips {tracked} diverge from tallyman pointers {pointers}"


@pytest.mark.integration
def test_back_then_forward_restores_xorq_recipe(project, orders_parquet):
    """Reset back then forward must restore entry 2's *durable* xorq recipe,
    not merely its bullpen dir copy. Pre-fix the ``.zip`` never existed (its add
    no-op'd), so a forward reset could not regain it — the divergence pinned end
    to end."""
    hashes = _make_entries(project, orders_parquet, 2)
    cs.reset_to(project, 1)  # back to entry-1-only
    cs.reset_to(project, 2)  # forward to both
    entry2 = hashes[1]
    assert entry2 in _tracked_entry_zips(project), (
        f"entry 2 recipe {entry2} not durable in xorq catalog after back→forward reset"
    )


# ---------------------------------------------------------------------------
# Group D — silent-swallow surfacing (#48 secondary)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_failed_registration_is_observable(project, orders_parquet):
    """A failed xorq catalog add must be observable, not reported as success.

    Pre-fix this failure happened on its own (entry 1's checkpoint committed the
    bookkeeping files, leaving the catalog inconsistent so the next add fails) —
    that was bug #48 itself, and ``add_entry`` swallowed the failure with no
    signal. The fix removes that spontaneous failure, so to test the *signal* we
    inject the same kind of inconsistency a stray tracked file gives, then assert
    ``build_and_persist`` reports ``catalog_registered is False`` rather than
    claiming success."""
    _make_entries(project, orders_parquet, 1)

    # A tracked file outside xorq's managed set is exactly the shape #48 created:
    # assert_consistency then rejects the repo and the next add fails.
    cd = paths.catalog_dir(project)
    (cd / "stray.txt").write_text("not xorq-managed\n")
    _git(project, "add", "stray.txt")
    subprocess.run(
        ["git", "-C", str(cd), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "stray"],
        capture_output=True,
        text=True,
    )

    result = build_and_persist(project, _agg_code(orders_parquet, _AGGS[1]))
    assert result.catalog_registered is False, (
        "failed xorq catalog add must be observable: build_and_persist should expose "
        f"catalog_registered=False, got {result.catalog_registered!r}"
    )


# ---------------------------------------------------------------------------
# Group E — reset rolls back alias state (regression guard shipping WITH the fix)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_reset_rolls_back_alias_state(project, orders_parquet):
    """A reset must roll alias bookkeeping back.

    The #48 fix carries alias state as ``catalog.yaml`` keys, so ``reset_to``'s
    ``git reset --hard`` rolls it back with the rest of the file and
    ``load_aliases`` (which reads catalog.yaml) reflects the reset target — no
    separate reconcile. This guards that round-trip."""
    _make_entries(project, orders_parquet, 2)  # creates aliases agg0, agg1
    assert set(al.load_aliases(project)) == {"agg0", "agg1"}
    cs.reset_to(project, 1)  # back to the one-entry step
    assert set(al.load_aliases(project)) == {"agg0"}, (
        f"reset_to(step-1) did not roll alias state back: {al.load_aliases(project)}"
    )


# ---------------------------------------------------------------------------
# Group F — alias registration durability (the #48 follow-up)
#
# build_and_persist now surfaces entry-add durability via catalog_registered.
# The *alias* add (`xorq catalog add-alias`, driven by set_alias) is a separate
# call with its own failure modes, and its outcome is still swallowed. These
# exercise that boundary: the xorq-side contract, the happy path the fix
# restores, and the unsurfaced-failure gap.
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_add_alias_returns_false_for_unregistered_entry(project, orders_parquet):
    """xorq refuses to alias an entry it doesn't know.

    ``add-alias`` constructs ``CatalogEntry(name, require_exists=True)``, whose
    ``__attrs_post_init__`` asserts the entry exists (xorq catalog.py:1214). For
    an unregistered hash the CLI exits non-zero, so ``_xcat.add_alias`` returns
    ``False``. This is exactly the signal ``set_alias`` discards — and the shape
    a failed ``add_entry`` leaves behind (the alias of a recipe that never
    reached git)."""
    cs.genesis(project)
    assert xc.add_alias(project, "deadbeefdeadbeef", "ghost") is False


@pytest.mark.integration
def test_registered_entry_gets_durable_alias_symlink(project, orders_parquet):
    """The happy path the #48 fix restores end to end.

    A real ``catalog_create`` registers the entry, so the following
    ``add-alias`` succeeds and xorq writes a git-tracked ``aliases/<name>``
    symlink — the durable record, distinct from tallyman's own bookkeeping in
    ``catalog.yaml``."""
    _make_entries(project, orders_parquet, 1)  # catalog_create('agg0') -> set_alias
    links = _tracked_alias_links(project)
    assert any("agg0" in link for link in links), f"alias symlink not durable in xorq repo: {links}"


@pytest.mark.integration
def test_set_alias_swallows_failed_xorq_registration(project, orders_parquet):
    """``set_alias`` reports success even when the alias never reaches xorq.

    Aliasing a hash xorq doesn't know makes ``_xcat.add_alias`` return ``False``,
    but ``set_alias`` discards that bool: it writes its ``catalog.yaml``
    bookkeeping and returns version info regardless. Unlike
    ``build_and_persist``'s ``catalog_registered``, nothing surfaces the failed
    alias registration. This pins that gap (characterization — flip the final
    assertion when the signal lands)."""
    cs.genesis(project)
    ghost = "deadbeefdeadbeef"

    info = al.set_alias(project, "ghost", ghost, expect_exists=False)

    # set_alias claims success and writes its own bookkeeping...
    assert info == {"name": "ghost", "hash": ghost, "version": 1}
    assert al.load_aliases(project) == {"ghost": ghost}
    # ...but the alias never reached the xorq catalog repo, and no signal said so.
    assert not _tracked_alias_links(project), (
        "alias reported as set but not durable in the xorq catalog — set_alias swallowed the failed registration"
    )
