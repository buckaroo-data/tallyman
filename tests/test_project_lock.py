"""One write at a time per project.

Red tests for ``plans/ADR-007-tallyman-owned-materialization.md`` D11. Every write takes the project's existing lock
(``.checkpoint.lock``): a build, a materialization, a promote, a recalc and the checkpoint that takes it today. FastMCP
runs tool calls on a thread pool, so two parallel tool calls used to build at once, and "two builds of one entry can
end with the failing one deleting the winner's directory" (``build.py:447-468`` and ``618-627``). The lock has to be
re-entrant per thread, because a promote builds and then checkpoints, and it has to be a real lock between threads and
between processes.

Threads here are always daemon threads joined with a timeout, so a regression shows up as a failed assertion and not as
a CI job that hangs.
"""

from __future__ import annotations

import threading

from tallyman_core import entry_dir, read_manifest
from tallyman_xorq import build_and_persist


def _agg_code(project: str, src: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r}, project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _cheap_code(project: str, src: str) -> str:
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({src!r}, project={project!r})
expr = t.filter(t.price > 0)
"""


def _run_in_threads(targets, join_timeout: float = 120.0) -> list[threading.Thread]:
    threads = [threading.Thread(target=target, daemon=True) for target in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=join_timeout)
    return threads


# ---------------------------------------------------------------------------
# the lock itself
# ---------------------------------------------------------------------------


def test_nested_acquire_in_one_thread_does_not_block(project):
    """ADR-007 D11 (one write at a time per project): a promote builds and then checkpoints, and a build
    materializes, so a thread that holds the lock takes it again. ``_project_lock`` takes ``flock`` on a fresh file
    descriptor, so today the nested acquire waits for the outer one, which is the same thread, forever."""
    from tallyman_core import catalog_state

    entered = threading.Event()

    def nested() -> None:
        with catalog_state._project_lock(project):
            with catalog_state._project_lock(project):
                entered.set()

    (worker,) = _run_in_threads([nested], join_timeout=3)

    assert not worker.is_alive(), "a nested acquire of the project lock in one thread blocked"
    assert entered.is_set()


def test_a_second_thread_waits_for_the_holder_and_proceeds_after_release(project):
    """ADR-007 D11: making the lock re-entrant must stay per thread. A thread that does not hold it waits, and gets it
    once the holder leaves. (This one already holds on today's code; it guards the re-entrant change.)"""
    from tallyman_core import catalog_state

    holding, release, second_in = threading.Event(), threading.Event(), threading.Event()

    def holder() -> None:
        with catalog_state._project_lock(project):
            holding.set()
            release.wait(timeout=10)

    def second() -> None:
        holding.wait(timeout=10)
        with catalog_state._project_lock(project):
            second_in.set()

    threads = [threading.Thread(target=t, daemon=True) for t in (holder, second)]
    for thread in threads:
        thread.start()
    assert holding.wait(timeout=5)
    assert not second_in.wait(timeout=0.5), "a second thread entered while the first still held the project lock"

    release.set()

    assert second_in.wait(timeout=5), "the second thread never got the lock after the holder released it"
    for thread in threads:
        thread.join(timeout=5)


def test_project_lock_is_public_and_the_private_name_is_an_alias():
    """ADR-007 D11: ``build`` and ``materialize`` live outside ``catalog_state``, so the lock gets a public name.
    ``_project_lock`` stays for the callers that exist."""
    from tallyman_core import catalog_state

    assert catalog_state.project_lock is catalog_state._project_lock


# ---------------------------------------------------------------------------
# what takes it
# ---------------------------------------------------------------------------


def test_two_builds_in_one_project_never_run_at_once(project, orders_src, monkeypatch):
    """ADR-007 D11: a build is a write, so two builds of two different entries in one project queue. ``build_expr`` is
    the first heavy step of a build; it is wrapped to count how many threads are inside it. Each waits (on an event,
    not a sleep) for the other to be inside too. Unserialized, they meet at once and the peak is 2. Serialized, the
    second cannot enter while the first is in, the wait times out, and the peak is 1."""
    import xorq.ibis_yaml.compiler as compiler

    real_build_expr = compiler.build_expr
    guard = threading.Lock()
    state = {"active": 0, "peak": 0}
    both_inside = threading.Event()

    def instrumented(*args, **kwargs):
        with guard:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            if state["active"] >= 2:
                both_inside.set()
        try:
            both_inside.wait(timeout=1.0)
            return real_build_expr(*args, **kwargs)
        finally:
            with guard:
                state["active"] -= 1

    monkeypatch.setattr(compiler, "build_expr", instrumented)
    start = threading.Barrier(2)
    results: list = []
    errors: list[BaseException] = []

    def builder(code: str):
        def run() -> None:
            try:
                start.wait(timeout=10)
                results.append(build_and_persist(project, code))
            except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
                errors.append(exc)

        return run

    threads = _run_in_threads([builder(_agg_code(project, orders_src)), builder(_cheap_code(project, orders_src))])

    assert not any(t.is_alive() for t in threads), "a build never finished"
    assert state["peak"] == 1, f"{state['peak']} builds of one project ran at the same time"
    assert errors == []
    assert len(results) == 2 and results[0].content_hash != results[1].content_hash


def test_a_failing_concurrent_build_of_one_entry_cannot_delete_the_winners_entry(project, orders_src, monkeypatch):
    """ADR-007 D11 (the audit's finding): two builds of one entry, and one of them fails after laying down the entry
    directory. Its cleanup removes the directory, which the other build is still using, so the winner then fails too.

    The failure is injected, so that this is not a race that may or may not happen: whichever thread reaches
    ``load_expr`` first is the primary and waits (on an event, with a timeout) for a second thread to arrive, and a
    second thread that does arrive raises, as a writer that lost a race for xorq's shared temp file does. Serialized,
    the second thread never arrives (it waits for the lock, then finds a complete entry and returns), so nothing is
    injected and both builds return the one entry."""
    import xorq.ibis_yaml.compiler as compiler

    real_load_expr = compiler.load_expr
    guard = threading.Lock()
    state: dict = {"primary": None}
    second_arrived = threading.Event()

    def instrumented(*args, **kwargs):
        me = threading.get_ident()
        with guard:
            first_call = state["primary"] is None
            if first_call:
                state["primary"] = me
            is_primary = state["primary"] == me
        if not is_primary:
            second_arrived.set()
            raise RuntimeError("simulated failure of a concurrent writer of the same entry")
        if first_call:
            second_arrived.wait(timeout=1.5)
        return real_load_expr(*args, **kwargs)

    # The source's clone and snapshot are written once by the import (ADR-011 D1), before either builder starts, so
    # nothing but the injection below distinguishes the two threads.
    monkeypatch.setattr(compiler, "load_expr", instrumented)
    start = threading.Barrier(2)
    results: list = []
    errors: list[BaseException] = []

    def builder() -> None:
        try:
            start.wait(timeout=10)
            results.append(build_and_persist(project, _agg_code(project, orders_src)))
        except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
            errors.append(exc)

    threads = _run_in_threads([builder, builder])

    assert not any(t.is_alive() for t in threads), "a build never finished"
    assert errors == [], f"a build failed: {[f'{type(e).__name__}: {e}' for e in errors]}"
    assert len(results) == 2
    assert results[0].content_hash == results[1].content_hash
    entry = entry_dir(project, results[0].content_hash)
    for name in ("manifest.json", "schema.json", "expr.py", "xorq_build/expr.yaml"):
        assert (entry / name).exists(), f"the entry directory lost {name}"
    assert read_manifest(entry).content_hash == results[0].content_hash


def _materialize(project: str, content_hash: str):
    from tallyman_xorq.materialize import materialize

    return materialize(project, content_hash)


def _snapshot_path(project: str, content_hash: str):
    from tallyman_xorq.materialize import snapshot_path

    return snapshot_path(project, content_hash)


def test_concurrent_materializations_of_one_entry_all_succeed(project, orders_src):
    """ADR-007 D11 and D4 (one writer, used by the build and by every heal): four threads materialize the same entry.
    The writer takes a unique temp name in the destination directory and renames it into place, under the project
    lock, so each call succeeds and the file is whole afterwards. xorq's writer used a fixed ``<key>.parquet.tmp``:
    four concurrent cold writers of one key gave three ``FileNotFoundError`` and a corrupt file that then counted as a
    permanent cache hit."""
    import pyarrow.parquet as pq

    from tallyman_xorq.result_cache import snapshot_file_digest

    h = build_and_persist(project, _agg_code(project, orders_src)).content_hash
    manifest = read_manifest(entry_dir(project, h))
    start = threading.Barrier(4)
    digests: list[str] = []
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            start.wait(timeout=10)
            digests.append(_materialize(project, h).digest)
        except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
            errors.append(exc)

    threads = _run_in_threads([writer] * 4)

    assert not any(t.is_alive() for t in threads), "a materialization never finished"
    assert errors == [], f"a materialization failed: {[f'{type(e).__name__}: {e}' for e in errors]}"
    assert digests == [manifest.result_digest] * 4
    path = _snapshot_path(project, h)
    assert pq.read_table(path).num_rows == manifest.row_count
    assert snapshot_file_digest(path) == manifest.result_digest
