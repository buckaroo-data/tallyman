"""Executions on the shared backend run one at a time per process (#118).

Tallyman executes an entry's reads on one default backend per process, xorq's DataFusion session
(``xorq.config.default_backend``). Two threads executing on it at once fail with ``RuntimeError: Already borrowed``.
The companion serves page reads from FastAPI's thread pool, and FastMCP runs tool calls on a thread pool, so both
processes execute from many threads. At 1f8cb02, 8 threads making 40 page reads each failed 180 of 320 times calling
the page directly, and about 100 of 320 times through the route. ``project_lock`` does not cover it: it is per project
and the backend is per process (reads under it in two projects failed 6 of 320 times).

The fix is one process-wide re-entrant lock, ``execution_lock``, held around every execution on the shared backend.
The lock order is ``project_lock`` first, then ``execution_lock``. Anything that can heal (``cached_result_expr``,
``ensure_materialized``) takes ``project_lock``, so it runs before the execution lock is taken, and ``project_lock``
refuses a new acquire from a thread that holds ``execution_lock`` rather than risk a deadlock with a thread that holds
the project lock and waits to execute.

Threads here are daemon threads joined with a timeout, so a deadlock fails an assertion instead of hanging CI.
"""

from __future__ import annotations

import ast
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tallyman_cli.fixtures import write_shoe_orders
from tallyman_core import data_dir, ensure_project, entry_dir, read_manifest
from tallyman_core.catalog_state import project_lock
from tallyman_xorq.build import build_and_persist
from tallyman_xorq.source_import import update_and_depend

THREADS, READS = 8, 40
SRC = "orders_src"  # the source alias conftest's ``orders_src`` fixture imports the shoe-orders file under
SRC_ROOT = Path(__file__).resolve().parent.parent / "src"


def _worthy_code(project: str, by: str = "region") -> str:  # an aggregate: materialized, read from its snapshot
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({SRC!r}, project={project!r})
expr = t.group_by({by!r}).aggregate(total=t.price.sum(), n=t.count())
"""


def _cheap_code(project: str) -> str:  # a filter: no snapshot, its loaded build runs on every read
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias({SRC!r}, project={project!r})
expr = t.filter(t.price > 0)
"""


def _second_project(name: str = "p2") -> str:
    """A second project with its own import of the orders and one worthy entry; returns the entry's hash.

    The project is passed explicitly everywhere: ``catalog_create`` sticks to the first project an MCP session used
    (#233), so it would build this project's entries in the first one.
    """
    ensure_project(name)
    path = write_shoe_orders(data_dir(name) / "orders.parquet", n_rows=200, seed=0)
    update_and_depend(path, SRC, project=name)
    return build_and_persist(name, _worthy_code(name)).content_hash


def _run(targets, join_timeout: float = 120.0) -> list[threading.Thread]:
    threads = [threading.Thread(target=target, daemon=True) for target in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=join_timeout)
    return threads


def _reader(client: TestClient, project: str, content_hash: str, failures: list[str], start=None):
    """READS page reads of one entry through the real route; every failure is recorded, none is raised."""

    def run() -> None:
        if start is not None:
            start.wait(timeout=30)
        for _ in range(READS):
            try:
                r = client.get(f"/{project}/api/data/{content_hash}?offset=0&limit=200")
                if r.status_code != 200:
                    failures.append(f"{r.status_code}: {r.text[:200]}")
            except Exception as exc:  # noqa: BLE001 - reported by the caller's assertion
                failures.append(f"{type(exc).__name__}: {exc}")

    return run


def _assert_all_read(threads, failures: list[str], reads: int) -> None:
    assert not any(t.is_alive() for t in threads), "a reader never finished"
    assert failures == [], f"{len(failures)} of {reads} page reads failed, e.g. {failures[:3]}"


# ---------------------------------------------------------------------------
# concurrent reads through the real route
# ---------------------------------------------------------------------------


def test_concurrent_page_reads_of_one_worthy_entry_all_succeed(fresh_companion_app, project, orders_src):
    """#118: 8 threads each make 40 page reads of one worthy entry through ``GET /{project}/api/data/{hash}``. Each
    read executes a page of the entry's snapshot on the shared default backend. At 1f8cb02 about a third of them
    fail with ``RuntimeError: Already borrowed``."""
    h = build_and_persist(project, _worthy_code(project)).content_hash
    assert read_manifest(entry_dir(project, h)).cache_worthy
    client = TestClient(fresh_companion_app)
    failures: list[str] = []
    start = threading.Barrier(THREADS)

    threads = _run([_reader(client, project, h, failures, start)] * THREADS)

    _assert_all_read(threads, failures, THREADS * READS)


def test_concurrent_page_reads_across_two_projects_all_succeed(fresh_companion_app, project, orders_src):
    """#118: the same reads split across two projects. ``project_lock`` is per project and the backend is per process,
    so a lock per project would still let the two projects' reads meet on the one backend."""
    h1 = build_and_persist(project, _worthy_code(project)).content_hash
    h2 = _second_project("p2")
    client = TestClient(fresh_companion_app)
    failures: list[str] = []
    start = threading.Barrier(THREADS)
    readers = [_reader(client, project, h1, failures, start), _reader(client, "p2", h2, failures, start)]

    threads = _run(readers * (THREADS // 2))

    _assert_all_read(threads, failures, THREADS * READS)


def test_a_heal_alongside_page_reads_of_another_entry_raises_nothing(fresh_companion_app, project, orders_src):
    """#118: one thread deletes an entry's snapshot and calls ``ensure_materialized``, over and over, while seven
    threads read another entry. A heal takes ``project_lock`` and then executes, so this is the lock order at work:
    no errors, and no deadlock between the healer and the readers.

    At 1f8cb02 this fails because the seven readers race one another on the shared backend. The heal itself does not
    race a read: it runs on a single-partition connection of its own (ADR-009 D1), and with the readers serialized
    among themselves it failed 0 of 320 times."""
    from tallyman_xorq.materialize import ensure_materialized, snapshot_path

    healed = build_and_persist(project, _worthy_code(project, by="region")).content_hash
    read = build_and_persist(project, _worthy_code(project, by="category")).content_hash
    client = TestClient(fresh_companion_app)
    failures: list[str] = []
    heal_errors: list[str] = []
    heals = 0
    readers_done = threading.Event()
    start = threading.Barrier(THREADS)

    def healer() -> None:
        nonlocal heals
        start.wait(timeout=30)
        while True:
            try:
                snapshot_path(project, healed).unlink(missing_ok=True)
                ensure_materialized(project, healed)
                heals += 1
            except Exception as exc:  # noqa: BLE001 - reported by the assertion below
                heal_errors.append(f"{type(exc).__name__}: {exc}")
            if readers_done.is_set():
                return

    heal_thread = threading.Thread(target=healer, daemon=True)
    heal_thread.start()
    reader_threads = _run([_reader(client, project, read, failures, start)] * (THREADS - 1))
    readers_done.set()
    heal_thread.join(timeout=120)

    assert not heal_thread.is_alive(), "the healer never finished"
    assert heal_errors == [], f"a heal failed: {heal_errors[:3]}"
    assert heals >= 1
    assert snapshot_path(project, healed).exists()
    _assert_all_read(reader_threads, failures, (THREADS - 1) * READS)


def test_concurrent_page_reads_of_one_cheap_entry_all_succeed(fresh_companion_app, project, orders_src):
    """#118: a cheap entry has no snapshot; each read runs its loaded build, rebound onto the default backend. The
    same 8 x 40 reads, all of which must succeed."""
    from tallyman_xorq.materialize import snapshot_path

    h = build_and_persist(project, _cheap_code(project)).content_hash
    assert read_manifest(entry_dir(project, h)).cache_worthy is False
    assert not snapshot_path(project, h).exists()
    client = TestClient(fresh_companion_app)
    failures: list[str] = []
    start = threading.Barrier(THREADS)

    threads = _run([_reader(client, project, h, failures, start)] * THREADS)

    _assert_all_read(threads, failures, THREADS * READS)


# ---------------------------------------------------------------------------
# the lock itself
# ---------------------------------------------------------------------------


def test_the_execution_lock_is_reentrant_and_knows_its_holder():
    """#118: the lock is re-entrant within a thread (a locked helper may call another), and the thread can ask whether
    it holds the lock, which is what ``project_lock`` checks."""
    from tallyman_core.execution import execution_lock, holds_execution_lock

    seen: list[bool] = []

    def nested() -> None:
        seen.append(holds_execution_lock())
        with execution_lock():
            with execution_lock():
                seen.append(holds_execution_lock())
            seen.append(holds_execution_lock())
        seen.append(holds_execution_lock())

    (worker,) = _run([nested], join_timeout=5)

    assert not worker.is_alive(), "a nested acquire of the execution lock in one thread blocked"
    assert seen == [False, True, True, False]


def test_a_second_thread_waits_for_the_execution_lock():
    """#118: the lock is process-wide. A second thread does not enter while the first holds it, does not think it
    holds it, and enters once the first releases it."""
    from tallyman_core.execution import execution_lock, holds_execution_lock

    holding, release, second_in = threading.Event(), threading.Event(), threading.Event()
    second_thought_it_held: list[bool] = []

    def holder() -> None:
        with execution_lock():
            holding.set()
            release.wait(timeout=10)

    def second() -> None:
        holding.wait(timeout=10)
        second_thought_it_held.append(holds_execution_lock())
        with execution_lock():
            second_in.set()

    threads = [threading.Thread(target=t, daemon=True) for t in (holder, second)]
    for thread in threads:
        thread.start()
    assert holding.wait(timeout=5)
    assert not second_in.wait(timeout=0.5), "a second thread entered while the first held the execution lock"

    release.set()

    assert second_in.wait(timeout=5), "the second thread never got the execution lock after the holder released it"
    for thread in threads:
        thread.join(timeout=5)
    assert second_thought_it_held == [False]


def test_a_new_project_lock_inside_the_execution_lock_raises(project):
    """#118, the lock order: ``project_lock`` first, then ``execution_lock``. A thread that holds the execution lock
    and would take a new project lock raises instead: another thread may hold that project lock and be waiting to
    execute, and the two would wait for each other forever. The refusal takes nothing, so the thread can take the
    project lock once it has left the execution lock."""
    from tallyman_core.execution import execution_lock

    with execution_lock():
        with pytest.raises(RuntimeError, match="project_lock"):
            with project_lock(project):
                pass

    entered = threading.Event()

    def other_thread() -> None:
        with project_lock(project):
            entered.set()

    (worker,) = _run([other_thread], join_timeout=5)
    assert not worker.is_alive() and entered.is_set(), "the refused project lock was left held"
    with project_lock(project):
        pass


def test_a_reentrant_project_lock_inside_the_execution_lock_is_allowed(project):
    """#118: a thread that took the project lock first, then the execution lock, may take the same project lock again
    (a re-entrant acquire takes no new ``flock``, so it cannot wait on anyone)."""
    from tallyman_core.execution import execution_lock

    entered = threading.Event()

    def nested() -> None:
        with project_lock(project):
            with execution_lock():
                with project_lock(project):
                    entered.set()

    (worker,) = _run([nested], join_timeout=5)

    assert not worker.is_alive(), "a re-entrant project lock inside the execution lock blocked"
    assert entered.is_set()


# ---------------------------------------------------------------------------
# every execution in src holds the lock
# ---------------------------------------------------------------------------

# Methods that execute an expression on the backend it is bound to. ``.schema().to_pyarrow()`` converts a schema and
# executes nothing, so a call on a ``.schema()`` result is not counted.
_EXECUTING_METHODS = frozenset({"execute", "to_pyarrow_batches", "to_pandas", "to_polars", "to_pyarrow"})
# buckaroo's xorq helpers that execute the expressions they are given (``buckaroo.compare``).
_EXECUTING_FUNCTIONS = frozenset({"stats_diff_xorq", "head_diff_xorq", "key_diff_xorq", "_max_group_xorq"})
# (path under src/, enclosing function) -> why it may execute outside the lock. The lock guards the one shared default
# backend, so only an execution on a connection of its own is exempt, and
# test_the_allowlisted_executions_never_run_on_the_default_backend checks that each one stays that way. Holding the lock
# around them would make every page read in the process wait for a whole build or heal.
_ALLOWED: dict[tuple[str, str], str] = {
    # A materialization's stream: _run_once rebinds the entry's build onto the single-partition connection it made for
    # this run (ADR-009 D1), never onto xorq.config.default_backend().
    ("tallyman_xorq/materialize.py", "_stream_to_parquet"): "its own single-partition connection",
    # A cheap build's row count: the build passes the expression load_expr returned, bound to the backend that load
    # created, never to xorq.config.default_backend().
    ("tallyman_xorq/result_cache.py", "stream_row_count"): "the backend load_expr created",
}


def _called_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _executes(call: ast.Call) -> bool:
    name = _called_name(call)
    if isinstance(call.func, ast.Attribute) and name in _EXECUTING_METHODS:
        on = call.func.value
        return not (isinstance(on, ast.Call) and _called_name(on) == "schema")
    return name in _EXECUTING_FUNCTIONS


def _is_execution_lock(expr: ast.expr) -> bool:
    return isinstance(expr, ast.Call) and _called_name(expr) == "execution_lock"


def _calls(tree: ast.AST, wanted) -> list[tuple[int, str, str, bool]]:
    """Every call ``wanted(call)`` accepts, as ``(line, enclosing function, source, inside the lock)``.

    Inside the lock means lexically inside a ``with execution_lock():`` block of the same function: a function defined
    in the block does not run there, so the walk forgets the lock when it enters one.
    """
    found: list[tuple[int, str, str, bool]] = []

    def visit(node: ast.AST, func: str, locked: bool) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func, locked = node.name, False
        elif isinstance(node, ast.Lambda):
            func, locked = f"{func}.<lambda>", False
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:  # evaluated before the block's locks are held
                visit(item, func, locked)
            inner = locked or any(_is_execution_lock(item.context_expr) for item in node.items)
            for stmt in node.body:
                visit(stmt, func, inner)
            return
        if isinstance(node, ast.Call) and wanted(node):
            found.append((node.lineno, func, ast.unparse(node), locked))
        for child in ast.iter_child_nodes(node):
            visit(child, func, locked)

    visit(tree, "<module>", False)
    return found


def test_every_execution_in_src_holds_the_execution_lock():
    """#118: every call in ``src/`` that executes an expression on the shared default backend sits inside a
    ``with execution_lock():`` block in the same function; ``_ALLOWED`` names the ones that execute on a connection of
    their own. At 1f8cb02 there is no such block, so each one is listed. An allowlist entry that no longer names an
    unlocked execution fails too, so the list cannot go stale."""
    found = 0
    outside: list[str] = []
    allowed_seen: set[tuple[str, str]] = set()
    for path in sorted(SRC_ROOT.rglob("*.py")):
        rel = path.relative_to(SRC_ROOT).as_posix()
        for line, func, text, locked in _calls(ast.parse(path.read_text()), _executes):
            found += 1
            if locked:
                continue
            if (rel, func) in _ALLOWED:
                allowed_seen.add((rel, func))
            else:
                outside.append(f"{rel}:{line} in {func}(): {text}")

    assert found >= 10, f"the scan found only {found} executions in src/; the walker is broken"
    assert outside == [], "these execute outside `with execution_lock():`\n" + "\n".join(outside)
    assert allowed_seen == set(_ALLOWED), f"stale allowlist entries: {sorted(set(_ALLOWED) - allowed_seen)}"


def _sources(expr) -> list:
    from xorq.common.utils.graph_utils import find_all_sources

    return list(find_all_sources(expr))


def test_the_allowlisted_executions_never_run_on_the_default_backend(project, orders_src, monkeypatch):
    """#118: the two functions ``_ALLOWED`` exempts execute without the lock because their expressions are never bound
    to the shared default backend. Each is spied on through its real callers (a worthy entry's create and heal for
    ``_stream_to_parquet``, a cheap entry's create for ``stream_row_count``), and every backend in every expression it
    is handed must be something other than ``xorq.config.default_backend()``. If either one is ever handed an
    expression on the shared backend, its allowlist entry is wrong and it needs the lock."""
    from xorq.config import default_backend

    import tallyman_xorq.materialize as materialize_module
    import tallyman_xorq.result_cache as result_cache_module
    from tallyman_xorq.materialize import ensure_materialized, snapshot_path

    handed: dict[str, list[list]] = {"_stream_to_parquet": [], "stream_row_count": []}
    real_stream = materialize_module._stream_to_parquet
    real_count = result_cache_module.stream_row_count

    def spy_stream(expr, dest):
        handed["_stream_to_parquet"].append(_sources(expr))
        return real_stream(expr, dest)

    def spy_count(expr):
        handed["stream_row_count"].append(_sources(expr))
        return real_count(expr)

    monkeypatch.setattr(materialize_module, "_stream_to_parquet", spy_stream)
    monkeypatch.setattr(result_cache_module, "stream_row_count", spy_count)

    worthy = build_and_persist(project, _worthy_code(project)).content_hash  # a create runs the query twice
    snapshot_path(project, worthy).unlink()
    ensure_materialized(project, worthy)  # a heal runs it once
    build_and_persist(project, _cheap_code(project))

    assert len(handed["_stream_to_parquet"]) == 3, handed
    assert len(handed["stream_row_count"]) == 1, handed
    shared = default_backend()
    for name, calls in handed.items():
        for sources in calls:
            assert sources, f"{name} was handed an expression with no backend; the check would prove nothing"
            assert all(s is not shared for s in sources), f"{name} executed on the shared default backend"


# Calls that can take a project lock: a heal (cached_result_expr and what it calls), a materialization or publish, a
# build, a checkpoint or reset, the primary-key search (it reads through cached_result_expr), and the lock itself.
_TAKES_A_PROJECT_LOCK = frozenset(
    {
        "cached_result_expr",
        "ensure_materialized",
        "_ensure",
        "_heal",
        "_heal_a_source",
        "_recreate",
        "materialize",
        "publish_snapshot",
        "rewrite_source_snapshot",
        "build_and_persist",
        "update_and_depend",
        "checkpoint_catalog",
        "reset_to",
        "resolve_primary_key",
        "diff_keys",
        "project_lock",
        "_project_lock",
    }
)


def test_nothing_that_can_take_a_project_lock_runs_inside_the_execution_lock():
    """#118, the lock order, checked in the source: no ``with execution_lock():`` block in ``src/`` calls anything that
    can take a project lock. ``project_lock`` raises at run time when it happens, but only when the call does take a
    new lock: ``with execution_lock(): cached_result_expr(...).execute()`` passes every test until the snapshot is
    missing and the read has to heal. (This passes on 1f8cb02 too, which has no such block.)"""
    inside: list[str] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        rel = path.relative_to(SRC_ROOT).as_posix()
        for line, func, text, locked in _calls(
            ast.parse(path.read_text()), lambda call: _called_name(call) in _TAKES_A_PROJECT_LOCK
        ):
            if locked:
                inside.append(f"{rel}:{line} in {func}(): {text}")

    assert inside == [], "these can take a project lock inside `with execution_lock():`\n" + "\n".join(inside)
