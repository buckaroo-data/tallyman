from __future__ import annotations

import os
import tempfile
from pathlib import Path

# Redirect xorq's cache dir BEFORE any xorq import. xorq freezes
# XORQ_CACHE_DIR at module-load time (caching_utils.py notes
# "modifying env var XORQ_CACHE_DIR won't have any impact after first import"),
# so this has to happen here at conftest module level — before any test or
# helper imports xorq. Without this, tests pollute the user's ~/.cache/xorq/.
_TEST_XORQ_CACHE = Path(tempfile.mkdtemp(prefix="tallyman_xorq_cache_"))
os.environ.setdefault("XORQ_CACHE_DIR", str(_TEST_XORQ_CACHE))

import pytest  # noqa: E402

from tallyman_cli.fixtures import write_shoe_orders  # noqa: E402
from tallyman_core import data_dir, ensure_project, set_active_project  # noqa: E402


@pytest.fixture
def isolated_home(tmp_path: Path, monkeypatch) -> Path:
    """Point TALLYMAN_HOME at a tmp dir for the duration of the test.

    xorq's cache is redirected at conftest module load (see top of file)
    because XORQ_CACHE_DIR is frozen at first xorq import.
    """
    monkeypatch.setenv("TALLYMAN_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def project(isolated_home: Path) -> str:
    """Create a project and mark it active.

    The active-project file lives under ``isolated_home`` (tmp), so the
    write is automatically isolated per-test. In-process callers of
    ``resolve_project()`` pick it up via the production code path —
    no monkeypatching needed.
    """
    name = "test"
    ensure_project(name)
    set_active_project(name)
    return name


@pytest.fixture
def orders_parquet(project: str) -> Path:
    """Generate a deterministic shoe-orders fixture under project/data/.

    The file only. A recipe cannot read it (ADR-011 D2) — depend on ``orders_src`` for that, and use
    this where the test is about the file itself: editing it, importing it under an alias of the test's own (not
    alongside ``orders_src``, since one set of bytes goes under one alias), or
    asserting on the bytes.
    """
    return write_shoe_orders(data_dir(project) / "orders.parquet", n_rows=200, seed=0)


ORDERS_SRC = "orders_src"


@pytest.fixture
def orders_src(orders_parquet: Path, project: str) -> str:
    """The shoe-orders fixture imported as a source alias, and the alias name (ADR-011 D1).

    A recipe never opens a file: it reads an alias. Depend on this wherever a test needs a recipe over
    the orders data, and read it in the recipe with ``tracked_expr_from_alias("orders_src")``. The
    import mints one ordinary entry — a content hash, a manifest, a snapshot carrying ``__row_order``
    — so a project that uses this fixture has one more entry and one more alias than it did before the
    rewrite, which is what the DAG gaining its roots looks like.

    The name is ``orders_src`` rather than ``orders`` because several tests already create a catalog
    alias called ``orders``, and one name is one kind or the other but never both.
    """
    from tallyman_xorq.source_import import update_and_depend

    update_and_depend(orders_parquet, ORDERS_SRC, project=project)
    return ORDERS_SRC


@pytest.fixture
def fresh_companion_app(project: str):
    """Create a companion app bound to the current isolated project."""
    from tallyman_companion import create_app

    return create_app(project)


@pytest.fixture(autouse=True)
def _clear_expr_lru_caches():
    """Clear the process-global expr/result caches around every test.

    ``result_cache`` and ``cached_result_expr`` (tallyman_xorq.result_cache) and
    ``_build_compare_expr`` (tallyman_companion.app) are ``functools.lru_cache``d
    keyed on ``(project, content_hash)``. The ``project`` fixture reuses the name
    "test" across tests while ``isolated_home`` gives each test a fresh tmp dir,
    so a cached entry from one test would otherwise point at another test's
    torn-down home. Production is unaffected — entries are immutable and
    content-addressed, so the cache stays correct within a real process.
    """

    def _clear() -> None:
        from tallyman_xorq.result_cache import cached_result_expr

        cached_result_expr.cache_clear()
        try:
            from tallyman_companion.app import _build_compare_expr

            _build_compare_expr.cache_clear()
        except Exception:
            pass

    _clear()
    yield
    _clear()


@pytest.fixture
def built_spa():
    """Skip when the React SPA hasn't been built.

    The SPA-serving routes (``/`` with no project, and the catch-all behind
    every UI path) read ``packages/app/dist/index.html``, which is gitignored
    and only exists after ``pnpm -C packages/app build``. CI builds it before
    the fast suite; a bare local ``pytest`` would otherwise see these tests
    fail with the 503 "React app not built" response. Tests that assert on
    served SPA HTML depend on this fixture so they skip cleanly instead.
    """
    from tallyman_companion.app import _REACT_DIST

    if not (_REACT_DIST / "index.html").exists():
        pytest.skip("React SPA not built — run `pnpm -C packages/app build`")
