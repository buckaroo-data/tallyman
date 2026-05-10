from __future__ import annotations

import shutil
from pathlib import Path

from pydata_cli.fixtures import write_shoe_orders
from pydata_core import data_dir, ensure_project, entry_dir, project_dir
from pydata_xorq import build_and_persist, load_entry
from pydata_xorq.portable import PLACEHOLDER


def _agg_code(project_name: str) -> str:
    # Use from_project so the recorded code uses the conventional path.
    return f"""
from pydata_xorq.io import from_project
t = from_project("orders.parquet", project={project_name!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def test_persisted_build_uses_placeholder(project: str, orders_parquet: Path, isolated_home: Path):
    res = build_and_persist(project, _agg_code(project), prompt="region totals")
    yaml = (entry_dir(project, res.content_hash) / "xorq_build" / "expr.yaml").read_text()
    assert PLACEHOLDER in yaml
    assert str(project_dir(project)) not in yaml


def test_persisted_expr_py_uses_placeholder(project: str, orders_parquet: Path):
    # Drive code that embeds an absolute path so we can prove the rewrite.
    abs_path = orders_parquet
    code = f"""
import xorq.api as xo
t = xo.deferred_read_parquet({str(abs_path)!r})
expr = t.group_by("region").aggregate(n=t.count())
"""
    res = build_and_persist(project, code)
    persisted = (entry_dir(project, res.content_hash) / "expr.py").read_text()
    assert PLACEHOLDER in persisted
    assert str(project_dir(project)) not in persisted


def test_load_entry_on_current_machine(project: str, orders_parquet: Path):
    res = build_and_persist(project, _agg_code(project))
    expr = load_entry(project, res.content_hash)
    df = expr.execute()
    assert set(df.columns) >= {"region", "total", "n"}
    assert len(df) == 4


def test_load_entry_under_relocated_home(
    project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path, monkeypatch
):
    """The strongest portability check: build under one PYDATA_HOME, then move
    the entire project tree to a fresh PYDATA_HOME and load from there.

    This simulates handing the project directory to a colleague whose
    home directory is at a different absolute path.
    """
    res = build_and_persist(project, _agg_code(project))

    new_home = tmp_path / "alt-home"
    new_project_root = new_home / "projects" / project
    new_project_root.parent.mkdir(parents=True)
    shutil.copytree(project_dir(project), new_project_root)

    monkeypatch.setenv("PYDATA_HOME", str(new_home))
    # Sanity: the build artifacts still reference the placeholder, not the old home.
    relocated_build = entry_dir(project, res.content_hash) / "xorq_build" / "expr.yaml"
    assert PLACEHOLDER in relocated_build.read_text()
    assert str(isolated_home) not in relocated_build.read_text()

    # The fixture's parquet is at new_project_root/data/orders.parquet because
    # we copied the whole tree; load_entry should resolve via the new PYDATA_HOME.
    expr = load_entry(project, res.content_hash)
    df = expr.execute()
    assert len(df) == 4


def test_load_entry_fails_clean_if_data_missing(
    project: str, orders_parquet: Path, isolated_home: Path, tmp_path: Path, monkeypatch
):
    """If the data file isn't present at the relocated path, we get a real error
    (not a silent wrong result)."""
    res = build_and_persist(project, _agg_code(project))
    new_home = tmp_path / "alt-home"
    new_project_root = new_home / "projects" / project
    (new_project_root / "catalog" / "entries").mkdir(parents=True)
    shutil.copytree(
        entry_dir(project, res.content_hash),
        new_project_root / "catalog" / "entries" / res.content_hash,
    )
    # NOTE: we deliberately do NOT copy data/ into the new tree.
    monkeypatch.setenv("PYDATA_HOME", str(new_home))

    try:
        expr = load_entry(project, res.content_hash)
        # load may succeed lazily; execute should fail.
        expr.execute()
    except Exception:
        return
    raise AssertionError("expected failure when data file is missing")


def test_idempotent_build_does_not_clobber_portable_form(project: str, orders_parquet: Path):
    """Running the same code twice must not un-portabilize the persisted build
    (e.g., by re-writing absolute paths back into the build dir)."""
    res1 = build_and_persist(project, _agg_code(project), prompt="first")
    yaml1 = (entry_dir(project, res1.content_hash) / "xorq_build" / "expr.yaml").read_text()
    assert PLACEHOLDER in yaml1

    res2 = build_and_persist(project, _agg_code(project), prompt="second")
    assert res2.content_hash == res1.content_hash
    yaml2 = (entry_dir(project, res2.content_hash) / "xorq_build" / "expr.yaml").read_text()
    assert PLACEHOLDER in yaml2
    assert yaml1 == yaml2
