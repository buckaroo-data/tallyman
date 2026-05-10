from __future__ import annotations

from pathlib import Path

from pydata_core import (
    catalog_dir,
    data_dir,
    ensure_project,
    entries_dir,
    entry_dir,
    project_dir,
    resolve_project,
)


def test_resolve_project_default(isolated_home: Path, monkeypatch):
    monkeypatch.delenv("PYDATA_PROJECT", raising=False)
    assert resolve_project() == "spike"


def test_resolve_project_env(isolated_home: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", "demo")
    assert resolve_project() == "demo"


def test_resolve_project_explicit_wins(isolated_home: Path, monkeypatch):
    monkeypatch.setenv("PYDATA_PROJECT", "demo")
    assert resolve_project("override") == "override"


def test_ensure_project_creates_layout(isolated_home: Path):
    p = ensure_project("alpha")
    assert p == project_dir("alpha")
    assert (p / "catalog" / "entries").is_dir()
    assert (p / "data").is_dir()
    assert (p / "notebooks").is_dir()


def test_ensure_project_idempotent(isolated_home: Path):
    a = ensure_project("alpha")
    b = ensure_project("alpha")
    assert a == b


def test_path_helpers_under_isolated_home(isolated_home: Path):
    ensure_project("alpha")
    assert project_dir("alpha") == isolated_home / "projects" / "alpha"
    assert catalog_dir("alpha") == isolated_home / "projects" / "alpha" / "catalog"
    assert entries_dir("alpha") == isolated_home / "projects" / "alpha" / "catalog" / "entries"
    assert entry_dir("alpha", "abc123") == entries_dir("alpha") / "abc123"
    assert data_dir("alpha") == isolated_home / "projects" / "alpha" / "data"
