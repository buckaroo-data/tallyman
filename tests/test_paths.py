from __future__ import annotations

from pathlib import Path

import pytest

from tallyman_core import (
    catalog_dir,
    data_dir,
    ensure_project,
    entries_dir,
    entry_dir,
    project_dir,
    resolve_project,
)
from tallyman_core.paths import (
    ARTIFACTS_DIRNAME,
    CATALOG_DIRNAME,
    ENTRIES_DIRNAME,
    ENTRY_ARTIFACT_NAMES,
    ENTRY_BUILD_DIRNAME,
    ENTRY_CACHE_NAMES,
    ENTRY_EXPANDED_BUILD_DIRNAME,
    ENTRY_MANIFEST_FILENAME,
    ENTRY_RESULT_FILENAME,
    ENTRY_SCHEMA_FILENAME,
    ENTRY_STAT_CACHE_DIRNAME,
    active_project_file_path,
    delete_project,
    entry_build_dir,
    entry_expanded_build_dir,
    entry_manifest_path,
    entry_result_path,
    entry_schema_path,
    entry_stat_cache_dir,
    errors_path,
    exports_dir,
    post_processing_dir,
    projects_root,
    set_active_project,
    stats_dir,
    validate_project_name,
)

# ---------------------------------------------------------------------------
# resolve_project — file is the source of truth at runtime
# ---------------------------------------------------------------------------


def test_resolve_project_returns_none_when_nothing_set(isolated_home: Path, monkeypatch):
    """No file, no env → None. No zombie 'spike' default."""
    monkeypatch.delenv("TALLYMAN_PROJECT", raising=False)
    assert resolve_project() is None


def test_resolve_project_reads_file(isolated_home: Path, monkeypatch):
    monkeypatch.delenv("TALLYMAN_PROJECT", raising=False)
    ensure_project("alpha")
    set_active_project("alpha")
    assert resolve_project() == "alpha"


def test_resolve_project_seeds_from_env_once(isolated_home: Path, monkeypatch):
    """File absent + env set → write env value to file, return it."""
    monkeypatch.setenv("TALLYMAN_PROJECT", "demo")
    ensure_project("demo")
    assert resolve_project() == "demo"
    assert active_project_file_path().read_text().strip() == "demo"


def test_resolve_project_ignores_env_after_seed(isolated_home: Path, monkeypatch):
    """Once file exists, env is ignored even if set to a different value."""
    ensure_project("alpha")
    set_active_project("alpha")
    monkeypatch.setenv("TALLYMAN_PROJECT", "demo")
    assert resolve_project() == "alpha"


def test_resolve_project_explicit_wins(isolated_home: Path, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", "demo")
    ensure_project("alpha")
    set_active_project("alpha")
    # Explicit wins over both env and file.
    assert resolve_project("override") == "override"


# ---------------------------------------------------------------------------
# set_active_project — atomic write, validation, project must exist
# ---------------------------------------------------------------------------


def test_set_active_project_writes_file(isolated_home: Path):
    ensure_project("alpha")
    returned = set_active_project("alpha")
    assert returned == "alpha"
    assert active_project_file_path().read_text().strip() == "alpha"


def test_set_active_project_requires_existing_project(isolated_home: Path):
    with pytest.raises(FileNotFoundError):
        set_active_project("never_created")


def test_set_active_project_validates_name(isolated_home: Path):
    with pytest.raises(ValueError):
        set_active_project("Invalid-Name")  # uppercase


def test_set_active_project_atomic_overwrite(isolated_home: Path):
    """Subsequent writes overwrite cleanly (os.replace semantics)."""
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    set_active_project("beta")
    assert resolve_project() == "beta"


# ---------------------------------------------------------------------------
# validate_project_name — strict regex + reserved names
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "a",
        "a1",
        "spike",
        "nyc_taxi",
        "abc-123",
        "x" * 32,
    ],
)
def test_validate_project_name_accepts(name: str):
    validate_project_name(name)  # raises ValueError if invalid


@pytest.mark.parametrize(
    "name",
    [
        "",  # empty
        "_foo",  # starts with underscore
        "-foo",  # starts with hyphen
        "FOO",  # uppercase
        "abc def",  # space
        "abc/def",  # slash
        "abc.def",  # dot
        ".hidden",  # leading dot
        "..",  # parent-dir
        ".",  # current-dir
        "x" * 33,  # too long
        "active_project",  # reserved
        "buckaroo_sessions",  # reserved
    ],
)
def test_validate_project_name_rejects(name: str):
    with pytest.raises(ValueError):
        validate_project_name(name)


# ---------------------------------------------------------------------------
# ensure_project / paths — artifacts/ layout
# ---------------------------------------------------------------------------


def test_ensure_project_creates_artifacts_layout(isolated_home: Path):
    p = ensure_project("alpha")
    assert p == project_dir("alpha")
    # New tree: artifacts/ holds everything the system produces.
    assert (p / "artifacts" / "catalog" / "entries").is_dir()
    assert (p / "artifacts" / "post_processing").is_dir()
    assert (p / "artifacts" / "stats").is_dir()
    assert (p / "artifacts" / "exports").is_dir()
    # Inputs and document stay at project root.
    assert (p / "data").is_dir()
    assert (p / "notebooks").is_dir()


def test_ensure_project_validates_name(isolated_home: Path):
    with pytest.raises(ValueError):
        ensure_project("BadName")


def test_ensure_project_idempotent(isolated_home: Path):
    a = ensure_project("alpha")
    b = ensure_project("alpha")
    assert a == b


def test_path_helpers_under_artifacts(isolated_home: Path):
    ensure_project("alpha")
    p = isolated_home / "projects" / "alpha"
    assert project_dir("alpha") == p
    assert catalog_dir("alpha") == p / "artifacts" / "catalog"
    assert entries_dir("alpha") == p / "artifacts" / "catalog" / "entries"
    assert entry_dir("alpha", "abc123") == entries_dir("alpha") / "abc123"
    assert post_processing_dir("alpha") == p / "artifacts" / "post_processing"
    assert stats_dir("alpha") == p / "artifacts" / "stats"
    assert exports_dir("alpha") == p / "artifacts" / "exports"
    assert errors_path("alpha") == p / "artifacts" / "errors.jsonl"
    # data/ and notebooks/ stay at project root.
    assert data_dir("alpha") == p / "data"


# ---------------------------------------------------------------------------
# Centralized layout constants + per-entry helpers (#66)
# ---------------------------------------------------------------------------


def test_layout_segment_constants_compose_the_helpers(isolated_home: Path):
    """The segment names are the single source of truth for the dir helpers."""
    p = project_dir("alpha")
    assert catalog_dir("alpha") == p / ARTIFACTS_DIRNAME / CATALOG_DIRNAME
    assert entries_dir("alpha") == p / ARTIFACTS_DIRNAME / CATALOG_DIRNAME / ENTRIES_DIRNAME


def test_entry_path_helpers_compose_from_constants(isolated_home: Path):
    base = entry_dir("alpha", "abc123")
    assert entry_build_dir("alpha", "abc123") == base / ENTRY_BUILD_DIRNAME
    assert entry_result_path("alpha", "abc123") == base / ENTRY_RESULT_FILENAME
    assert entry_manifest_path("alpha", "abc123") == base / ENTRY_MANIFEST_FILENAME
    assert entry_schema_path("alpha", "abc123") == base / ENTRY_SCHEMA_FILENAME
    assert entry_stat_cache_dir("alpha", "abc123") == base / ENTRY_STAT_CACHE_DIRNAME
    assert entry_expanded_build_dir("alpha", "abc123") == base / ENTRY_EXPANDED_BUILD_DIRNAME


def test_entry_artifact_cache_partition_is_disjoint():
    """Artifacts (symlinked read-only by the perf overlay) and caches (left fresh
    for a cold hit) must not overlap — a name in both would break the overlay's
    cold guarantee."""
    assert set(ENTRY_ARTIFACT_NAMES).isdisjoint(ENTRY_CACHE_NAMES)
    # The immutable build outputs the overlay needs on the read path.
    assert ENTRY_BUILD_DIRNAME in ENTRY_ARTIFACT_NAMES
    assert ENTRY_RESULT_FILENAME in ENTRY_ARTIFACT_NAMES
    assert ENTRY_MANIFEST_FILENAME in ENTRY_ARTIFACT_NAMES
    assert ENTRY_SCHEMA_FILENAME in ENTRY_ARTIFACT_NAMES
    # The per-entry caches are classified as caches, never symlinked.
    assert ENTRY_STAT_CACHE_DIRNAME in ENTRY_CACHE_NAMES
    assert ENTRY_EXPANDED_BUILD_DIRNAME in ENTRY_CACHE_NAMES


def test_active_project_file_path_under_tallyman_home(isolated_home: Path):
    """File lives at $TALLYMAN_HOME/active_project; no per-project location."""
    assert active_project_file_path() == isolated_home / "active_project"


def test_projects_root_under_tallyman_home(isolated_home: Path):
    assert projects_root() == isolated_home / "projects"


# ---------------------------------------------------------------------------
# delete_project — removes the on-disk tree and clears the active-project file
# ---------------------------------------------------------------------------


def test_delete_project_removes_directory(isolated_home: Path):
    ensure_project("alpha")
    assert project_dir("alpha").is_dir()
    delete_project("alpha")
    assert not project_dir("alpha").exists()


def test_delete_project_clears_active_file_when_it_points_at_deleted(isolated_home: Path):
    ensure_project("alpha")
    set_active_project("alpha")
    assert active_project_file_path().exists()
    delete_project("alpha")
    # Active file is cleared so resolve_project() falls back to None.
    assert not active_project_file_path().exists()
    assert resolve_project() is None


def test_delete_project_keeps_active_file_for_other_project(isolated_home: Path):
    ensure_project("alpha")
    ensure_project("beta")
    set_active_project("alpha")
    delete_project("beta")
    # Deleting a non-active project leaves the active pointer untouched.
    assert resolve_project() == "alpha"


def test_delete_project_unknown_raises(isolated_home: Path):
    with pytest.raises(FileNotFoundError):
        delete_project("no_such")


def test_delete_project_validates_name(isolated_home: Path):
    with pytest.raises(ValueError):
        delete_project("Bad/Name")
