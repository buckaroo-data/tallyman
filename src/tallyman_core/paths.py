"""Filesystem layout + active-project resolution.

Layout (per-project):

    ~/.tallyman/
    ├── active_project                 # one-line plain text; source of truth
    ├── buckaroo_sessions.json         # global Buckaroo session map
    └── projects/<name>/
        ├── artifacts/                 # everything the system produces
        │   ├── catalog/               # the native catalog git repo
        │   │   ├── entries/<hash>.zip     # immutable recipe (one blob per entry)
        │   │   ├── entries/<hash>/        # untracked build dir (gitignored)
        │   │   ├── aliases.jsonl          # tracked alias bookkeeping
        │   │   ├── notebook.jsonl         # tracked notebook cells
        │   │   ├── chart_specs/<hash>.vl.json
        │   │   ├── display_configs/<hash>.json
        │   │   ├── post_processing/<name>.py , stats/<name>.py
        │   │   ├── prompts/<hash>.jsonl
        │   │   └── entries.jsonl , compute_cache.jsonl   # untracked-artifact pointers
        │   ├── exports/...            # marimo .py, screenshots, CSVs
        │   └── errors.jsonl
        └── data/                      # input parquets (fixtures or user)

The active project lives in a single one-line file at
``~/.tallyman/active_project``. ``resolve_project()`` reads it on every
call; ``set_active_project()`` writes it atomically. ``TALLYMAN_PROJECT``
env is consulted only to seed the file on first resolution when nothing
is configured — once the file exists, env is ignored.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Roots
# ---------------------------------------------------------------------------


def tallyman_home() -> Path:
    """Resolve TALLYMAN_HOME at call time so test fixtures can override via env."""
    return Path(os.environ.get("TALLYMAN_HOME", Path.home() / ".tallyman-notebooks"))


def projects_root() -> Path:
    return tallyman_home() / "projects"


def active_project_file_path() -> Path:
    return tallyman_home() / "active_project"


def buckaroo_sessions_path() -> Path:
    """Global Buckaroo session table (one file, all projects).

    Schema: ``{<content_hash>: {session_id, project, buckaroo_started_at}}``.
    Migrates to a Buckaroo-side enumeration endpoint once
    buckaroo-data/buckaroo#860 lands.
    """
    return tallyman_home() / "buckaroo_sessions.json"


# ---------------------------------------------------------------------------
# Layout segment + per-entry artifact/cache names (single source of truth)
# ---------------------------------------------------------------------------

# Per-project layout segments. Whole-tree relocation is the TALLYMAN_HOME knob
# (above); these name the fixed structure underneath a project, defined once so
# the dir helpers and ``ensure_project`` can't drift from each other.
ARTIFACTS_DIRNAME = "artifacts"
CATALOG_DIRNAME = "catalog"
ENTRIES_DIRNAME = "entries"

# Per-entry artifacts: immutable build outputs. Safe to symlink read-only into a
# write-isolated overlay (the perf harness) because nothing rewrites them.
ENTRY_BUILD_DIRNAME = "xorq_build"
ENTRY_MANIFEST_FILENAME = "manifest.json"
ENTRY_SCHEMA_FILENAME = "schema.json"

# Per-entry caches: regenerated on demand. An overlay leaves these absent so the
# first hit is honestly cold; production deletes them to recompute.
ENTRY_STAT_CACHE_DIRNAME = ".buckaroo_stat_cache"
ENTRY_EXPANDED_BUILD_DIRNAME = ".xorq_build_expanded"
# No per-entry result.parquet exists: an expensive entry's rows live in its baked
# result cache (under the per-project compute_cache), a cheap entry recomputes on
# read. The single materialised copy is the xorq .cache() snapshot — nothing
# writes a <entry>/result.parquet, at build time or on demand (#73 + follow-up).

# Canonical artifact-vs-cache partition of the per-entry names the overlay cares
# about. The write-isolated perf overlay symlinks ENTRY_ARTIFACT_NAMES read-only
# and deliberately omits ENTRY_CACHE_NAMES so the cache starts cold; the two
# must stay disjoint or a name would be both linked and expected-fresh. Adding a
# new per-entry cache here keeps the overlay's cold guarantee honest instead of
# leaving the classification in the test harness. (expr.py / prompts.jsonl are
# real per-entry files but aren't on the overlay read path, so they're neither.)
ENTRY_ARTIFACT_NAMES = (
    ENTRY_BUILD_DIRNAME,
    ENTRY_MANIFEST_FILENAME,
    ENTRY_SCHEMA_FILENAME,
)
ENTRY_CACHE_NAMES = (
    ENTRY_STAT_CACHE_DIRNAME,
    ENTRY_EXPANDED_BUILD_DIRNAME,
)


# ---------------------------------------------------------------------------
# Project name validation
# ---------------------------------------------------------------------------

# Lowercase + alphanum + hyphen + underscore, max 32 chars, must start with
# an alphanum. Matches Docker image / npm package / GitHub slug conventions
# and avoids macOS case-folding collisions.
PROJECT_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")

# Names blocked even if they otherwise match the regex (or trivially don't).
RESERVED_PROJECT_NAMES = frozenset({"active_project", "buckaroo_sessions", ".", "..", ""})


def validate_project_name(name: str) -> None:
    """Raise ValueError if *name* is not a legal project name.

    Rules: lowercase + alphanum + hyphen + underscore, max 32 chars,
    must start with an alphanum. Reserved names blocked.
    """
    if name in RESERVED_PROJECT_NAMES:
        raise ValueError(f"{name!r} is a reserved project name")
    if not isinstance(name, str) or not PROJECT_NAME_RE.match(name):
        raise ValueError(
            f"{name!r} is not a valid project name "
            r"(must match ^[a-z0-9][a-z0-9_-]{0,31}$)"
        )


# ---------------------------------------------------------------------------
# Project directories
# ---------------------------------------------------------------------------


def project_dir(name: str) -> Path:
    """Project root.

    Supports ``TALLYMAN_PROJECT_PATH`` for ``tallyman serve <path>`` mode:
    if the override is set AND *name* matches the active project, returns
    the override path. Otherwise ``<projects_root>/<name>``.

    Serve mode is the narrow case where ``TALLYMAN_PROJECT`` env is still
    consulted at runtime — serving someone else's project tree shouldn't
    clobber the active-project file used by the main companion. Outside
    serve mode the file is the only source of truth (see ``resolve_project``).
    """
    override = os.environ.get("TALLYMAN_PROJECT_PATH")
    if override:
        active = os.environ.get("TALLYMAN_PROJECT") or _read_active_project_file()
        if active is not None and name == active:
            return Path(override)
    return projects_root() / name


def artifacts_dir(project: str) -> Path:
    return project_dir(project) / ARTIFACTS_DIRNAME


def catalog_dir(project: str) -> Path:
    return artifacts_dir(project) / CATALOG_DIRNAME


def entries_dir(project: str) -> Path:
    return catalog_dir(project) / ENTRIES_DIRNAME


def entry_dir(project: str, content_hash: str) -> Path:
    return entries_dir(project) / content_hash


# ---------------------------------------------------------------------------
# Per-entry artifact/cache paths (compose from the names above)
# ---------------------------------------------------------------------------


def entry_build_dir(project: str, content_hash: str) -> Path:
    """The xorq build dir (expr.yaml + deps) under an entry."""
    return entry_dir(project, content_hash) / ENTRY_BUILD_DIRNAME


def entry_manifest_path(project: str, content_hash: str) -> Path:
    """The entry's ``manifest.json``."""
    return entry_dir(project, content_hash) / ENTRY_MANIFEST_FILENAME


def entry_schema_path(project: str, content_hash: str) -> Path:
    """The entry's ``schema.json``."""
    return entry_dir(project, content_hash) / ENTRY_SCHEMA_FILENAME


def entry_stat_cache_dir(project: str, content_hash: str) -> Path:
    """Buckaroo summary-stat cache for the entry (regenerated on demand)."""
    return entry_dir(project, content_hash) / ENTRY_STAT_CACHE_DIRNAME


def entry_expanded_build_dir(project: str, content_hash: str) -> Path:
    """Stable expanded-build dir beside the entry (regenerated on demand)."""
    return entry_dir(project, content_hash) / ENTRY_EXPANDED_BUILD_DIRNAME


def compute_cache_dir(project: str) -> Path:
    """Per-project xorq compute cache, redirected off the global ``~/.cache/xorq``.

    Lives inside the catalog dir so it travels with the project. ``reset-to``
    prunes it to the revision's recorded warm-set, so a baseline expression
    stays warm while a freshly added one computes cold — the honest cold add
    the demo is built to show. Untracked by git (content-addressed parquet).
    """
    return catalog_dir(project) / "compute_cache"


def bullpen_dir(project: str) -> Path:
    """Holding area for artifacts a reset evicts (untracked, content-addressed).

    A backward reset moves pruned entries/caches here instead of deleting
    them; a forward reset copies the step's recorded set back. Live
    operations never read it, so an evicted entry still re-adds cold.
    """
    return catalog_dir(project) / "bullpen"


def diff_stat_cache_root(project: str) -> Path:
    """Parent of all per-pair diff stat caches for a project."""
    return catalog_dir(project) / "diff_stat_cache"


def diff_stat_cache_dir(project: str, a_hash: str, b_hash: str) -> Path:
    """Buckaroo summary-stat cache for a comparison (diff) session.

    Keyed by the entry pair, not the joined data — Buckaroo stores only the
    small per-column summary stats here, so re-opening the same diff reuses
    them instead of recomputing over the full join.
    """
    return diff_stat_cache_root(project) / f"{a_hash[:12]}-{b_hash[:12]}"


def post_processing_dir(project: str) -> Path:
    # Under the catalog repo (was artifacts/) so the native store tracks the
    # scripts directly instead of round-tripping them through catalog.yaml.
    return catalog_dir(project) / "post_processing"


def stats_dir(project: str) -> Path:
    return catalog_dir(project) / "stats"


def prompts_dir(project: str) -> Path:
    return catalog_dir(project) / "prompts"


def prompts_path(project: str, content_hash: str) -> Path:
    """Tracked per-entry authoring-prompt history (was a loose
    ``entries/<hash>/prompts.jsonl``, lost on a clone now that build dirs are
    gitignored)."""
    return prompts_dir(project) / f"{content_hash}.jsonl"


def display_dir(project: str) -> Path:
    return artifacts_dir(project) / "display"


def exports_dir(project: str) -> Path:
    return artifacts_dir(project) / "exports"


def errors_path(project: str) -> Path:
    return artifacts_dir(project) / "errors.jsonl"


def data_dir(project: str) -> Path:
    return project_dir(project) / "data"


def notebooks_dir(project: str) -> Path:
    return project_dir(project) / "notebooks"


# ---------------------------------------------------------------------------
# Active-project file: source of truth at runtime
# ---------------------------------------------------------------------------


def _read_active_project_file() -> str | None:
    p = active_project_file_path()
    if not p.exists():
        return None
    name = p.read_text().strip()
    return name or None


def _write_active_project_file(name: str) -> None:
    """Atomic write via tempfile + os.replace."""
    p = active_project_file_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp") if p.suffix else p.with_name(p.name + ".tmp")
    tmp.write_text(name + "\n")
    os.replace(tmp, p)


def set_active_project(name: str) -> str:
    """Validate, ensure the project exists on disk, write the file atomically.

    Raises ``ValueError`` on invalid name, ``FileNotFoundError`` if the
    project directory doesn't exist yet (callers should ``ensure_project``
    first, or use the higher-level ``/api/projects/new`` route).
    """
    validate_project_name(name)
    if not (projects_root() / name).is_dir():
        raise FileNotFoundError(f"project {name!r} not found at {projects_root() / name}")
    _write_active_project_file(name)
    return name


def resolve_project(explicit: str | None = None) -> str | None:
    """Return the currently-active project name, or None.

    Precedence:

    1. *explicit* argument (used by ``tallyman <cmd> --project foo``).
    2. ``~/.tallyman/active_project`` file.
    3. ``TALLYMAN_PROJECT`` env, **only as a one-shot seed** when the file
       is absent and the named project already exists on disk. The seed
       writes the file so subsequent calls don't re-read env.
    4. ``None`` — caller decides (typically: redirect to empty-state page).
    """
    if explicit:
        return explicit
    from_file = _read_active_project_file()
    if from_file is not None:
        return from_file
    from_env = os.environ.get("TALLYMAN_PROJECT")
    if from_env and (projects_root() / from_env).is_dir():
        _write_active_project_file(from_env)
        return from_env
    return None



# ---------------------------------------------------------------------------
# Project lifecycle
# ---------------------------------------------------------------------------


def ensure_project(project: str) -> Path:
    """Create the project tree if absent; idempotent.

    Validates the name first; ``ensure_project("BadName")`` raises
    ``ValueError`` without touching disk. ``errors.jsonl`` is a file, not
    a dir — created lazily on first write by ``tallyman_core.errors``.
    """
    validate_project_name(project)
    p = project_dir(project)
    for d in (
        entries_dir(project),
        post_processing_dir(project),
        stats_dir(project),
        display_dir(project),
        exports_dir(project),
        data_dir(project),
    ):
        d.mkdir(parents=True, exist_ok=True)
    return p


def list_projects() -> list[str]:
    """Return alphabetically-sorted names of every project under projects_root.

    Skips entries that aren't directories or whose names fail validation
    (defensive against debris in ``projects/``).
    """
    root = projects_root()
    if not root.is_dir():
        return []
    out = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        try:
            validate_project_name(child.name)
        except ValueError:
            continue
        out.append(child.name)
    return out


def delete_project(name: str) -> None:
    """Permanently remove a project directory from disk.

    Clears the active-project file when it points at *name*, so the next
    ``resolve_project()`` returns None and the UI lands on the empty
    state. Raises ``ValueError`` on an invalid name and
    ``FileNotFoundError`` when the project doesn't exist.
    """
    validate_project_name(name)
    p = project_dir(name)
    if not p.is_dir():
        raise FileNotFoundError(f"project {name!r} not found")
    shutil.rmtree(p)
    if _read_active_project_file() == name:
        active_project_file_path().unlink(missing_ok=True)
