"""Project-authored display klasses (ColAnalysis subclasses with df_display_name).

Each file under artifacts/catalog/display/ is exec'd by buckaroo's server at
session load and reload time.  Any class found that subclasses ColAnalysis
and carries a ``df_display_name`` string overrides the built-in styling for
that display view.

The canonical use is to extend ``DefaultMainStyling`` (df_display_name="main")
or ``DefaultSummaryStatsStyling`` (df_display_name="summary") with additional
pinned rows for project-specific summary stats.

Pinned-row entry format (from ``buckaroo.styling_helpers``)::

    {'primary_key_val': '<stat_name>', 'displayer_args': {'displayer': 'inherit'}}

``displayer: 'inherit'`` picks the display format from the column's type
(string → string displayer, numeric → float displayer, etc.).
"""

from __future__ import annotations

import tempfile
import traceback
from pathlib import Path

from tallyman_core.execution import execution_lock
from tallyman_core.paths import buckaroo_project_root, display_dir

# How much of each entry the render check styles: the first rows of the most recently built entries that have a
# snapshot on disk. Enough for buckaroo to compute every stat a column's metadata carries; the check never computes
# or heals an entry.
SAMPLE_ROWS = 100
SAMPLE_ENTRIES = 3


class DisplayKlassError(ValueError):
    """Raised when a display klass file fails validation."""


def validate_display_klass_source(name: str, source: str, project: str | None = None) -> None:
    """Load the source the way buckaroo's server does, then style sample columns with each class it defines.

    Raises ``DisplayKlassError`` if the file wouldn't load, defines no qualifying class, or a class fails on a column.
    The samples are a synthetic column of each dtype and, with *project*, the first rows of its latest entries.
    """
    if not name.isidentifier():
        raise DisplayKlassError(f"{name!r} is not a valid Python identifier")

    try:
        compile(source, f"{name}.py", "exec")
    except SyntaxError as e:
        raise DisplayKlassError(f"syntax error: {e}") from None

    try:
        from buckaroo.server import xorq_loading
    except ImportError as e:
        raise DisplayKlassError(f"buckaroo not installed: {e}") from None

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"{name}.py"
        path.write_text(source)
        try:
            klasses = _compile_like_buckaroo(xorq_loading, path)
        except Exception as e:
            raise DisplayKlassError(f"source raised at exec time: {e!r}") from None
    if not klasses:
        raise DisplayKlassError(
            "source must define at least one class that subclasses ColAnalysis "
            "and has a string df_display_name attribute")

    # The project's stats go in as buckaroo's session gets them, so a class may read them from column_metadata.
    stat_klasses = xorq_loading.load_project_stat_klasses(buckaroo_project_root(project)) if project else []
    for label, table in _samples(project):
        try:
            _check_styling(xorq_loading, klasses, stat_klasses, table)
        except _StylingFailed as f:
            raise DisplayKlassError(_describe(f, label, str(path), xorq_loading)) from None


def _compile_like_buckaroo(xorq_loading, path: Path) -> list[type]:
    """buckaroo's own display loader: the same exec, sandbox builtins and base classes a session load uses."""
    from buckaroo.customizations.styling import DefaultMainStyling, DefaultSummaryStatsStyling, StylingAnalysis
    from buckaroo.pluggable_analysis_framework.col_analysis import ColAnalysis

    return xorq_loading._compile_project_display(
        path, ColAnalysis, DefaultMainStyling, DefaultSummaryStatsStyling, StylingAnalysis)


def _samples(project: str | None) -> list[tuple[str, object]]:
    """``(label, pyarrow table)`` pairs: one synthetic column per dtype, then the latest entries' first rows."""
    import datetime as dt

    import pyarrow as pa

    synthetic = pa.table({
        "integer": pa.array([1, 2, None], pa.int64()),
        "float": pa.array([1.5, None, 3.25], pa.float64()),
        "string": pa.array(["a", None, "c"], pa.string()),
        "boolean": pa.array([True, None, False], pa.bool_()),
        "timestamp": pa.array([dt.datetime(2024, 1, 1), None, dt.datetime(2024, 1, 3)], pa.timestamp("us")),
        "date": pa.array([dt.date(2024, 1, 1), None, dt.date(2024, 1, 3)], pa.date32()),
        "all_null": pa.array([None, None, None], pa.null()),
    })
    out: list[tuple[str, object]] = [("a synthetic sample (one column per dtype)", synthetic)]
    if project is None:
        return out

    import pyarrow.parquet as pq

    from tallyman_core.aliases import alias_for_hash
    from tallyman_xorq.build import list_entries
    from tallyman_xorq.materialize import snapshot_path

    for entry in list_entries(project):
        if len(out) > SAMPLE_ENTRIES:
            break
        content_hash = entry["content_hash"]
        snap = snapshot_path(project, content_hash)
        if not snap.exists():
            continue
        try:
            head = next(pq.ParquetFile(snap).iter_batches(batch_size=SAMPLE_ROWS), None)
        except Exception:
            continue  # an unreadable snapshot is the viewer's problem to report, not this check's
        if head is None:
            continue
        label = alias_for_hash(project, content_hash) or content_hash[:12]
        out.append((f"entry {label} (first {head.num_rows} rows)", pa.Table.from_batches([head])))
    return out


class _StylingFailed(Exception):
    def __init__(self, klass: type, exc: BaseException, column=None, column_metadata=None):
        super().__init__(repr(exc))
        self.klass, self.exc, self.column, self.column_metadata = klass, exc, column, column_metadata


def _check_styling(xorq_loading, klasses: list[type], stat_klasses: list, table) -> None:
    """Build buckaroo's server dataflow over *table*, then style its columns with each class, raising on a failure.

    PROTOTYPE of an API that belongs in buckaroo. buckaroo builds the summary stats and styles every column here the
    way a session does; the one change is the strict subclass below. buckaroo's
    ``StylingAnalysis.style_column_with_fallback`` catches a failing ``style_column``, logs it and falls back to the
    parent class, so a render never tells its caller that a class failed. Replace this with buckaroo's own strict
    styling check once it has one.
    """
    import xorq.api as xo

    try:
        with execution_lock():  # the dataflow runs its stat queries on the shared backend (#118)
            dataflow = xorq_loading.XorqServerDataflow(
                xo.memtable(table), skip_main_serial=True, extra_klasses=list(stat_klasses) + list(klasses))
            _, processed_df, merged_sd = dataflow.widget_args_tuple
    except Exception as exc:
        raise _StylingFailed(klasses[0], exc) from exc

    for klass in klasses:
        class Strict(klass):
            @classmethod
            def style_column_with_fallback(cls, col, col_meta, orig_col_name, _klass=klass):
                try:
                    return cls.fix_column_config(col, orig_col_name, cls.style_column(col, dict(col_meta)))
                except Exception as exc:
                    raise _StylingFailed(_klass, exc, orig_col_name, col_meta) from exc

        try:
            Strict.get_dfviewer_config(merged_sd, processed_df)
        except _StylingFailed:
            raise
        except Exception as exc:
            raise _StylingFailed(klass, exc) from exc


def _describe(f: _StylingFailed, label: str, filename: str, xorq_loading) -> str:
    lines = [frame.lineno for frame in traceback.extract_tb(f.exc.__traceback__) if frame.filename == filename]
    at = f" at line {lines[-1]}" if lines else ""
    if f.column is None:
        msg = f"buckaroo failed to render {label} with {f.klass.__qualname__}: {f.exc!r}{at}"
    else:
        msg = f"{f.klass.__qualname__}.style_column raised {f.exc!r}{at} on column {f.column!r} of {label}"
    if isinstance(f.exc, NameError):
        allowed = (*xorq_loading._SAFE_BUILTIN_NAMES, "classmethod", "staticmethod", "super")
        msg += f". buckaroo runs display klasses with only these builtins: {', '.join(allowed)}"
    elif isinstance(f.exc, KeyError) and f.column_metadata is not None:
        msg += f". That column's column_metadata has: {', '.join(sorted(map(str, f.column_metadata)))}"
    return msg + ". Nothing was written."


def write_display_klass(project: str, name: str, source: str) -> Path:
    """Validate then persist. Returns the path of the written file.

    Overwrites an existing same-named file — that is the intended path for
    editing a display klass, since there is no separate update tool.
    """
    validate_display_klass_source(name, source, project)
    d = display_dir(project)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.py"
    path.write_text(source if source.endswith("\n") else source + "\n")
    return path


def remove_display_klass(project: str, name: str) -> Path | None:
    """Soft-delete by moving ``display/<name>.py`` to ``display/_disabled/<name>.py``.

    Returns the new path, or None if the klass didn't exist.
    """
    src = display_dir(project) / f"{name}.py"
    if not src.exists():
        return None
    dd = display_dir(project) / "_disabled"
    dd.mkdir(parents=True, exist_ok=True)
    dst = dd / f"{name}.py"
    if dst.exists():
        dst.unlink()
    src.rename(dst)
    return dst


def list_display_klasses(project: str) -> list[dict]:
    """Return ``[{name, path, source, disabled}]`` sorted active-first."""
    d = display_dir(project)
    results: list[dict] = []
    if d.is_dir():
        for path in sorted(d.glob("*.py")):
            if path.name.startswith("_"):
                continue
            results.append({
                "name": path.stem,
                "path": str(path),
                "source": path.read_text(),
                "disabled": False,
            })
    disabled = d / "_disabled"
    if disabled.is_dir():
        for path in sorted(disabled.glob("*.py")):
            results.append({
                "name": path.stem,
                "path": str(path),
                "source": path.read_text(),
                "disabled": True,
            })
    return results
