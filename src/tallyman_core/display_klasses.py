"""Project-authored display klasses (ColAnalysis subclasses with df_display_name).

Each file under artifacts/display/ is exec'd by buckaroo's server at
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

from pathlib import Path

from tallyman_core.paths import display_dir


class DisplayKlassError(ValueError):
    """Raised when a display klass file fails validation."""


def _safe_builtins() -> dict:
    import builtins
    names = (
        "True", "False", "None", "abs", "min", "max", "round", "sum", "len",
        "int", "float", "str", "bool", "list", "tuple", "dict", "set",
        "range", "enumerate", "zip", "map", "filter",
        "isinstance", "issubclass", "type", "getattr", "hasattr",
    )
    safe = {n: getattr(builtins, n) for n in names}
    safe["__build_class__"] = __build_class__
    safe["classmethod"] = classmethod
    safe["staticmethod"] = staticmethod
    safe["super"] = super
    return safe


def validate_display_klass_source(name: str, source: str) -> None:
    """Exec source with the standard base classes in scope and verify at least
    one qualifying ColAnalysis subclass with ``df_display_name`` is produced."""
    if not name.isidentifier():
        raise DisplayKlassError(f"{name!r} is not a valid Python identifier")

    try:
        compile(source, f"{name}.py", "exec")
    except SyntaxError as e:
        raise DisplayKlassError(f"syntax error: {e}") from None

    try:
        from buckaroo.customizations.styling import DefaultMainStyling, DefaultSummaryStatsStyling, StylingAnalysis
        from buckaroo.pluggable_analysis_framework.col_analysis import ColAnalysis
    except ImportError as e:
        raise DisplayKlassError(f"buckaroo not installed: {e}") from None

    globs: dict = {
        "__builtins__": _safe_builtins(),
        "__name__": name,
        "ColAnalysis": ColAnalysis,
        "DefaultMainStyling": DefaultMainStyling,
        "DefaultSummaryStatsStyling": DefaultSummaryStatsStyling,
        "StylingAnalysis": StylingAnalysis,
    }

    try:
        exec(compile(source, f"{name}.py", "exec"), globs)
    except Exception as e:
        raise DisplayKlassError(f"source raised at exec time: {e!r}") from None

    injected_ids = {id(ColAnalysis), id(DefaultMainStyling),
                    id(DefaultSummaryStatsStyling), id(StylingAnalysis)}
    found = [
        obj for obj in globs.values()
        if (isinstance(obj, type)
            and id(obj) not in injected_ids
            and issubclass(obj, ColAnalysis)
            and isinstance(getattr(obj, "df_display_name", None), str))
    ]
    if not found:
        raise DisplayKlassError(
            "source must define at least one class that subclasses ColAnalysis "
            "and has a string df_display_name attribute")


def write_display_klass(project: str, name: str, source: str) -> Path:
    """Validate then persist. Returns the path of the written file.

    Overwrites an existing same-named file — that is the intended path for
    editing a display klass, since there is no separate update tool.
    """
    validate_display_klass_source(name, source)
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
