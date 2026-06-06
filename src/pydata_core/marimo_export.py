"""Export a pydata notebook to a Marimo notebook Python file.

Each notebook cell becomes two Marimo cells:
  1. A markdown cell (if the cell has narrative text).
  2. A code cell containing the entry's expr.py source + ``expr.execute()``
     as the final expression so Marimo displays the DataFrame inline.

The generated file is self-contained: it sets PYDATA_PROJECT via
``os.environ.setdefault`` so ``from_project`` / ``from_catalog`` resolve
correctly when the user runs it outside the pydata-app environment.
"""

from __future__ import annotations

import re
import textwrap
from pathlib import Path

from pydata_core.aliases import load_aliases, version_of_hash
from pydata_core.display_configs import get_display_config
from pydata_core.manifest import read_manifest
from pydata_core.notebook import load as load_notebook
from pydata_core.paths import entry_dir


def _safe_ident(alias: str) -> str:
    """Turn an alias into a valid Python identifier for a Marimo cell function."""
    ident = re.sub(r"[^A-Za-z0-9_]", "_", alias)
    if ident and ident[0].isdigit():
        ident = "_" + ident
    return ident or "_cell"


def _indent(text: str, prefix: str = "    ") -> str:
    return textwrap.indent(text.rstrip(), prefix)


def _marimo_version() -> str:
    try:
        import marimo  # noqa: PLC0415

        return marimo.__version__
    except ImportError:
        return "0.10.0"


def _diff_cell_lines(
    project: str,
    content_hash: str,
    diff_provenance: dict,
    display_config: dict,
) -> list[str]:
    """Generate fully-inlined marimo cell lines for a promoted diff entry.

    Both source expressions are inlined verbatim (with their hashes as
    comments) so the exported notebook is self-contained.  The outer join
    is reconstructed via build_compare_expr with the pinned keys.
    """
    source_alias = diff_provenance["source_alias"]
    va = diff_provenance["va"]
    vb = diff_provenance["vb"]
    a_hash = diff_provenance["a_hash"]
    b_hash = diff_provenance["b_hash"]
    keys = diff_provenance["keys"]
    column_config_overrides = display_config.get("column_config_overrides", {})

    a_expr_py = (entry_dir(project, a_hash) / "expr.py").read_text().strip()
    b_expr_py = (entry_dir(project, b_hash) / "expr.py").read_text().strip()

    def _ind(text: str) -> list[str]:
        return [f"    {line}" if line.strip() else "" for line in text.splitlines()]

    overrides_repr = repr(column_config_overrides)
    keys_repr = repr(keys)

    lines = [
        "    import os",
        f'    os.environ.setdefault("PYDATA_PROJECT", {project!r})',
        "",
        f"    # {source_alias} V{va} (catalog entry {a_hash})",
    ]
    lines += _ind(a_expr_py)
    lines += [
        "    a_expr = expr",
        "",
        f"    # {source_alias} V{vb} (catalog entry {b_hash})",
    ]
    lines += _ind(b_expr_py)
    lines += [
        "    b_expr = expr",
        "",
        f"    # outer-join diff — keys: {keys_repr}  (catalog diff entry {content_hash})",
        "    from pydata_companion.diff import build_compare_expr",
        f"    _expr, _overrides = build_compare_expr(a_expr, b_expr, keys={keys_repr})",
        f"    _overrides = {overrides_repr}",
        "",
        "    from buckaroo.xorq_buckaroo import XorqBuckarooInfiniteWidget",
        "    _widget = XorqBuckarooInfiniteWidget(_expr, column_config_overrides=_overrides)",
        "    return (_widget,)",
    ]
    return lines


def notebook_to_marimo(project: str) -> str:
    """Return the contents of a Marimo ``.py`` notebook for *project*.

    Reads the default notebook (``notebooks/default.json``), resolves each
    alias to its current entry, and emits a Marimo cell pair per notebook
    cell.  Raises ``FileNotFoundError`` if an entry's ``expr.py`` is missing.
    """
    data = load_notebook(project)
    aliases = load_aliases(project)

    parts: list[str] = []

    # ---- file header -------------------------------------------------------
    parts.append("import marimo")
    parts.append("")
    parts.append(f'__generated_with = "{_marimo_version()}"')
    parts.append('app = marimo.App(width="medium")')
    parts.append("")

    # ---- bootstrap cell (imports mo) ---------------------------------------
    parts += [
        "",
        "@app.cell",
        "def _():",
        "    import marimo as mo",
        "    return (mo,)",
    ]

    # ---- title cell --------------------------------------------------------
    parts += [
        "",
        "",
        "@app.cell",
        "def _(mo):",
        f'    mo.md(r"""# notebook · {project}',
        "",
        "Exported from pydata-app. Each cell runs its xorq expression and",
        "displays the result DataFrame. Set PYDATA_PROJECT if the environment",
        "is not already configured.",
        '    """)',
        "    return ()",
    ]

    # ---- one pair of cells per notebook cell --------------------------------
    for c in data.get("cells", []):
        alias = c["alias"]
        content_hash = aliases.get(alias)
        if content_hash is None:
            continue

        edir = entry_dir(project, content_hash)
        expr_py = edir / "expr.py"
        if not expr_py.exists():
            continue

        code = expr_py.read_text().strip()
        markdown = (c.get("markdown") or "").strip()
        fn_name = _safe_ident(alias)

        try:
            mf = read_manifest(edir)
            row_info = f"{mf.row_count:,} rows" if mf.row_count else ""
        except Exception:
            row_info = ""

        version_info = version_of_hash(project, content_hash)
        v_label = f"V{version_info[1]}" if version_info else ""

        # -- markdown cell ---------------------------------------------------
        md_header = f"**{alias}**"
        if v_label:
            md_header += f"  ·  {v_label}"
        if row_info:
            md_header += f"  ·  {row_info}"

        md_body = markdown if markdown else ""
        md_content = md_header
        if md_body:
            md_content += "\n\n" + md_body

        # Escape triple-quotes inside markdown
        md_content = md_content.replace('"""', r"\"\"\"")

        parts += [
            "",
            "",
            "@app.cell",
            "def _(mo):",
            '    mo.md(r"""',
            textwrap.indent(md_content, "    "),
            '    """)',
            "    return ()",
        ]

        # -- code cell -------------------------------------------------------
        display_config = get_display_config(project, content_hash)
        diff_provenance = (display_config or {}).get("diff_provenance")

        if diff_provenance:
            code_lines = _diff_cell_lines(project, content_hash, diff_provenance, display_config)
        else:
            code_lines = [
                "    import os",
                f'    os.environ.setdefault("PYDATA_PROJECT", {project!r})',
                "",
            ]
            code_lines += [f"    {line}" if line.strip() else "" for line in code.splitlines()]
            code_lines += [
                "",
                "    _df = expr.execute()",
                "    return (_df,)",
            ]

        parts += [
            "",
            "",
            "@app.cell",
            f"def {fn_name}():",
        ] + code_lines

    # ---- footer ------------------------------------------------------------
    parts += [
        "",
        "",
        'if __name__ == "__main__":',
        "    app.run()",
        "",
    ]

    return "\n".join(parts)


def export_notebook_path(project: str) -> Path:
    """Write the Marimo export to ``<project_dir>/notebook_marimo.py`` and return the path."""
    from pydata_core.paths import project_dir  # noqa: PLC0415

    content = notebook_to_marimo(project)
    out = project_dir(project) / "notebook_marimo.py"
    out.write_text(content)
    return out
