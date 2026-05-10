"""Make xorq builds portable across machines/users.

xorq writes absolute filesystem paths into its build artifacts (see
`expr.yaml` → `read_kwargs.hash_path`). On a colleague's machine those paths
don't exist, and `load_expr` fails. The fix is symmetric:

- **On write**, replace every `<project_root>/...` substring with the
  placeholder `${PYDATA_PROJECT_ROOT}/...` before persisting the build under
  the catalog entry. The xorq-internal node names (which embed a hash of
  the original path) are left alone — xorq doesn't re-validate those at load
  time, they're just identifiers.
- **On load**, copy the persisted build into a tmp dir, expand the
  placeholder back to the current machine's absolute project root, then call
  `xorq.ibis_yaml.compiler.load_expr` on the tmp dir.

The verified-against-xorq experiment that established this is the docstring
of `_load_expr_portable` below; if you're touching this module, re-run that
experiment in a python repl before pushing.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

PLACEHOLDER = "${PYDATA_PROJECT_ROOT}"


def make_portable_inplace(build_dir: Path, project_root: Path) -> int:
    """Replace `project_root` substrings in every file under `build_dir`.

    Returns the number of files modified.
    """
    project_root_str = str(project_root)
    modified = 0
    for f in build_dir.iterdir():
        if not f.is_file():
            continue
        try:
            text = f.read_text()
        except UnicodeDecodeError:
            continue  # binary file, skip
        if project_root_str not in text:
            continue
        f.write_text(text.replace(project_root_str, PLACEHOLDER))
        modified += 1
    return modified


def expand_to_tmp(build_dir: Path, project_root: Path) -> Path:
    """Copy `build_dir` to a tmp dir, expanding placeholders. Caller must clean up."""
    target = Path(tempfile.mkdtemp(prefix="pydata_load_"))
    project_root_str = str(project_root)
    for f in build_dir.iterdir():
        if not f.is_file():
            continue
        try:
            text = f.read_text()
        except UnicodeDecodeError:
            shutil.copyfile(f, target / f.name)
            continue
        (target / f.name).write_text(text.replace(PLACEHOLDER, project_root_str))
    return target


def load_expr_portable(build_dir: Path, project_root: Path, cache_dir):
    """Wrap xorq.load_expr so it sees absolute paths even when the persisted build is portable."""
    from xorq.ibis_yaml.compiler import load_expr

    tmp = expand_to_tmp(build_dir, project_root)
    try:
        return load_expr(tmp, cache_dir=cache_dir)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
