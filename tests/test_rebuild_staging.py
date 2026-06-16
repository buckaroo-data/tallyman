"""Regression: ``scripts/rebuild_parking_catalog.py`` ``_stage_sources`` must
stream the first N rows of each source, never materialise the whole file.

Truncating a source to ``--rows N`` only needs its first N rows, but the
original code read the entire parquet (``pq.read_table(src)``) and then sliced.
On the 107M-row ``parking_plt_only4`` source that full read spikes resident
memory to tens of GB — a perf-corpus-rebuild blowup the memory watchdog
(``scripts/run_under_watchdog.py``) caught mid-staging. Staging must instead
pull row-group batches until it has N rows and stop.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_rebuild():
    """Import the rebuild script as a module (it is a script, not a package).

    Registered in ``sys.modules`` before exec so the module's ``@dataclass``
    can resolve its own annotations (``dataclasses`` looks the class's module up
    in ``sys.modules`` to screen for ClassVar/InitVar).
    """
    path = REPO_ROOT / "scripts" / "rebuild_parking_catalog.py"
    spec = importlib.util.spec_from_file_location("rebuild_parking_catalog", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_stage_sources_streams_first_rows_without_full_read(project, tmp_path, monkeypatch):
    rebuild = _load_rebuild()

    # Each source has far more rows than we stage, split into many small row
    # groups so a streaming read can stop after the first few.
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    n = 40_000
    tbl = pa.table({"plate": [str(i) for i in range(n)], "v": list(range(n))})
    for name in rebuild.SOURCES:
        pq.write_table(tbl, src_dir / name, row_group_size=2_000)

    # Full materialisation is the blowup we removed: fail loudly if staging
    # reaches for pq.read_table() of the whole source.
    def _no_full_read(*args, **kwargs):
        raise AssertionError("_stage_sources must not pq.read_table() the whole source")

    monkeypatch.setattr(pq, "read_table", _no_full_read)

    rebuild._stage_sources(src_dir, project, rows=1_000)

    from tallyman_core.paths import data_dir

    for name in rebuild.SOURCES:
        staged = data_dir(project) / name
        assert pq.ParquetFile(staged).metadata.num_rows == 1_000, name
