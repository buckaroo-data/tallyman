from __future__ import annotations

from fastapi.testclient import TestClient

import tallyman_companion.app as app_mod
from tallyman_core.paths import catalog_dir, result_cache_dir


def _write(path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)


def test_disk_usage_counts_result_cache_and_diff_cache(fresh_companion_app, project, orders_parquet):
    # Two project-level caches added in #12 that the endpoint omitted: xorq's
    # ParquetSnapshotCache (result_cache/) and the per-pair Buckaroo diff stat
    # cache (diff_stat_cache/). Both can be the largest item on disk.
    _write(result_cache_dir(project) / "snap.parquet", b"x" * 4096)
    _write(catalog_dir(project) / "diff_stat_cache" / "aaaaaaaaaaaa-bbbbbbbbbbbb" / "stats.arrow", b"y" * 2048)

    d = TestClient(fresh_companion_app).get(f"/{project}/api/disk_usage").json()

    assert d["result_cache"] >= 4096
    assert d["diff_cache"] >= 2048
    assert "result_cache" in d["formatted"] and "diff_cache" in d["formatted"]
    # Both fold into the headline total.
    assert d["total"] == d["data"] + d["results"] + d["builds"] + d["cache"] + d["result_cache"] + d["diff_cache"]


def test_disk_usage_coalesces_repeated_walks(fresh_companion_app, project, orders_parquet, monkeypatch):
    # The pill is refetched on every page mount and every new_entry SSE event,
    # across tabs. Within the TTL those bursts must share one filesystem walk
    # instead of each re-walking the whole project.
    calls = {"n": 0}
    real = app_mod._dir_size

    def counting(path):
        calls["n"] += 1
        return real(path)

    monkeypatch.setattr(app_mod, "_dir_size", counting)

    c = TestClient(fresh_companion_app)
    c.get(f"/{project}/api/disk_usage")
    after_first = calls["n"]
    assert after_first > 0  # the first call genuinely walks
    c.get(f"/{project}/api/disk_usage")
    assert calls["n"] == after_first  # second call served from cache, no new walk
