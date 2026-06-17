from __future__ import annotations

from fastapi.testclient import TestClient

import tallyman_companion.app as app_mod
from tallyman_core.paths import catalog_dir


def _write(path, blob: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(blob)


def test_disk_usage_counts_diff_cache(fresh_companion_app, project, orders_parquet):
    # The per-pair Buckaroo diff stat cache (diff_stat_cache/, #12) was omitted
    # by the endpoint and can be the largest item on disk. (The retired
    # result_cache/ dir is gone as of #73 — baked results live in the compute
    # cache, counted under builds/entries.)
    _write(catalog_dir(project) / "diff_stat_cache" / "aaaaaaaaaaaa-bbbbbbbbbbbb" / "stats.arrow", b"y" * 2048)

    d = TestClient(fresh_companion_app).get(f"/{project}/api/disk_usage").json()

    assert d["diff_cache"] >= 2048
    assert "diff_cache" in d["formatted"]
    assert "result_cache" not in d and "result_cache" not in d["formatted"]
    # No per-entry "results" bucket any more — no result.parquet is written, and
    # baked snapshots are counted under compute_cache.
    assert "results" not in d and "results" not in d["formatted"]
    # It folds into the headline total.
    assert d["total"] == d["data"] + d["builds"] + d["cache"] + d["diff_cache"]


def test_disk_usage_counts_compute_cache(fresh_companion_app, project, orders_parquet):
    # #87 / #74 follow-up: baked result snapshots and source-read caches live in
    # the per-project compute_cache/, which the endpoint omitted — so the
    # headline total under-reported the cache #74 introduced (and the byte
    # denominator the cost rubric needs went uncounted). Count it.
    from tallyman_core.paths import compute_cache_dir

    _write(compute_cache_dir(project) / "result_cache" / "snap.parquet", b"z" * 4096)

    d = TestClient(fresh_companion_app).get(f"/{project}/api/disk_usage").json()

    assert d["compute_cache"] >= 4096
    assert "compute_cache" in d["formatted"]
    # It folds into the headline total alongside the other cache buckets.
    assert d["total"] == (
        d["data"] + d["builds"] + d["cache"] + d["diff_cache"] + d["compute_cache"]
    )


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
