"""The runtime check that Buckaroo's server and renderer are the versions pinned.

``test_buckaroo_version_lockstep.py`` asserts the same pins under pytest; these
cover ``check_versions``, which runs at companion startup, behind
``GET /api/version``, and is what the SPA's banner reads.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tallyman_companion import create_app

BUILD_INFO_NAME = "build-info.json"


def check_versions(*args, **kwargs):
    # Imported on call so the file is collected, and its imports sort the same, whether or not the module exists yet.
    from tallyman_core.buckaroo_versions import check_versions as real_check

    return real_check(*args, **kwargs)


def _repo(
    tmp_path: Path,
    *,
    server="0.15.10",
    js="0.15.10",
    installed="0.15.10",
    bundled="0.15.10",
    dist=True,
) -> Path:
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "x"\nversion = "0"\ndependencies = ["buckaroo=={server}"]\n'
    )
    app = tmp_path / "packages" / "app"
    app.mkdir(parents=True)
    (app / "package.json").write_text(json.dumps({"dependencies": {"buckaroo-js-core": js}}))
    if installed is not None:
        core = app / "node_modules" / "buckaroo-js-core"
        core.mkdir(parents=True)
        (core / "package.json").write_text(json.dumps({"version": installed}))
    if dist:
        (app / "dist").mkdir()
        (app / "dist" / "index.html").write_text("<html></html>")
        if bundled is not None:
            (app / "dist" / BUILD_INFO_NAME).write_text(json.dumps({"buckaroo_js_core": bundled}))
    return tmp_path


def test_everything_agreeing_reports_no_problems(tmp_path):
    report = check_versions("0.15.10", repo=_repo(tmp_path))
    assert report["problems"] == []
    assert report["seen"] == {
        "buckaroo_server": "0.15.10",
        "js_core_installed": "0.15.10",
        "js_core_bundled": "0.15.10",
    }


def test_stale_node_modules_is_named(tmp_path):
    """The 0.15.6-installed-against-0.15.10-pinned case."""
    report = check_versions("0.15.10", repo=_repo(tmp_path, installed="0.15.6", bundled="0.15.6"))
    assert any("node_modules has buckaroo-js-core 0.15.6" in p for p in report["problems"])


def test_dist_older_than_node_modules_is_named(tmp_path):
    """`pnpm install` ran after the last `pnpm build`: node_modules is right, the served bundle is not."""
    report = check_versions("0.15.10", repo=_repo(tmp_path, bundled="0.15.6"))
    assert [p for p in report["problems"] if "served dist bundles buckaroo-js-core 0.15.6" in p]
    assert not [p for p in report["problems"] if "node_modules" in p]


def test_dist_without_build_info_is_flagged(tmp_path):
    report = check_versions("0.15.10", repo=_repo(tmp_path, bundled=None))
    assert any(BUILD_INFO_NAME in p for p in report["problems"])


def test_no_dist_is_not_a_problem(tmp_path):
    """A Python-only checkout has no dist to be stale."""
    report = check_versions("0.15.10", repo=_repo(tmp_path, installed=None, dist=False))
    assert report["problems"] == []


def test_running_server_differs_from_pin(tmp_path):
    report = check_versions("0.15.6", repo=_repo(tmp_path))
    assert any("running Buckaroo server is 0.15.6" in p for p in report["problems"])


def test_no_running_server_skips_the_server_check(tmp_path):
    assert check_versions(None, repo=_repo(tmp_path))["problems"] == []


def test_source_pins_disagreeing(tmp_path):
    report = check_versions("0.15.10", repo=_repo(tmp_path, js="0.15.8", installed="0.15.8", bundled="0.15.8"))
    assert any("share a wire protocol" in p for p in report["problems"])


def test_missing_files_do_not_raise(tmp_path):
    report = check_versions(None, repo=tmp_path)
    assert report["problems"] == []
    assert report["pins"] == {"buckaroo": None, "buckaroo_js_core": None}


def test_api_version_carries_the_buckaroo_report(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    body = TestClient(create_app(project)).get("/api/version").json()
    assert set(body["buckaroo"]) == {"pins", "seen", "problems"}
    assert isinstance(body["buckaroo"]["problems"], list)


@pytest.mark.parametrize("field", ["buckaroo", "buckaroo_js_core"])
def test_this_repos_pins_are_readable(field):
    assert check_versions(None)["pins"][field]
