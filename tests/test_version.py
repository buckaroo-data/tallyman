from __future__ import annotations

from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_core.version import git_revision, version_info

# tallyman has no releases yet, so the version is the git revision. Each
# long-lived component stamps the source it runs from so drift is visible
# (the stale-bundle class of bug behind #132): the companion reports its
# revision at GET /api/version and on an X-Tallyman-Revision response header.


def test_git_revision_is_nonempty_token():
    rev = git_revision()
    assert rev and " " not in rev  # a sha (optionally -dirty), or "unknown"


def test_version_info_shape():
    info = version_info()
    assert set(info) >= {"revision", "dirty"}
    assert isinstance(info["dirty"], bool)
    assert info["revision"] == git_revision()


def test_api_version_reports_companion_revision(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(create_app(project))
    r = c.get("/api/version")
    assert r.status_code == 200
    body = r.json()
    assert body["component"] == "companion"
    assert body["revision"] == git_revision()
    assert "dirty" in body


class _BuckarooAt:
    """A BuckarooManager-shaped stub whose server reports a fixed version."""

    is_running = True

    def __init__(self, version: str | None):
        self._version = version

    def server_version(self) -> str | None:
        return self._version


def test_api_version_reports_the_buckaroo_server_version(project, orders_parquet, monkeypatch):
    # The SPA compares this with the renderer it is running and the version package.json pins.
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(create_app(project, buckaroo=_BuckarooAt("0.15.10")))  # type: ignore[arg-type]
    assert c.get("/api/version").json()["buckaroo_server"] == "0.15.10"


def test_api_version_has_no_buckaroo_server_when_buckaroo_is_not_running(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(create_app(project))
    assert c.get("/api/version").json()["buckaroo_server"] is None


def test_revision_header_rides_every_response(project, orders_parquet, monkeypatch):
    monkeypatch.setenv("TALLYMAN_PROJECT", project)
    c = TestClient(create_app(project))
    # The stamp is on all responses, not just /api/version — so any request the
    # SPA makes carries the backend's revision for the drift check.
    r = c.get("/api/projects")
    assert r.headers.get("x-tallyman-revision") == git_revision()
