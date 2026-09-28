"""The Buckaroo hand-off: tallyman gives Buckaroo files that already exist, and remembers no sessions.

Red tests for ``plans/ADR-007-tallyman-owned-materialization.md`` D6 (Buckaroo is handed something that already exists)
and D12 (files are deleted only by an explicit user action), plus the payload hint of
``plans/ADR-008-row-order-of-reads.md`` D8 (Buckaroo's half of the row-order contract).

Buckaroo is faked with an ``httpx.MockTransport``. The fake follows the parts of ``buckaroo/server/handlers.py``
(0.15.6) that tallyman relies on: the session id comes from the POST body, and a ``/load_expr`` is a warm hit (the
pipeline is not re-run) only when that id already exists with the same ``build_dir``, ``force_reload`` is not set, and
the body carries none of ``component_config``, ``column_config_overrides``, ``extra_grid_config``, ``init_sd`` and
``skip_stat_columns``. ``/reload_expr/<id>`` answers 404 for a session Buckaroo does not have.
"""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from tallyman_companion import create_app
from tallyman_companion.buckaroo_lifecycle import BuckarooManager
from tallyman_core import (
    entry_dir,
    entry_expanded_build_dir,
    entry_manifest_path,
    entry_stat_cache_dir,
    get_alias,
    read_manifest,
)
from tallyman_core.paths import compute_cache_dir, tallyman_home
from tallyman_xorq import build_and_persist

_CONFIG_KEYS = ("component_config", "column_config_overrides", "extra_grid_config", "init_sd", "skip_stat_columns")


def _agg_code(project: str) -> str:  # an Aggregate: a worthy entry, materialized to a snapshot when it is created
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("region").aggregate(total=t.price.sum(), n=t.count())
"""


def _second_agg_code(project: str) -> str:  # a different worthy entry in the same project
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.group_by("category").aggregate(n=t.count())
"""


def _cheap_code(project: str) -> str:  # a filter over one source alias: a cheap entry, re-run on every read
    return f"""
from tallyman_xorq.io import tracked_expr_from_alias
t = tracked_expr_from_alias("orders_src", project={project!r})
expr = t.filter(t.price > 0)
"""


def _sid(project: str, content_hash: str) -> str:
    """The session id ADR-007 D6 derives from the project and the content hash."""
    return f"entry-{project}-{content_hash}"


# Names this change introduces are imported inside one-line helpers. Imported at module level, or next to other imports
# in one block, they would be classified by ruff's isort as third-party until the module exists, and the lint job that
# gates the test jobs on CI would fail before any test ran.


def _snapshot_path(project: str, content_hash: str) -> Path:
    from tallyman_xorq.materialize import snapshot_path

    return snapshot_path(project, content_hash)


def _view_build_dir(project: str, content_hash: str) -> Path:
    from tallyman_core.paths import entry_view_build_dir

    return entry_view_build_dir(project, content_hash)


class _AliveProc:
    def poll(self):
        return None  # looks alive, so BuckarooManager.is_running is True


class FakeBuckaroo:
    """The slice of Buckaroo's HTTP API that tallyman uses, keeping its own sessions and a log of every request."""

    def __init__(self) -> None:
        self.sessions: dict[str, dict] = {}
        self.requests: list[dict] = []
        self.warm_hits = 0
        # Called with the parsed body when a /load_expr arrives, before it is answered: lets a test look at the
        # disk at the moment "Buckaroo is called".
        self.on_load = None

    def loads(self) -> list[dict]:
        return [r["body"] for r in self.requests if r["path"] == "/load_expr"]

    def reloads(self) -> list[str]:
        return [r["path"] for r in self.requests if r["path"].startswith("/reload_expr/")]

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        self.requests.append({"method": request.method, "path": path, "body": body})
        if path == "/load_expr":
            if self.on_load is not None:
                self.on_load(body)
            session = body.get("session") or uuid.uuid4().hex
            existing = self.sessions.get(session)
            has_config = any(body.get(k) for k in _CONFIG_KEYS)
            same_build = bool(existing) and existing["build_dir"] == body["build_dir"]
            if same_build and not body.get("force_reload") and not has_config:
                self.warm_hits += 1
                return httpx.Response(200, json={"session": session})
            self.sessions[session] = {"build_dir": body["build_dir"]}
            return httpx.Response(200, json={"session": session})
        if path.startswith("/reload_expr/"):
            if path.rsplit("/", 1)[1] not in self.sessions:
                return httpx.Response(404, json={"error": "unknown session"})
            return httpx.Response(200, json={})
        return httpx.Response(404, json={"error": f"unhandled {path}"})


def _manager(fake: FakeBuckaroo) -> BuckarooManager:
    bk = BuckarooManager()
    bk.proc = _AliveProc()
    bk.bound_port = 8799
    bk._maybe_restart = lambda: None
    bk._client = httpx.Client(transport=httpx.MockTransport(fake.handler))
    return bk


# ---------------------------------------------------------------------------
# sessions are derived, never remembered (ADR-007 D6)
# ---------------------------------------------------------------------------


def test_a_session_buckaroo_forgot_is_posted_again(project, orders_src):
    """ADR-007 D6 (Buckaroo is handed something that already exists): Buckaroo drops a session that has had no
    browser attached for an hour, and tallyman used to answer the next open from its own session map, handing the grid
    an id Buckaroo no longer knew. Now every open posts ``/load_expr`` with the derived id; Buckaroo's own warm-hit
    rule makes the repeat cheap when it still has the session."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)

    assert bk.load_session(h, project)["status"] == "ok"
    assert len(fake.loads()) == 1

    fake.sessions.clear()  # Buckaroo evicts the idle session
    result = bk.load_session(h, project)

    assert result["status"] == "ok"
    assert len(fake.loads()) == 2, "tallyman answered from its own session map instead of posting to Buckaroo"
    assert fake.loads()[1]["session"] == _sid(project, h)
    assert _sid(project, h) in fake.sessions
    assert result["session_id"] == _sid(project, h)


def test_a_repeat_open_of_an_ordinary_entry_is_a_warm_hit_in_buckaroo(project, orders_src):
    """ADR-007 D6: an ordinary entry posts none of the config-bearing fields and the same ``build_dir`` every time, so
    the second post is a no-op inside Buckaroo. This is what replaces tallyman's session map."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)

    assert bk.load_session(h, project)["status"] == "ok"
    assert bk.load_session(h, project)["status"] == "ok"

    assert len(fake.loads()) == 2, "the second open never reached Buckaroo"
    assert fake.warm_hits == 1


def test_session_id_is_derived_from_the_project_and_the_hash(isolated_home):
    """ADR-007 D6: the id is a function of the project and the content hash. Putting the project in it also closes
    #172, one project's session served to another on a hash collision."""
    bk = BuckarooManager()

    assert bk.session_id_for("proj", "abc123") == "entry-proj-abc123"
    assert bk.session_id_for("one", "abc123") != bk.session_id_for("two", "abc123")


def test_tallyman_keeps_no_record_of_buckaroo_sessions(project, orders_src):
    """ADR-007 D6: ``_sessions``, ``evict_session``, ``~/.tallyman/buckaroo_sessions.json`` and the session count are
    retired, because nothing needs to know which grids are open."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    bk = _manager(FakeBuckaroo())

    assert bk.load_session(h, project)["status"] == "ok"

    assert not (tallyman_home() / "buckaroo_sessions.json").exists(), "a session file was written"
    assert not hasattr(bk, "_sessions")
    assert not hasattr(bk, "evict_session")
    assert "session_count" not in bk.status()


# ---------------------------------------------------------------------------
# what Buckaroo is handed (ADR-007 D6)
# ---------------------------------------------------------------------------


def test_worthy_entry_is_handed_a_view_build_of_its_snapshot(project, orders_src):
    """ADR-007 D6: a worthy entry's grid is posted a *view build*: a build whose whole graph is one bare read of the
    entry's snapshot. Buckaroo then never runs the aggregate, join or sort, and never writes a snapshot."""
    import xorq.vendor.ibis.expr.operations as ops
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import Read
    from xorq.ibis_yaml.compiler import load_expr
    from xorq.vendor.ibis.expr.operations.core import Node

    h = build_and_persist(project, _agg_code(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)
    assert bk.load_session(h, project)["status"] == "ok"

    posted = Path(fake.loads()[0]["build_dir"])
    loaded = load_expr(posted)
    relations = sorted({type(n).__name__ for n in walk_nodes((Node,), loaded) if isinstance(n, ops.Relation)})
    assert relations == ["Read"], f"the posted build does more than read a file: {relations}"

    view_dir = _view_build_dir(project, h)
    assert posted == view_dir or view_dir in posted.parents, f"{posted} is not under {view_dir}"
    reads = list(walk_nodes(Read, loaded))
    assert len(reads) == 1
    assert Path(dict(reads[0].read_kwargs)["hash_path"]).resolve() == _snapshot_path(project, h).resolve()


def test_view_build_directory_is_stable_across_opens(project, orders_src):
    """ADR-007 D6: Buckaroo's stat-cache keys include the build directory's path, so the view build is written once
    to a stable per-entry directory and the same path is posted every time."""
    h = build_and_persist(project, _agg_code(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)

    assert bk.load_session(h, project)["status"] == "ok"
    fake.sessions.clear()
    assert bk.load_session(h, project)["status"] == "ok"

    bodies = fake.loads()
    assert len(bodies) == 2, "the second open never reached Buckaroo"
    assert bodies[0]["build_dir"] == bodies[1]["build_dir"]


def test_cheap_entry_is_handed_its_own_expanded_build_over_files_that_exist(project, orders_src):
    """ADR-007 D6 and D13: a cheap entry is a stored plan over files that exist. ``load_session`` makes them exist
    first (``ensure_materialized``), so with every cached copy of the data deleted the build posted to Buckaroo
    still reads files that are there at the moment it is posted.

    The clone of the imported bytes under ``data/.cas`` stays. It is what a source version's snapshot is written
    again from (ADR-011 D1), so deleting it as well would make the rows unrecoverable instead of exercising the
    heal.
    """
    from xorq.common.utils.graph_utils import walk_nodes
    from xorq.expr.relations import Read
    from xorq.ibis_yaml.compiler import load_expr

    h = build_and_persist(project, _cheap_code(project)).content_hash
    shutil.rmtree(compute_cache_dir(project), ignore_errors=True)

    reads_at_post: list[Path] = []

    def at_post(body: dict) -> None:
        loaded = load_expr(Path(body["build_dir"]))
        reads_at_post.extend(Path(dict(r.read_kwargs)["hash_path"]) for r in walk_nodes(Read, loaded))

    fake = FakeBuckaroo()
    fake.on_load = at_post
    bk = _manager(fake)
    assert bk.load_session(h, project)["status"] == "ok"

    assert fake.loads()[0]["build_dir"] == str(entry_expanded_build_dir(project, h))
    assert reads_at_post, "the posted build reads no file at all"
    missing = [p for p in reads_at_post if not p.exists()]
    assert not missing, f"Buckaroo was handed a build that reads files which do not exist: {missing}"


@pytest.mark.parametrize("recipe", [_agg_code, _cheap_code], ids=["worthy", "cheap"])
def test_every_load_expr_body_names_the_row_order_column(project, orders_src, recipe):
    """ADR-008 D8 (Buckaroo's half is one hint): every ``/load_expr`` payload carries ``row_order_column`` so that
    Buckaroo can order and page by it (buckaroo-data/buckaroo#974). An ordinary entry sends none of the fields that
    would defeat Buckaroo's warm-hit short-circuit."""
    h = build_and_persist(project, recipe(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)

    assert bk.load_session(h, project)["status"] == "ok"

    body = fake.loads()[0]
    assert body.get("row_order_column") == "__row_order"
    assert body.get("session") == _sid(project, h)
    assert not any(body.get(k) for k in _CONFIG_KEYS)


# ---------------------------------------------------------------------------
# a deleted or unfaithful file is put right before Buckaroo is called (ADR-007 D5, D6, D12)
# ---------------------------------------------------------------------------


def test_explicit_delete_then_open_heals_and_verifies_before_buckaroo_is_called(project, orders_src):
    """ADR-007 D12 (files are deleted only by an explicit user action) and D5 (``ensure_materialized``): the Cache
    page's delete removes the snapshot, and the next open rewrites it and checks it against the recorded digest
    before the grid is posted. Buckaroo never has to repair a file."""
    from tallyman_xorq.result_cache import baked_snapshot_path, snapshot_file_digest

    h = build_and_persist(project, _agg_code(project)).content_hash
    snap = baked_snapshot_path(project, h)
    assert snap is not None and snap.exists()
    client = TestClient(create_app(project))
    assert client.delete(f"/{project}/api/result_cache/{h}").status_code == 200
    assert not snap.exists()

    seen: dict = {}

    def at_post(body: dict) -> None:
        seen["exists"] = snap.exists()
        seen["digest"] = snapshot_file_digest(snap) if snap.exists() else None

    fake = FakeBuckaroo()
    fake.on_load = at_post
    bk = _manager(fake)
    assert bk.load_session(h, project)["status"] == "ok"

    assert seen.get("exists"), "Buckaroo was called before the deleted snapshot was rewritten"
    assert seen["digest"] == read_manifest(entry_dir(project, h)).result_digest


def test_unfaithful_heal_forces_a_reload_of_the_open_grid(project, orders_src):
    """ADR-007 D6: an unfaithful heal is the one event that leaves an open session wrong, since the path now holds
    other rows than the stats Buckaroo computed. tallyman keeps no session record to drop, so the companion's hook
    wipes the entry's stat cache and posts ``/load_expr`` for the derived id with ``force_reload`` set."""
    from tallyman_xorq.result_cache import baked_snapshot_path, cached_result_expr

    h = build_and_persist(project, _agg_code(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)
    with TestClient(create_app(project, buckaroo=bk)):  # entering runs the startup handlers that register the hook
        assert bk.load_session(h, project)["status"] == "ok"  # the grid is open

        stale = entry_stat_cache_dir(project, h) / "parquet" / "stale-stats.parquet"
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_text("stale")
        manifest_path = entry_manifest_path(project, h)
        doc = json.loads(manifest_path.read_text())
        doc["result_digest"] = "arrow-sha256:" + "0" * 64  # no rewrite can match this
        manifest_path.write_text(json.dumps(doc))
        snap = baked_snapshot_path(project, h)
        assert snap is not None
        snap.unlink()
        cached_result_expr.cache_clear()

        seen: list[tuple] = []
        fake.on_load = lambda body: seen.append((body.get("force_reload"), stale.exists()))
        cached_result_expr(project, h)  # the heal; its digest cannot match the recorded one

    forced = [b for b in fake.loads() if b.get("force_reload")]
    assert len(forced) == 1, "the unfaithful-heal hook did not post a forced reload to Buckaroo"
    assert forced[0]["session"] == _sid(project, h)
    assert forced[0].get("row_order_column") == "__row_order"
    assert forced[0].get("no_browser") is True
    assert Path(forced[0]["build_dir"]).is_dir()
    assert seen == [(True, False)], "the reload must be posted after the stale stats are wiped"


def test_klass_reload_reaches_every_entry_without_a_session_record(project, orders_src):
    """ADR-007 D6: ``reload_project_sessions`` used to walk tallyman's own record of open sessions, which the design
    deletes, and Buckaroo has no route that lists sessions. With derived ids it posts ``/reload_expr/<id>`` for every
    entry of the project and treats Buckaroo's 404 as "not open". An entry that was never opened gets no session."""
    src = get_alias(project, orders_src)  # the import's own entry is in the project too, and gets a post as well
    a = build_and_persist(project, _agg_code(project)).content_hash
    b = build_and_persist(project, _second_agg_code(project)).content_hash
    fake = FakeBuckaroo()
    bk = _manager(fake)
    assert bk.load_session(a, project)["status"] == "ok"  # A is open in a tab; B and the source never were

    reloaded = bk.reload_project_sessions(project)

    assert sorted(fake.reloads()) == sorted(f"/reload_expr/{_sid(project, h)}" for h in (src, a, b))
    assert reloaded == 1, "only the open grid can have been reloaded"
    assert _sid(project, b) not in fake.sessions
    assert not hasattr(bk, "_sessions")
