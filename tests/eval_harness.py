"""Eval harness: drive a tallyman project through a multi-step session and check it after every step.

A scenario (``tests/eval_scenarios.py``) is a script of the things a user and an LLM do over a working session:
MCP tool calls with the prompts that asked for them, the SPA opening entries, Buckaroo painting grids, a reset to
an earlier step in the middle, a source file edited on disk and imported again, the cache emptied, the companion
restarted. The
harness runs those steps in one process against an isolated ``TALLYMAN_HOME`` and, after each one, asks the oracle
(``tests/eval_oracle.py``) whether the project still agrees with itself. The oracle keeps a ledger of what every
content hash served the first time it was read, so a disagreement between step 3 and step 11 is caught, which a
single-prompt test cannot see.

The pieces are the real ones wherever the process boundary allows:

- **MCP tools** are the functions in ``tallyman_mcp.server``, called the way ``tallyman replay`` calls them, so the
  checkpoint decorator, ``_tag_project`` and auto-recalc all run. Their best-effort ``_notify`` POSTs are routed
  into the in-process companion (the real ``_notify`` body runs; only its transport is swapped), so the
  cross-process invalidation path is exercised instead of silently failing against a port nobody is on.
- **The companion** is ``create_app`` behind a ``TestClient``, entered as a context manager so its startup hooks
  (the warm-up and the unfaithful-heal hook) run.
- **Buckaroo** is simulated by ``SimulatedBuckaroo`` on an ``httpx.MockTransport``. It keeps sessions with
  Buckaroo 0.15.6's warm-hit rule (as ``tests/test_buckaroo_handoff.py``'s fake does), and on every cold
  ``/load_expr`` it does what the real server does with the body: ``load_expr`` the posted build in a fresh load,
  count it, and page it. So a grid that would fail or show different rows from ``/api/data`` fails here too.
- **Reset** goes through ``POST /api/reset`` or through the CLI path (``reset_to`` in-process, then the
  ``project_reset`` notify the CLI sends).

Run it through ``tests/test_eval.py``; the harness itself does not assert, it records. Every step appends a
``StepRecord`` and any ``Finding`` the oracle raised, and ``Session.report()`` returns both as JSON.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

import httpx
from fastapi.testclient import TestClient

# The fields whose presence makes Buckaroo 0.15.6 re-run its pipeline even for a session it holds with the same
# build_dir (buckaroo/server/handlers.py). Mirrors tests/test_buckaroo_handoff.py.
_CONFIG_KEYS = ("component_config", "column_config_overrides", "extra_grid_config", "init_sd", "skip_stat_columns")

# Messages that name a known failure class wherever they appear: in a tool reply, an HTTP body, a Buckaroo load.
# Each maps to the finding code the oracle raises (see tests/eval_oracle.py for what each one means).
KNOWN_MESSAGES = {
    "Already borrowed": "already_borrowed",  # #118: two plans on the shared DataFusion session at once
    "At least one path is required": "read_of_missing_file",  # a build executed before its files existed
    # ibis's IntegrityError when a join's leftover __row_order_right reached execution; the build's own error for a
    # three-way join names the column too, so the needle is ibis's wording, not the column name
    "Name collisions": "row_order_collision",
}

# MCP tools that must never append a revision (tallyman_mcp.server._NO_CHECKPOINT, restated so that a tool that
# silently changes class is a finding here rather than a quiet agreement with the code under test).
READ_ONLY_TOOLS = frozenset(
    {
        "catalog_list",
        "catalog_diff",
        "catalog_scan_staleness",
        "catalog_list_summary_stats",
        "catalog_list_post_processings",
        "catalog_list_display_klasses",
        "catalog_run_post_processing",
        "catalog_export_marimo",
        "catalog_chart_errors",
        "project_list",
    }
)
# Tools that take their own checkpoint (so a real run lands one step and a dry run none).
SELF_CHECKPOINTING_TOOLS = frozenset({"catalog_recalc", "catalog_promote_diff"})


def rows_of(df) -> list[dict]:
    """A dataframe as the JSON rows ``/api/data`` returns, by the same conversion, so rows compare exactly."""
    return json.loads(df.to_json(orient="records"))


def fingerprint(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


def known_message_codes(text: str) -> list[str]:
    return [code for needle, code in KNOWN_MESSAGES.items() if needle in (text or "")]


@dataclass
class Finding:
    """One thing the oracle saw that the system contract says cannot happen (or, at ``warn``, should not)."""

    check: str  # a stable code, e.g. "page_changed"; the catalogue is in tests/eval_oracle.py
    severity: str  # "error" | "warn"
    message: str
    step: int | None = None
    detail: dict = field(default_factory=dict)


@dataclass
class StepRecord:
    index: int
    kind: str  # "mcp" | "http" | "open" | "page" | "diff" | "reset" | "edit_source" | "evict" | "restart" | ...
    label: str
    args: dict
    ok: bool = True
    seconds: float = 0.0
    result: Any = None  # a short summary, never a full payload
    error: str | None = None
    step_before: int | None = None  # the catalog's git step before and after
    step_after: int | None = None
    # How many step tags the step created. Not step_after - step_before: a checkpoint numbers its step one past the
    # highest tag, so after a reset back the first new step is several numbers above the step it was taken from.
    steps_added: int = 0
    findings: list[Finding] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Buckaroo
# ---------------------------------------------------------------------------


class SimulatedBuckaroo:
    """Buckaroo's HTTP surface as tallyman uses it, backed by a real load of each build it is handed.

    On a cold ``/load_expr`` it does what Buckaroo's server does with the body before a browser sees anything:
    load the build from ``build_dir`` (a fresh ``load_expr``, so fresh backend objects, as in another process),
    run the equivalent of the stats pipeline's row count, and paint the first window. Any exception there is the
    grid failing: it is answered with a 500, and kept in ``failures`` so the oracle can report it with its text.

    ``honor_row_order_hint`` says whether windows are ordered by the posted ``row_order_column``. Buckaroo 0.15.6
    ignores the hint (buckaroo-data/buckaroo#974), so ``False`` simulates today's grid and ``True`` the contract's.
    """

    def __init__(self, *, page_limit: int = 50, honor_row_order_hint: bool = True) -> None:
        self.page_limit = page_limit
        self.honor_row_order_hint = honor_row_order_hint
        self.sessions: dict[str, dict] = {}
        self.requests: list[dict] = []
        self.failures: list[dict] = []
        self.warm_hits = 0
        self._lock = threading.Lock()  # handlers can run on several companion threads at once

    # -- the HTTP surface ---------------------------------------------------

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        with self._lock:
            self.requests.append({"path": path, "body": body, "t": time.time()})
        if path == "/load_expr":
            return self._load(body)
        if path.startswith("/reload_expr/"):
            sid = path.rsplit("/", 1)[1]
            if sid not in self.sessions:
                return httpx.Response(404, json={"error": "unknown session"})
            try:
                self._first_paint(sid)
            except Exception as exc:  # a klass reload that breaks the grid
                return self._fail(sid, path, exc)
            return httpx.Response(200, json={})
        return httpx.Response(404, json={"error": f"unhandled {path}"})

    def _load(self, body: dict) -> httpx.Response:
        sid = body.get("session") or uuid.uuid4().hex
        existing = self.sessions.get(sid)
        has_config = any(body.get(k) for k in _CONFIG_KEYS)
        if existing and existing["build_dir"] == body["build_dir"] and not body.get("force_reload") and not has_config:
            self.warm_hits += 1
            return httpx.Response(200, json={"session": sid})
        self.sessions[sid] = {
            "build_dir": body["build_dir"],
            "row_order_column": body.get("row_order_column"),
            "column_config_overrides": body.get("column_config_overrides"),
            "loads": (existing or {}).get("loads", 0) + 1,
        }
        try:
            self._first_paint(sid)
        except Exception as exc:
            self.sessions.pop(sid, None)
            return self._fail(sid, "/load_expr", exc)
        return httpx.Response(200, json={"session": sid})

    def _fail(self, sid: str, path: str, exc: Exception) -> httpx.Response:
        msg = f"{type(exc).__name__}: {exc}"
        self.failures.append({"session": sid, "path": path, "error": msg, "traceback": traceback.format_exc()})
        return httpx.Response(500, json={"error": msg})

    # -- what a grid does ---------------------------------------------------

    def loaded(self, sid: str):
        from xorq.ibis_yaml.compiler import load_expr

        return load_expr(Path(self.sessions[sid]["build_dir"]))

    def read_paths(self, sid: str) -> list[Path]:
        from xorq.common.utils.graph_utils import walk_nodes
        from xorq.expr.relations import Read

        out = []
        for node in walk_nodes(Read, self.loaded(sid)):
            p = dict(node.read_kwargs).get("hash_path")
            if p:
                out.append(Path(str(p)))
        return out

    def relation_kinds(self, sid: str) -> list[str]:
        import xorq.vendor.ibis.expr.operations as ops
        from xorq.common.utils.graph_utils import walk_nodes
        from xorq.vendor.ibis.expr.operations.core import Node

        return sorted({type(n).__name__ for n in walk_nodes((Node,), self.loaded(sid)) if isinstance(n, ops.Relation)})

    def window(self, sid: str, offset: int = 0, limit: int | None = None, sort: tuple = ()) -> list[dict]:
        """One window of the grid, as Buckaroo would query it."""
        expr = self.loaded(sid)
        col = self.sessions[sid].get("row_order_column") if self.honor_row_order_hint else None
        keys = [*sort, col] if col and col in expr.columns else list(sort)
        if keys:
            expr = expr.order_by(keys)
        return rows_of(expr.limit(limit or self.page_limit, offset=offset).execute())

    def _first_paint(self, sid: str) -> None:
        missing = [str(p) for p in self.read_paths(sid) if not p.exists()]
        if missing:
            raise FileNotFoundError(f"the posted build reads files that do not exist: {missing}")
        expr = self.loaded(sid)
        count = int(expr.count().execute())
        rows = self.window(sid, 0, self.page_limit)
        self.sessions[sid].update(count=count, first_window=rows, columns=list(expr.columns))

    def forget_all(self) -> None:
        """Buckaroo restarted, or evicted every idle session (it does after an hour with no browser)."""
        self.sessions.clear()


def _buckaroo_manager(sim: SimulatedBuckaroo):
    from tallyman_companion.buckaroo_lifecycle import BuckarooManager

    class _AliveProc:
        def poll(self):
            return None

    bk = BuckarooManager()
    bk.proc = _AliveProc()
    bk.bound_port = 8799
    bk._maybe_restart = lambda: None
    bk._client = httpx.Client(transport=httpx.MockTransport(sim.handler))
    return bk


# ---------------------------------------------------------------------------
# Routing MCP notifies into the in-process companion
# ---------------------------------------------------------------------------


class _NotifyTransport:
    """Stands in for the ``httpx`` module inside ``tallyman_mcp.server`` so ``_notify``'s real body runs and its
    POST lands on the in-process companion. Every notify is kept, with the companion's answer, for the oracle."""

    def __init__(self, session: "Session") -> None:
        self._session = session

    def __getattr__(self, name):
        return getattr(httpx, name)

    def Client(self, *args, **kwargs):  # noqa: N802 (mirrors httpx.Client)
        session = self._session

        class _C:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def post(self, url, json=None, **_kw):
                path = "/" + url.split("://", 1)[-1].split("/", 1)[1]
                resp = session.client.post(path, json=json)
                session.notifies.append({"path": path, "body": json, "status": resp.status_code})
                return resp

        return _C()


# ---------------------------------------------------------------------------
# The session
# ---------------------------------------------------------------------------


class Session:
    """One scenario's run: an isolated project, a companion, a simulated Buckaroo, and the oracle.

    Use as a context manager. Every public action is a step: it is timed, recorded, its failure is caught and
    turned into a finding (a scenario keeps going after an error, the way a user does), and after a step that can
    change state the oracle sweeps the project.
    """

    def __init__(
        self,
        home: Path,
        *,
        scenario: str,
        project: str = "eval",
        auto_recalc: bool = True,
        page_limit: int = 50,
        sweep: str = "mutations",  # "mutations" | "every" | "manual"
        stall_seconds: float = 20.0,
        honor_row_order_hint: bool = True,
        monkeypatch=None,
    ) -> None:
        from tests.eval_oracle import Oracle

        self.home = Path(home)
        self.scenario = scenario
        self.project = project
        self.auto_recalc = auto_recalc
        self.page_limit = page_limit
        self.sweep_policy = sweep
        self.stall_seconds = stall_seconds
        self.buckaroo = SimulatedBuckaroo(page_limit=page_limit, honor_row_order_hint=honor_row_order_hint)
        self.steps: list[StepRecord] = []
        self.findings: list[Finding] = []
        self.notifies: list[dict] = []
        self.notes: list[str] = []
        self.last_reply: dict | None = None  # the last MCP tool's full reply (a step keeps only a summary)
        self.oracle = Oracle(self)
        self._mp = monkeypatch
        self._client_cm = None
        self.client: TestClient | None = None
        self.app = None
        self.bk = None

    # -- lifecycle ----------------------------------------------------------

    def __enter__(self) -> "Session":
        from tallyman_core import ensure_project, set_active_project
        from tallyman_core.catalog_state import genesis
        from tallyman_core.config import set_auto_recalc

        if self._mp is not None:
            self._mp.setenv("TALLYMAN_HOME", str(self.home))
            self._mp.delenv("TALLYMAN_PROJECT", raising=False)
            self._mp.delenv("TALLYMAN_AUTO_RECALC", raising=False)  # the scenario's switch goes through config.json
        else:
            os.environ["TALLYMAN_HOME"] = str(self.home)
            os.environ.pop("TALLYMAN_PROJECT", None)
            os.environ.pop("TALLYMAN_AUTO_RECALC", None)
        ensure_project(self.project)
        set_active_project(self.project)
        genesis(self.project)
        set_auto_recalc(self.project, self.auto_recalc)
        self._reset_mcp_globals()
        self._start_companion()
        self.oracle.begin()
        return self

    def __exit__(self, *exc) -> None:
        self._stop_companion()

    def _reset_mcp_globals(self) -> None:
        import tallyman_mcp.server as server

        server._last_project = None
        server._mcp_active_project = None
        if self._mp is not None:
            self._mp.setattr(server, "httpx", _NotifyTransport(self))
            self._mp.setattr(server, "COMPANION_URL", "http://companion.eval")
        else:
            server.httpx = _NotifyTransport(self)
            server.COMPANION_URL = "http://companion.eval"

    def _start_companion(self) -> None:
        from tallyman_companion import create_app

        self.bk = _buckaroo_manager(self.buckaroo)
        self.app = create_app(self.project, buckaroo=self.bk)
        self._client_cm = TestClient(self.app, raise_server_exceptions=False)
        self.client = self._client_cm.__enter__()

    def _stop_companion(self) -> None:
        if self._client_cm is not None:
            with contextlib.suppress(Exception):
                self._client_cm.__exit__(None, None, None)
        self._client_cm = None
        self.client = None

    # -- step machinery -----------------------------------------------------

    def _current_step(self) -> int | None:
        from tallyman_core.catalog_state import current_step

        try:
            return current_step(self.project)
        except Exception:
            return None

    def _step_tags(self) -> set[int]:
        from tallyman_core.catalog_state import _step_tags

        try:
            return set(_step_tags(self.project))
        except Exception:
            return set()

    @contextlib.contextmanager
    def _step(self, kind: str, label: str, args: dict, *, mutating: bool, expect_error: bool = False):
        rec = StepRecord(index=len(self.steps), kind=kind, label=label, args=_short(args))
        rec.step_before = self._current_step()
        tags_before = self._step_tags()
        self.steps.append(rec)
        pre = self.oracle.pre_step(rec)
        t0 = time.perf_counter()
        try:
            yield rec
        except BaseException as exc:
            # BaseException, not Exception: a polars panic (pyo3's PanicException) is a BaseException, and a
            # scenario must record it as a finding rather than end there.
            if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            # Always a finding, expected error or not: an MCP tool reports failure as {"error"} and a route as a
            # status code, so an exception out of either is a defect of its own.
            rec.ok = False
            rec.error = f"{type(exc).__name__}: {exc}"
            self.flag("step_raised", "error", f"{label} raised {rec.error}", rec, traceback=traceback.format_exc())
        finally:
            rec.seconds = round(time.perf_counter() - t0, 3)
            rec.step_after = self._current_step()
            rec.steps_added = len(self._step_tags() - tags_before)
            self.oracle.post_step(rec, pre, mutating=mutating, expect_error=expect_error)
            if self.sweep_policy == "every" or (self.sweep_policy == "mutations" and mutating):
                self.oracle.sweep(rec)

    def flag(self, check: str, severity: str, message: str, rec: StepRecord | None = None, **detail) -> Finding:
        f = Finding(check, severity, message, rec.index if rec else None, detail)
        self.findings.append(f)
        if rec is not None:
            rec.findings.append(f)
        return f

    def note(self, text: str) -> None:
        """Narration: why the next steps are here (shown in the report, like a storyboard's narration)."""
        self.notes.append(f"[{len(self.steps)}] {text}")

    # -- MCP ----------------------------------------------------------------

    def mcp(
        self,
        tool: str,
        *,
        expect_error: bool = False,
        expect_message: str | None = None,
        label: str | None = None,
        **args,
    ) -> dict:
        """Call one MCP tool as the LLM would. ``expect_error`` marks a call a real session made that failed (a
        wrong column name, a self-referencing revise): the failure is then the expected outcome, and the oracle
        checks it left nothing behind. ``expect_message`` is text the error should contain for the LLM to fix the
        call from the reply alone (the column list, the remedy); its absence is a warning."""
        import tallyman_mcp.server as server

        fn = getattr(server, tool)
        mutating = tool not in READ_ONLY_TOOLS and not (tool == "catalog_recalc" and args.get("dry_run", True))
        out: dict = {}
        step_args = {"tool": tool, **args}
        with self._step("mcp", label or tool, step_args, mutating=mutating, expect_error=expect_error) as rec:
            if tool == "catalog_recalc" and not args.get("dry_run", True):
                rec.args["_state_fp"] = self.oracle.state_fingerprint()
            self.last_reply = None
            out = fn(**args)
            self.last_reply = out
            err = out.get("error") if isinstance(out, dict) else None
            rec.result = _summarize_tool(out)
            if err:
                rec.ok = False
                rec.error = str(err)
                for code in known_message_codes(str(err)):
                    self.flag(code, "error", f"{tool}: {str(err)[:300]}", rec)
                if not expect_error:
                    self.flag("tool_error_unexpected", "error", f"{tool} failed: {str(err)[:300]}", rec)
            elif expect_error:
                self.flag("expected_error_missing", "warn", f"{tool} succeeded where the session saw an error", rec)
            if err and expect_message and expect_message.lower() not in str(err).lower():
                self.flag(
                    "error_message_unhelpful",
                    "warn",
                    f"{tool}'s error does not mention {expect_message!r}: {str(err)[:300]}",
                    rec,
                )
        return out

    def expect(self, condition: bool, message: str, **detail) -> None:
        """A scenario-level assertion about what the last steps produced (a recalc re-pointed an alias, a revise
        carried the chart over). A failure is a finding on the last step, never an exception."""
        if not condition:
            self.flag("scenario_expectation", "error", message, self.steps[-1] if self.steps else None, **detail)

    def replay(self, storyboard: dict, *, stop_before: int | None = None, start_at: int = 0) -> None:
        """Run a ``tallyman replay`` storyboard's steps (demo/storyboard.json) as MCP steps, with its narration
        kept as notes. ``${TALLYMAN_PROJECT_ROOT}`` in an argument is expanded as ``tallyman replay`` expands it,
        and a ``cell_id`` of ``PLACEHOLDER`` is resolved to the notebook's last cell."""
        from tallyman_cli.main import _expand_project_root
        from tallyman_core import notebook

        for step in storyboard["steps"][start_at:stop_before]:
            if step.get("skip"):
                continue
            args = {k: _expand_project_root(v, self.project) for k, v in step.get("args", {}).items()}
            if args.get("cell_id") == "PLACEHOLDER":
                cells = notebook.load(self.project).get("cells", [])
                args["cell_id"] = cells[-1]["id"] if cells else "PLACEHOLDER"
            if step.get("narration"):
                self.note(step["narration"])
            self.mcp(step["tool"], **args)

    # -- the companion and the grid -------------------------------------------

    def http(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        expect=(200,),
        label: str | None = None,
        mutating: bool | None = None,
    ) -> httpx.Response | None:
        if mutating is None:
            mutating = method.upper() not in ("GET", "HEAD")
        resp = None
        with self._step(
            "http", label or f"{method} {path}", {"method": method, "path": path, "json": json_body}, mutating=mutating
        ) as rec:
            resp = self._request(method, path, json_body, rec, expect=expect)
            rec.result = {"status": resp.status_code}
        return resp

    def _request(self, method: str, path: str, json_body, rec: StepRecord, *, expect=(200,)) -> httpx.Response:
        t0 = time.perf_counter()
        resp = self.client.request(method, path, json=json_body)
        dt = time.perf_counter() - t0
        if dt > self.stall_seconds:
            self.flag("stall", "warn", f"{method} {path} took {dt:.1f}s", rec, seconds=round(dt, 2))
        text = resp.text[:2000]
        for code in known_message_codes(text):
            self.flag(code, "error", f"{method} {path} -> {resp.status_code}: {text[:300]}", rec)
        if resp.status_code >= 500 and resp.status_code not in expect:
            self.flag("http_5xx", "error", f"{method} {path} -> {resp.status_code}: {text[:300]}", rec)
        elif resp.status_code not in expect:
            self.flag("http_unexpected_status", "error", f"{method} {path} -> {resp.status_code}: {text[:300]}", rec)
        return resp

    def resolve(self, ref: str) -> str:
        """An alias (``name``), a version ref (``name-v2``) or a hash, to a content hash."""
        from tallyman_core import get_alias
        from tallyman_core.aliases import resolve_version_ref

        return get_alias(self.project, ref) or resolve_version_ref(self.project, ref) or ref

    def open(self, ref: str, *, label: str | None = None) -> dict:
        """The SPA opening an entry page: entry detail, then the Buckaroo session, then the grid's first window,
        which the oracle compares with ``/api/data``'s first page (I5: the grid and the API read the same rows)."""
        out: dict = {}
        with self._step("open", label or f"open {ref}", {"ref": ref}, mutating=False) as rec:
            out = self.oracle.open_entry(ref, rec)
            rec.result = {k: out.get(k) for k in ("hash", "status", "rows")}
        return out

    def open_project(self, *, label: str = "open project") -> None:
        """What the SPA fetches on a project load: the notebook with entry metadata, the entry list, staleness."""
        with self._step("open", label, {}, mutating=False) as rec:
            for path in ("api/notebook_full", "api/entries", "api/staleness", "api/aliases", "api/errors"):
                self._request("GET", f"/{self.project}/{path}", None, rec)

    def page(self, ref: str, offset: int = 0, limit: int | None = None, *, label: str | None = None) -> list[dict]:
        rows: list[dict] = []
        with self._step(
            "page", label or f"page {ref}@{offset}", {"ref": ref, "offset": offset, "limit": limit}, mutating=False
        ) as rec:
            rows = self.oracle.check_page(self.resolve(ref), offset, limit or self.page_limit, rec)
            rec.result = {"rows": len(rows)}
        return rows

    def diff(self, alias: str, va: int | None = None, vb: int | None = None, *, label: str | None = None) -> dict:
        """The diff view: ``/api/diff_data`` for two versions (the latest two by default), requested twice, since a
        timed-out key search once poisoned every request after it (#176)."""
        out: dict = {}
        with self._step("diff", label or f"diff {alias}", {"alias": alias, "va": va, "vb": vb}, mutating=False) as rec:
            out = self.oracle.check_diff(alias, va, vb, rec)
            rec.result = {"status": out.get("status")}
        return out

    # -- the world changing underneath --------------------------------------------

    def reset(self, ref: int | str, *, via: str = "api", label: str | None = None) -> None:
        """Reset the catalog to a step or label, through the SPA's scrubber (``via="api"``) or the CLI
        (``via="cli"``: ``reset_to`` in this process, then the ``project_reset`` notify the CLI sends)."""
        with self._step("reset", label or f"reset to {ref} via {via}", {"ref": ref, "via": via}, mutating=True) as rec:
            before = self.oracle.compute_cache_listing()
            if via == "api":
                self._request("POST", f"/{self.project}/api/reset", {"ref": ref}, rec)
            elif via == "cli":
                from tallyman_cli.main import _notify_companion_reset
                from tallyman_core.catalog_state import reset_to

                reset_to(self.project, int(ref) if str(ref).isdigit() else ref)
                with self._patched_httpx_post():
                    _notify_companion_reset(self.project)
            else:
                raise ValueError(f"unknown reset path {via!r}")
            self.oracle.after_reset(rec, ref, before)

    @contextlib.contextmanager
    def _patched_httpx_post(self):
        real = httpx.post

        def _post(url, json=None, **_kw):
            path = "/" + url.split("://", 1)[-1].split("/", 1)[1]
            resp = self.client.post(path, json=json)
            self.notifies.append({"path": path, "body": json, "status": resp.status_code})
            return resp

        httpx.post = _post
        try:
            yield
        finally:
            httpx.post = real

    def label_step(self, name: str) -> None:
        from tallyman_core.catalog_state import label_step

        with self._step("label", f"label {name}", {"name": name}, mutating=False):
            label_step(self.project, self._current_step(), name)

    def edit_source(self, rel_path: str, change: Callable, *, label: str | None = None) -> None:
        """The user overwrites a data file that was imported (``change`` maps the old dataframe to the new one).

        After an import the file is provenance only (ADR-011 D2): nothing reads it again until it is imported
        again, so the edit must leave staleness exactly as it was. The ledger checks the rows on the next reads."""
        import pandas as pd

        from tallyman_core import data_dir

        with self._step("edit_source", label or f"edit {rel_path}", {"rel_path": rel_path}, mutating=False) as rec:
            before = self.oracle.stale_hashes(rec)
            p = data_dir(self.project) / rel_path
            if p.suffix == ".csv":
                change(pd.read_csv(p)).to_csv(p, index=False)
            else:
                change(pd.read_parquet(p)).to_parquet(p)
            after = self.oracle.stale_hashes(rec)
            if after != before:
                self.flag(
                    "source_edit_leaked",
                    "error",
                    f"editing {rel_path} on disk changed staleness without an import",
                    rec,
                    newly_stale=sorted(after - before),
                    no_longer_stale=sorted(before - after),
                )

    def evict(self, what: str = "all", *, label: str | None = None) -> None:
        """The user empties the cache from the Cache page, or deletes files by hand. ``what`` is ``cache_page``
        (``DELETE /api/result_cache/<hash>`` for every snapshot the page lists; it refuses a pinned one), ``all``
        (the whole ``compute_cache/``, by hand), ``snapshots`` (every snapshot, sources' included, by hand), or a ref
        whose snapshot alone goes."""
        from tallyman_core.paths import compute_cache_dir
        from tallyman_xorq.materialize import snapshot_path, snapshots_dir

        with self._step("evict", label or f"evict {what}", {"what": what}, mutating=False) as rec:
            if what == "cache_page":
                listing = self._request("GET", f"/{self.project}/api/result_cache", None, rec).json()
                for row in listing.get("entries", []):
                    expect = (409,) if row.get("pinned") else (200,)
                    self._request("DELETE", f"/{self.project}/api/result_cache/{row['hash']}", None, rec, expect=expect)
            elif what == "all":
                shutil.rmtree(compute_cache_dir(self.project), ignore_errors=True)
            elif what == "snapshots":
                shutil.rmtree(snapshots_dir(self.project), ignore_errors=True)
            else:
                snapshot_path(self.project, self.resolve(what)).unlink(missing_ok=True)

    def restart(self, *, buckaroo: bool = False, label: str | None = None) -> None:
        """The companion (and the MCP process with it) restarts: every in-process memo is gone and the startup
        hooks run again. ``buckaroo=True`` restarts Buckaroo too, so it holds no sessions."""
        from tallyman_companion.app import _build_compare_expr
        from tallyman_xorq.result_cache import cached_result_expr

        with self._step(
            "restart",
            label or ("restart all" if buckaroo else "restart companion"),
            {"buckaroo": buckaroo},
            mutating=False,
        ) as rec:
            before = self.oracle.compute_cache_listing()
            self._stop_companion()
            cached_result_expr.cache_clear()
            _build_compare_expr.cache_clear()
            self._reset_mcp_globals()
            if buckaroo:
                self.buckaroo.forget_all()
            self._start_companion()
            self.oracle.after_restart(rec, before)

    def buckaroo_forgets(self) -> None:
        with self._step("buckaroo_forgets", "buckaroo evicts idle sessions", {}, mutating=False):
            self.buckaroo.forget_all()

    def hammer(
        self,
        refs: list[str],
        *,
        threads: int = 8,
        per_thread: int = 6,
        with_diff: str | None = None,
        with_build: str | None = None,
        label: str | None = None,
    ) -> dict:
        """Concurrent page requests (and optionally a diff) against the companion, as several open tabs and a
        scrolling grid produce. Every answer must equal the sequential one; #118's "Already borrowed" shows here.

        ``with_build`` is recipe code run through ``catalog_run`` on another thread while the pages are requested:
        a build holds the project lock, and nothing a reader does should wait on it or fail because of it (#190)."""
        out: dict = {}
        args = {"refs": refs, "threads": threads, "with_diff": with_diff, "with_build": with_build}
        mutating = with_build is not None
        with self._step("hammer", label or f"hammer {refs}", args, mutating=mutating) as rec:
            build_thread = None
            build_out: dict = {}
            if with_build is not None:
                import tallyman_mcp.server as server

                def _build():
                    t0 = time.perf_counter()
                    try:
                        build_out["reply"] = server.catalog_run(code=with_build, prompt="built while pages load")
                    except Exception as exc:
                        build_out["reply"] = {"error": f"{type(exc).__name__}: {exc}"}
                    build_out["seconds"] = time.perf_counter() - t0

                build_thread = threading.Thread(target=_build)
                build_thread.start()
            out = self.oracle.hammer([self.resolve(r) for r in refs], threads, per_thread, with_diff, rec)
            if build_thread is not None:
                build_thread.join()
                reply = build_out.get("reply") or {}
                if reply.get("error"):
                    self.flag("tool_error_unexpected", "error", f"concurrent build failed: {reply['error'][:300]}", rec)
                    for code in known_message_codes(str(reply["error"])):
                        self.flag(code, "error", f"concurrent build: {reply['error'][:300]}", rec)
                out["build_seconds"] = round(build_out.get("seconds", 0.0), 2)
            rec.result = {k: out.get(k) for k in ("requests", "failed", "mismatched", "slowest", "build_seconds")}
        return out

    def cross_process(self, *, label: str = "read every entry in a fresh process") -> None:
        """I6's "in any process": page every entry the ledger knows from a new Python process and compare."""
        with self._step("cross_process", label, {}, mutating=False) as rec:
            self.oracle.cross_process(rec)

    def check(self, label: str, fn: Callable[["Session", StepRecord], Any], *, mutating: bool = False) -> Any:
        """A scenario-specific inspection run as its own step; ``fn`` flags through ``session.flag(..., rec)``."""
        out = None
        with self._step("check", label, {}, mutating=mutating) as rec:
            out = fn(self, rec)
        return out

    def sweep(self, label: str = "sweep") -> None:
        with self._step("sweep", label, {}, mutating=False) as rec:
            self.oracle.sweep(rec)

    def finish(self) -> None:
        """The closing checks every scenario gets: a restart, a warm sweep, then everything read again after the
        Cache page deleted every snapshot it may (I2's cold seam), the verify sweep, and a read from a fresh process.

        The cold seam goes through the Cache page rather than ``rm -rf compute_cache/`` because a pinned snapshot
        is the only copy of a non-reproducible entry's rows, not a cache, and the page refuses to delete it. A
        scenario that deletes files by hand says so with ``evict("all")``."""
        self.restart(buckaroo=True, label="finish: restart")
        self.sweep("finish: warm sweep")
        self.evict("cache_page", label="finish: empty the cache from the Cache page")
        self.sweep("finish: cold sweep")
        with self._step("verify", "finish: verify results", {}, mutating=False) as rec:
            self.oracle.verify_results(rec)
        self.cross_process(label="finish: fresh process")
        with self._step("global_state", "finish: global state", {}, mutating=False) as rec:
            self.oracle.global_state(rec)

    # -- reporting ------------------------------------------------------------------

    def report(self) -> dict:
        return {
            "scenario": self.scenario,
            "project": self.project,
            "auto_recalc": self.auto_recalc,
            "steps": [_step_json(s) for s in self.steps],
            "findings": [asdict(f) for f in self.findings],
            "notes": self.notes,
            "notifies": self.notifies,
            "buckaroo": {
                "loads": sum(1 for r in self.buckaroo.requests if r["path"] == "/load_expr"),
                "warm_hits": self.buckaroo.warm_hits,
                "failures": self.buckaroo.failures,
            },
            "ledger": self.oracle.ledger_summary(),
            "seconds": round(sum(s.seconds for s in self.steps), 2),
        }


def _short(obj, limit: int = 400):
    if isinstance(obj, dict):
        return {k: _short(v, limit) for k, v in obj.items() if v is not None}
    if isinstance(obj, str) and len(obj) > limit:
        return obj[:limit] + f"… ({len(obj)} chars)"
    if callable(obj):
        return getattr(obj, "__name__", repr(obj))
    return obj


def _summarize_tool(out) -> Any:
    if not isinstance(out, dict):
        return str(out)[:200]
    keep = (
        "hash",
        "alias",
        "version",
        "created",
        "row_count",
        "error",
        "status",
        "remap",
        "stale",
        "orphan_stale",
        "carried_over",
        "reproducible",
        "lint_warnings",
        "name",
    )
    s = {k: out[k] for k in keep if k in out}
    if "recalc" in out and isinstance(out["recalc"], dict):
        s["recalc"] = {k: out["recalc"].get(k) for k in ("status", "remap", "checkpoint_step", "orphan_stale")}
    if "items" in out:
        s["items"] = len(out["items"])
    return _short(s, 300)


def _step_json(s: StepRecord) -> dict:
    d = asdict(s)
    d["findings"] = [f"{f.severity}:{f.check}" for f in s.findings]
    return d


def cross_process_script() -> str:
    """The child side of ``Session.cross_process``: page each hash on stdin, print fingerprints."""
    return (
        "import json, sys\n"
        "from tallyman_xorq.result_cache import cached_result_expr\n"
        "from tallyman_xorq.row_order import page\n"
        "req = json.load(sys.stdin)\n"
        "out = {}\n"
        "for h, (offset, limit) in req['pages'].items():\n"
        "    try:\n"
        "        df = page(cached_result_expr(req['project'], h), offset=offset, limit=limit).execute()\n"
        "        out[h] = json.loads(df.to_json(orient='records'))\n"
        "    except Exception as exc:\n"
        "        out[h] = {'error': f'{type(exc).__name__}: {exc}'}\n"
        "json.dump(out, sys.stdout)\n"
    )


def run_cross_process(project: str, pages: dict[str, tuple[int, int]]) -> dict:
    env = {**os.environ}
    # close_fds=False lets CPython start the child with posix_spawn on macOS instead of fork + exec. In a full run
    # of the suite (not in one scenario alone) a forked child of this process segfaults before its exec, which the
    # check would report as the fresh process failing with -11.
    proc = subprocess.run(
        [sys.executable, "-c", cross_process_script()],
        input=json.dumps({"project": project, "pages": pages}),
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
        close_fds=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"fresh process failed ({proc.returncode}): {proc.stderr[-2000:]}")
    return json.loads(proc.stdout)


def run_threads(calls: list[Callable[[], Any]], threads: int) -> list[tuple[Any, str | None]]:
    """Run zero-argument calls on a pool; each result is ``(value, None)`` or ``(None, "Type: message")``."""

    def _one(fn):
        try:
            return fn(), None
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

    with ThreadPoolExecutor(max_workers=threads) as pool:
        return list(pool.map(_one, calls))
