"""The eval oracle: what must stay true about a tallyman project after every step of a session.

Most checks restate an invariant of ``docs/system-contract.md`` (Part 4) or a failure a past session hit. The one
that needs history is the **ledger**: the first time a content hash serves a page, the page's fingerprint is kept,
and every later read of that hash, after resets, cache wipes, restarts, source edits, in a fresh process or on a
grid, must serve the same rows (I1: a content hash names a fixed result; I2: caches affect latency, never results;
I6: a page is a function of its request).

Finding codes. ``error`` is a contract violation; ``warn`` is a behaviour a session should not see but the contract
does not forbid (a stall, a timeout, a failure a user caused and the system handled).

Session and step bookkeeping
    step_raised              a step raised out of the harness (the tool, a route, or the harness itself)
    tool_error_unexpected    an MCP tool returned {"error"} where the scenario expected success
    expected_error_missing   warn: a call that failed in the recorded session succeeded here
    http_5xx / http_unexpected_status
    stall                    warn: one request took longer than the scenario's stall budget
    already_borrowed         #118: "Already borrowed" anywhere (two plans on the shared DataFusion session)
    read_of_missing_file     "At least one path is required": something executed a build before its files existed
    row_order_collision      ibis "Name collisions" at execution (a join the build should have rejected, B2)

Revisions (plans/adr-reset-to-revision.md)
    multi_checkpoint         one operation landed more than one git step (#33)
    checkpoint_on_error      a failed tool call still appended a step
    checkpoint_on_read       a read-only tool, GET or view step appended a step
    catalog_dirty            warn: tracked catalog files differ from HEAD after a checkpointed step
    failed_build_left_entry  warn: a failed tool call left a new entry directory behind
    reset_step_mismatch      after a reset, the current step is not the one asked for
    reset_touched_compute_cache  a reset wrote or deleted under compute_cache/ (ADR-007 D14)
    warmup_wrote_files       a companion start wrote or deleted under compute_cache/ (ADR-007 D12)

Catalog structure
    catalog_inconsistent     assert_catalog_consistent raised (pointers vs recipes vs sidecars, #52)
    pointer_without_entry    entry_hashes names a hash with no complete entry on disk
    entry_without_pointer    warn: a complete entry on disk that no pointer names
    alias_dangling           an alias head or history names a hash with no entry
    parent_missing           a surviving entry's manifest names a parent that is gone
    notebook_dangling_alias  a notebook cell names an alias that does not exist
    alias_detail_mismatch    /api/entry/<alias> resolves to something other than the alias head or version

Reads (I1, I2, I5, I6)
    page_changed             a (hash, offset, limit) page differs from the first time it was read
    total_mismatch           /api/data's total, the entry detail's total_rows and the manifest row_count disagree
    short_page               a page has the wrong number of rows for its offset and the total
    row_order_missing        a page of an entry has no __row_order column
    row_order_not_last       __row_order is not the last column
    page_not_row_ordered     __row_order is not strictly increasing within an unsorted page
    row_order_gap_in_snapshot  a worthy entry's page is not exactly positions offset..offset+n
    snapshot_unfaithful      a snapshot's content digest differs from the manifest's result_digest
    snapshot_rowcount_mismatch
    snapshot_row_order_broken  a snapshot's __row_order is not 0..N-1 in file order, or not last
    unfaithful_heal          errors.jsonl gained an unfaithful_heal record
    error_recorded           warn: errors.jsonl gained a record during a step that succeeded
    error_message_unhelpful  warn: an expected error did not name what the LLM needs to fix the call
    verify_error             the verify sweep could not read a snapshot (a corrupt file, C4)
    cross_process_changed / cross_process_failed

Buckaroo (the grid)
    grid_session_failed      /api/session answered anything but ok
    grid_load_failed         Buckaroo's load of the posted build failed (its message is kept)
    grid_api_mismatch        the grid's first window differs from /api/data's first page (I5)
    grid_count_mismatch      the grid's row count differs from the manifest
    view_build_not_bare_read a worthy entry's grid was handed more than one read of its own snapshot (ADR-007 D6)
    notify_failed            an MCP -> companion notify was refused

Staleness and recalc
    staleness_failed         /api/staleness failed
    cascade_missed           with auto-recalc on, a live head is stale on the alias axis after the step settled
    orphan_stale             warn: the scan-on-load backstop reports orphans
    source_edit_leaked       editing an imported file on disk changed staleness before any re-import (ADR-011 D2)
    recalc_noop_while_stale  a real recalc reported noop for an entry it also reported stale (seen in 6180b849)
    recalc_not_deterministic the same recalc from the same state minted different hashes

Diff
    diff_failed              /api/diff_data errored (not a 504 key-search timeout)
    diff_timeout             warn: 504 from the time-boxed primary-key search
    diff_poisoned            the second identical diff request failed differently from the first (#176)
    diff_changed             the same pair of hashes diffed to a different result

Concurrency and global state
    concurrent_failure       a request failed when sent alongside others and succeeded alone
    concurrent_mismatch      a request returned different rows alongside others than alone
    xorq_global_cache_written  something wrote into XORQ_CACHE_DIR (tallyman builds hold no cache nodes)
    temp_files_left          warn: *.tmp files under compute_cache/ or the entries after the step

Scenario
    scenario_expectation     a scenario's own check on what a step produced (Session.expect) failed
    source_type_changed      a source entry's column type differs from the imported file's (#197)
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from tallyman_core.git_util import run_git
from tallyman_core.paths import catalog_dir
from tests.eval_harness import (
    READ_ONLY_TOOLS,
    StepRecord,
    fingerprint,
    known_message_codes,
    run_cross_process,
    run_threads,
)

ROW_ORDER = "__row_order"
_TIMING_KEYS = ("_ms", "_seconds", "elapsed", "duration")


def _strip_timing(obj):
    if isinstance(obj, dict):
        return {k: _strip_timing(v) for k, v in obj.items() if not any(t in k for t in _TIMING_KEYS)}
    if isinstance(obj, list):
        return [_strip_timing(v) for v in obj]
    return obj


def _first_difference(a: list[dict], b: list[dict]) -> dict:
    if len(a) != len(b):
        return {"rows_before": len(a), "rows_now": len(b)}
    for i, (x, y) in enumerate(zip(a, b)):
        if x != y:
            cols = sorted(k for k in set(x) | set(y) if x.get(k) != y.get(k))
            return {
                "row": i,
                "columns": cols[:10],
                "before": {c: x.get(c) for c in cols[:5]},
                "now": {c: y.get(c) for c in cols[:5]},
            }
    return {}


class Oracle:
    def __init__(self, session) -> None:
        self.s = session
        # hash -> {"first_step", "pages": {key: fp}, "rows": {key: rows}, "total"}
        self.ledger: dict[str, dict] = {}
        self.diffs: dict[tuple[str, str], str] = {}
        self.recalcs: dict[str, dict] = {}  # fingerprint of the state a recalc started from -> its remap
        self._errors_seen = 0
        self._bk_failures_seen = 0
        self._notifies_seen = 0
        self._xorq_cache_baseline: set[str] = set()

    @property
    def project(self) -> str:
        return self.s.project

    # ------------------------------------------------------------------
    # step hooks
    # ------------------------------------------------------------------

    def begin(self) -> None:
        self._errors_seen = len(self._errors())
        self._xorq_cache_baseline = self._xorq_cache_listing()

    def pre_step(self, rec: StepRecord) -> dict:
        return {"entries": self._entry_hashes_on_disk()}

    def post_step(self, rec: StepRecord, pre: dict, *, mutating: bool, expect_error: bool) -> None:
        s = self.s
        delta = rec.steps_added
        if rec.kind == "mcp":
            tool = rec.args.get("tool")
            if tool in READ_ONLY_TOOLS and delta:
                s.flag("checkpoint_on_read", "error", f"{tool} appended {delta} step(s)", rec)
            elif not rec.ok and delta:
                s.flag("checkpoint_on_error", "error", f"failed {tool} appended {delta} step(s)", rec)
            elif delta and delta > 1:
                s.flag("multi_checkpoint", "error", f"{tool} landed {delta} steps", rec)
            if not rec.ok:
                new = self._entry_hashes_on_disk() - pre["entries"]
                if new:
                    s.flag("failed_build_left_entry", "warn", f"failed {tool} left entries {sorted(new)}", rec)
            elif mutating and delta:
                dirty = self._catalog_dirty()
                if dirty:
                    s.flag(
                        "catalog_dirty", "warn", f"tracked files differ from HEAD after {tool}", rec, files=dirty[:20]
                    )
            if tool == "catalog_recalc" and rec.ok and not rec.args.get("dry_run", True):
                self._check_recalc_report(rec)
        elif not mutating and rec.kind not in ("reset", "http", "label") and delta:
            s.flag("checkpoint_on_read", "error", f"{rec.label} appended {delta} step(s)", rec)

        errors = self._errors()
        for e in errors[self._errors_seen :]:
            text = f"{e.get('tool')}: {e.get('message', '')}"
            for code in known_message_codes(text):
                s.flag(code, "error", f"errors.jsonl: {text[:300]}", rec)
            if e.get("code") == "unfaithful_heal":
                s.flag("unfaithful_heal", "error", f"errors.jsonl: {text[:300]}", rec, record=e)
            elif rec.ok and not expect_error and e.get("tool") not in ("chart_render",):
                s.flag("error_recorded", "warn", f"errors.jsonl gained: {text[:300]}", rec)
        self._errors_seen = len(errors)

        for f in s.buckaroo.failures[self._bk_failures_seen :]:
            s.flag(
                "grid_load_failed",
                "error",
                f"Buckaroo {f['path']} {f['session']}: {f['error'][:300]}",
                rec,
                traceback=f["traceback"][-3000:],
            )
            for code in known_message_codes(f["error"]):
                s.flag(code, "error", f"Buckaroo: {f['error'][:300]}", rec)
        self._bk_failures_seen = len(s.buckaroo.failures)

        for n in s.notifies[self._notifies_seen :]:
            if n["status"] != 200:
                s.flag("notify_failed", "error", f"notify {n['body']} -> {n['status']}", rec)
        self._notifies_seen = len(s.notifies)

        tmp = self._temp_files()
        if tmp:
            s.flag("temp_files_left", "warn", f"{len(tmp)} temp file(s) left", rec, files=tmp[:10])

    # ------------------------------------------------------------------
    # the sweep
    # ------------------------------------------------------------------

    def sweep(self, rec: StepRecord) -> None:
        """Everything that must hold between steps, over every entry on disk."""
        s = self.s
        from tallyman_core import catalog
        from tallyman_core.aliases import load_aliases, load_history
        from tallyman_core.catalog_state import read_tallyman_state

        on_disk = self._entry_hashes_on_disk()
        try:
            pointers = set(read_tallyman_state(self.project)["entry_hashes"])
            catalog.assert_catalog_consistent(self.project, pointers)
        except Exception as exc:
            s.flag("catalog_inconsistent", "error", f"{type(exc).__name__}: {exc}"[:600], rec)
            pointers = set()
        for h in sorted(pointers - on_disk):
            s.flag("pointer_without_entry", "error", f"entry_hashes names {h}, which has no entry", rec)
        for h in sorted(on_disk - pointers) if pointers else []:
            s.flag("entry_without_pointer", "warn", f"entry {h} is on disk but no pointer names it", rec)

        heads = load_aliases(self.project)
        history = load_history(self.project)
        for name, hashes in history.items():
            for h in hashes:
                if h not in on_disk:
                    s.flag("alias_dangling", "error", f"alias {name} history names {h}, which has no entry", rec)
        for name, h in heads.items():
            if h not in on_disk:
                s.flag("alias_dangling", "error", f"alias {name} points at {h}, which has no entry", rec)

        from tallyman_core import notebook

        for cell in notebook.load(self.project).get("cells", []):
            alias = cell.get("alias")
            if alias and alias not in heads:
                s.flag("notebook_dangling_alias", "error", f"notebook cell {cell.get('id')} names {alias}", rec)

        manifests = {h: self._manifest(h) for h in on_disk}
        for h, m in manifests.items():
            for p in m.get("parents") or []:
                if p["hash"] not in on_disk:
                    s.flag("parent_missing", "error", f"{h} was built on {p['ref']}={p['hash']}, which is gone", rec)

        for name, h in sorted(heads.items()):
            resp = s._request("GET", f"/{self.project}/api/entry/{name}", None, rec)
            if resp.status_code == 200:
                d = resp.json()
                if d.get("content_hash") != h or d.get("version") != len(history.get(name, [])):
                    s.flag(
                        "alias_detail_mismatch",
                        "error",
                        f"/api/entry/{name} -> {d.get('content_hash')} v{d.get('version')}, head is {h} "
                        f"v{len(history.get(name, []))}",
                        rec,
                    )

        for h in sorted(on_disk):
            m = manifests[h]
            resp = s._request("GET", f"/{self.project}/api/entry/{h}", None, rec)
            if resp.status_code == 200 and resp.json().get("total_rows") != m.get("row_count"):
                s.flag(
                    "total_mismatch",
                    "error",
                    f"{h}: entry detail total_rows {resp.json().get('total_rows')} != manifest {m.get('row_count')}",
                    rec,
                )
            total = m.get("row_count") or 0
            self.check_page(h, 0, s.page_limit, rec)
            if total > s.page_limit:
                self.check_page(h, total - s.page_limit, s.page_limit, rec)
            self._check_snapshot(h, m, rec)

        for name in sorted(heads):
            self.open_entry(name, rec)

        self._check_staleness(rec)
        for name, hashes in sorted(history.items()):
            if len(hashes) >= 2 and name in heads:
                self.check_diff(name, None, None, rec)

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------

    def observe(self, h: str, key: tuple, rows: list[dict], rec: StepRecord, *, check: str = "page_changed") -> None:
        entry = self.ledger.setdefault(h, {"first_step": rec.index, "pages": {}, "rows": {}})
        fp = fingerprint(rows)
        known = entry["pages"].get(key)
        if known is None:
            entry["pages"][key] = fp
            entry["rows"][key] = rows
        elif known != fp:
            self.s.flag(
                check,
                "error",
                f"{h} {key} served different rows than at step {entry['first_step']}",
                rec,
                difference=_first_difference(entry["rows"][key], rows),
            )

    def check_page(self, h: str, offset: int, limit: int, rec: StepRecord) -> list[dict]:
        s = self.s
        resp = s._request("GET", f"/{self.project}/api/data/{h}?offset={offset}&limit={limit}", None, rec)
        if resp.status_code != 200:
            return []
        body = resp.json()
        rows = body.get("data") or []
        m = self._manifest(h)
        total = body.get("total")
        if m and total != m.get("row_count"):
            s.flag("total_mismatch", "error", f"{h}: /api/data total {total} != manifest {m.get('row_count')}", rec)
        want = max(0, min(limit, (total or 0) - offset))
        if len(rows) != want:
            s.flag("short_page", "error", f"{h}@{offset}+{limit}: {len(rows)} rows, expected {want}", rec)
        if rows:
            cols = list(rows[0].keys())
            if ROW_ORDER not in cols:
                s.flag("row_order_missing", "error", f"{h}: page has no {ROW_ORDER} ({cols[:8]}…)", rec)
            else:
                if cols[-1] != ROW_ORDER:
                    s.flag("row_order_not_last", "error", f"{h}: last column is {cols[-1]}", rec)
                ro = [r[ROW_ORDER] for r in rows]
                if any(b <= a for a, b in zip(ro, ro[1:])):
                    s.flag(
                        "page_not_row_ordered", "error", f"{h}@{offset}: {ROW_ORDER} not increasing", rec, head=ro[:10]
                    )
                elif m.get("cache_worthy") and ro != list(range(offset, offset + len(ro))):
                    s.flag(
                        "row_order_gap_in_snapshot",
                        "error",
                        f"{h}@{offset}: {ROW_ORDER} {ro[:3]}… is not {offset}..",
                        rec,
                    )
        self.observe(h, ("page", offset, limit), rows, rec)
        return rows

    def open_entry(self, ref: str, rec: StepRecord) -> dict:
        """Entry detail -> /api/session -> Buckaroo's first paint, compared with /api/data's first page."""
        s = self.s
        resp = s._request("GET", f"/{self.project}/api/entry/{ref}", None, rec)
        if resp.status_code != 200:
            return {"status": f"entry {resp.status_code}"}
        h = resp.json()["content_hash"]
        sess = s._request("GET", f"/{self.project}/api/session/{h}", None, rec).json()
        if sess.get("status") != "ok":
            s.flag("grid_session_failed", "error", f"{ref} ({h}): {sess.get('status')}: {sess.get('detail')}", rec)
            for code in known_message_codes(str(sess.get("detail"))):
                s.flag(code, "error", f"session {h}: {str(sess.get('detail'))[:300]}", rec)
            return {"hash": h, "status": sess.get("status")}
        sid = s.bk.session_id_for(self.project, h)
        grid = s.buckaroo.sessions.get(sid)
        if grid is None:
            s.flag("grid_session_failed", "error", f"{ref}: session ok but Buckaroo holds no {sid}", rec)
            return {"hash": h, "status": "missing"}
        m = self._manifest(h)
        if grid.get("count") != m.get("row_count"):
            s.flag(
                "grid_count_mismatch",
                "error",
                f"{ref} ({h}): grid counts {grid.get('count')} rows, manifest {m.get('row_count')}",
                rec,
            )
        api_rows = self.check_page(h, 0, s.page_limit, rec)
        window = s.buckaroo.window(sid, 0, s.page_limit)
        if window != api_rows:
            sev = "error" if s.buckaroo.honor_row_order_hint else "warn"
            s.flag(
                "grid_api_mismatch",
                sev,
                f"{ref} ({h}): the grid's first window differs from /api/data",
                rec,
                difference=_first_difference(api_rows, window),
            )
        self.observe(h, ("grid", 0, s.page_limit), window, rec)
        if m.get("cache_worthy"):
            from tallyman_xorq.materialize import snapshot_path

            kinds = s.buckaroo.relation_kinds(sid)
            reads = [p.resolve() for p in s.buckaroo.read_paths(sid)]
            if kinds != ["Read"] or reads != [snapshot_path(self.project, h).resolve()]:
                s.flag("view_build_not_bare_read", "error", f"{ref} ({h}): grid build is {kinds} over {reads}", rec)
        return {"hash": h, "status": "ok", "rows": len(window)}

    def check_diff(self, alias: str, va: int | None, vb: int | None, rec: StepRecord) -> dict:
        from tallyman_core.aliases import history_for

        s = self.s
        hashes = history_for(self.project, alias)
        if len(hashes) < 2:
            return {"status": "skipped"}
        vb = vb or len(hashes)
        va = va or vb - 1
        path = f"/{self.project}/api/diff_data/{alias}/{va}/{vb}"
        first = s._request("GET", path, None, rec, expect=(200, 504))
        second = s._request("GET", path, None, rec, expect=(200, 504))
        if first.status_code == 504:
            s.flag("diff_timeout", "warn", f"{alias} V{va}->V{vb}: {first.json().get('detail')}", rec)
        if second.status_code != first.status_code:
            s.flag(
                "diff_poisoned",
                "error",
                f"{alias} V{va}->V{vb}: {first.status_code} then {second.status_code}: {second.text[:300]}",
                rec,
            )
        if first.status_code >= 500 and first.status_code != 504:
            s.flag("diff_failed", "error", f"{alias} V{va}->V{vb}: {first.text[:300]}", rec)
        if first.status_code != 200:
            return {"status": first.status_code}
        body = first.json()
        key = (body["a_hash"], body["b_hash"])
        fp = fingerprint(_strip_timing(body["diff"]))
        if key in self.diffs and self.diffs[key] != fp:
            s.flag("diff_changed", "error", f"{alias} {key}: the same pair diffed differently", rec)
        self.diffs.setdefault(key, fp)
        if fingerprint(_strip_timing(second.json().get("diff"))) != fp:
            s.flag("diff_changed", "error", f"{alias} {key}: two identical requests diffed differently", rec)
        return {"status": 200, "a": key[0], "b": key[1], "compare_session": body.get("compare_session")}

    def hammer(self, hashes: list[str], threads: int, per_thread: int, with_diff: str | None, rec: StepRecord) -> dict:
        s = self.s
        limit = s.page_limit
        reqs = []
        for h in hashes:
            total = self._manifest(h).get("row_count") or 0
            span = max(total - limit, 0)
            offsets = sorted({(span * k) // max(per_thread - 1, 1) for k in range(per_thread)})
            reqs += [f"/{self.project}/api/data/{h}?offset={o}&limit={limit}" for o in offsets]
        if with_diff:
            from tallyman_core.aliases import history_for

            n = len(history_for(self.project, with_diff))
            if n >= 2:
                reqs += [f"/{self.project}/api/diff_data/{with_diff}/{n - 1}/{n}"] * 2

        timings: list[float] = []

        def fetch(path):
            t0 = time.perf_counter()
            r = s.client.get(path)
            timings.append(time.perf_counter() - t0)
            if r.status_code != 200:
                raise RuntimeError(f"{r.status_code}: {r.text[:300]}")
            body = r.json()
            return fingerprint(_strip_timing(body.get("data", body.get("diff"))))

        # Each request alone first, for the answer the concurrent ones must equal. The diff's key search is
        # time-boxed, so a 504 there is diff_timeout (a warn, as in check_diff) and the diff sits out the run.
        alone = {}
        for p in dict.fromkeys(reqs):
            try:
                alone[p] = fetch(p)
            except RuntimeError as exc:
                if "/api/diff_data/" not in p or not str(exc).startswith("504"):
                    raise
                s.flag("diff_timeout", "warn", f"{p} alone: {str(exc)[:300]}", rec)
        reqs = [p for p in reqs if p in alone]
        timings.clear()
        together = run_threads([lambda p=p: fetch(p) for p in reqs * threads], threads)
        slowest = max(timings, default=0.0)
        if slowest > s.stall_seconds:
            s.flag("stall", "warn", f"a concurrent request took {slowest:.1f}s", rec, seconds=round(slowest, 2))
        failed = [(p, err) for p, (_, err) in zip(reqs * threads, together) if err]
        mismatched = [p for p, (fp, err) in zip(reqs * threads, together) if not err and fp != alone[p]]
        if failed:
            s.flag(
                "concurrent_failure",
                "error",
                f"{len(failed)}/{len(together)} concurrent requests failed; first: {failed[0][1][:300]}",
                rec,
                examples=[e for _, e in failed[:5]],
            )
            for code in {c for _, e in failed for c in known_message_codes(e)}:
                s.flag(code, "error", f"under concurrency ({len(failed)} failures)", rec)
        if mismatched:
            s.flag(
                "concurrent_mismatch",
                "error",
                f"{len(mismatched)} requests returned different rows alongside others",
                rec,
                examples=mismatched[:5],
            )
        return {
            "requests": len(together),
            "failed": len(failed),
            "mismatched": len(mismatched),
            "slowest": round(slowest, 2),
        }

    def cross_process(self, rec: StepRecord) -> None:
        s = self.s
        on_disk = self._entry_hashes_on_disk()
        key = ("page", 0, s.page_limit)
        pages = {h: (0, s.page_limit) for h, e in self.ledger.items() if h in on_disk and key in e["pages"]}
        if not pages:
            return
        try:
            out = run_cross_process(self.project, pages)
        except Exception as exc:
            s.flag("cross_process_failed", "error", str(exc)[:600], rec)
            return
        for h, rows in out.items():
            if isinstance(rows, dict) and "error" in rows:
                s.flag("cross_process_failed", "error", f"{h}: {rows['error'][:300]}", rec)
                for code in known_message_codes(rows["error"]):
                    s.flag(code, "error", f"fresh process, {h}", rec)
            else:
                self.observe(h, key, rows, rec, check="cross_process_changed")

    def verify_results(self, rec: StepRecord) -> None:
        from tallyman_xorq import staleness

        out = staleness.verify_sweep(self.project)
        for h in out.get("unfaithful") or []:
            self.s.flag("snapshot_unfaithful", "error", f"verify sweep: {h}", rec)
        for h, msg in (out.get("errors") or {}).items():
            self.s.flag("verify_error", "error", f"verify sweep: {h}: {str(msg)[:300]}", rec)

    # ------------------------------------------------------------------
    # resets, restarts, recalcs
    # ------------------------------------------------------------------

    def after_reset(self, rec: StepRecord, ref, before: dict[str, int]) -> None:
        from tallyman_core.catalog_state import _resolve_tag, current_step

        s = self.s
        step = current_step(self.project)
        tag = f"step-{int(ref):03d}" if str(ref).isdigit() else str(ref)
        try:
            want_commit = _resolve_tag(self.project, tag)

            _, head, _ = run_git(["rev-parse", "HEAD"], cwd=catalog_dir(self.project))
            if head.strip() != want_commit.strip():
                s.flag("reset_step_mismatch", "error", f"reset to {ref}: HEAD is step {step}", rec)
        except Exception as exc:
            s.flag("reset_step_mismatch", "error", f"reset to {ref}: {exc}", rec)
        after = self.compute_cache_listing()
        if after != before:
            s.flag(
                "reset_touched_compute_cache",
                "error",
                "a reset changed compute_cache/",
                rec,
                **_listing_delta(before, after),
            )

    def after_restart(self, rec: StepRecord, before: dict[str, int]) -> None:
        after = self.compute_cache_listing()
        if after != before:
            self.s.flag(
                "warmup_wrote_files",
                "error",
                "a companion start changed compute_cache/",
                rec,
                **_listing_delta(before, after),
            )

    def _check_recalc_report(self, rec: StepRecord) -> None:
        s = self.s
        full = s.last_reply or {}
        for e in full.get("entries") or []:
            if e.get("action") == "noop" and e.get("stale"):
                s.flag(
                    "recalc_noop_while_stale",
                    "error",
                    f"recalc left {e.get('content_hash')} ({e.get('alias')}) noop although it is stale",
                    rec,
                )
        state = rec.args.get("_state_fp")
        if state:
            remap = full.get("remap") or {}
            if state in self.recalcs and self.recalcs[state] != remap:
                s.flag(
                    "recalc_not_deterministic",
                    "error",
                    "the same recalc from the same state minted different hashes",
                    rec,
                    before=self.recalcs[state],
                    now=remap,
                )
            self.recalcs.setdefault(state, remap)

    def state_fingerprint(self) -> str:
        """What a recalc's outcome may depend on: the alias heads and histories. A source is an alias too, and the
        file it was imported from is never read again (ADR-011 D2), so no file on disk belongs here."""
        from tallyman_core.aliases import load_aliases, load_history

        return fingerprint({"heads": load_aliases(self.project), "history": load_history(self.project)})

    def stale_hashes(self, rec: StepRecord) -> set[str]:
        """The entries ``/api/staleness`` calls stale right now."""
        resp = self.s._request("GET", f"/{self.project}/api/staleness", None, rec)
        if resp.status_code != 200:
            return set()
        return {h for h, v in resp.json().get("entries", {}).items() if v.get("stale")}

    def _check_staleness(self, rec: StepRecord) -> None:
        s = self.s
        resp = s._request("GET", f"/{self.project}/api/staleness", None, rec)
        if resp.status_code != 200:
            s.flag("staleness_failed", "error", f"/api/staleness -> {resp.status_code}", rec)
            return
        body = resp.json()
        if s.auto_recalc:
            for h, v in body.get("entries", {}).items():
                alias_reasons = [r for r in v.get("reasons", []) if r.get("axis") == "alias"]
                if v.get("live") and v.get("stale") and alias_reasons:
                    s.flag(
                        "cascade_missed",
                        "error",
                        f"{h} follows {alias_reasons[0]['ref']} at {alias_reasons[0]['was']}, head is "
                        f"{alias_reasons[0]['now']}",
                        rec,
                    )
            if body.get("orphan_stale"):
                s.flag("orphan_stale", "warn", f"orphans: {body['orphan_stale']}", rec)

    # ------------------------------------------------------------------
    # files
    # ------------------------------------------------------------------

    def _check_snapshot(self, h: str, m: dict, rec: StepRecord) -> None:
        import pyarrow.parquet as pq

        from tallyman_xorq.materialize import snapshot_path
        from tallyman_xorq.result_cache import verify_result_faithful

        snap = snapshot_path(self.project, h)
        if not m.get("cache_worthy") or not snap.exists():
            return
        try:
            if verify_result_faithful(self.project, h) is False:
                self.s.flag("snapshot_unfaithful", "error", f"{h}: snapshot digest != result_digest", rec)
            names = pq.read_schema(snap).names
            ro = pq.read_table(snap, columns=[ROW_ORDER]).column(0).to_pylist() if ROW_ORDER in names else None
        except Exception as exc:
            self.s.flag("verify_error", "error", f"{h}: {type(exc).__name__}: {exc}"[:400], rec)
            return
        if ro is None or names[-1] != ROW_ORDER or ro != list(range(len(ro))):
            self.s.flag("snapshot_row_order_broken", "error", f"{h}: {ROW_ORDER} is not 0..N-1 and last", rec)
        if ro is not None and len(ro) != m.get("row_count"):
            self.s.flag(
                "snapshot_rowcount_mismatch",
                "error",
                f"{h}: snapshot has {len(ro)} rows, manifest {m.get('row_count')}",
                rec,
            )

    def compute_cache_listing(self) -> dict[str, int]:
        from tallyman_core.paths import compute_cache_dir

        root = compute_cache_dir(self.project)
        if not root.is_dir():
            return {}
        return {str(p.relative_to(root)): p.stat().st_size for p in root.rglob("*") if p.is_file()}

    def _xorq_cache_listing(self) -> set[str]:
        root = os.environ.get("XORQ_CACHE_DIR")
        if not root or not Path(root).is_dir():
            return set()
        return {str(p) for p in Path(root).rglob("*") if p.is_file()}

    def global_state(self, rec: StepRecord) -> None:
        written = self._xorq_cache_listing() - self._xorq_cache_baseline
        if written:
            self.s.flag(
                "xorq_global_cache_written",
                "error",
                f"{len(written)} file(s) under XORQ_CACHE_DIR",
                rec,
                files=sorted(written)[:10],
            )

    def _temp_files(self) -> list[str]:
        from tallyman_core.paths import compute_cache_dir, entries_dir

        out = []
        for root in (compute_cache_dir(self.project), entries_dir(self.project)):
            if root.is_dir():
                out += [str(p) for p in root.rglob("*.tmp")]
        return out

    def _entry_hashes_on_disk(self) -> set[str]:
        from tallyman_core.paths import ENTRY_MANIFEST_FILENAME, entries_dir

        base = entries_dir(self.project)
        if not base.is_dir():
            return set()
        return {p.name for p in base.iterdir() if p.is_dir() and (p / ENTRY_MANIFEST_FILENAME).exists()}

    def _manifest(self, h: str) -> dict:
        from tallyman_core.paths import entry_manifest_path

        p = entry_manifest_path(self.project, h)
        try:
            return json.loads(p.read_text())
        except (OSError, ValueError):
            return {}

    def _errors(self) -> list[dict]:
        from tallyman_core.paths import errors_path

        p = errors_path(self.project)
        if not p.exists():
            return []
        out = []
        for line in p.read_text().splitlines():
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
        return out

    def _catalog_dirty(self) -> list[str]:
        rc, out, _ = run_git(["status", "--porcelain", "--untracked-files=no"], cwd=catalog_dir(self.project))
        return out.splitlines() if rc == 0 else []

    def ledger_summary(self) -> dict:
        return {
            h: {"first_step": e["first_step"], "observed": sorted(str(k) for k in e["pages"])}
            for h, e in self.ledger.items()
        }


def _listing_delta(before: dict[str, int], after: dict[str, int]) -> dict:
    return {
        "added": sorted(set(after) - set(before))[:20],
        "removed": sorted(set(before) - set(after))[:20],
        "changed": sorted(k for k in set(before) & set(after) if before[k] != after[k])[:20],
    }
