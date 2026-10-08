import { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { formatDuration, layoutTimeline, queueWaitMs, traceLabel, type TimingResponse } from "../activity";
import { api } from "../api";
import { useSSE } from "../SSEContext";

// What the companion and Buckaroo spent their time on, on one clock. The companion's blocking actions come from its
// own tracker; Buckaroo's spans are the ones it posts back as each finishes (buckaroo#943). Buckaroo serves one request
// at a time, so a `buckaroo load_expr` that arrives during another session's `stats.complete` shows as overlapping bars,
// and the page says how long after the POST started Buckaroo began handling it (queueing plus the request's own
// transfer and parsing, which this does not separate).

const WINDOWS = [
  { label: "5 min", seconds: 300 },
  { label: "15 min", seconds: 900 },
  { label: "1 hour", seconds: 3600 },
];

// The Buckaroo spans worth a row; the rest (stat.xorq.*, window_to_parquet) are inside these.
const BUCKAROO_SPANS = new Set([
  "firstpull.load_expr",
  "stats.complete",
  "firstpull.ws_first_payload",
  "firstpull.ws_second_payload",
]);

interface Row {
  key: string;
  lane: "companion" | "buckaroo";
  label: string;
  start_ms: number;
  end_ms: number | null;
  note: string;
  failed: boolean;
}

function clock(ms: number): string {
  const d = new Date(ms);
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(Math.floor(d.getMilliseconds() / 100), 1)}`;
}

function buildRows(t: TimingResponse, aliasOf: (hash: string) => string): Row[] {
  const rows: Row[] = t.actions.map((a) => {
    const wait = queueWaitMs(a, t.spans);
    const queued = wait !== null && wait >= 200 ? `Buckaroo began handling it ${formatDuration(wait)} after the POST started` : "";
    const detail = /^[0-9a-f]{12}$/.test(a.detail) ? aliasOf(a.detail) : a.detail;
    return {
      key: `a${a.id}`,
      lane: "companion",
      label: detail ? `${a.name} ${detail}` : a.name,
      start_ms: a.started_ms,
      end_ms: a.ended_ms,
      note: [queued, a.error ?? ""].filter(Boolean).join(" · "),
      failed: a.status === "error",
    };
  });
  t.spans
    .filter((s) => BUCKAROO_SPANS.has(s.name))
    .forEach((s, i) => {
      const gen = s.attrs?.stats_gen;
      rows.push({
        key: `s${i}-${s.trace}-${s.name}-${s.t_start_ms}`,
        lane: "buckaroo",
        label: `${s.name.replace(/^firstpull\./, "")} ${aliasOf(traceLabel(s.trace))}`,
        start_ms: s.t_start_ms,
        end_ms: s.t_end_ms,
        note: gen !== undefined && gen !== null ? `stats_gen=${gen}` : "",
        failed: s.attrs?.errored === "true" || s.attrs?.errored === true,
      });
    });
  return rows.sort((a, b) => a.start_ms - b.start_ms);
}

// Rows that start within BURST_GAP_MS of the latest end so far belong to one burst of activity. Each burst gets its own
// time axis: on one axis for the whole window, a half-second action next to another half minutes later is a sliver.
const BURST_GAP_MS = 2000;

function groupBursts(rows: Row[], nowMs: number): Row[][] {
  const bursts: Row[][] = [];
  let latestEnd = -Infinity;
  for (const r of rows) {
    if (bursts.length === 0 || r.start_ms - latestEnd > BURST_GAP_MS) bursts.push([]);
    bursts[bursts.length - 1].push(r);
    latestEnd = Math.max(latestEnd, r.end_ms ?? nowMs);
  }
  return bursts;
}

export function TimingPage() {
  const { project } = useParams<{ project: string }>();
  const { activity } = useSSE();
  const [windowS, setWindowS] = useState(900);
  const [data, setData] = useState<TimingResponse | null>(null);
  const [aliases, setAliases] = useState<Record<string, string>>({});
  const [err, setErr] = useState(false);

  useEffect(() => {
    if (!project) return;
    api
      .entries(project)
      .then((r) => {
        const m: Record<string, string> = {};
        for (const e of r.entries) if (e.alias) m[e.content_hash.slice(0, 12)] = e.alias;
        setAliases(m);
      })
      .catch(() => {});
  }, [project]);

  // Refetch when an action starts or ends, and every 2 s so Buckaroo's spans (which arrive as they finish) and a
  // running action's bar stay current.
  const activityKey = `${activity.running.map((a) => a.id).join(",")}|${activity.last?.id ?? ""}`;
  useEffect(() => {
    if (!project) return;
    let live = true;
    const load = () =>
      api
        .timing(project, windowS)
        .then((t) => {
          if (live) {
            setData(t);
            setErr(false);
          }
        })
        .catch(() => live && setErr(true));
    load();
    const t = setInterval(load, 2000);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, [project, windowS, activityKey]);

  const rows = useMemo(
    () => (data ? buildRows(data, (h) => aliases[h] ?? h) : []),
    [data, aliases],
  );
  const bursts = useMemo(() => groupBursts(rows, data?.now_ms ?? Date.now()), [rows, data]);

  return (
    <div className="timing-page">
      <h2>timing</h2>
      <p className="meta">
        The companion&apos;s blocking actions and Buckaroo&apos;s spans on one clock. Buckaroo handles one request at a time, so
        a load that arrives while another session&apos;s stats run is going waits for them: look for a companion bar that
        overlaps a Buckaroo <code>stats.complete</code> bar.
      </p>
      <div className="timing-controls">
        {WINDOWS.map((w) => (
          <button
            key={w.seconds}
            type="button"
            className={w.seconds === windowS ? "active" : ""}
            onClick={() => setWindowS(w.seconds)}
          >
            {w.label}
          </button>
        ))}
        <span className="timing-legend">
          <span className="swatch companion" /> companion <span className="swatch buckaroo" /> buckaroo
        </span>
      </div>
      {err && <div className="meta">timing unavailable</div>}
      {!err && data === null && <div className="meta">loading…</div>}
      {data !== null && rows.length === 0 && (
        <div className="meta">nothing in the last {WINDOWS.find((w) => w.seconds === windowS)?.label ?? `${windowS}s`}.</div>
      )}
      {bursts.length > 0 && (
        <div className="timing-rows">
          {[...bursts].reverse().map((burst) => {
            const nowMs = data!.now_ms;
            const layout = layoutTimeline(burst, nowMs);
            const last = Math.max(...burst.map((r) => r.end_ms ?? nowMs));
            return (
              <div key={burst[0].key} className="timing-burst">
                <div className="timing-axis meta">
                  {clock(burst[0].start_ms)} → {clock(last)} · {formatDuration(last - burst[0].start_ms)}
                </div>
                {burst.map((r, i) => {
                  const ms = (r.end_ms ?? nowMs) - r.start_ms;
                  return (
                    <div key={r.key} className={`timing-row ${r.lane}`}>
                      <span className="timing-start">{clock(r.start_ms)}</span>
                      <span className="span-name timing-name" title={r.label}>
                        {r.label}
                      </span>
                      <span className="span-track">
                        <span
                          className={`span-bar ${r.lane}${r.failed ? " errored" : ""}${r.end_ms === null ? " running" : ""}`}
                          style={{ left: `${layout[i].left}%`, width: `${layout[i].width}%` }}
                          title={`${r.label} · ${formatDuration(ms)}`}
                        />
                      </span>
                      <span className="span-ms timing-ms">{formatDuration(ms)}</span>
                      {r.note && <span className={`meta timing-note${r.failed ? " failed" : ""}`}>{r.note}</span>}
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
