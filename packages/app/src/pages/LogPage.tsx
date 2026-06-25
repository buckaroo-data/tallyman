import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { useSSE } from "../SSEContext";
import type { ActivityLog, LogEvent } from "../types";

// The project activity log: a linear, cross-session feed of MCP build runs
// (success/failure with the full traceback), alias creations, and Buckaroo grid
// loads. Filterable by category (mcp/alias/buckaroo) and by the session that
// produced the events — multiple Claude Code sessions write to one project.

const CATEGORIES = ["mcp", "alias", "buckaroo"] as const;
type Category = (typeof CATEGORIES)[number];

const ICON: Record<string, string> = { build_error: "✗", build_ok: "✓", alias_set: "★", buckaroo: "▤" };
const SESSION_COLORS = ["#6ea8fe", "#73ae80", "#e0915e", "#b39ddb", "#7fd1c4", "#d6b35e", "#e8849b", "#9ab07a"];

function sessionColor(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return SESSION_COLORS[h % SESSION_COLORS.length];
}

function fmtTime(ts: string): string {
  return ts.slice(11, 19); // HH:MM:SS (UTC, as stored)
}

function summarize(e: LogEvent): string {
  switch (e.kind) {
    case "build_error":
      return `build failed · ${e.tool ?? "?"} · ${(e.message ?? "").split("\n")[0]}`;
    case "build_ok":
      return `ran · ${e.tool ?? "?"}${e.hash ? ` · ${e.hash.slice(0, 12)}` : ""}`;
    case "alias_set":
      return `${e.alias} V${e.version} · ${e.tool ?? "?"}`;
    case "buckaroo":
      return `grid ${e.status ?? "?"}${e.hash ? ` · ${e.hash.slice(0, 12)}` : ""}${e.load_ms != null ? ` · ${e.load_ms}ms` : ""}`;
    default:
      return e.kind;
  }
}

function hasDetail(e: LogEvent): boolean {
  return Boolean(e.code || e.prompt || e.traceback || e.detail || e.message || e.load_ms != null || e.hash);
}

export function LogPage() {
  const { project } = useParams<{ project: string }>();
  const { version } = useSSE();
  const [data, setData] = useState<ActivityLog | null>(null);
  const [cats, setCats] = useState<Set<Category>>(new Set(CATEGORIES));
  const [session, setSession] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [oldestFirst, setOldestFirst] = useState(false);

  useEffect(() => {
    if (!project) return;
    api
      .log(project)
      .then(setData)
      .catch(() => {});
  }, [project, version]); // SSE bumps `version` on new_entry/build_failed → live refresh

  const events = useMemo(() => {
    if (!data) return [];
    let evs = data.events.filter((e) => cats.has(e.category as Category));
    if (session) evs = evs.filter((e) => e.session === session);
    if (oldestFirst) evs = [...evs].reverse();
    return evs;
  }, [data, cats, session, oldestFirst]);

  if (!project) return null;

  const toggleCat = (c: Category) =>
    setCats((s) => {
      const n = new Set(s);
      if (n.has(c)) n.delete(c);
      else n.add(c);
      return n;
    });
  const toggleExpand = (key: string) =>
    setExpanded((s) => {
      const n = new Set(s);
      if (n.has(key)) n.delete(key);
      else n.add(key);
      return n;
    });

  return (
    <main style={{ padding: "8px 12px", overflow: "auto" }}>
      <div className="log-page">
        <div className="log-filters">
          <span className="meta">show</span>
          {CATEGORIES.map((c) => (
            <button key={c} type="button" className={`chip${cats.has(c) ? " on" : ""}`} onClick={() => toggleCat(c)}>
              {c}
            </button>
          ))}
          <span className="log-sep" />
          <span className="meta">session</span>
          <button type="button" className={`chip${session === null ? " on" : ""}`} onClick={() => setSession(null)}>
            all
          </button>
          {data?.sessions.map((s) => (
            <button
              key={s.session}
              type="button"
              className={`chip${session === s.session ? " on" : ""}`}
              onClick={() => setSession(session === s.session ? null : s.session)}
              title={`${s.count} events · last ${fmtTime(s.last_ts)}`}
            >
              <span className="session-dot" style={{ background: sessionColor(s.session) }} /> {s.session}
            </button>
          ))}
          <span className="log-sep" />
          <button type="button" className="chip" onClick={() => setOldestFirst((v) => !v)}>
            {oldestFirst ? "oldest first" : "newest first"}
          </button>
        </div>

        {!data ? (
          <div className="meta">loading…</div>
        ) : events.length === 0 ? (
          <div className="meta">{data.events.length ? "no events match the filters." : "no activity recorded yet."}</div>
        ) : (
          <ol className="log-list">
            {events.map((e, i) => {
              const key = `${e.ts}-${i}`;
              const open = expanded.has(key);
              const detail = hasDetail(e);
              return (
                <li key={key} className={`log-row kind-${e.kind}`}>
                  <div
                    className="log-head"
                    role={detail ? "button" : undefined}
                    tabIndex={detail ? 0 : undefined}
                    aria-expanded={detail ? open : undefined}
                    onClick={() => {
                      // Don't toggle if the user is selecting text to copy.
                      if (detail && !window.getSelection()?.toString()) toggleExpand(key);
                    }}
                    onKeyDown={(ev) => {
                      if (detail && (ev.key === "Enter" || ev.key === " ")) {
                        ev.preventDefault();
                        toggleExpand(key);
                      }
                    }}
                    style={{ cursor: detail ? "pointer" : "default" }}
                  >
                    <span className="log-time">{fmtTime(e.ts)}</span>
                    <span className={`cat-badge cat-${e.category}`}>
                      {ICON[e.kind] ?? "•"} {e.category}
                    </span>
                    {e.session ? (
                      <span
                        className="session-badge"
                        style={{ borderColor: sessionColor(e.session), color: sessionColor(e.session) }}
                      >
                        {e.session}
                      </span>
                    ) : e.origin === "companion" ? (
                      <span className="session-badge muted">companion</span>
                    ) : null}
                    <span className="log-summary">{summarize(e)}</span>
                    {detail && <span className="log-caret">{open ? "▾" : "▸"}</span>}
                  </div>
                  {open && (
                    <div className="log-detail">
                      {e.prompt && (
                        <div className="log-prompt">
                          <strong>prompt:</strong> {e.prompt}
                        </div>
                      )}
                      {e.hash && (
                        <div className="meta">
                          entry <Link to={`/${project}/catalog/${e.hash}`}>{e.hash.slice(0, 12)}</Link>
                        </div>
                      )}
                      {e.detail && <div className="meta">{e.detail}</div>}
                      {(e.load_ms != null || e.load_expr_ms != null) && (
                        <div className="meta">
                          timing:{e.load_ms != null && ` ${e.load_ms}ms total`}
                          {e.load_expr_ms != null && ` · ${e.load_expr_ms}ms load_expr`}
                        </div>
                      )}
                      {e.message && (
                        <>
                          <div className="meta">message</div>
                          <pre className="code log-message">{e.message}</pre>
                        </>
                      )}
                      {e.code && (
                        <>
                          <div className="meta">code</div>
                          <pre className="code log-message">{e.code}</pre>
                        </>
                      )}
                      {e.traceback && (
                        <>
                          <div className="meta">traceback</div>
                          <pre className="code log-traceback">{e.traceback}</pre>
                        </>
                      )}
                      {e.kind === "build_error" && !e.traceback && (
                        <div className="meta">
                          no stacktrace stored for this build — full tracebacks are captured only for
                          failures recorded after the activity-log change; this one predates it.
                        </div>
                      )}
                    </div>
                  )}
                </li>
              );
            })}
          </ol>
        )}
      </div>
    </main>
  );
}
