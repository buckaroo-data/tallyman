import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { useSSE } from "../SSEContext";
import { recalcTarget } from "../recalcRefresh";
import { CatalogSidebar } from "../components/CatalogSidebar";
import { BuckarooEmbed } from "../components/BuckarooEmbed";
import { VegaChart } from "../components/VegaChart";
import { linkifyRefs } from "../codeLinks";
import type { Entry, AppError, EntryDetail, EntryCache, SessionResult } from "../types";

// ── Error banner ──────────────────────────────────────────────────────────────

function ErrorBanner({
  errors,
  project,
  onCleared,
}: {
  errors: AppError[];
  project: string;
  onCleared: () => void;
}) {
  const [hidden, setHidden] = useState(false);
  const [collapsed, setCollapsed] = useState(false);

  if (errors.length === 0 || hidden) return null;

  const handleClear = () => {
    api
      .clearErrors(project)
      .then(onCleared)
      .catch(() => {});
  };

  return (
    <div className="error-banner">
      <button
        type="button"
        className="dismiss"
        aria-label="hide"
        title="hide for now"
        onClick={() => (collapsed ? setHidden(true) : setCollapsed(true))}
      >
        ×
      </button>
      <strong>recent build failures:</strong>{" "}
      <button type="button" className="clear-errors" onClick={handleClear}>
        clear all
      </button>
      {!collapsed && (
        <ul>
          {errors.map((err) => (
            <li key={err.id}>
              <Link to={`/${project}/errors/${err.id}`}>{err.id}</Link>
              {err.tool && <code className="pill">{err.tool}</code>}
              <span className="meta"> · {err.created_at}</span>
              {err.prompt && <em> · {err.prompt}</em>}
              {" — "}
              <code>{err.message.split("\n")[0].slice(0, 120)}</code>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ── Entry detail pane ─────────────────────────────────────────────────────────

type DetailTab = "data" | "code" | "metadata";

function EntryDetailPane({ project, hash }: { project: string; hash: string }) {
  const navigate = useNavigate();
  const [detail, setDetail] = useState<EntryDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState<DetailTab>("data");
  const [editingCode, setEditingCode] = useState(false);
  const [codeValue, setCodeValue] = useState("");
  const [codeStatus, setCodeStatus] = useState("");

  useEffect(() => {
    setLoading(true);
    setTab("data");
    setEditingCode(false);
    api
      .entryDetail(project, hash)
      .then((d) => {
        setDetail(d);
        setCodeValue(d.code);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [project, hash]);

  if (loading)
    return (
      <div className="spinner-row">
        <span className="spinner" /> loading…
      </div>
    );
  if (!detail) return <div className="meta">entry not found</div>;

  const { manifest, alias, version, forensic_history, prompt_history, chart_spec,
    display_config, build_artifacts, total_rows, code } = detail;
  const diffProvenance = display_config?.diff_provenance;

  const handleSaveCode = async () => {
    if (!alias) return;
    setCodeStatus("building…");
    try {
      const res = await api.putCode(project, alias, codeValue);
      navigate(`/${project}/catalog/${res.hash}`);
    } catch (e) {
      setCodeStatus(`build failed: ${e instanceof Error ? e.message.slice(0, 200) : e}`);
    }
  };

  return (
    <div className="detail entry-detail">
      <div className="entry-header">
        <h2>
          {alias ? (
            <>
              {alias} <span className="vchip">V{version}</span>
            </>
          ) : (
            manifest.content_hash
          )}
        </h2>
        <span className="pill">{manifest.content_hash}</span>
        <span className="pill">{manifest.row_count} rows</span>
        <span className="pill">{manifest.execute_seconds}s</span>
        <span className="pill">{manifest.created_at}</span>
      </div>

      <div className="prompt-row">
        <div className="prompt-text">
          {manifest.prompt ? (
            <>
              <strong>prompt:</strong> {manifest.prompt}
            </>
          ) : (
            <span className="meta">no prompt recorded</span>
          )}
        </div>
      </div>

      {prompt_history.length > 1 && (
        <details className="meta-aside">
          <summary className="meta">{prompt_history.length} prompts seen for this hash</summary>
          <ul>
            {prompt_history.map((p, i) => (
              <li key={i}>
                <span className="meta">{p.at}</span> — {p.prompt}
              </li>
            ))}
          </ul>
        </details>
      )}

      {forensic_history.length > 1 && (
        <div className="forensic-history">
          <strong>versions of {alias}</strong>
          <ol>
            {forensic_history.map((h) => (
              <li key={h.hash} className={h.is_current ? "current" : ""}>
                <Link to={`/${project}/catalog/${h.hash}`}>
                  V{h.version} · {h.hash}
                  {h.is_current && " (current)"}
                </Link>
              </li>
            ))}
          </ol>
          <p className="meta">
            diff:{" "}
            {forensic_history.slice(0, -1).map((h) => (
              <span key={h.version}>
                <Link to={`/${project}/diff/${alias}/${h.version}/${h.version + 1}`}>
                  V{h.version}→V{h.version + 1}
                </Link>{" "}
                ·{" "}
              </span>
            ))}
          </p>
        </div>
      )}

      {chart_spec && (
        <VegaChart spec={chart_spec} dataHash={manifest.content_hash} project={project} />
      )}

      <div className="detail-tabs" role="tablist">
        <button
          type="button"
          role="tab"
          aria-selected={tab === "data"}
          className={`detail-tab${tab === "data" ? " active" : ""}`}
          onClick={() => setTab("data")}
        >
          data
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={tab === "code"}
          className={`detail-tab${tab === "code" ? " active" : ""}`}
          onClick={() => setTab("code")}
        >
          code
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={tab === "metadata"}
          className={`detail-tab${tab === "metadata" ? " active" : ""}`}
          onClick={() => setTab("metadata")}
        >
          metadata
        </button>
      </div>

      {/* Keep the data pane mounted (hidden when inactive) so switching to the
          code tab doesn't tear down Buckaroo's websocket session. */}
      <div className="tab-panel" role="tabpanel" hidden={tab !== "data"}>
        {diffProvenance && (
          <div className="meta" style={{ marginBottom: 6, fontSize: 12 }}>
            diff ·{" "}
            <Link to={`/${project}/diff/${diffProvenance.source_alias}/${diffProvenance.va}/${diffProvenance.vb}`}>
              {diffProvenance.source_alias} V{diffProvenance.va}→V{diffProvenance.vb}
            </Link>
            {"  "}
            <span style={{ display: "inline-block", width: 10, height: 10, background: "#e8b4b8", borderRadius: 2, margin: "0 2px" }} />
            {" "}only in V{diffProvenance.va} ·{" "}
            <span style={{ display: "inline-block", width: 10, height: 10, background: "#90b2b3", borderRadius: 2, margin: "0 2px" }} />
            {" "}only in V{diffProvenance.vb} ·{" "}
            <span style={{ display: "inline-block", width: 10, height: 10, background: "#73ae80", borderRadius: 2, margin: "0 2px" }} />
            {" "}matched
          </div>
        )}
        <BuckarooDataPane project={project} hash={manifest.content_hash} totalRows={total_rows} />
      </div>

      {tab === "code" && (
        <div className="tab-panel metadata-section" role="tabpanel">
          <h3>
            code
            {alias && (
              <button
                type="button"
                className="edit-code-btn"
                onClick={() => setEditingCode((v) => !v)}
              >
                {editingCode ? "cancel" : "edit + revise"}
              </button>
            )}
          </h3>
          {editingCode ? (
            <>
              <textarea
                className="nb-markdown-edit"
                style={{ minHeight: 200, fontFamily: "ui-monospace, monospace", fontSize: 12 }}
                value={codeValue}
                onChange={(e) => setCodeValue(e.target.value)}
              />
              <div style={{ marginTop: 6, display: "flex", gap: 8 }}>
                <button type="button" onClick={handleSaveCode}>
                  save → revise
                </button>
                <button type="button" onClick={() => { setEditingCode(false); setCodeValue(code); }}>
                  cancel
                </button>
              </div>
              {codeStatus && <div className="meta" style={{ marginTop: 4 }}>{codeStatus}</div>}
            </>
          ) : (
            <>
              {manifest.parents && manifest.parents.length > 0 && (
                <p className="meta code-refs-note">
                  loads {manifest.parents.length === 1 ? "a catalog entry" : "catalog entries"}:{" "}
                  {manifest.parents.map((p, i) => (
                    <span key={p.hash}>
                      {i > 0 && ", "}
                      <Link to={`/${project}/catalog/${p.hash}`}>{p.ref}</Link>
                      {" @ "}
                      <code className="hash">{p.hash.slice(0, 12)}</code>
                      {p.follow ? " (alias head at build)" : " (pinned)"}
                    </span>
                  ))}
                </p>
              )}
              <CodeWithLinks code={code} project={project} parents={manifest.parents} />
            </>
          )}

          {build_artifacts.length > 0 && (
            <details>
              <summary className="meta">
                build artifacts ({build_artifacts.length} files)
              </summary>
              {build_artifacts.map((f) => (
                <div key={f.name}>
                  <h4 style={{ marginBottom: 4 }}>{f.name}</h4>
                  <pre className="code">{f.text}</pre>
                </div>
              ))}
            </details>
          )}
        </div>
      )}

      {tab === "metadata" && (
        <div className="tab-panel metadata-section" role="tabpanel">
          <EntryCacheView project={project} hash={manifest.content_hash} />
        </div>
      )}
    </div>
  );
}

// ── Code with clickable catalog refs (#135) ──────────────────────────────────

function CodeWithLinks({
  code,
  project,
  parents,
}: {
  code: string;
  project: string;
  parents?: { hash: string; ref: string; follow: boolean }[] | null;
}) {
  const segments = linkifyRefs(code, parents ?? null);
  return (
    <pre className="code">
      {segments.map((seg, i) =>
        typeof seg === "string" ? (
          seg
        ) : (
          <Link
            key={i}
            to={`/${project}/catalog/${seg.hash}`}
            className="code-ref"
            title={`${seg.ref} → ${seg.hash}${seg.follow ? " (alias head at build time)" : " (pinned revision)"}`}
          >
            "{seg.ref}"
          </Link>
        ),
      )}
    </pre>
  );
}

// ── Lazy Buckaroo grid: spinner → grid, or a precise error + retry (#133) ─────

function BuckarooDataPane({ project, hash, totalRows }: { project: string; hash: string; totalRows: number }) {
  const [result, setResult] = useState<SessionResult | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setResult(null);
    api
      .session(project, hash)
      .then((d) => {
        if (!cancelled) setResult(d);
      })
      .catch((e) => {
        if (!cancelled)
          setResult({ status: "error", ws_url: null, detail: e instanceof Error ? e.message : String(e) });
      });
    return () => {
      cancelled = true;
    };
  }, [project, hash, attempt]);

  if (result === null) {
    return (
      <div className="buckaroo-loading">
        <span className="spinner" /> loading data…
      </div>
    );
  }
  if (result.status === "ok" && result.ws_url) {
    return <BuckarooEmbed wsUrl={result.ws_url} className="buckaroo-embed" />;
  }

  const retry = () => setAttempt((n) => n + 1);

  if (result.status === "unavailable" || result.status === "no_build") {
    return (
      <div className="meta">
        {result.status === "no_build"
          ? "no build for this entry"
          : totalRows > 0
            ? `${totalRows} rows (Buckaroo not available — run tallyman with --buckaroo)`
            : "no rows"}
        {result.status === "unavailable" && (
          <>
            {" · "}
            <button type="button" className="link-btn" onClick={retry}>
              retry
            </button>
          </>
        )}
      </div>
    );
  }

  // timeout | error — surface what went wrong and a way out.
  return (
    <div className="buckaroo-error">
      <strong>
        {result.status === "timeout"
          ? "Buckaroo timed out loading this entry."
          : "Buckaroo couldn’t load this entry."}
      </strong>
      {result.detail && <div className="meta detail">{result.detail}</div>}
      <div className="meta">
        <button type="button" className="link-btn" onClick={retry}>
          retry
        </button>
        {" · to debug, check "}
        <code>~/.tallyman-notebooks/projects/{project}/buckaroo.log</code>
        {" and the companion log, or run the "}
        <code>debug-tallyman</code>
        {" skill."}
      </div>
    </div>
  );
}

// ── Per-expression cache footprint (metadata tab) ─────────────────────────────

function ShareBar({ value, total }: { value: number; total: number }) {
  const pct = total > 0 ? Math.max(value > 0 ? 2 : 0, (value / total) * 100) : 0;
  return (
    <div className="cache-bar-track" aria-hidden="true">
      <div className="cache-bar-fill" style={{ width: `${pct}%` }} />
    </div>
  );
}

function EntryCacheView({ project, hash }: { project: string; hash: string }) {
  const [data, setData] = useState<EntryCache | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setData(null);
    setError(null);
    api
      .entryCache(project, hash)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [project, hash]);

  if (error) return <div className="meta">could not load cache metadata: {error}</div>;
  if (!data) return <div className="meta">measuring on-disk footprint…</div>;

  return (
    <div className="cache-meta">
      <div className="cache-summary">
        <strong>{data.total_formatted} on disk for this expression</strong>
        <span className="meta">
          {data.cache_formatted} reclaimable cache · {data.artifact_formatted} build artifact
          {data.diff_cache_bytes > 0 && <> · {data.diff_cache_formatted} shared diff cache</>}
        </span>
        <span className="meta">
          {data.source_tracked
            ? `${data.source_formatted} raw source${data.sources.length > 1 ? ` (${data.sources.length} files)` : ""}`
            : "raw source size not tracked (source-identity off)"}
          {data.modified_at && <> · last modified {data.modified_at.slice(0, 19).replace("T", " ")}</>}
        </span>
        <span className="meta cache-note">
          <strong>{data.cache_worthy ? "Expensive" : "Cheap"} alias.</strong>{" "}
          {data.cache_worthy
            ? "Its result is kept in the xorq snapshot cache and recomputed on a miss."
            : "No snapshot copy — the result is recomputed from the build on every read."}
          {data.cache_worthy_why && <span className="cache-why"> ({data.cache_worthy_why})</span>}
        </span>
      </div>

      <table className="data-table cache-table">
        <thead>
          <tr>
            <th>Component</th>
            <th>Type</th>
            <th className="num">Size</th>
            <th className="num">Share</th>
          </tr>
        </thead>
        <tbody>
          {data.components.map((c) => {
            const na = c.key === "snapshot_cache" && c.applicable === false;
            return (
              <tr key={c.key}>
                <td>
                  <div>{c.label}</div>
                  <div className="meta cache-detail">
                    {na ? "not applicable to a cheap expression" : c.detail}
                  </div>
                </td>
                <td>
                  <span className={`pill ${c.kind === "cache" ? "pill-cache" : "pill-artifact"}`}>
                    {c.kind === "cache" ? "cache · reclaimable" : "artifact · kept"}
                  </span>
                </td>
                <td className="num size">{na ? "—" : c.formatted}</td>
                <td className="num">
                  {na ? <span className="muted">—</span> : <ShareBar value={c.bytes} total={data.total_bytes} />}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      {data.diff_caches.length > 0 && (
        <div className="cache-diffs">
          <h4>diff stat caches involving this entry</h4>
          <p className="meta">
            Keyed by a version pair, so each is shared with the other version and counted apart from the
            per-expression subtotal above.
          </p>
          <table className="data-table cache-table">
            <thead>
              <tr>
                <th>Paired with</th>
                <th className="num">Size</th>
              </tr>
            </thead>
            <tbody>
              {data.diff_caches.map((d) => (
                <tr key={d.pair}>
                  <td>
                    {d.other_alias ? <span className="alias-name">{d.other_alias}</span> : null}{" "}
                    {d.other_hash ? (
                      <Link className="hash" to={`/${project}/catalog/${d.other_hash}`}>
                        {d.other_hash.slice(0, 12)}
                      </Link>
                    ) : (
                      <code className="hash">{d.other_hash_prefix}…</code>
                    )}
                  </td>
                  <td className="num size">{d.formatted}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {(data.parents.length > 0 || data.children.length > 0) && (
        <div className="cache-lineage">
          {data.parents.length > 0 && (
            <div>
              <h4>built from</h4>
              <ul className="lineage-list">
                {data.parents.map((p) => (
                  <li key={p.hash}>
                    <Link to={`/${project}/catalog/${p.hash}`}>{p.alias ?? p.ref ?? p.hash.slice(0, 12)}</Link>
                    {p.alias && p.ref !== p.alias && <span className="meta"> (as {p.ref})</span>}
                    <span className="meta"> · {p.follow ? "follows alias" : "pinned"}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
          {data.children.length > 0 && (
            <div>
              <h4>used by</h4>
              <ul className="lineage-list">
                {data.children.map((ch) => (
                  <li key={ch.hash}>
                    <Link to={`/${project}/catalog/${ch.hash}`}>{ch.alias ?? ch.hash.slice(0, 12)}</Link>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Error detail pane ─────────────────────────────────────────────────────────

function ErrorDetailPane({ project, errorId }: { project: string; errorId: string }) {
  const [data, setData] = useState<{ error: AppError } | null>(null);

  useEffect(() => {
    api.errorDetail(project, errorId).then(setData).catch(() => {});
  }, [project, errorId]);

  if (!data) return <div className="meta">loading…</div>;
  const { error } = data;

  return (
    <div className="detail">
      <h2>build failure · {error.id}</h2>
      <div className="meta">
        {error.tool && <span className="pill">tool: {error.tool}</span>}
        <span className="pill">{error.created_at}</span>
      </div>
      {error.prompt && (
        <p>
          <strong>prompt:</strong> {error.prompt}
        </p>
      )}
      <h3>error</h3>
      <pre className="code">{error.message}</pre>
      <h3>code</h3>
      <pre className="code">{error.code}</pre>
    </div>
  );
}

// ── Catalog page ──────────────────────────────────────────────────────────────

export function CatalogPage() {
  const { project, hash, errorId } = useParams<{
    project: string;
    hash?: string;
    errorId?: string;
  }>();
  const { version, lastEvent } = useSSE();
  const navigate = useNavigate();
  const [entries, setEntries] = useState<Entry[]>([]);
  const [errors, setErrors] = useState<AppError[]>([]);
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem("catalog.sidebarCollapsed") === "1",
  );

  useEffect(() => {
    if (!project) return;
    api.entries(project).then((d) => setEntries(d.entries)).catch(() => {});
    api.errors(project, 5).then((d) => setErrors(d.errors)).catch(() => {});
  }, [project, version]);

  // A recalc re-points aliases to recomputed hashes; if the open entry view is
  // one of them (and this tab is backgrounded — see recalcTarget), navigate to
  // the new hash so the detail pane (keyed on the URL hash) refetches the fresh
  // result. Process each SSE version at most once (StrictMode double-fires
  // effects), mirroring NewEntryPill. Seed `seen` with the mount-time version so
  // a recalc that fired before this page mounted isn't replayed on first render.
  const seenRecalc = useRef(version);
  useEffect(() => {
    if (version === seenRecalc.current) return;
    seenRecalc.current = version;
    if (!project || !lastEvent || lastEvent.kind !== "recalc") return;
    const focused = typeof document !== "undefined" && document.hasFocus();
    const target = recalcTarget(hash, lastEvent.remap as Record<string, string> | undefined, focused);
    if (target && target !== hash) navigate(`/${project}/catalog/${target}`);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [version]);

  useEffect(() => {
    localStorage.setItem("catalog.sidebarCollapsed", sidebarCollapsed ? "1" : "0");
  }, [sidebarCollapsed]);

  if (!project) return null;

  return (
    <main style={{ padding: "8px 12px", flex: "1 1 auto", minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column" }}>
      <ErrorBanner errors={errors} project={project} onCleared={() => setErrors([])} />
      <div className={`layout${sidebarCollapsed ? " sidebar-collapsed" : ""}`} style={{ flex: "1 1 auto", minHeight: 0 }}>
        <aside className="panel sidebar-panel">
          <CatalogSidebar
            entries={entries}
            project={project}
            currentHash={hash}
            collapsed={sidebarCollapsed}
            onToggleCollapse={() => setSidebarCollapsed((v) => !v)}
          />
        </aside>
        <section className="panel" id="catalog-detail">
          {hash ? (
            <EntryDetailPane project={project} hash={hash} />
          ) : errorId ? (
            <ErrorDetailPane project={project} errorId={errorId} />
          ) : (
            <div className="detail">
              <h2>catalog</h2>
              <p className="meta">
                {entries.length} entries in project <code>{project}</code>. Pick one on the left.
              </p>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}
