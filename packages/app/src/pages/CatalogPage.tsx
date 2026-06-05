import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api";
import { useSSE } from "../SSEContext";
import { CatalogSidebar } from "../components/CatalogSidebar";
import { BuckarooEmbed } from "../components/BuckarooEmbed";
import { VegaChart } from "../components/VegaChart";
import type { Entry, AppError, EntryDetail } from "../types";

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

type DetailTab = "data" | "code";

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

  if (loading) return <div className="meta">loading…</div>;
  if (!detail) return <div className="meta">entry not found</div>;

  const { manifest, alias, version, forensic_history, prompt_history, chart_spec,
    build_artifacts, total_rows, buckaroo_session, buckaroo_ws_base, code } = detail;

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

  const wsUrl =
    buckaroo_session && buckaroo_ws_base ? `${buckaroo_ws_base}/ws/${buckaroo_session}` : null;

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
      </div>

      {/* Keep the data pane mounted (hidden when inactive) so switching to the
          code tab doesn't tear down Buckaroo's websocket session. */}
      <div className="tab-panel" role="tabpanel" hidden={tab !== "data"}>
        {wsUrl ? (
          <BuckarooEmbed wsUrl={wsUrl} className="buckaroo-embed" />
        ) : (
          <div className="meta">
            {total_rows > 0
              ? `${total_rows} rows (Buckaroo not available — run pydata with --buckaroo)`
              : "no result.parquet"}
          </div>
        )}
      </div>

      {tab === "code" && (
        <div className="tab-panel metadata-section" role="tabpanel">
          <h3>
            code{" "}
            <Link to={`/${project}/lineage/${manifest.content_hash}`} className="meta">
              view internal lineage
            </Link>
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
            <pre className="code">{code}</pre>
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
  const { version } = useSSE();
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
