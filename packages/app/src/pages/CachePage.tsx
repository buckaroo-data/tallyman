import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { useSSE } from "../SSEContext";
import type { CacheEntry } from "../types";

export function CachePage() {
  const { project } = useParams<{ project: string }>();
  const { version } = useSSE();
  const [entries, setEntries] = useState<CacheEntry[]>([]);
  const [totalFormatted, setTotalFormatted] = useState("");
  const [deleting, setDeleting] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!project) return;
    api.resultCache(project).then((d) => {
      setEntries(d.entries);
      setTotalFormatted(d.total_formatted);
    }).catch(() => {});
  }, [project, version]);

  const handleDelete = async (hash: string) => {
    if (!project) return;
    if (!confirm(`Delete the cached snapshot for ${hash.slice(0, 12)}?\n\nThe entry, code and alias stay — it re-bakes on next view.`)) return;
    setDeleting((s) => new Set(s).add(hash));
    try {
      await api.deleteResultCache(project, hash);
      setEntries((prev) => prev.filter((e) => e.hash !== hash));
    } catch {
      alert("delete failed");
    } finally {
      setDeleting((s) => { const n = new Set(s); n.delete(hash); return n; });
    }
  };

  const cached = entries.length;

  return (
    <main style={{ overflow: "auto" }}>
      <div className="cache-pane">
        <div className="cache-summary">
          <strong>Cached result snapshots</strong>
          <span className="meta">
            {cached} cached &middot;{" "}
            <span>{totalFormatted}</span> on disk
          </span>
          <span className="meta cache-note">
            Deleting frees the snapshot only — code, build and stat cache stay; the entry re-bakes on next view.
          </span>
        </div>

        {entries.length === 0 ? (
          <div className="nb-empty">no cached result snapshots</div>
        ) : (
          <table className="data-table cache-table">
            <thead>
              <tr>
                <th className="num">Size ▼</th>
                <th className="num">Rows</th>
                <th>Created</th>
                <th>Alias</th>
                <th>Hash</th>
                <th>Prompt</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => (
                <tr key={e.hash} data-hash={e.hash}>
                  <td className="num size">{e.size_formatted}</td>
                  <td className="num">{e.row_count.toLocaleString()}</td>
                  <td className="created">{e.created}</td>
                  <td>
                    {e.alias ? (
                      <>
                        <span className="alias-name">{e.alias}</span>
                        {e.version != null && (
                          <span className="vchip">V{e.version}</span>
                        )}
                        {e.is_current && (
                          <span className="current-tag">current</span>
                        )}
                      </>
                    ) : (
                      <span className="muted">(scratch)</span>
                    )}
                  </td>
                  <td>
                    <Link className="hash" to={`/${project}/catalog/${e.hash}`}>
                      {e.hash}
                    </Link>
                  </td>
                  <td className="prompt" title={e.prompt ?? ""}>
                    {e.prompt ?? <span className="muted">—</span>}
                  </td>
                  <td className="action">
                    <button
                      className="del-btn"
                      disabled={deleting.has(e.hash)}
                      onClick={() => handleDelete(e.hash)}
                    >
                      {deleting.has(e.hash) ? "deleting…" : "delete"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </main>
  );
}
