import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { BuckarooEmbed } from "../components/BuckarooEmbed";
import type { DiffData } from "../types";

export function DiffPage() {
  const { project, alias, va, vb } = useParams<{
    project: string;
    alias: string;
    va?: string;
    vb?: string;
  }>();
  const [data, setData] = useState<DiffData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!project || !alias) return;
    setLoading(true);
    setError("");

    const fetchDiff = va && vb
      ? api.diffData(project, alias, parseInt(va), parseInt(vb))
      : api.entries(project).then((d) => {
          const entry = d.entries.find((e) => e.alias === alias);
          if (!entry || !entry.version || entry.version < 2)
            throw new Error(`alias ${alias} has fewer than 2 versions`);
          return api.diffData(project, alias, entry.version - 1, entry.version);
        });

    fetchDiff
      .then((d) => { setData(d); setLoading(false); })
      .catch((e) => { setError(String(e)); setLoading(false); });
  }, [project, alias, va, vb]);

  if (!project || !alias) return null;

  if (loading) return <main><div className="meta" style={{ padding: 16 }}>computing diff…</div></main>;
  if (error)
    return (
      <main>
        <section className="panel detail" style={{ margin: "12px 16px" }}>
          <h2 style={{ margin: "0 0 4px 0" }}>diff unavailable</h2>
          <p className="meta" style={{ color: "#ffb4b4", margin: "0 0 12px 0" }}>{error}</p>
          <p className="meta" style={{ margin: 0 }}>
            This diff URL doesn&apos;t resolve (the alias, versions, or pairing may be stale).{" "}
            <Link to={`/${project}/catalog/${alias}`}>view {alias} in the catalog</Link>
            {" · "}
            <Link to={`/${project}/catalog`}>back to catalog</Link>
          </p>
        </section>
      </main>
    );
  if (!data) return null;

  const { diff, compare_session, buckaroo_ws_base } = data;
  const n = data.hashes.length;
  const wsUrl = compare_session && buckaroo_ws_base ? `${buckaroo_ws_base}/ws/${compare_session}` : null;

  return (
    <main>
      <section
        className="panel detail"
        style={{ margin: "12px 16px", display: "flex", flexDirection: "column", gap: 16 }}
      >
        <div>
          <h2 style={{ margin: "0 0 4px 0" }}>
            diff · {alias} <span className="vchip">V{data.va}</span> →{" "}
            <span className="vchip">V{data.vb}</span>
          </h2>
          <p className="meta" style={{ margin: 0 }}>
            {data.a_hash} → {data.b_hash}.{" "}
            {n > 2 && (
              <>
                Other pairs:{" "}
                {Array.from({ length: n - 1 }, (_, i) => i + 1).map((i) => (
                  <span key={i}>
                    <Link to={`/${project}/diff/${alias}/${i}/${i + 1}`}>
                      V{i}→V{i + 1}
                    </Link>{" "}
                    ·{" "}
                  </span>
                ))}
                {n > 3 && (
                  <Link to={`/${project}/diff/${alias}/1/${n}`}>V1→V{n}</Link>
                )}
              </>
            )}
          </p>
        </div>

        {/* Keyed-diff zero-match warning */}
        {diff.keyed && diff.keyed.matched === 0 && (diff.keyed.only_before > 0 || diff.keyed.only_after > 0) && (
          <div style={{
            background: "#3a2000",
            border: "1px solid #b86a00",
            borderRadius: 4,
            padding: "8px 12px",
            fontSize: 13,
            color: "#ffc266",
          }}>
            <strong>Key mismatch:</strong> the auto-detected key ({diff.keyed.keys.map(k => <code key={k} style={{ margin: "0 2px" }}>{k}</code>)}) matched 0 of {diff.keyed.only_before + diff.keyed.only_after} rows across both versions — every value changed, so this column is not a stable join key. The keyed diff table below is not meaningful. The stats and head comparison are still accurate.
          </div>
        )}

        {/* Schema summary */}
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap", fontSize: 13 }}>
          <span className="meta">
            rows: <strong>{diff.schema.row_count.before}</strong> →{" "}
            <strong>{diff.schema.row_count.after}</strong>
          </span>
          {diff.schema.added.map((c) => (
            <span key={c} style={{ color: "#6db36d" }}>+ {c}</span>
          ))}
          {diff.schema.removed.map((c) => (
            <span key={c} style={{ color: "#c97070" }}>− {c}</span>
          ))}
          {diff.schema.changed_type.map((c) => (
            <span key={c.name} className="meta">
              <code>{c.name}</code> {c.before}→{c.after}
            </span>
          ))}
          {!diff.schema.added.length && !diff.schema.removed.length && !diff.schema.changed_type.length && (
            <span className="meta">no schema changes</span>
          )}
        </div>

        {/* Buckaroo comparison OR static tables */}
        {wsUrl ? (
          <div>
            <div className="meta" style={{ marginBottom: 6 }}>
              Color:{" "}
              <span style={{ display: "inline-block", width: 10, height: 10, background: "#e8b4b8", borderRadius: 2, margin: "0 2px" }} />{" "}
              only in V{data.va} ·{" "}
              <span style={{ display: "inline-block", width: 10, height: 10, background: "#90b2b3", borderRadius: 2, margin: "0 2px" }} />{" "}
              only in V{data.vb} ·{" "}
              <span style={{ display: "inline-block", width: 10, height: 10, background: "#73ae80", borderRadius: 2, margin: "0 2px" }} />{" "}
              matched (hover for V{data.vb} value)
            </div>
            <BuckarooEmbed wsUrl={wsUrl} className="buckaroo-embed" style={{ height: "60vh", minHeight: 300 }} />
          </div>
        ) : (
          <>
            <div>
              <h3 style={{ margin: "0 0 8px 0" }}>stats per column</h3>
              <div style={{ overflow: "auto" }}>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>column</th>
                      <th>rows V{data.va}</th><th>rows V{data.vb}</th>
                      <th>nulls V{data.va}</th><th>nulls V{data.vb}</th>
                      <th>distinct V{data.va}</th><th>distinct V{data.vb}</th>
                      <th>mean V{data.va}</th><th>mean V{data.vb}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {diff.stats.map((s) => (
                      <tr key={s.name}>
                        <td><code>{s.name}</code></td>
                        <td>{s.before?.count ?? "—"}</td>
                        <td>{s.after?.count ?? "—"}</td>
                        <td>{s.before?.nulls ?? "—"}</td>
                        <td>{s.after?.nulls ?? "—"}</td>
                        <td>{s.before?.distinct ?? "—"}</td>
                        <td>{s.after?.distinct ?? "—"}</td>
                        <td>{s.before?.numeric ? s.before.numeric.mean.toFixed(3) : "—"}</td>
                        <td>{s.after?.numeric ? s.after.numeric.mean.toFixed(3) : "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {diff.keyed && (
              <div>
                <h3 style={{ margin: "0 0 6px 0" }}>
                  keyed diff · {diff.keyed.keys.join(", ")}
                </h3>
                <p className="meta" style={{ margin: "0 0 6px 0" }}>
                  {diff.keyed.matched} matched · {diff.keyed.only_before} only in V{data.va} ·{" "}
                  {diff.keyed.only_after} only in V{data.vb}
                </p>
                {/* Server-generated HTML from pandas to_html() — safe, no user-controlled markup */}
                <div
                  style={{ overflow: "auto", maxHeight: "40vh" }}
                  dangerouslySetInnerHTML={{ __html: diff.keyed.table_html }}
                />
              </div>
            )}

            <div>
              <h3 style={{ margin: "0 0 8px 0" }}>head — side by side</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, overflow: "auto" }}>
                <div>
                  <div className="meta">V{data.va} (first {diff.head.n} of {diff.head.a_total})</div>
                  <div dangerouslySetInnerHTML={{ __html: diff.head.before }} />
                </div>
                <div>
                  <div className="meta">V{data.vb} (first {diff.head.n} of {diff.head.b_total})</div>
                  <div dangerouslySetInnerHTML={{ __html: diff.head.after }} />
                </div>
              </div>
            </div>
          </>
        )}

        <details>
          <summary className="meta" style={{ cursor: "pointer", userSelect: "none" }}>
            code diff
          </summary>
          {/* Server-generated Pygments HTML — safe, no user-controlled markup */}
          <div
            style={{ overflow: "auto", marginTop: 8 }}
            dangerouslySetInnerHTML={{ __html: diff.code }}
          />
        </details>
      </section>
    </main>
  );
}
