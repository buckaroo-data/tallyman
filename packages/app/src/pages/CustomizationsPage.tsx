import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { api } from "../api";
import { useSSE } from "../SSEContext";
import type { CustomizationItem, Customizations } from "../types";

const SECTIONS: { key: keyof Omit<Customizations, "project">; title: string; blurb: string }[] = [
  {
    key: "summary_stats",
    title: "Summary stats",
    blurb: "Per-column statistics computed by Buckaroo. def compute(col) -> ibis.Expr",
  },
  {
    key: "display_klasses",
    title: "Styling classes",
    blurb: "ColAnalysis subclasses that control table rendering and pinned rows.",
  },
  {
    key: "post_processings",
    title: "Post-processing",
    blurb: "Expression transformers offered in Buckaroo's post-processing dropdown. def process(expr) -> ibis.Expr | DataFrame",
  },
];

function Section({ title, blurb, items }: { title: string; blurb: string; items: CustomizationItem[] }) {
  const active = items.filter((i) => !i.disabled).length;
  return (
    <div className="cust-section">
      <div className="cust-section-head">
        <strong>{title}</strong>
        <span className="meta">
          {items.length === 0
            ? "none"
            : `${active} active${items.length - active ? ` · ${items.length - active} disabled` : ""}`}
        </span>
      </div>
      <span className="meta cache-note">{blurb}</span>

      {items.length === 0 ? (
        <div className="nb-empty">none defined</div>
      ) : (
        items.map((item) => (
          <details key={item.path} className="cust-item">
            <summary>
              <span className={`cust-name${item.disabled ? " muted" : ""}`}>{item.name}</span>
              {item.disabled && <span className="vchip">disabled</span>}
              <span className="meta cust-path" title={item.path}>
                {item.path}
              </span>
            </summary>
            <pre className="code">{item.source}</pre>
          </details>
        ))
      )}
    </div>
  );
}

export function CustomizationsPage() {
  const { project } = useParams<{ project: string }>();
  const { version } = useSSE();
  const [data, setData] = useState<Customizations | null>(null);

  useEffect(() => {
    if (!project) return;
    api.customizations(project).then(setData).catch(() => {});
  }, [project, version]);

  return (
    <main style={{ overflow: "auto" }}>
      <div className="cache-pane">
        <div className="cache-summary">
          <strong>Catalog customizations</strong>
          <span className="meta cache-note">
            Project-wide Buckaroo customizations. They apply to every entry in the catalog and are loaded
            into each session. Read-only here — edit via the MCP tools.
          </span>
        </div>

        {SECTIONS.map((s) => (
          <Section key={s.key} title={s.title} blurb={s.blurb} items={data ? data[s.key] : []} />
        ))}
      </div>
    </main>
  );
}
