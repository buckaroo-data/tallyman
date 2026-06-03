import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { LineageDagSvg } from "../components/DagSvg";
import type { LineageLayout } from "../types";

export function LineageEntryPage() {
  const { project, hash } = useParams<{ project: string; hash: string }>();
  const [data, setData] = useState<LineageLayout | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!project || !hash) return;
    api
      .lineageLayout(project, hash)
      .then(setData)
      .catch((e) => setError(String(e)));
  }, [project, hash]);

  if (!project || !hash) return null;

  return (
    <main>
      <section className="panel detail" style={{ margin: 0 }}>
        <h2>internal lineage · {hash}</h2>
        <p className="meta">
          The expression DAG inside this entry. Each node is an operation (Read, Aggregate,
          Field, Sum, …); each edge is a dependency (consumer → producer). Root is the
          entry&apos;s output operation.{" "}
          <Link to={`/${project}/catalog/${hash}`}>back to entry</Link>
        </p>

        {error && <div className="meta" style={{ color: "#ffb4b4" }}>{error}</div>}

        {data && (
          data.lineage.nodes.length === 0 ? (
            <div className="dag-empty">no lineage data on disk.</div>
          ) : (
            <div className="dag">
              <LineageDagSvg
                nodes={data.lineage.nodes}
                edges={data.lineage.edges}
                positions={data.positions}
                width={data.width}
                height={data.height}
                root={data.lineage.root}
              />
            </div>
          )
        )}

        {data && Object.keys(data.column_trees).length > 0 && (
          <>
            <h3 style={{ marginTop: 20 }}>column lineage</h3>
            <p className="meta">
              For each output column, the operations that produced its value, all the way back
              to source reads. From xorq&apos;s <code>build_column_trees</code>.
            </p>
            {Object.entries(data.column_trees).map(([col, tree], i) => (
              <details key={col} open={i === 0}>
                <summary>
                  <code>{col}</code>
                </summary>
                <pre className="code" style={{ fontSize: 11, lineHeight: 1.4 }}>
                  {tree}
                </pre>
              </details>
            ))}
          </>
        )}
      </section>
    </main>
  );
}
