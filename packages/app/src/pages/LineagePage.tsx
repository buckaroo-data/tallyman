import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { api } from "../api";
import { CatalogDagSvg } from "../components/DagSvg";
import type { CatalogDagLayout } from "../types";

export function LineagePage() {
  const { project } = useParams<{ project: string }>();
  const [data, setData] = useState<CatalogDagLayout | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!project) return;
    api
      .catalogDagLayout(project)
      .then(setData)
      .catch((e) => setError(String(e)));
  }, [project]);

  if (!project) return null;

  return (
    <main>
      <section className="panel detail" style={{ margin: 0 }}>
        <h2>catalog lineage</h2>
        <p className="meta">
          {data ? `${data.nodes.length} entries, ${data.edges.length} cross-entry edges.` : "loading…"}
          {" "}Catalog-level dependencies emerge when an entry uses{" "}
          <code>from_catalog(alias_or_hash)</code> to read another entry&apos;s result. Click a node
          to focus that entry in the catalog.
        </p>

        {error && <div className="meta" style={{ color: "#ffb4b4" }}>{error}</div>}

        {data && (
          data.nodes.length === 0 ? (
            <div className="dag-empty">no entries in this project yet.</div>
          ) : (
            <div className="dag">
              <CatalogDagSvg
                nodes={data.nodes}
                edges={data.edges}
                positions={data.positions}
                width={data.width}
                height={data.height}
                annotated={data.annotated}
                project={project}
              />
            </div>
          )
        )}
      </section>
    </main>
  );
}
