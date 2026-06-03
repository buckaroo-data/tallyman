import { useNavigate } from "react-router-dom";

interface CatalogNode {
  hash: string;
  row_count: number;
}

interface LineageNode {
  id: string;
  type: string;
  label: string | null;
}

interface CatalogDagProps {
  nodes: CatalogNode[];
  edges: Array<{ from: string; to: string }>;
  positions: Record<string, [number, number]>;
  width: number;
  height: number;
  annotated: Record<string, { alias: string | null; version: number | null }>;
  project: string;
  nodeWidth?: number;
}

export function CatalogDagSvg({
  nodes,
  edges,
  positions,
  width,
  height,
  annotated,
  project,
  nodeWidth = 110,
}: CatalogDagProps) {
  const navigate = useNavigate();

  return (
    <svg width={width} height={height} xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker
          id="arrow-catalog"
          viewBox="0 0 10 10"
          refX="8"
          refY="5"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--muted)" />
        </marker>
      </defs>
      {edges.map((e, i) => {
        const src = positions[e.from];
        const dst = positions[e.to];
        if (!src || !dst) return null;
        return (
          <line
            key={i}
            className="edge"
            x1={src[0] + nodeWidth}
            y1={src[1] + 20}
            x2={dst[0]}
            y2={dst[1] + 20}
            markerEnd="url(#arrow-catalog)"
          />
        );
      })}
      {nodes.map((n) => {
        const pos = positions[n.hash];
        const ann = annotated[n.hash] || {};
        if (!pos) return null;
        const label = ann.alias ? `${ann.alias} V${ann.version}` : n.hash.slice(0, 10);
        return (
          <g
            key={n.hash}
            style={{ cursor: "pointer" }}
            onClick={() => navigate(`/${project}/catalog/${n.hash}`)}
          >
            <rect
              className={`node-box${ann.alias ? " named" : ""}`}
              x={pos[0]}
              y={pos[1]}
              width={nodeWidth}
              height={40}
              rx={4}
            />
            <text className="node-label" x={pos[0] + 6} y={pos[1] + 16}>
              {label}
            </text>
            <text className="node-sublabel" x={pos[0] + 6} y={pos[1] + 32}>
              {n.row_count} rows
            </text>
          </g>
        );
      })}
    </svg>
  );
}

interface LineageDagProps {
  nodes: LineageNode[];
  edges: Array<[string, string]>;
  positions: Record<string, [number, number]>;
  width: number;
  height: number;
  root: string;
  nodeWidth?: number;
}

export function LineageDagSvg({
  nodes,
  edges,
  positions,
  width,
  height,
  root,
  nodeWidth = 130,
}: LineageDagProps) {
  return (
    <svg width={width} height={height} xmlns="http://www.w3.org/2000/svg">
      <defs>
        <marker
          id="arrow-lineage"
          viewBox="0 0 10 10"
          refX="8"
          refY="5"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--muted)" />
        </marker>
      </defs>
      {edges.map(([from, to], i) => {
        const src = positions[from];
        const dst = positions[to];
        if (!src || !dst) return null;
        return (
          <line
            key={i}
            className="edge"
            x1={src[0] + nodeWidth}
            y1={src[1] + 20}
            x2={dst[0]}
            y2={dst[1] + 20}
            markerEnd="url(#arrow-lineage)"
          />
        );
      })}
      {nodes.map((n) => {
        const pos = positions[n.id];
        if (!pos) return null;
        return (
          <g key={n.id}>
            <rect
              className={`node-box${n.id === root ? " root" : ""}`}
              x={pos[0]}
              y={pos[1]}
              width={nodeWidth}
              height={40}
              rx={4}
            />
            <text className="node-label" x={pos[0] + 6} y={pos[1] + 16}>
              {n.label || n.type}
            </text>
            <text className="node-sublabel" x={pos[0] + 6} y={pos[1] + 32}>
              {n.type}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
