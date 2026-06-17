import { useEffect, useRef } from "react";
import vegaEmbed from "vega-embed";

interface Props {
  spec: Record<string, unknown>;
  dataHash: string;
  project: string;
  className?: string;
}

export function VegaChart({ spec, dataHash, project, className = "chart-panel" }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    let cancelled = false;
    let finalize: (() => void) | undefined;

    (async () => {
      try {
        const resolvedSpec = { ...spec };
        const hasInlineGeo =
          (resolvedSpec.data as { values?: Array<{ geometry?: unknown }> } | undefined)
            ?.values?.[0]?.geometry != null;

        if (!hasInlineGeo) {
          const r = await fetch(`/${project}/api/data/${dataHash}?limit=100000`);
          if (!r.ok) throw new Error(`GET /api/data failed: ${r.status}`);
          const { data } = await r.json();
          resolvedSpec.data = { values: data };
        }

        if (resolvedSpec.width == null) resolvedSpec.width = "container";
        if (cancelled || !containerRef.current) return;
        const result = await vegaEmbed(containerRef.current, resolvedSpec as Parameters<typeof vegaEmbed>[1], {
          actions: false,
        });
        if (cancelled) { result.finalize(); return; }
        finalize = result.finalize.bind(result);
      } catch (err) {
        if (el && !cancelled) {
          el.textContent = `chart error: ${err instanceof Error ? err.message : String(err)}`;
        }
      }
    })();

    return () => {
      cancelled = true;
      finalize?.();
    };
  }, [spec, dataHash, project]);

  return <div ref={containerRef} className={className} />;
}
