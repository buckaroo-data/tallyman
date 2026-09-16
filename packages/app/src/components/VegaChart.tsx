import { useEffect, useRef } from "react";
import vegaEmbed from "vega-embed";

// Matches vega-util's LoggerInterface shape (not imported: vega-util is only a
// transitive dependency, pulled in by vega/vega-lite, not one we depend on
// directly). Vega's dataflow catches errors thrown deep inside transform
// evaluation (e.g. a `calculate` expression that chokes on a null field) and
// routes them through this logger instead of rejecting the embed() promise —
// including ones raised well after the initial render, from a later
// signal-driven re-render (a hover, say). Without this hook those errors only
// ever reach console.error, and the chart is left blank with no visible sign
// anything went wrong.
interface LoggerInterface {
  level(l?: number): number | LoggerInterface;
  error(...args: unknown[]): LoggerInterface;
  warn(...args: unknown[]): LoggerInterface;
  info(...args: unknown[]): LoggerInterface;
  debug(...args: unknown[]): LoggerInterface;
}

interface Props {
  spec: Record<string, unknown>;
  dataHash: string;
  project: string;
  className?: string;
}

// Some render failures never throw at all — e.g. a `calculate` transform
// expression that silently produces NaN for a null field poisons the scale
// without Vega ever calling its error logger (see makeChartLogger above,
// which can't catch this). The only reliable signal is the pixels
// themselves: sample the canvas after render and treat "every sampled pixel
// is identical" as a failure when we know there was data to plot. SVG-backed
// charts (no <canvas>) skip this check — there's no equivalent visual proxy
// worth the DOM-walk cost.
function canvasLooksBlank(container: HTMLElement): boolean {
  const canvas = container.querySelector("canvas");
  if (!canvas) return false;
  const ctx = canvas.getContext("2d");
  if (!ctx || canvas.width === 0 || canvas.height === 0) return false;
  const { data } = ctx.getImageData(0, 0, canvas.width, canvas.height);
  const r0 = data[0], g0 = data[1], b0 = data[2];
  for (let i = 4; i < data.length; i += 4) {
    if (data[i] !== r0 || data[i + 1] !== g0 || data[i + 2] !== b0) return false;
  }
  return true;
}

function makeChartLogger(onError: (message: string) => void): LoggerInterface {
  let level = 1;
  const chartLogger = {
    level(l?: number) {
      if (l === undefined) return level;
      level = l;
      return chartLogger;
    },
    error(...args: unknown[]) {
      console.error(...args);
      onError(args.map((a) => (a instanceof Error ? a.message : String(a))).join(" "));
      return chartLogger;
    },
    warn(...args: unknown[]) {
      console.warn(...args);
      return chartLogger;
    },
    info(...args: unknown[]) {
      console.info(...args);
      return chartLogger;
    },
    debug(...args: unknown[]) {
      console.debug(...args);
      return chartLogger;
    },
  };
  return chartLogger as unknown as LoggerInterface;
}

export function VegaChart({ spec, dataHash, project, className = "chart-panel" }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    el.className = className;
    let cancelled = false;
    let finalize: (() => void) | undefined;

    const showError = (message: string) => {
      if (!el || cancelled) return;
      finalize?.();
      finalize = undefined;
      el.textContent = "";
      el.className = "chart-error";
      el.textContent = `chart error: ${message}`;
    };

    const reportError = (message: string) => {
      fetch(`/${project}/api/chart_error`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ hash: dataHash, message }),
      }).catch(() => {});
    };

    const fail = (message: string) => {
      showError(message);
      reportError(message);
    };

    (async () => {
      try {
        const resolvedSpec = { ...spec };
        const hasInlineGeo =
          (resolvedSpec.data as { values?: Array<{ geometry?: unknown }> } | undefined)
            ?.values?.[0]?.geometry != null;

        let rowCount = (resolvedSpec.data as { values?: unknown[] } | undefined)?.values?.length ?? 0;
        if (!hasInlineGeo) {
          const r = await fetch(`/${project}/api/data/${dataHash}?limit=100000`);
          if (!r.ok) throw new Error(`GET /api/data failed: ${r.status}`);
          const { data } = await r.json();
          resolvedSpec.data = { values: data };
          rowCount = data.length;
        }

        if (resolvedSpec.width == null) resolvedSpec.width = "container";
        if (cancelled || !containerRef.current) return;
        const result = await vegaEmbed(containerRef.current, resolvedSpec as Parameters<typeof vegaEmbed>[1], {
          actions: false,
        });
        if (cancelled) { result.finalize(); return; }
        finalize = result.finalize.bind(result);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any -- structural
        // interop with vega-util's LoggerInterface, which we don't import (see above).
        result.view.logger(makeChartLogger(fail) as any);

        // Let the canvas paint (Vega's canvas renderer draws on the next
        // frame, not synchronously within runAsync) before sampling it.
        await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
        if (cancelled || !containerRef.current) return;
        if (rowCount > 0 && canvasLooksBlank(containerRef.current)) {
          fail(
            "chart rendered but nothing is visible on the canvas, despite " +
              `${rowCount} row(s) of data — likely a transform or scale ` +
              "producing NaN (e.g. a `calculate` expression fed a null field) " +
              "rather than a thrown error.",
          );
        }
      } catch (err) {
        if (!cancelled) fail(err instanceof Error ? err.message : String(err));
      }
    })();

    return () => {
      cancelled = true;
      finalize?.();
    };
  }, [spec, dataHash, project, className]);

  return <div ref={containerRef} className={className} />;
}
