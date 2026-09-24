import { createContext, useContext, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import type { SSEEvent } from "./types";

interface SSEState {
  status: "connecting" | "live" | "offline";
  version: number;
  lastEvent: SSEEvent | null;
}

const SSEContext = createContext<SSEState>({ status: "connecting", version: 0, lastEvent: null });

export function SSEProvider({ project, children }: { project: string | null; children: React.ReactNode }) {
  const [state, setState] = useState<SSEState>({ status: "connecting", version: 0, lastEvent: null });
  const navigate = useNavigate();

  useEffect(() => {
    if (!project) {
      setState({ status: "offline", version: 0, lastEvent: null });
      return;
    }

    console.log(`[tallyman-sse] opening EventSource /${project}/api/sse`);
    const es = new EventSource(`/${project}/api/sse`);

    const bump = (e: MessageEvent, kind: string) => {
      const data: SSEEvent = JSON.parse(e.data);
      console.log(`[tallyman-sse] event kind=${kind}`, data);
      setState((s) => ({ status: "live", version: s.version + 1, lastEvent: { ...data, kind } }));
    };

    es.addEventListener("hello", (e) => {
      console.log("[tallyman-sse] hello", (e as MessageEvent).data);
      setState((s) => ({ ...s, status: "live" }));
    });
    es.addEventListener("ping", () => {});
    es.addEventListener("new_entry", (e) => bump(e, "new_entry"));
    es.addEventListener("build_failed", (e) => bump(e, "build_failed"));
    es.addEventListener("notebook_changed", (e) => bump(e, "notebook_changed"));
    es.addEventListener("chart_attached", (e) => bump(e, "chart_attached"));
    es.addEventListener("post_processing_changed", (e) => bump(e, "post_processing_changed"));
    es.addEventListener("summary_stat_changed", (e) => bump(e, "summary_stat_changed"));
    // An auto-recalc / explicit recalc re-points aliases; the event carries the
    // {oldHash: newHash} remap. Bumping version refetches the catalog list (heads
    // moved); CatalogPage additionally navigates an open entry view that was
    // remapped (see recalcTarget). Without this the SPA never saw the nine other
    // kinds change but missed `recalc`, so an open view went silently stale.
    es.addEventListener("recalc", (e) => bump(e, "recalc"));
    // The companion sends each event with its kind as the event name, and an
    // EventSource drops a name nobody listens for, so every kind it publishes
    // needs a line here. tests/test_sse_listeners.py fails when one is missing
    // (#235).
    // project_reset: a reset moved heads, so the catalog, notebook and Cache
    // page refetch. unfaithful_heal: the heal recorded an error (the error
    // banner) and pinned the snapshot (the Cache page's pin reason).
    // entry_added: a promoted diff got an alias. alias_changed, alias_renamed:
    // the sidebar's names changed. display_changed: a display klass changed,
    // handled like the other klass kinds above.
    es.addEventListener("project_reset", (e) => bump(e, "project_reset"));
    es.addEventListener("unfaithful_heal", (e) => bump(e, "unfaithful_heal"));
    es.addEventListener("entry_added", (e) => bump(e, "entry_added"));
    es.addEventListener("alias_changed", (e) => bump(e, "alias_changed"));
    es.addEventListener("alias_renamed", (e) => bump(e, "alias_renamed"));
    es.addEventListener("display_changed", (e) => bump(e, "display_changed"));
    es.addEventListener("project_switched", (e) => {
      const data: SSEEvent = JSON.parse(e.data);
      console.log("[tallyman-sse] project_switched", data);
      if (data.name) navigate(`/${data.name}/catalog`);
    });
    es.onopen = () => console.log("[tallyman-sse] connection open (readyState=1)");
    es.onerror = (e) => {
      console.warn(`[tallyman-sse] error/offline (readyState=${es.readyState})`, e);
      setState((s) => ({ ...s, status: "offline" }));
    };

    return () => {
      console.log(`[tallyman-sse] closing EventSource /${project}/api/sse`);
      es.close();
    };
  }, [project, navigate]);

  return <SSEContext.Provider value={state}>{children}</SSEContext.Provider>;
}

export function useSSE() {
  return useContext(SSEContext);
}
