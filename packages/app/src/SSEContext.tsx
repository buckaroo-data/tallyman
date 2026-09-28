import { createContext, useContext, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import type { SSEEvent } from "./types";

// One received event and its sequence number. seq counts every event this
// provider received, starting at 1, and never goes back.
export interface SSEDelivery {
  seq: number;
  event: SSEEvent;
}

interface SSEState {
  status: "connecting" | "live" | "offline";
  // The seq of the latest event. A page that refetches whatever changed keys
  // its fetch on this.
  version: number;
  // The latest events, oldest first. Several can arrive before React renders
  // once (the companion sends `recalc` then `entry_added` back to back), so a
  // consumer that acts on a kind reads them all with useSSEEvents, never only
  // the newest (#235).
  events: SSEDelivery[];
}

// More than this many events between two renders drops the oldest.
const MAX_EVENTS = 50;

const SSEContext = createContext<SSEState>({ status: "connecting", version: 0, events: [] });

export function SSEProvider({ project, children }: { project: string | null; children: React.ReactNode }) {
  const [state, setState] = useState<SSEState>({ status: "connecting", version: 0, events: [] });
  const navigate = useNavigate();

  useEffect(() => {
    if (!project) {
      // Keep version and events: seq must never go back, or useSSEEvents
      // would skip the events that reuse old numbers.
      setState((s) => ({ ...s, status: "offline" }));
      return;
    }

    console.log(`[tallyman-sse] opening EventSource /${project}/api/sse`);
    const es = new EventSource(`/${project}/api/sse`);

    const bump = (e: MessageEvent, kind: string) => {
      const data: SSEEvent = JSON.parse(e.data);
      console.log(`[tallyman-sse] event kind=${kind}`, data);
      setState((s) => {
        const seq = s.version + 1;
        const events = [...s.events, { seq, event: { ...data, kind } }].slice(-MAX_EVENTS);
        return { status: "live", version: seq, events };
      });
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

// Calls `handle` once for each event that arrives after the calling component
// mounts, in the order they arrived, including events that arrived together
// before one render. `handle` may change between renders; the latest is used.
export function useSSEEvents(handle: (event: SSEEvent) => void) {
  const { version, events } = useSSE();
  // Seeded with the mount-time version, so an event from before the mount is
  // not replayed. A ref, so StrictMode's second effect run handles nothing twice.
  const seen = useRef(version);
  const handleRef = useRef(handle);
  handleRef.current = handle;
  useEffect(() => {
    for (const { seq, event } of events) {
      if (seq <= seen.current) continue;
      seen.current = seq;
      handleRef.current(event);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [version]);
}
