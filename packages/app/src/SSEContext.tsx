import { createContext, useContext, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { emptyActivity, reduceActivity, type ActivityEvent, type ActivityState } from "./activity";
import { api } from "./api";
import type { SSEEvent } from "./types";

interface SSEState {
  status: "connecting" | "live" | "offline";
  version: number;
  lastEvent: SSEEvent | null;
  // The companion's blocking actions: what is running now and the last to finish. These events do not bump `version`,
  // which refetches the catalog and disk usage.
  activity: ActivityState;
}

const SSEContext = createContext<SSEState>({
  status: "connecting",
  version: 0,
  lastEvent: null,
  activity: emptyActivity,
});

export function SSEProvider({ project, children }: { project: string | null; children: React.ReactNode }) {
  const [state, setState] = useState<SSEState>({
    status: "connecting",
    version: 0,
    lastEvent: null,
    activity: emptyActivity,
  });
  const navigate = useNavigate();

  useEffect(() => {
    if (!project) {
      setState({ status: "offline", version: 0, lastEvent: null, activity: emptyActivity });
      return;
    }

    console.log(`[tallyman-sse] opening EventSource /${project}/api/sse`);
    const es = new EventSource(`/${project}/api/sse`);

    const bump = (e: MessageEvent, kind: string) => {
      const data: SSEEvent = JSON.parse(e.data);
      console.log(`[tallyman-sse] event kind=${kind}`, data);
      setState((s) => ({ ...s, status: "live", version: s.version + 1, lastEvent: { ...data, kind } }));
    };

    // An action announced while this tab was not connected (or before it loaded) is not replayed, so a page load
    // reads the companion's recent actions once to learn what is running and what finished last.
    const seedActivity = () =>
      api
        .timing(project, 600)
        .then((t) => {
          let seeded = emptyActivity;
          const byEnd = [...t.actions].sort((a, b) => (a.ended_ms ?? a.started_ms) - (b.ended_ms ?? b.started_ms));
          for (const a of byEnd) {
            seeded = reduceActivity(seeded, { kind: a.status === "running" ? "action_start" : "action_end", action: a });
          }
          // A live event that landed before this response is newer than it.
          setState((s) => (s.activity.running.length || s.activity.last ? s : { ...s, activity: seeded }));
        })
        .catch(() => {});

    const onAction = (e: MessageEvent, kind: ActivityEvent["kind"]) => {
      const data = JSON.parse(e.data);
      setState((s) => ({ ...s, activity: reduceActivity(s.activity, { kind, action: data.action }) }));
    };

    es.addEventListener("hello", (e) => {
      console.log("[tallyman-sse] hello", (e as MessageEvent).data);
      setState((s) => ({ ...s, status: "live" }));
      seedActivity();
    });
    es.addEventListener("action_start", (e) => onAction(e as MessageEvent, "action_start"));
    es.addEventListener("action_end", (e) => onAction(e as MessageEvent, "action_end"));
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
