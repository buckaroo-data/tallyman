import { createContext, useContext, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  emptyActivity,
  isForProject,
  reduceActivity,
  seedActivity,
  type Action,
  type ActivityEvent,
  type ActivitySnapshot,
  type ActivityState,
} from "./activity";
import type { SSEEvent } from "./types";

interface SSEState {
  status: "connecting" | "live" | "offline";
  version: number;
  lastEvent: SSEEvent | null;
}

const SSEContext = createContext<SSEState>({
  status: "connecting",
  version: 0,
  lastEvent: null,
});

// The companion's blocking actions: what is running now and the last to finish. Their own context, because they change
// on every request while `status` and `version` do not, and only the header strip and the timing page read them. These
// events do not bump `version`, which refetches the catalog and disk usage.
const ActivityContext = createContext<ActivityState>(emptyActivity);

export function SSEProvider({ project, children }: { project: string | null; children: React.ReactNode }) {
  const [state, setState] = useState<SSEState>({
    status: "connecting",
    version: 0,
    lastEvent: null,
  });
  const [activity, setActivity] = useState<ActivityState>(emptyActivity);
  const navigate = useNavigate();

  useEffect(() => {
    // Another project starts with an empty header; the new stream's `hello` says what is running.
    setActivity(emptyActivity);
    if (!project) {
      setState({ status: "offline", version: 0, lastEvent: null });
      return;
    }

    console.log(`[tallyman-sse] opening EventSource /${project}/api/sse`);
    const es = new EventSource(`/${project}/api/sse`);

    const bump = (e: MessageEvent, kind: string) => {
      const data: SSEEvent = JSON.parse(e.data);
      console.log(`[tallyman-sse] event kind=${kind}`, data);
      setState((s) => ({ ...s, status: "live", version: s.version + 1, lastEvent: { ...data, kind } }));
    };

    // The fan-out is shared by every project the companion serves, so an action of another project is not this one's.
    const onAction = (e: MessageEvent, kind: ActivityEvent["kind"]) => {
      const { action } = JSON.parse(e.data) as { action: Action };
      if (isForProject(action, project)) setActivity((a) => reduceActivity(a, { kind, action }));
    };

    // An action announced while this tab was not connected is not replayed. The first event of every stream, the
    // first connection and each reconnect alike, carries what is running and what finished last, and replaces the
    // header's state: an `action_end` missed while disconnected would otherwise leave its action running here for good.
    es.addEventListener("hello", (e) => {
      console.log("[tallyman-sse] hello", (e as MessageEvent).data);
      setState((s) => ({ ...s, status: "live" }));
      const snapshot = (JSON.parse((e as MessageEvent).data) as { activity?: ActivitySnapshot }).activity;
      if (snapshot) setActivity(seedActivity(snapshot));
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

  return (
    <SSEContext.Provider value={state}>
      <ActivityContext.Provider value={activity}>{children}</ActivityContext.Provider>
    </SSEContext.Provider>
  );
}

export function useSSE() {
  return useContext(SSEContext);
}

export function useActivity() {
  return useContext(ActivityContext);
}
