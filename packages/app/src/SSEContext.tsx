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

    const es = new EventSource(`/${project}/api/sse`);

    const bump = (e: MessageEvent, kind: string) => {
      const data: SSEEvent = JSON.parse(e.data);
      setState((s) => ({ status: "live", version: s.version + 1, lastEvent: { ...data, kind } }));
    };

    es.addEventListener("hello", () =>
      setState((s) => ({ ...s, status: "live" }))
    );
    es.addEventListener("ping", () => {});
    es.addEventListener("new_entry", (e) => bump(e, "new_entry"));
    es.addEventListener("build_failed", (e) => bump(e, "build_failed"));
    es.addEventListener("notebook_changed", (e) => bump(e, "notebook_changed"));
    es.addEventListener("chart_attached", (e) => bump(e, "chart_attached"));
    es.addEventListener("post_processing_changed", (e) => bump(e, "post_processing_changed"));
    es.addEventListener("summary_stat_changed", (e) => bump(e, "summary_stat_changed"));
    es.addEventListener("project_switched", (e) => {
      const data: SSEEvent = JSON.parse(e.data);
      if (data.name) navigate(`/${data.name}/catalog`);
    });
    es.onerror = () => setState((s) => ({ ...s, status: "offline" }));

    return () => es.close();
  }, [project, navigate]);

  return <SSEContext.Provider value={state}>{children}</SSEContext.Provider>;
}

export function useSSE() {
  return useContext(SSEContext);
}
