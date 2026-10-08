import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { lastLabel, nowLabel } from "../activity";
import { useSSE } from "../SSEContext";

// The companion's blocking actions in the header: what it is waiting on now (shown
// the moment the action starts, with a running clock) and the last action to finish
// with how long it took. The timing page has the whole history.
export function ActivityStrip({ project }: { project: string }) {
  const { activity } = useSSE();
  const [nowMs, setNowMs] = useState(() => Date.now());
  const busy = activity.running.length > 0;

  useEffect(() => {
    if (!busy) return;
    setNowMs(Date.now());
    const t = setInterval(() => setNowMs(Date.now()), 200);
    return () => clearInterval(t);
  }, [busy]);

  const now = nowLabel(activity, nowMs);
  const last = lastLabel(activity);
  if (!now && !last) return null;

  return (
    <Link to={`/${project}/timing`} className="activity-strip" title="Open the timing page">
      {now && <span className="pill activity-now">{now}</span>}
      {last && (
        <span className={`pill activity-last${activity.last?.status === "error" ? " failed" : ""}`}>{last}</span>
      )}
    </Link>
  );
}
