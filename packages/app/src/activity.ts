// The companion's blocking actions as the header and the timing page show them.
// The companion sends `action_start` before an action runs and `action_end` when
// it finishes (src/tallyman_core/activity.py); times are epoch milliseconds.

export interface Action {
  id: number;
  name: string;
  detail: string;
  project: string | null;
  started_ms: number;
  ended_ms: number | null;
  status: "running" | "ok" | "error";
  error: string | null;
}

export interface ActivityState {
  running: Action[];
  last: Action | null;
}

export interface ActivityEvent {
  kind: "action_start" | "action_end";
  action: Action;
}

// A Buckaroo perf span, as /api/timing returns it.
export interface Span {
  trace?: string | null;
  name: string;
  t_start_ms: number;
  t_end_ms: number;
  attrs?: Record<string, unknown> | null;
}

export interface TimingResponse {
  project: string;
  now_ms: number;
  window_s: number;
  actions: Action[];
  spans: Span[];
}

export const emptyActivity: ActivityState = { running: [], last: null };

export function reduceActivity(state: ActivityState, ev: ActivityEvent): ActivityState {
  const others = state.running.filter((a) => a.id !== ev.action.id);
  if (ev.kind === "action_start") {
    return { running: [...others, ev.action], last: state.last };
  }
  return { running: others, last: ev.action };
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const total = Math.round(ms / 1000);
  return `${Math.floor(total / 60)}m ${total % 60}s`;
}

function title(a: Action): string {
  return a.detail ? `${a.name} ${a.detail}` : a.name;
}

// What is running now: the longest-running action, with a count of the others.
export function nowLabel(state: ActivityState, nowMs: number): string | null {
  if (state.running.length === 0) return null;
  const first = [...state.running].sort((a, b) => a.started_ms - b.started_ms)[0];
  const more = state.running.length > 1 ? ` (+${state.running.length - 1} more)` : "";
  return `now: ${title(first)} · ${formatDuration(Math.max(0, nowMs - first.started_ms))}${more}`;
}

// The last action to finish, with how long it took.
export function lastLabel(state: ActivityState): string | null {
  const a = state.last;
  if (!a || a.ended_ms === null) return null;
  const failed = a.status === "error" ? " (failed)" : "";
  return `last: ${title(a)} · ${formatDuration(a.ended_ms - a.started_ms)}${failed}`;
}

// How long a `buckaroo load_expr` POST waited before Buckaroo began handling it.
// Buckaroo serves one request at a time, so a POST that arrives while another
// session's stats run is going starts late. The wait is the gap between the POST
// starting and Buckaroo's own `firstpull.load_expr` span for the same entry
// starting (both clocks are this machine's, so a small negative gap is jitter).
export function queueWaitMs(action: Action, spans: Span[]): number | null {
  if (action.name !== "buckaroo load_expr") return null;
  const match = spans.find(
    (s) =>
      s.name === "firstpull.load_expr" &&
      traceLabel(s.trace) === action.detail &&
      s.t_start_ms >= action.started_ms - 50 &&
      (action.ended_ms === null || s.t_start_ms <= action.ended_ms),
  );
  if (!match) return null;
  return Math.max(0, match.t_start_ms - action.started_ms);
}

// Buckaroo names an entry's session `entry-<project>-<content hash>`; the hash
// is what the companion's actions carry as their detail.
export function traceLabel(trace: string | null | undefined): string {
  if (!trace) return "";
  const m = /^entry-.+-([0-9a-f]{12})$/.exec(trace);
  return m ? m[1] : trace;
}

export interface TimelineRow {
  start_ms: number;
  end_ms: number | null;
}

const MIN_WIDTH_PCT = 0.5;

// Place rows on one axis as percentages. The axis runs from the first start to the
// last end; a row still running ends at `nowMs`, which extends the axis only when
// there is one.
export function layoutTimeline(rows: TimelineRow[], nowMs: number): { left: number; width: number }[] {
  if (rows.length === 0) return [];
  const start = Math.min(...rows.map((r) => r.start_ms));
  const end = Math.max(...rows.map((r) => r.end_ms ?? nowMs));
  const span = Math.max(end - start, 1);
  return rows.map((r) => ({
    left: ((r.start_ms - start) / span) * 100,
    width: Math.max((((r.end_ms ?? nowMs) - r.start_ms) / span) * 100, MIN_WIDTH_PCT),
  }));
}
