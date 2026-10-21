import { describe, it, expect } from "vitest";
import {
  aliasLabels,
  emptyActivity,
  formatDuration,
  isForProject,
  lastLabel,
  layoutTimeline,
  nowLabel,
  queueWaitMs,
  reduceActivity,
  seedActivity,
  spanKey,
  traceLabel,
  type Action,
  type Span,
} from "./activity";

// The header announces a blocking action before it runs ("now: …", from an
// `action_start` event) and, once it ends, keeps its name and duration ("last: …",
// from `action_end`). The timing page lays the companion's actions and
// Buckaroo's spans on one axis so a load that queued behind another session's
// stats shows as two overlapping bars.
const action = (over: Partial<Action> = {}): Action => ({
  id: 1,
  name: "buckaroo load_expr",
  detail: "cc6487dd59df",
  project: "parking2",
  started_ms: 1_000_000,
  ended_ms: null,
  status: "running",
  error: null,
  ...over,
});

describe("reduceActivity", () => {
  it("starts empty", () => {
    expect(emptyActivity).toEqual({ running: [], last: null });
  });

  it("an action_start adds the action to running and leaves last alone", () => {
    const s = reduceActivity(emptyActivity, { kind: "action_start", action: action() });
    expect(s.running.map((a) => a.id)).toEqual([1]);
    expect(s.last).toBeNull();
  });

  it("an action_end removes it from running and makes it the last action", () => {
    const started = reduceActivity(emptyActivity, { kind: "action_start", action: action() });
    const done = action({ ended_ms: 1_003_270, status: "ok" });
    const s = reduceActivity(started, { kind: "action_end", action: done });
    expect(s.running).toEqual([]);
    expect(s.last).toEqual(done);
  });

  it("the next action starting keeps showing the previous one as last", () => {
    const first = action({ id: 1, name: "prepare entry", ended_ms: 1_000_500, status: "ok" });
    let s = reduceActivity(emptyActivity, { kind: "action_start", action: action({ id: 1, name: "prepare entry" }) });
    s = reduceActivity(s, { kind: "action_end", action: first });
    s = reduceActivity(s, { kind: "action_start", action: action({ id: 2 }) });
    expect(s.running.map((a) => a.id)).toEqual([2]);
    expect(s.last?.name).toBe("prepare entry");
  });

  it("two overlapping actions are both running, and ending one leaves the other", () => {
    let s = reduceActivity(emptyActivity, { kind: "action_start", action: action({ id: 1 }) });
    s = reduceActivity(s, { kind: "action_start", action: action({ id: 2, name: "scan staleness" }) });
    expect(s.running).toHaveLength(2);
    s = reduceActivity(s, { kind: "action_end", action: action({ id: 1, ended_ms: 1_000_100, status: "ok" }) });
    expect(s.running.map((a) => a.id)).toEqual([2]);
  });

  it("an end for an action it never saw start still becomes the last action", () => {
    const s = reduceActivity(emptyActivity, {
      kind: "action_end",
      action: action({ id: 9, ended_ms: 1_000_050, status: "ok" }),
    });
    expect(s.last?.id).toBe(9);
  });
});

describe("formatDuration", () => {
  it.each([
    [12, "12 ms"],
    [999, "999 ms"],
    [3270, "3.27 s"],
    [12_345, "12.3 s"],
    [125_000, "2m 5s"],
  ])("%d ms -> %s", (ms, text) => {
    expect(formatDuration(ms)).toBe(text);
  });

  // The unit is chosen from the rounded value, so a number never prints at the edge of the wrong unit.
  it.each([
    [999.6, "1.00 s"],
    [9996, "10.0 s"],
    [59_960, "1m 0s"],
    [119_600, "2m 0s"],
  ])("%d ms rounds up into the next unit: %s", (ms, text) => {
    expect(formatDuration(ms)).toBe(text);
  });
});

describe("labels", () => {
  it("nowLabel names the running action, its short detail and the elapsed time", () => {
    const s = reduceActivity(emptyActivity, { kind: "action_start", action: action() });
    expect(nowLabel(s, 1_003_100)).toBe("now: buckaroo load_expr cc6487dd59df · 3.10 s");
  });

  it("nowLabel is null when nothing is running", () => {
    expect(nowLabel(emptyActivity, 1_000_000)).toBeNull();
  });

  it("nowLabel shows the longest-running action and counts the others", () => {
    let s = reduceActivity(emptyActivity, { kind: "action_start", action: action({ id: 1 }) });
    s = reduceActivity(s, {
      kind: "action_start",
      action: action({ id: 2, name: "scan staleness", detail: "", started_ms: 1_000_500 }),
    });
    expect(nowLabel(s, 1_001_000)).toBe("now: buckaroo load_expr cc6487dd59df · 1.00 s (+1 more)");
  });

  it("lastLabel shows the last action's name and time, and marks a failure", () => {
    const ok = reduceActivity(emptyActivity, {
      kind: "action_end",
      action: action({ ended_ms: 1_003_270, status: "ok" }),
    });
    expect(lastLabel(ok)).toBe("last: buckaroo load_expr cc6487dd59df · 3.27 s");
    const bad = reduceActivity(emptyActivity, {
      kind: "action_end",
      action: action({ ended_ms: 1_000_400, status: "error", error: "TimeoutException" }),
    });
    expect(lastLabel(bad)).toBe("last: buckaroo load_expr cc6487dd59df · 400 ms (failed)");
  });

  it("lastLabel is null before anything has finished", () => {
    expect(lastLabel(emptyActivity)).toBeNull();
  });

  it("omits an empty detail", () => {
    const s = reduceActivity(emptyActivity, {
      kind: "action_end",
      action: action({ name: "scan staleness", detail: "", ended_ms: 1_000_200, status: "ok" }),
    });
    expect(lastLabel(s)).toBe("last: scan staleness · 200 ms");
  });
});

describe("layoutTimeline", () => {
  it("places rows as percentages of the span from the first start to the last end", () => {
    const rows = [
      { start_ms: 1000, end_ms: 2000 },
      { start_ms: 1500, end_ms: 3000 },
    ];
    const out = layoutTimeline(rows, 5000);
    expect(out[0].left).toBeCloseTo(0);
    expect(out[0].width).toBeCloseTo(50);
    expect(out[1].left).toBeCloseTo(25);
    expect(out[1].width).toBeCloseTo(75);
  });

  it("a running row (no end) extends to now", () => {
    const out = layoutTimeline([{ start_ms: 1000, end_ms: 2000 }, { start_ms: 1500, end_ms: null }], 3000);
    expect(out[1].left + out[1].width).toBeCloseTo(100);
  });

  it("gives every row a visible minimum width", () => {
    const out = layoutTimeline([{ start_ms: 0, end_ms: 10_000 }, { start_ms: 5000, end_ms: 5000 }], 10_000);
    expect(out[1].width).toBeGreaterThan(0);
  });

  it("returns an empty layout for no rows", () => {
    expect(layoutTimeline([], 1000)).toEqual([]);
  });

  it("keeps a zero-length row at the end of the burst inside the track", () => {
    const out = layoutTimeline([{ start_ms: 0, end_ms: 10_000 }, { start_ms: 10_000, end_ms: 10_000 }], 10_000);
    expect(out[1].left + out[1].width).toBeLessThanOrEqual(100);
  });
});

// A tab that reconnects learns what is running from the snapshot on the stream's first event, and takes it as the
// whole truth: an action_end it missed while disconnected must not leave a 'now:' behind.
describe("seedActivity", () => {
  it("running is the snapshot's running, and last is the newest finished action", () => {
    const newest = action({ id: 7, ended_ms: 1_000_900, status: "ok" });
    const older = action({ id: 6, ended_ms: 1_000_300, status: "ok" });
    const s = seedActivity({ running: [action({ id: 8 })], recent: [newest, older] });
    expect(s.running.map((a) => a.id)).toEqual([8]);
    expect(s.last?.id).toBe(7);
  });

  it("an empty snapshot clears a stale running action", () => {
    const stale = reduceActivity(emptyActivity, { kind: "action_start", action: action({ id: 3 }) });
    expect(stale.running).toHaveLength(1);
    expect(seedActivity({ running: [], recent: [] })).toEqual(emptyActivity);
  });

  it("lists the longest-running action first", () => {
    const s = seedActivity({
      running: [action({ id: 2, started_ms: 1_000_500 }), action({ id: 1, started_ms: 1_000_000 })],
      recent: [],
    });
    expect(s.running.map((a) => a.id)).toEqual([1, 2]);
  });
});

describe("isForProject", () => {
  it("is true for the open project's actions and for actions that name no project", () => {
    expect(isForProject(action({ project: "parking2" }), "parking2")).toBe(true);
    expect(isForProject(action({ project: null }), "parking2")).toBe(true);
  });

  it("is false for another project's action", () => {
    expect(isForProject(action({ project: "nfl" }), "parking2")).toBe(false);
  });
});

// The timing page keys a row by what the span is, not where it sorts: a long span that started early but is posted
// when it ends sorts ahead of spans already shown, and a key that moves with position restarts every row after it.
describe("spanKey", () => {
  it("does not depend on the span's position in a list", () => {
    const s = span({ name: "stats.complete", t_start_ms: 1_000_100, t_end_ms: 1_009_000 });
    expect(spanKey(s)).toBe(spanKey({ ...s }));
  });

  it("differs between spans of one session that start at different times", () => {
    expect(spanKey(span({ t_start_ms: 1 }))).not.toBe(spanKey(span({ t_start_ms: 2 })));
  });

  it("differs between spans of different sessions and names", () => {
    expect(spanKey(span({ trace: "entry-p-aaaaaaaaaaaa" }))).not.toBe(spanKey(span({ trace: "entry-p-bbbbbbbbbbbb" })));
    expect(spanKey(span({ name: "stats.complete" }))).not.toBe(spanKey(span()));
  });
});

describe("aliasLabels", () => {
  const entry = (hash: string, alias: string | null, version: number | null, version_count: number) => ({
    content_hash: hash.padEnd(64, "0"),
    alias,
    version,
    version_count,
  });

  it("names each version of an alias that has several", () => {
    const labels = aliasLabels([entry("aaaaaaaaaaaa", "cars", 1, 3), entry("bbbbbbbbbbbb", "cars", 2, 3)]);
    expect(labels["aaaaaaaaaaaa"]).toBe("cars v1");
    expect(labels["bbbbbbbbbbbb"]).toBe("cars v2");
  });

  it("leaves an alias with one version unversioned, and skips entries with no alias", () => {
    const labels = aliasLabels([entry("cccccccccccc", "trips", 1, 1), entry("dddddddddddd", null, null, 0)]);
    expect(labels).toEqual({ cccccccccccc: "trips" });
  });
});

// The timing page shows how long a `buckaroo load_expr` POST waited before
// Buckaroo's own handler began: Buckaroo serves one request at a time, so a POST
// that arrives during another session's stats run starts late. The wait is the
// gap between the POST starting (companion clock) and Buckaroo's `firstpull.load_expr`
// span for the same entry starting (Buckaroo's clock, same machine).
const span = (over: Partial<Span> = {}): Span => ({
  trace: "entry-parking2-cc6487dd59df",
  name: "firstpull.load_expr",
  t_start_ms: 1_003_230,
  t_end_ms: 1_003_270,
  ...over,
});

describe("queueWaitMs", () => {
  const post = action({ ended_ms: 1_003_270, status: "ok" });

  it("is the gap between the POST starting and Buckaroo's load_expr span starting", () => {
    expect(queueWaitMs(post, [span()])).toBe(3230);
  });

  it("is null for an action that is not a load_expr POST", () => {
    expect(queueWaitMs(action({ name: "scan staleness", ended_ms: 1_000_100, status: "ok" }), [span()])).toBeNull();
  });

  it("is null until Buckaroo's span has arrived", () => {
    expect(queueWaitMs(action(), [])).toBeNull();
  });

  it("ignores spans of other entries", () => {
    expect(queueWaitMs(post, [span({ trace: "entry-parking2-71d4f4fc7932" })])).toBeNull();
  });

  it("ignores spans of an earlier load of the same entry", () => {
    expect(queueWaitMs(post, [span({ t_start_ms: 900_000, t_end_ms: 900_040 })])).toBeNull();
  });

  it("ignores Buckaroo spans that are not the load_expr handler", () => {
    expect(queueWaitMs(post, [span({ name: "stats.complete" })])).toBeNull();
  });

  it("is zero, not negative, when the clocks disagree by a few milliseconds", () => {
    expect(queueWaitMs(post, [span({ t_start_ms: 999_990, t_end_ms: 1_000_030 })])).toBe(0);
  });
});

describe("traceLabel", () => {
  it("shows the content hash of an entry session", () => {
    expect(traceLabel("entry-parking2-cc6487dd59df")).toBe("cc6487dd59df");
  });

  it("leaves other session ids as they are", () => {
    expect(traceLabel("diff-aaaaaaaaaaaa-bbbbbbbbbbbb-cccccccccccc")).toBe("diff-aaaaaaaaaaaa-bbbbbbbbbbbb-cccccccccccc");
    expect(traceLabel(null)).toBe("");
  });
});
