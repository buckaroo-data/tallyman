import { describe, it, expect } from "vitest";
import {
  emptyActivity,
  formatDuration,
  lastLabel,
  layoutTimeline,
  nowLabel,
  reduceActivity,
  type Action,
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
});
