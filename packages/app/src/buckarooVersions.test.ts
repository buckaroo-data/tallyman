import { describe, it, expect } from "vitest";
import { buckarooVersionProblems } from "./buckarooVersions";

const agreeing = { pinned: "0.15.10", renderer: "0.15.10", server: "0.15.10" };

describe("buckarooVersionProblems", () => {
  it("has no problems when the pin, the renderer and the server agree", () => {
    expect(buckarooVersionProblems(agreeing)).toEqual([]);
  });

  it("has no problems when no Buckaroo server is running", () => {
    expect(buckarooVersionProblems({ ...agreeing, server: null })).toEqual([]);
  });

  it("flags a renderer that is not the pinned version", () => {
    // The server matches the renderer, so the stale renderer is the only problem.
    const problems = buckarooVersionProblems({ pinned: "0.15.10", renderer: "0.15.6", server: "0.15.6" });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain("0.15.6");
    expect(problems[0]).toContain("0.15.10");
  });

  it("flags a server that is not the renderer's version", () => {
    const problems = buckarooVersionProblems({ ...agreeing, server: "0.15.8" });
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain("0.15.8");
    expect(problems[0]).toContain("0.15.10");
  });

  it("flags both when a stale renderer sits next to the pinned server", () => {
    const problems = buckarooVersionProblems({ ...agreeing, renderer: "0.15.6" });
    expect(problems).toHaveLength(2);
  });
});
