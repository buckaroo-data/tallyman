import { describe, it, expect } from "vitest";
import { recalcTarget } from "./recalcRefresh";

// An auto-recalc (or explicit recalc) re-points an alias from an old content
// hash to a freshly recomputed one and publishes a `recalc` SSE event carrying
// the {oldHash: newHash} remap. The open entry view is addressed by content
// hash in the URL, so a view parked on a remapped head must navigate to the new
// hash to refresh — but only that view; an unrelated or older hash stays put.
describe("recalcTarget", () => {
  it("returns null when no hash is being viewed (catalog index)", () => {
    expect(recalcTarget(undefined, { old: "new" }, false)).toBeNull();
  });

  it("returns null when there is no remap", () => {
    expect(recalcTarget("abc", undefined, false)).toBeNull();
    expect(recalcTarget("abc", {}, false)).toBeNull();
  });

  it("returns null when the viewed hash was not remapped", () => {
    expect(recalcTarget("abc", { other: "new" }, false)).toBeNull();
  });

  it("returns the recomputed hash for a backgrounded view that was remapped", () => {
    expect(recalcTarget("old", { old: "new", x: "y" }, false)).toBe("new");
  });

  // Don't yank a user actively looking at the page (mirrors NewEntryPill / #27):
  // a focused view — which includes a mid-edit code editor whose unsaved text the
  // hash-keyed remount would discard — stays put even when its head was remapped.
  it("returns null when the page is focused, even if the viewed entry was remapped", () => {
    expect(recalcTarget("old", { old: "new" }, true)).toBeNull();
  });
});
