import { describe, it, expect } from "vitest";
import { emptyNotice, receiveNewEntry } from "./newEntryNotice";

describe("receiveNewEntry", () => {
  it("ignores events without a hash", () => {
    const r = receiveNewEntry(emptyNotice, {}, true);
    expect(r.state).toEqual(emptyNotice);
    expect(r.navigateTo).toBeNull();
  });

  it("foreground: announces via a pill instead of navigating", () => {
    const r = receiveNewEntry(emptyNotice, { hash: "abc", alias: "orders" }, true);
    expect(r.navigateTo).toBeNull();
    expect(r.state.count).toBe(1);
    expect(r.state.hash).toBe("abc");
    expect(r.state.alias).toBe("orders");
  });

  it("foreground: coalesces a burst into one pill with a running count", () => {
    let s = emptyNotice;
    s = receiveNewEntry(s, { hash: "a" }, true).state;
    s = receiveNewEntry(s, { hash: "b" }, true).state;
    const r = receiveNewEntry(s, { hash: "c", alias: "latest" }, true);
    expect(r.navigateTo).toBeNull();
    expect(r.state.count).toBe(3);
    // The pill always points at the most recent entry.
    expect(r.state.hash).toBe("c");
    expect(r.state.alias).toBe("latest");
  });

  it("background: announces via a pill and never auto-navigates", () => {
    // The viewer parked on a second monitor is permanently backgrounded; it
    // must still raise the pill rather than silently moving the view (#27
    // follow-up). The pill never navigates on its own, so the view is safe.
    const r = receiveNewEntry(emptyNotice, { hash: "xyz", alias: "orders" }, false);
    expect(r.navigateTo).toBeNull();
    expect(r.state.count).toBe(1);
    expect(r.state.hash).toBe("xyz");
    expect(r.state.alias).toBe("orders");
  });

  it("background: coalesces onto a pending pill instead of clearing it", () => {
    const pending = receiveNewEntry(emptyNotice, { hash: "a" }, true).state;
    expect(pending.count).toBe(1);
    const r = receiveNewEntry(pending, { hash: "b" }, false);
    expect(r.navigateTo).toBeNull();
    expect(r.state.count).toBe(2);
    expect(r.state.hash).toBe("b");
  });
});
