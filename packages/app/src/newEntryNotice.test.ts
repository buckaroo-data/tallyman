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

  it("background: navigates directly so the result is on screen on return", () => {
    const r = receiveNewEntry(emptyNotice, { hash: "xyz" }, false);
    expect(r.navigateTo).toBe("xyz");
    // No pill accumulates while backgrounded — the view already moved.
    expect(r.state).toEqual(emptyNotice);
  });

  it("background: a direct navigation clears any pending foreground pill", () => {
    const pending = receiveNewEntry(emptyNotice, { hash: "a" }, true).state;
    expect(pending.count).toBe(1);
    const r = receiveNewEntry(pending, { hash: "b" }, false);
    expect(r.navigateTo).toBe("b");
    expect(r.state).toEqual(emptyNotice);
  });
});
