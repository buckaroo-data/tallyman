import { describe, it, expect } from "vitest";
import { linkifyRefs, type CodeRef, type CodeSegment } from "./codeLinks";

// #135: a derived entry's code references its parent by alias (e.g.
// from_catalog("parking_2015")). The build records ref → exact hash, so the
// code view can linkify just those refs to the exact entry, without rewriting
// the recipe. These pin the split that the rendering maps over.

const parents: CodeRef[] = [{ ref: "parking_2015", hash: "7aeab99ec387", follow: true }];

const refs = (segs: CodeSegment[]): CodeRef[] => segs.filter((s): s is CodeRef => typeof s !== "string");

describe("linkifyRefs", () => {
  it("returns the code unchanged when there are no parent edges", () => {
    expect(linkifyRefs('t = read_csv("x.csv")', [])).toEqual(['t = read_csv("x.csv")']);
    expect(linkifyRefs("code", null)).toEqual(["code"]);
  });

  it("splits the quoted ref out as a CodeRef, keeping the surrounding text", () => {
    const segs = linkifyRefs('t = from_catalog("parking_2015")\n', parents);
    expect(segs).toEqual(["t = from_catalog(", { ref: "parking_2015", hash: "7aeab99ec387", follow: true }, ")\n"]);
  });

  it("only linkifies refs that are recorded parent edges", () => {
    const segs = linkifyRefs('a = from_catalog("parking_2015"); b = read_csv("other.csv")', parents);
    expect(refs(segs)).toHaveLength(1);
    expect(refs(segs)[0].ref).toBe("parking_2015");
    // The unrelated read_csv literal stays as plain text, never a link.
    expect(segs.some((s) => typeof s === "string" && s.includes('"other.csv"'))).toBe(true);
  });

  it("linkifies every occurrence of the same ref", () => {
    const segs = linkifyRefs('from_catalog("parking_2015")\nfrom_catalog("parking_2015")', parents);
    expect(refs(segs)).toHaveLength(2);
  });

  it("escapes regex-special characters in a ref", () => {
    const odd: CodeRef[] = [{ ref: "a.b(c)+d", hash: "deadbeef00", follow: false }];
    const segs = linkifyRefs('x("a.b(c)+d")', odd);
    expect(refs(segs)).toHaveLength(1);
    expect(refs(segs)[0].hash).toBe("deadbeef00");
  });
});
