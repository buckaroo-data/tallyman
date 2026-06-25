// Linkify a recipe's catalog-load references (#135). A revised or derived entry's
// expr.py loads its input via tracked_expr_from_alias("X") / from_catalog("X"),
// and the build records each as a parent edge (ref → the exact build-time hash).
// We split the source text on those quoted refs so the code view can render each
// as a link to the exact entry it resolved to — no codegen change, just making
// the otherwise-opaque "parking_2015" actionable and pinned to what it meant at
// build time.

export interface CodeRef {
  ref: string;
  hash: string;
  follow?: boolean;
}

export type CodeSegment = string | CodeRef;

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Split `code` into plain-text runs and the recorded parent refs that appear in
 * it (as `"ref"`). Only refs present in `parents` are linkified, so an unrelated
 * string literal is never turned into a link. Returns `[code]` unchanged when
 * there are no parent edges.
 */
export function linkifyRefs(code: string, parents: CodeRef[] | null | undefined): CodeSegment[] {
  const edges = (parents ?? []).filter((p) => p.ref);
  if (edges.length === 0) return [code];

  const byRef = new Map(edges.map((p) => [p.ref, p]));
  const re = new RegExp(`"(${edges.map((p) => escapeRegExp(p.ref)).join("|")})"`, "g");

  const out: CodeSegment[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(code)) !== null) {
    if (m.index > last) out.push(code.slice(last, m.index));
    out.push(byRef.get(m[1])!);
    last = m.index + m[0].length;
  }
  if (last < code.length) out.push(code.slice(last));
  return out;
}
