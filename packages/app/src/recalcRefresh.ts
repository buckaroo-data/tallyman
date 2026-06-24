// An auto-recalc (or explicit recalc) re-points an alias from an old content
// hash to a freshly recomputed one and publishes a `recalc` SSE event carrying
// the {oldHash: newHash} remap. The open entry view is addressed by content hash
// in the URL, so a backgrounded view parked on a head that was just remapped
// navigates to the recomputed hash to refresh. Only that view moves: an
// unrelated or older hash (not a remap key) stays put. The navigation glue lives
// in CatalogPage.tsx.
//
// A *focused* view stays put even when remapped — mirroring the NewEntryPill /
// #27 policy of never yanking an active user. This also protects an in-progress
// code edit: navigating remounts the hash-keyed detail pane and would discard
// unsaved text. The common case (revise via Claude, the browser backgrounded)
// still refreshes; only an actively-watched tab is left to refresh on its own.

// Returns the hash the open view should navigate to, or null when nothing moves.
export function recalcTarget(
  currentHash: string | undefined,
  remap: Record<string, string> | undefined,
  focused: boolean,
): string | null {
  if (focused || !currentHash || !remap) return null;
  return remap[currentHash] ?? null;
}
