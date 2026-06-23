// An auto-recalc (or explicit recalc) re-points an alias from an old content
// hash to a freshly recomputed one and publishes a `recalc` SSE event carrying
// the {oldHash: newHash} remap. The open entry view is addressed by content hash
// in the URL, so a view parked on a head that was just remapped must navigate to
// the recomputed hash to refresh. Only that view moves: an unrelated or older
// hash (not a remap key) stays put. The navigation glue lives in CatalogPage.tsx.

// Returns the hash the open view should navigate to, or null when nothing moves.
export function recalcTarget(
  currentHash: string | undefined,
  remap: Record<string, string> | undefined,
): string | null {
  if (!currentHash || !remap) return null;
  return remap[currentHash] ?? null;
}
