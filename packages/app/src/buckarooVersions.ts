// A Buckaroo renderer that is older than the Buckaroo server does not advertise `?caps=stats_update`, so the server
// computes every summary stat before it sends the first row. Nothing errors; the grid is only slow to appear. Three
// versions have to agree to rule that out.
export type BuckarooVersions = {
  /** The buckaroo-js-core version package.json pins, baked into the bundle when it was built. */
  pinned: string;
  /** The version buckaroo-js-core reports about itself: the renderer this page is running. */
  renderer: string;
  /** The version the Buckaroo server reports on /health, or null when no server is running. */
  server: string | null;
};

/** One sentence per disagreement between the three versions; empty when they agree. */
export function buckarooVersionProblems({ pinned, renderer, server }: BuckarooVersions): string[] {
  const problems: string[] = [];
  if (renderer !== pinned) {
    problems.push(
      `This page runs buckaroo-js-core ${renderer}, but package.json pins ${pinned}. ` +
        "Run `pnpm install` in packages/app and build again.",
    );
  }
  if (server !== null && server !== renderer) {
    problems.push(
      `The Buckaroo server is ${server}, but this page runs buckaroo-js-core ${renderer}. ` +
        "They share a wire protocol and must be the same version.",
    );
  }
  return problems;
}
