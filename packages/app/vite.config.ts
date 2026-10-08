import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { execSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

// Bake the git revision into the bundle so the running SPA knows which source it
// was built from and can flag drift against the companion's /api/version (the
// stale-dist failure mode behind #132). Best-effort: "unknown" if git isn't
// available at build time.
function gitRevision(): string {
  try {
    return execSync("git describe --always --dirty --abbrev=7", { encoding: "utf8" }).trim() || "unknown";
  } catch {
    return "unknown";
  }
}

// The renderer (buckaroo-js-core) version the bundle is built with. node_modules can lag package.json when
// `pnpm install` was skipped; a renderer older than the Python server doesn't advertise `?caps=stats_update`, so the
// server computes every summary stat before it sends the first row, with no error anywhere. Refuse to build against
// a mismatch, and record the version in dist/build-info.json so the companion can check the served bundle too.
function rendererVersions(): { pinned: string; installed: string } {
  const pkg = JSON.parse(readFileSync(resolve(__dirname, "package.json"), "utf8"));
  const pinned = String(pkg.dependencies["buckaroo-js-core"]).replace(/^[\^~=]+/, "");
  const installed = JSON.parse(
    readFileSync(resolve(__dirname, "node_modules/buckaroo-js-core/package.json"), "utf8"),
  ).version;
  return { pinned, installed };
}

const renderer = rendererVersions();
if (renderer.pinned !== renderer.installed) {
  throw new Error(
    `node_modules has buckaroo-js-core ${renderer.installed} but package.json pins ${renderer.pinned}. ` +
      "Run `pnpm install` in packages/app, then build again.",
  );
}

// Dev: Vite runs on :5173 and proxies API calls to the FastAPI companion.
// The companion's port is `tallyman run --port` (default 7860); when you run it
// on another port, point the proxy at it with TALLYMAN_API_PORT, e.g.
// `TALLYMAN_API_PORT=7861 pnpm dev`.
// Prod: build lands in dist/, FastAPI serves index.html as the SPA catch-all.
const apiTarget = `http://localhost:${process.env.TALLYMAN_API_PORT || "7860"}`;

export default defineConfig({
  define: {
    __APP_GIT_REVISION__: JSON.stringify(gitRevision()),
  },
  plugins: [
    react(),
    {
      name: "build-info",
      apply: "build",
      closeBundle() {
        writeFileSync(
          resolve(__dirname, "dist/build-info.json"),
          JSON.stringify({ buckaroo_js_core: renderer.installed, revision: gitRevision() }) + "\n",
        );
      },
    },
  ],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: apiTarget, changeOrigin: true },
      "/internal": { target: apiTarget, changeOrigin: true },
      "^/[^/]+/api/": { target: apiTarget, changeOrigin: true },
      "^/[^/]+/export/": { target: apiTarget, changeOrigin: true },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
