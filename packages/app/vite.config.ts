import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { execSync } from "node:child_process";

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
  plugins: [react()],
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
