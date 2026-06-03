import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: Vite runs on :5173 and proxies API calls to the FastAPI companion.
// The companion's port is `pydata run --port` (default 7860); when you run it
// on another port, point the proxy at it with PYDATA_API_PORT, e.g.
// `PYDATA_API_PORT=7861 pnpm dev`.
// Prod: build lands in dist/, FastAPI serves index.html as the SPA catch-all.
const apiTarget = `http://localhost:${process.env.PYDATA_API_PORT || "7860"}`;

export default defineConfig({
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
