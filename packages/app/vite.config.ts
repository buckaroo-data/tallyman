import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: Vite runs on :5173, proxies API calls to FastAPI on :7860.
// Prod: build lands in dist/, FastAPI serves index.html as SPA catch-all.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: "http://localhost:7860", changeOrigin: true },
      "/internal": { target: "http://localhost:7860", changeOrigin: true },
      "^/[^/]+/api/": { target: "http://localhost:7860", changeOrigin: true },
      "^/[^/]+/export/": { target: "http://localhost:7860", changeOrigin: true },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
