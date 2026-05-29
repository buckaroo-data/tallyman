import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Library build: emits a single ES module + its CSS into the FastAPI
// static dir. base.html loads it with <script type="module">.
//
// Self-contained on purpose — React + buckaroo-js-core are bundled in so
// the Jinja host page doesn't need to manage them. Increases bundle size
// (~1.4MB minified) but keeps the embed drop-in.
export default defineConfig({
  plugins: [react()],
  // Library mode leaves `process.env.NODE_ENV` unreplaced so downstream
  // bundlers can choose; we are the final consumer (browser), so inline
  // it ourselves to (a) avoid a `process is not defined` ReferenceError
  // and (b) let React + buckaroo-js-core dead-code-eliminate dev paths.
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  build: {
    lib: {
      entry: resolve(__dirname, "src/buckaroo-embed.tsx"),
      formats: ["es"],
      fileName: () => "buckaroo-embed.js",
    },
    outDir: resolve(__dirname, "../../src/pydata_companion/static"),
    emptyOutDir: false,
    cssCodeSplit: false,
    minify: true,
    rollupOptions: {
      output: {
        assetFileNames: (info) =>
          info.name?.endsWith(".css")
            ? "buckaroo-embed.css"
            : "[name][extname]",
      },
    },
  },
});
