import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import "./styles.css";
import App from "./App";

// Stamp the bundle's build-time git revision and flag drift against the server.
// A bundle built from a commit the running backend doesn't have is exactly the
// #132 failure mode; surfacing it in the console turns a silent 404 into a clue.
const appRevision = __APP_GIT_REVISION__;
console.info(`tallyman app revision ${appRevision}`);
fetch("/api/version")
  .then((r) => (r.ok ? r.json() : null))
  .then((v) => {
    if (
      v?.revision &&
      appRevision !== "unknown" &&
      v.revision !== "unknown" &&
      v.revision !== appRevision
    ) {
      console.warn(
        `tallyman: SPA bundle (${appRevision}) and companion (${v.revision}) are on different ` +
          `revisions — the served dist is stale. Rebuild it (restart-tallyman rebuilds on restart) to re-sync.`,
      );
    }
    // A Buckaroo server or renderer that is not the pinned version degrades without an error (the grid just waits
    // for every summary stat before it shows rows), so say so on the page, not only in the console.
    const problems: string[] = v?.buckaroo?.problems ?? [];
    if (problems.length > 0) {
      console.error(`tallyman: Buckaroo version mismatch:\n${problems.join("\n")}`);
      const banner = document.createElement("div");
      banner.setAttribute("role", "alert");
      banner.style.cssText =
        "position:sticky;top:0;z-index:99999;padding:8px 16px;background:#b3261e;color:#fff;" +
        "font:13px/1.4 system-ui,sans-serif;white-space:pre-wrap";
      banner.textContent = `Buckaroo version mismatch\n${problems.join("\n")}`;
      document.body.prepend(banner);
    }
  })
  .catch(() => {});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
);
