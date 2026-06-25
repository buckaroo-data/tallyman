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
  })
  .catch(() => {});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
);
