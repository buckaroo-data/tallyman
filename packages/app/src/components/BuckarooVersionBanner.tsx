import { useEffect, useState } from "react";
import { version as rendererVersion } from "buckaroo-js-core";
import { api } from "../api";
import { buckarooVersionProblems } from "../buckarooVersions";

/** Says so when the pinned version, the renderer this page runs and the Buckaroo server are not one version. */
export function BuckarooVersionBanner() {
  const [problems, setProblems] = useState<string[]>([]);
  const [hidden, setHidden] = useState(false);

  useEffect(() => {
    api
      .version()
      .then(({ buckaroo_server }) => {
        const found = buckarooVersionProblems({
          pinned: __BUCKAROO_JS_CORE_PIN__,
          renderer: rendererVersion,
          server: buckaroo_server,
        });
        if (found.length > 0) console.error(`tallyman: Buckaroo version mismatch:\n${found.join("\n")}`);
        setProblems(found);
      })
      .catch(() => {});
  }, []);

  if (problems.length === 0 || hidden) return null;

  return (
    <div className="error-banner" role="alert">
      <button type="button" className="dismiss" aria-label="hide" title="hide for now" onClick={() => setHidden(true)}>
        ×
      </button>
      <strong>Buckaroo version mismatch:</strong>
      <ul>
        {problems.map((p) => (
          <li key={p}>{p}</li>
        ))}
      </ul>
    </div>
  );
}
