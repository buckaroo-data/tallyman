import { Link } from "react-router-dom";
import type { Entry } from "../types";

interface Props {
  entries: Entry[];
  project: string;
  currentHash?: string;
}

function EntryItem({ e, project, currentHash }: { e: Entry; project: string; currentHash?: string }) {
  const isCurrent = currentHash === e.content_hash;
  const classes = [
    e.alias ? "named" : "scratch",
    e.alias && !e.is_current ? "forensic" : "",
    isCurrent ? "current" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <li className={classes}>
      <Link
        to={`/${project}/catalog/${e.content_hash}`}
        aria-current={isCurrent ? "page" : undefined}
      >
        {e.alias ? (
          <span className="alias">
            {e.alias}
            {e.version != null && <span className="vchip">V{e.version}</span>}
            {!e.is_current && <span className="forensic-tag">forensic</span>}
          </span>
        ) : (
          <span className="hash">{e.content_hash}</span>
        )}
        <span className="prompt">{e.prompt ?? "(no prompt)"}</span>
        <span className="meta">
          {e.row_count} rows · {e.execute_seconds}s
        </span>
      </Link>
      {e.alias && e.is_current && e.version_count > 1 && (
        <Link
          className="diff-link"
          to={`/${project}/diff/${e.alias}`}
          title={`Diff V${(e.version ?? 1) - 1} → V${e.version}`}
        >
          diff
        </Link>
      )}
    </li>
  );
}

export function CatalogSidebar({ entries, project, currentHash }: Props) {
  const namedCurrent = entries.filter((e) => e.alias && e.is_current);
  const namedForensic = entries.filter((e) => e.alias && !e.is_current);
  const scratch = entries.filter((e) => !e.alias);

  if (entries.length === 0) {
    return (
      <ul className="entry-list">
        <li>
          <span className="meta">
            <em>no entries yet — drive Claude with a tool call</em>
          </span>
        </li>
      </ul>
    );
  }

  return (
    <>
      {namedCurrent.length > 0 && (
        <>
          <div className="section-header">named ({namedCurrent.length})</div>
          <ul className="entry-list">
            {namedCurrent.map((e) => (
              <EntryItem key={e.content_hash} e={e} project={project} currentHash={currentHash} />
            ))}
          </ul>
        </>
      )}
      {namedForensic.length > 0 && (
        <>
          <div className="section-header">forensic ({namedForensic.length})</div>
          <ul className="entry-list">
            {namedForensic.map((e) => (
              <EntryItem key={e.content_hash} e={e} project={project} currentHash={currentHash} />
            ))}
          </ul>
        </>
      )}
      {scratch.length > 0 && (
        <>
          <div className="section-header">scratch ({scratch.length})</div>
          <ul className="entry-list">
            {scratch.map((e) => (
              <EntryItem key={e.content_hash} e={e} project={project} currentHash={currentHash} />
            ))}
          </ul>
        </>
      )}
    </>
  );
}
