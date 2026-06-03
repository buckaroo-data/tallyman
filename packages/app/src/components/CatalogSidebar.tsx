import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import type { Entry } from "../types";

type ViewMode = "compact" | "detailed";

function fmtRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1).replace(/\.0$/, "")}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1).replace(/\.0$/, "")}k`;
  return String(n);
}

function fmtSecs(s: number): string {
  return `${+s.toFixed(s >= 1 ? 1 : 3)}s`;
}

interface Props {
  entries: Entry[];
  project: string;
  currentHash?: string;
  collapsed: boolean;
  onToggleCollapse: () => void;
}

function EntryItem({
  e,
  project,
  currentHash,
  showPrompt,
}: {
  e: Entry;
  project: string;
  currentHash?: string;
  showPrompt: boolean;
}) {
  const isCurrent = currentHash === e.content_hash;
  const promptText = e.prompt ?? "(no prompt)";

  return (
    <li className={isCurrent ? "current" : undefined}>
      <Link
        to={`/${project}/catalog/${e.content_hash}`}
        aria-current={isCurrent ? "page" : undefined}
        title={promptText}
      >
        <span className="entry-main">
          <span className="entry-name">
            {e.alias ? (
              <>
                <span className="alias">{e.alias}</span>
                {e.version != null && <span className="vchip">V{e.version}</span>}
              </>
            ) : (
              <span className="hash">{e.content_hash}</span>
            )}
          </span>
          <span className="meta-inline" title={`${e.row_count} rows · ${e.execute_seconds}s`}>
            {fmtRows(e.row_count)} · {fmtSecs(e.execute_seconds)}
          </span>
        </span>
        {showPrompt && <span className="prompt">{promptText}</span>}
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

export function CatalogSidebar({ entries, project, currentHash, collapsed, onToggleCollapse }: Props) {
  const [view, setView] = useState<ViewMode>(
    () => (localStorage.getItem("catalog.sidebarView") === "detailed" ? "detailed" : "compact"),
  );

  useEffect(() => {
    localStorage.setItem("catalog.sidebarView", view);
  }, [view]);

  if (collapsed) {
    return (
      <div className="sidebar-rail">
        <button
          type="button"
          className="sidebar-toggle"
          title="Expand catalog list"
          aria-label="Expand catalog list"
          onClick={onToggleCollapse}
        >
          ›
        </button>
      </div>
    );
  }

  // Only the current version of each alias and unnamed scratch entries are
  // listed here. Previous revisions (forensic) live in the entry detail pane
  // — see issue #28.
  const namedCurrent = entries.filter((e) => e.alias && e.is_current);
  const scratch = entries.filter((e) => !e.alias);
  const showPrompt = view === "detailed";

  return (
    <div className="sidebar-inner">
      <div className="sidebar-toolbar">
        <div className="view-toggle" role="group" aria-label="List view">
          <button
            type="button"
            className={view === "compact" ? "active" : undefined}
            onClick={() => setView("compact")}
          >
            compact
          </button>
          <button
            type="button"
            className={view === "detailed" ? "active" : undefined}
            onClick={() => setView("detailed")}
          >
            detailed
          </button>
        </div>
        <button
          type="button"
          className="sidebar-toggle"
          title="Collapse catalog list"
          aria-label="Collapse catalog list"
          onClick={onToggleCollapse}
        >
          ‹
        </button>
      </div>

      {entries.length === 0 ? (
        <ul className="entry-list">
          <li>
            <span className="meta">
              <em>no entries yet — drive Claude with a tool call</em>
            </span>
          </li>
        </ul>
      ) : (
        <>
          {namedCurrent.length > 0 && (
            <>
              <div className="section-header">named ({namedCurrent.length})</div>
              <ul className={`entry-list ${view}`}>
                {namedCurrent.map((e) => (
                  <EntryItem
                    key={e.content_hash}
                    e={e}
                    project={project}
                    currentHash={currentHash}
                    showPrompt={showPrompt}
                  />
                ))}
              </ul>
            </>
          )}
          {scratch.length > 0 && (
            <>
              <div className="section-header">scratch ({scratch.length})</div>
              <ul className={`entry-list ${view}`}>
                {scratch.map((e) => (
                  <EntryItem
                    key={e.content_hash}
                    e={e}
                    project={project}
                    currentHash={currentHash}
                    showPrompt={showPrompt}
                  />
                ))}
              </ul>
            </>
          )}
        </>
      )}
    </div>
  );
}
