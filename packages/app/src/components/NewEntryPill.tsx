import { useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useSSEEvents } from "../SSEContext";
import { emptyNotice, receiveNewEntry, type NoticeState } from "../newEntryNotice";

// A new expression announces itself with a dismissable pill instead of yanking
// the view out from under you (#27). When the page is backgrounded we navigate
// straight to it; the decision logic lives in newEntryNotice.ts.
export function NewEntryPill({ project }: { project: string }) {
  const navigate = useNavigate();
  const [notice, setNotice] = useState<NoticeState>(emptyNotice);
  // Mirror the latest notice so each event reads the one before it, even when
  // a burst of events is handled before React re-renders.
  const noticeRef = useRef<NoticeState>(emptyNotice);
  const update = (s: NoticeState) => {
    noticeRef.current = s;
    setNotice(s);
  };
  // Every event, once and in order: a `notebook_changed` sent right after a
  // `new_entry` (MCP catalog_create) must not hide it.
  useSSEEvents((event) => {
    console.log(`[tallyman-pill] kind=${event.kind}`, event);
    if (event.kind !== "new_entry") return;

    // `document.hasFocus()` is true only when this window/tab is the one the
    // user is actively looking at — the signal the issue asks for.
    const focused = typeof document !== "undefined" && document.hasFocus();
    const { state, navigateTo } = receiveNewEntry(
      noticeRef.current,
      { hash: event.hash, alias: event.alias },
      focused,
    );
    console.log(
      `[tallyman-pill] new_entry hash=${event.hash} alias=${event.alias} focused=${focused} -> navigateTo=${navigateTo} pillCount=${state.count}`,
    );
    update(state);
    if (navigateTo) navigate(`/${project}/catalog/${navigateTo}`);
  });

  if (notice.count === 0 || !notice.hash) return null;

  console.log(
    `[tallyman-pill] rendering pill count=${notice.count} hash=${notice.hash} alias=${notice.alias}`,
  );

  const label =
    notice.count === 1
      ? notice.alias
        ? `new expression: ${notice.alias}`
        : "new expression"
      : `${notice.count} new expressions`;

  return (
    <div className="new-entry-notice">
      <button
        type="button"
        className="view"
        onClick={() => {
          console.log(
            `[tallyman-pill] view clicked -> navigate /${project}/catalog/${notice.hash}`,
          );
          navigate(`/${project}/catalog/${notice.hash}`);
          update(emptyNotice);
        }}
      >
        {label} · click to view
      </button>
      <button
        type="button"
        className="dismiss"
        aria-label="dismiss"
        title="dismiss"
        onClick={() => {
          console.log("[tallyman-pill] dismiss clicked");
          update(emptyNotice);
        }}
      >
        ×
      </button>
    </div>
  );
}
