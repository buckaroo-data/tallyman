import { useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useSSE } from "../SSEContext";
import { emptyNotice, receiveNewEntry, type NoticeState } from "../newEntryNotice";

// A new expression announces itself with a dismissable pill instead of yanking
// the view out from under you (#27). When the page is backgrounded we navigate
// straight to it; the decision logic lives in newEntryNotice.ts.
export function NewEntryPill({ project }: { project: string }) {
  const { version, lastEvent } = useSSE();
  const navigate = useNavigate();
  const [notice, setNotice] = useState<NoticeState>(emptyNotice);
  // Mirror the latest notice so the version-keyed effect never reads a stale
  // closure (a burst of events can land before React re-renders).
  const noticeRef = useRef<NoticeState>(emptyNotice);
  const update = (s: NoticeState) => {
    noticeRef.current = s;
    setNotice(s);
  };
  // SSE version bumps on every event kind and StrictMode double-fires effects;
  // process each version at most once.
  const seen = useRef(0);

  useEffect(() => {
    if (version === seen.current) return;
    seen.current = version;
    console.log(
      `[pydata-pill] version=${version} kind=${lastEvent?.kind ?? "<none>"}`,
      lastEvent,
    );
    if (!lastEvent || lastEvent.kind !== "new_entry") return;

    // `document.hasFocus()` is true only when this window/tab is the one the
    // user is actively looking at — the signal the issue asks for.
    const focused = typeof document !== "undefined" && document.hasFocus();
    const { state, navigateTo } = receiveNewEntry(
      noticeRef.current,
      { hash: lastEvent.hash, alias: lastEvent.alias },
      focused,
    );
    console.log(
      `[pydata-pill] new_entry hash=${lastEvent.hash} alias=${lastEvent.alias} focused=${focused} -> navigateTo=${navigateTo} pillCount=${state.count}`,
    );
    update(state);
    if (navigateTo) navigate(`/${project}/catalog/${navigateTo}`);
    // Keyed on version so one event produces one update; current notice is
    // read from noticeRef rather than the closure.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [version]);

  if (notice.count === 0 || !notice.hash) return null;

  console.log(
    `[pydata-pill] rendering pill count=${notice.count} hash=${notice.hash} alias=${notice.alias}`,
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
            `[pydata-pill] view clicked -> navigate /${project}/catalog/${notice.hash}`,
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
          console.log("[pydata-pill] dismiss clicked");
          update(emptyNotice);
        }}
      >
        ×
      </button>
    </div>
  );
}
