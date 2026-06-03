// Decides how a `new_entry` SSE event affects the UI without stealing the
// view (#27). A new entry always announces itself via a dismissable pill that
// coalesces a burst into one running count and points at the latest entry. The
// pill never navigates on its own — the user clicks it — so the view is never
// yanked out from under them regardless of focus.
//
// Earlier this branched on `document.hasFocus()` and auto-navigated when the
// tab was backgrounded. That silently moved the view in the common workflow of
// a viewer parked on a second monitor while expressions are created from
// another session, so the pill never appeared. `focused` is retained in the
// signature (the component still passes it) but no longer changes behavior.
//
// Pure and transport-independent so it can be unit-tested in isolation.

export interface NoticeState {
  /** How many new entries have landed since the pill was last cleared. */
  count: number;
  /** Hash of the most recent entry — where clicking the pill navigates. */
  hash: string | null;
  /** Alias of the most recent entry, if it has one. */
  alias: string | null;
}

export const emptyNotice: NoticeState = { count: 0, hash: null, alias: null };

export interface NewEntry {
  hash?: string;
  alias?: string | null;
}

export interface NoticeResult {
  state: NoticeState;
  /** Hash to navigate to now, or null to leave the view untouched. */
  navigateTo: string | null;
}

export function receiveNewEntry(
  state: NoticeState,
  entry: NewEntry,
  focused: boolean,
): NoticeResult {
  if (!entry.hash) return { state, navigateTo: null };

  // Always announce via the pill, coalescing onto the newest entry; never
  // auto-navigate. `focused` is intentionally unused (see file header).
  void focused;
  return {
    state: {
      count: state.count + 1,
      hash: entry.hash,
      alias: entry.alias ?? null,
    },
    navigateTo: null,
  };
}
