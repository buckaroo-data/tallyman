// Decides how a `new_entry` SSE event affects the UI without stealing the
// view (#27). The split is whether the user is currently looking at this page:
//
//   - Foreground (page focused): never navigate. Accumulate a single pill that
//     coalesces a burst into one running count and points at the latest entry.
//   - Background (another window/tab focused): navigate straight to the entry,
//     so it's already on screen when the user comes back.
//
// Pure and transport-independent so it can be unit-tested in isolation; the
// component layer supplies `focused` (document.hasFocus()) and performs the
// navigation.

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

  if (!focused) {
    // Backgrounded: move the view directly and drop any pending pill — the
    // user isn't watching, so there's nothing to protect.
    return { state: emptyNotice, navigateTo: entry.hash };
  }

  // Foreground: announce via the pill, coalescing onto the newest entry.
  return {
    state: {
      count: state.count + 1,
      hash: entry.hash,
      alias: entry.alias ?? null,
    },
    navigateTo: null,
  };
}
