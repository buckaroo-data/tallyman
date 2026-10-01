# Tallyman demo — NFL QB contracts (3-5 minute cut)

Intro states the thesis, then one real bug-catch arc pays off every promise
the intro makes: named/versioned results, a real grid you can actually
verify against, and a shareable git-backed catalog. Written for viewers who
don't follow football — EPA and APY get defined inline the moment they
first appear, not in a glossary.

---

## Pre-flight (before recording, NOT on camera)

Two nflverse-data sources, no signup:

```sh
curl -L -o nfl_contracts.parquet https://github.com/nflverse/nflverse-data/releases/download/contracts/historical_contracts.parquet
curl -L -o player_stats_season.parquet https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_season.parquet
```

Start a fresh project, put both files in its data dir, launch the stack:

```sh
uv run --project /Users/paddy/tallyman tallyman init nfl-demo
mv nfl_contracts.parquet player_stats_season.parquet ~/.tallyman-notebooks/projects/nfl-demo/data/
uv run tallyman run --project nfl-demo    # companion at http://127.0.0.1:7860
```

Claude Code left, browser right.

**Build the "before" table off camera.** This is the first prompt in a
fresh Claude Code session for this project — copy-paste as-is, no editing:

```
Use tallyman via the MCP server, against the nfl-demo project. Load nfl_contracts.parquet as contracts and player_stats_season.parquet as player_stats. Make a result called qb_epa: one row per QB per regular season (attempts + carries >= 200), with passing EPA, rushing EPA, and the team, year signed, and APY from the contract that's currently active for that player (assume the contract flagged is_active is the right one). Every QB should end up with exactly one contract row, even players with more than one contract on file. Add a column epa_per_apy_million = (passing EPA + rushing EPA) / APY — APY is already expressed in millions of dollars, so no further scaling is needed.
```

> **Say "use the MCP server" explicitly, only here.** Without it, Claude
> tends to answer a "load this parquet and compute X" request by writing
> raw pandas/pyarrow itself — technically correct, but it never touches
> the tallyman catalog, so nothing gets named or versioned. Once it's used
> the MCP server once in a session, it keeps using it for follow-up
> requests without being told again — which is why the formatting prompt
> below and Beat 3's prompt don't repeat the instruction. This also means
> **the same Claude Code session has to carry through from this pre-flight
> build into the recording** — if you start a fresh session right before
> hitting record, Beat 3's prompt will need "use the MCP server" added
> back in, or it may reach for raw Python instead of revising `qb_epa`.

Building it exactly this way — not the fixed way — is the point: it's the
same reasonable-looking assumption a first pass reaches for, not a
strawman. Do NOT pre-fix it. Confirm it built with the bug intact before you
record (see checklist below).

> **Revision history on this prompt:** an earlier version of this prompt
> ("pick the contract where `is_active` is true" / "APY in millions")
> produced two *unintended* bugs on top of the one we want: dividing by
> `apy / 1_000_000` (apy's already in millions, so this inflated every
> ratio ~1,000,000x) and filtering to `is_active == True` before joining
> instead of deduplicating (which silently drops any QB with zero
> `is_active` contracts — 249 of ~942 rows survived instead of ~690). The
> wording above fixes both while keeping the one bug we actually want:
> trusting the `is_active` flag. Verified against a throwaway experiment
> entry (`qb_bang_for_buck_experiment` in this same project) — 687 rows, no
> duplicates, Tua Tagovailoa sitting at rank #2 when sorted descending.

> **Why stage this instead of building it live:** whether a live agent
> reaches for `is_active` unprimed isn't guaranteed. Staging the "before"
> state guarantees the beat lands, the same way the taxi demo pre-builds its
> chart so it can flatten live instead of being built in frame.

**Format the numbers (off camera, separate step).** Buckaroo's default
styling shows `year_signed`/`season` with thousands-grouping ("2,024") and
has no compact format for money columns. This was solved once already in
the `nfl-salaries` project this demo is drawn from, and re-verified live
against `nfl-demo` today — three distinct buckets, not two, because money
columns in this data show up in two different units:

```
Add a display style to this project: year_signed and season should render as plain integers with no thousands separator (2024, not 2,024). Any raw-dollar money column should render compactly with a magnitude suffix and a dollar sign (e.g. $34.4M) instead of the full number. Columns already expressed in millions (like APY) should get the same $…M look without re-scaling the number.
```

> **Verified today, both the formatting and a wrong assumption from the
> original session.** `compact_number` *does* support a literal `$`
> prefix — I'd assumed it didn't (based on an unresolved buckaroo issue
> filed in that session), but the installed version supports generic
> `prefix`/`suffix` on any displayer now. Confirmed by extracting and
> running buckaroo's actual formatter code in Node against real values:
> `34400000` → `"$34.4M"`, `53.1` → `"$53.1M"`. Say "$34.4M" on camera,
> not "34.4M" — the sign really is there.
>
> The two-unit problem: `apy` is already expressed in millions (`53.1`,
> not `53,100,000`) while `value`/`guaranteed`/`inflated_value`/
> `inflated_guaranteed` are raw-dollar figures once rescaled (see below).
> `compact_number` picks its K/M/B suffix from the *raw magnitude* of the
> number, so feeding it `53.1` directly renders `$53`, no suffix — it
> needs a fixed `$…M` prefix/suffix instead, not magnitude detection.
> Working code, three buckets:
>
> ```python
> MONEY_COLS = {"value", "guaranteed", "inflated_value", "inflated_guaranteed"}
> # Already expressed in millions (e.g. 53.1, not 53100000) — compact_number
> # would auto-detect magnitude and fail to add an "M" suffix to a number
> # this small, so these get a fixed $…M prefix/suffix instead.
> MILLIONS_COLS = {"apy", "inflated_apy"}
> # Plain years, not quantities — "2,024" reads as a count, not the year 2024.
> NO_GROUPING_COLS = {"year", "season", "year_signed"}
>
>
> class MoneyMillionsStyling(DefaultMainStyling):
>     df_display_name = "main"
>
>     @classmethod
>     def style_column(kls, col, column_metadata):
>         base_config = super().style_column(col, column_metadata)
>         orig_name = column_metadata.get("orig_col_name", col)
>         if orig_name in MONEY_COLS:
>             base_config["displayer_args"] = {"displayer": "compact_number", "prefix": "$"}
>         elif orig_name in MILLIONS_COLS:
>             base_config["displayer_args"] = {
>                 "displayer": "float", "max_fraction_digits": 1, "prefix": "$", "suffix": "M",
>             }
>         elif orig_name in NO_GROUPING_COLS:
>             base_config["displayer_args"] = {"displayer": "obj"}
>         return base_config
> ```

> **What this actually changes on `qb_epa` today:** only `season` /
> `year_signed` (no-grouping) and `apy` (fixed `$…M`) — `qb_epa` never
> selects `value` or `guaranteed`, so `MONEY_COLS` has nothing to act on
> in this table regardless. `contracts` itself has already been revised
> to rescale those four fields to raw dollars (matches how `nfl-salaries`
> did it) — that part's done, it just isn't consumed by anything `qb_epa`
> shows yet. It matters for the extended-cut "best signings ever" table
> (Feature notes). That `contracts` revision also leaves `qb_epa` marked
> stale (bookkeeping only — `apy` wasn't touched by the rescale, so
> nothing about its actual values changed); rebuild it once MCP is
> reconnected so the pointer is clean before recording.
>
> **Unresolved as of this writing:** `apy`'s new styling hasn't been
> visually confirmed on screen yet. If it's not showing once you check:
> `qb_epa` may be marked stale from an unrelated `contracts` revision
> (bookkeeping only, not a real value change — safe to rebuild), the
> browser may be on the `stock_main` view instead of `main`, or the MCP
> server may need reconnecting (see the checklist item below on that).

Have the `qb_epa` grid open, sorted by `epa_per_apy_million` descending,
before you hit record.

---

## The voiceover

### Intro — the thesis (0:00–0:45)

*(on screen: can open on the Claude Code + browser split, no UI drama yet)*

> "LLMs are great at writing code. They're okay at understanding data. But
> they're crap at the kind of verifiable, shareable data science notebooks
> have done for well over a decade — you ask a question, get an answer, and
> the moment you ask the next one, the last answer's gone. Nothing's named,
> nothing's versioned, and nobody else can open what you just did and trust
> it.
>
> I built tallyman to fix that, specifically for agents. It's not a
> notebook, and it's not a chat window — it's a data catalog with an LLM
> analyst wired into it. You talk to it in plain English, and instead of
> an answer that scrolls by and disappears, you get a named table right
> there in the UI. You build the next question on top of a table you
> already have, not a fresh blob of text. Update the original table, and
> everything downstream that depends on it updates automatically. And the
> UI itself is built for looking at the actual data, not a description of
> it — every version sticks around, so you can diff any two and see
> exactly what moved. Because it's backed by git, you can hand the whole
> project to someone else and they get the same answers you got.
>
> Let me show you what that looks like, on a real dataset."

### Beat 1 — two datasets, one table (0:45–1:05)

**On screen:** the `qb_epa` grid, already built, EPA/APY columns visible.

**Voiceover:**

> "This one's about football — specifically the NFL, and specifically
> quarterbacks. If sports aren't your thing: quarterback is the most
> important, highest-paid position in the game, the player the whole
> offense runs through — which makes 'is this guy worth what he's being
> paid' a real question, not a trivia stat.
>
> To answer it, I'm joining two datasets that don't normally talk to each
> other: nflverse's play-by-play data — every pass and run, graded and
> summed up per season — and OTC's contract data, what every quarterback
> has actually signed for. I asked for one table: each QB's season, and
> the deal he was playing under at the time.
>
> Two stats you'll see: EPA — Expected Points Added — is a per-play score
> for how much a play helped or hurt your team's chances of scoring, added
> up over a season. Higher is better quarterback play. APY is just
> contract-speak for 'average per year' — a $200 million, four-year deal
> has an APY of $50 million. Divide one by the other and you get a
> bang-for-buck number: points produced per million dollars paid."

### Beat 2 — sort it, and something's wrong (1:05–1:40)

**On screen:** grid already sorted by `epa_per_apy_million` descending.
Tua Tagovailoa is at the very top — verified today, ranks #1–3 of 249
rows (his three best seasons at ratios of 72.6, 70.1, 69.5).

**Voiceover:**

> "Sort by that number — and right at the top, here's Tua Tagovailoa, a
> quarterback most people have heard of. His row says he's under contract
> with the Falcons, signed in 2026, making $1.2 million a year.
>
> [A few other names near the top are genuinely cheap backup deals — that
> part's real, not broken. Tua isn't one of those: he's a $200M-plus
> starting quarterback, not a $1.2M journeyman, which is what makes his
> row the one worth stopping on.]
>
> None of that is true. Tua plays for Miami. And a contract 'signed in
> 2026' can't explain what he was paid in 2020, 2021, 2022 — those seasons
> already happened. Something in the join picked the wrong contract."

### Beat 3 — ask why, watch it get fixed (1:40–2:35)

**Type:**

```
Tua Tagovailoa's row can't be right — wrong team, and a contract signed in the future. Find out why the join picked the wrong contract for him and fix it so it always finds each player's real, current deal.
```

**On screen:** the agent's diagnosis appears, then the fix, then `qb_epa`
gets a new version.

**Voiceover:**

> "I don't have to know *why* it's wrong — I just point at the row that's
> obviously broken and ask. And I can only catch it in the first place
> because I'm looking at the actual data, not a chat summary of it —
> that's the tool doing its job before I even type anything. [Read the
> agent's finding aloud if it's a clean one-liner — expected shape: the
> `is_active` flag it trusted isn't reliable; some players carry a stale
> or duplicate 'active' contract that isn't actually their real one. The
> fix: stop trusting that flag, just take whichever contract is worth the
> most money.]
>
> One instruction, and it goes and re-derives the join logic itself."

### Beat 4 — the diff proves it (2:35–3:25)

**On screen:** Diff `qb_epa`, v1 vs v2. Tua's row (team, year signed, APY,
the ratio) changes. Scroll — most rows are untouched.

**Voiceover:**

> "Here's the diff I mentioned a minute ago. Tua's row now shows his real
> deal — Miami, 2024, over fifty million a year — and his bang-for-buck
> number drops from something impossible to something sane. [Name 1–2 more
> real fixes visible in this diff if the grid shows them clearly — expect
> several other well-known starters to be corrected too.] Everything else
> in this table — hundreds of other player-seasons — doesn't move. Not
> re-rendered, not re-guessed. The diff shows you the exact rows that
> changed and proves everything else didn't."

### Beat 5 — the loop, named (3:25–3:45)

**On screen:** back to the corrected `qb_epa` grid.

**Voiceover:**

> "That's the loop: ask a real question in plain English, catch the answer
> that doesn't add up, ask why, and get proof — not a promise — that the
> fix was exactly as big as it needed to be and no bigger.
>
> Notice what I didn't do: I didn't ask the LLM to sort a column or scroll
> through rows — that's what the UI is for, and it's faster and more
> reliable than typing a question about it. The agent only gets pulled in
> for the part a UI can't do on its own: noticing something looks wrong
> and figuring out why."

### Close — solid foundations, not a walled garden (3:45–4:50)

*(on screen: `tallyman pack nfl-demo`; then, in the companion's Notebook
view, the export-to-Marimo action, opening the resulting file)*

> "One more thing. Tallyman's an ambitious idea, but it's not asking you
> to trust something built out of nothing. Of course it's all sitting in
> git — that part's almost not worth mentioning, it's just how you'd build
> this. What's actually useful is that you're not locked in: export any
> tallyman project straight into a Marimo notebook — a real, open-source
> Python notebook — for presenting it or picking it apart by hand in a
> tool you already know.
>
> So: everything you just watched happen is real, committed history. I
> can hand this whole project to someone else, and they don't get my chat
> log — they get the data, every version of that table, the exact query
> behind each one, the fix, the diff. They can pick up exactly where I
> left off, open it as a notebook if that's more comfortable, or ask their
> own question and branch off from here. That's tallyman."

**End card.**

---

## Prompt sheet

Pre-flight (off camera): the unprimed `qb_epa` build prompt, then the
number-formatting prompt, both above.

On camera:

```
Tua Tagovailoa's row can't be right — wrong team, and a contract signed in the future. Find out why the join picked the wrong contract for him and fix it so it always finds each player's real, current deal.
```

That's the only prompt typed on camera. Everything else is narration,
pointing, scrolling, `tallyman pack`, and the Marimo export at the close.

---

## Things to check before you hit record

- **Re-run the "before" build fresh** and confirm it still reproduces the
  bug — nflverse's contracts data is a live, continuously-updated release,
  so exact numbers (and possibly which players are affected) drift over
  time. Re-verify Tua's before/after team, year, and APY the day you
  record.
- **Confirm Tua is a clean example on the day**: wrong team, future
  year-signed, an APY far below his real one. If the live data has changed
  and his row is no longer obviously broken, pick another affected starter
  from the same before/after diff — recent runs turned up 20-30 affected
  starting QBs, including some very recognizable names — and rewrite Beat
  2's line to name them instead.
- **Watch what the agent actually says in Beat 3.** The voiceover assumes
  it identifies the `is_active` flag as unreliable and switches the
  tiebreak to highest contract value. If it explains it differently, adjust
  the voiceover to match what's really on screen.
- **Confirm the diff view renders cleanly**: changed rows should be
  visually distinguishable from unchanged ones without you having to
  explain which is which.
- **Confirm `tallyman pack` produces a real, openable bundle**, and that
  the companion's export-to-Marimo action produces a file that actually
  opens in Marimo, before claiming either on camera — run both once in
  pre-flight against this exact project.
- **Reconnect the MCP server (`/mcp` in Claude Code) right before
  recording**, especially if pre-flight involved a lot of back-and-forth
  iteration first. Hit a real, reproducible bug today: a long-running MCP
  server process started raising `ImportError: cannot import name
  'rewrite_cache_dirs' from 'tallyman_xorq.portable'` on every
  `tracked_expr_from_alias` call — a real function that exists in the
  current source and works fine invoked directly, so this looks like
  stale in-memory state in that one process, not a code bug. If a build
  or revise call fails with an unfamiliar ImportError mid-recording,
  this is the first thing to check, and it can't be fixed by restarting
  the companion server (that's a different process) — only by
  reconnecting MCP itself.
- **If Beat 2's sort position ever stops being clean** (data drift, or
  the top of the list gets noisier), the more robust fallback is: filter
  to each player's single most recent season, then flag rows where the
  contract's team doesn't match the team they actually played for that
  season (normalize mascot names to abbreviations first — "Falcons" →
  "ATL" — or every row false-positives on format alone). Verified
  against live data: 31 of 56 QBs fail that check, and Tua ranks #1,
  Kyler Murray #2 — cleaner than sort position because it isolates the
  actual inconsistency instead of relying on ratio magnitude, which
  legitimate cheap contracts also produce.
- Windows already side by side, browser already on the `qb_epa` grid so
  the first sort you show is live, not a page you're loading fresh.

---

## Feature notes (what this demo leans on)

- **The intro makes three promises the body has to keep.** Versioning
  (Beat 1/5), real-grid verifiability (Beat 2), and shareability (the
  close). Don't cut the close without also cutting the shareability line
  from the intro — an unpaid promise is worse than not making it.
- **Diff as the payoff, not a formula fix.** The story isn't "the number
  was wrong, now it's right" — it's "here's proof the fix's blast radius
  was exactly the players it should've been." Same mechanism as the taxi
  demo's hour-level diff, applied to a correctness bug instead of a
  hypothesis test.
- **A real, unstaged data-quality bug.** The `is_active` unreliability
  isn't synthetic — it's a genuine property of the OTC contracts release (a
  player's most recent restructure or extension doesn't always get the
  flag flipped promptly), which is why an unprimed first-pass join reaches
  for it and gets burned. Worth saying on camera that this wasn't planted.
- **What this cut skips.** No chart. The number-formatting step covers
  `qb_epa` fully (year columns, and `apy` as `$…M`), but its `MONEY_COLS`
  bucket (raw-dollar `compact_number`, e.g. `$34.4M`) has nothing to act
  on in this table — `qb_epa` never selects `value` or `guaranteed`, even
  though `contracts` itself already carries them rescaled to raw dollars.
  A longer cut could add: the whole-contract "best signings ever" table
  (which does expose raw contract value, finally giving `MONEY_COLS`
  something to render) and the EPA-per-dollar scatter chart — all real
  work from the session this demo is drawn from.
