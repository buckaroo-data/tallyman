# Tallyman demo — NFL QB contracts (5 minutes)

## Setup (off camera)

1. Download the data:

   ```sh
   curl -L -o nfl_contracts.parquet https://github.com/nflverse/nflverse-data/releases/download/contracts/historical_contracts.parquet
   curl -L -o player_stats_season.parquet https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_season.parquet
   ```

2. Create the project and start tallyman:

   ```sh
   uv run --project /Users/paddy/tallyman tallyman init nfl-demo
   mv nfl_contracts.parquet player_stats_season.parquet ~/.tallyman-notebooks/projects/nfl-demo/data/
   uv run tallyman run --project nfl-demo    # companion at http://127.0.0.1:7860
   ```

3. In a fresh Claude Code session, paste this prompt unedited. Record in the same session.

   ```
   Use tallyman via the MCP server, against the nfl-demo project. Load nfl_contracts.parquet as contracts and player_stats_season.parquet as player_stats. Make a result called qb_epa: one row per QB per regular season (attempts + carries >= 200), with passing EPA, rushing EPA, and the team, year signed, and APY from the contract that's currently active for that player (assume the contract flagged is_active is the right one). Every QB should end up with exactly one contract row, even players with more than one contract on file. Add a column epa_per_apy_million = (passing EPA + rushing EPA) / APY — APY is already expressed in millions of dollars, so no further scaling is needed.
order qb_epa to player_name, season, team, epa_per_apy_million, epa, apy, then the rest

   ```

4. Format the numbers:

   ```
   Add a display style to this project: year_signed and season should render as plain integers with no thousands separator (2024, not 2,024). Any raw-dollar money column should render compactly with a magnitude suffix and a dollar sign (e.g. $34.4M) instead of the full number. Columns already expressed in millions (like APY) should get the same $…M look without re-scaling the number.
   ```

5. Before recording:
   - Reconnect the MCP server (`/mcp`).
   - Check Tua Tagovailoa's row is still wrong: Falcons, signed 2026, about $1.2M a year. If the data has drifted, pick another affected starter and change Beat 2's line to match.
   - Run `tallyman pack nfl-demo` and the Marimo export once to confirm both work.
   - Claude Code on the left, browser on the right, `qb_epa` open and sorted by `epa_per_apy_million` descending.

## Script

### Intro (0:00–0:45)

**On screen:** Claude Code and the browser side by side.

> "LLMs are great at writing code. They're okay at understanding data. But
> they fall short at the kind of literate, verifiable, and shareable data science workflows
> that have led jupyter notebooks to be the go to solution for explainable data science work for over a decade.
> With coding agent workflows — you ask a question, get an answer, and
> the moment you ask the next one, the last answer has scrolled up and its essentially forgotten without archaelogical work. 
> Chat isn't an interface built for data work nothing's named, nothing's versioned
> and most importantly it is extremely cumbersome to look at the data and code that produced it. If you can't see the data how can you trust the work?

> I built tallyman to fix that, it combines the relevant parts of
> notebooks with a system meant to be driven by agents. Tallyman
> displays notebook work as a catalog of named expression that are
> created by an agent via MCP with a UI meant to understand data.

> You talk to your agent in plain English, and instead of text table
> emitted at the speed of a 1200 baud modem, you get an interactive
> table with sorting, histograms, summary stats, and search capable of
> showing millions of rows.

> When you revise a named table, you can see the new version in the UI
> and run a visual diff against any other version to see what has
> actually changed.


> Let me show you what that looks like, on a real dataset."

### Beat 1 — two datasets, one table (0:45–1:05)

**On screen:** the `qb_epa` grid.

> "This one's about football — specifically the NFL, and specifically
> quarterbacks. If sports aren't your thing: quarterback is the most
> important, highest-paid position in the game, the player the whole
> offense runs through — which makes 'is this guy worth what he's being
> paid' a real question, not a trivia stat.
>
> To answer it, I'm joining two datasets that don't normally talk to each
> other: nflverse's play-by-play data — every pass and run, graded and
> summed up per season — and Over The Cap's contract data, what every quarterback
> has actually signed for. I asked for one table: each QB's season, and
> the deal he was playing under at the time.
>
> Two stats you'll see: EPA — Expected Points Added — is a per-play score
> for how much a play helped or hurt your team's chances of scoring, added
> up over a season. Higher is better quarterback play. APY is just
> contract-speak for 'average per year' — a $200 million, four-year deal
> has an APY of $50 million. Divide one by the other and you get a
> bang-for-buck number: points produced per million dollars paid."
> I setup tallyman to this point before the video, but it was quick.

### Beat 2 — sort it, and something's wrong (1:05–1:40)

**On screen:** sorted by `epa_per_apy_million` descending, Tua at the top.

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

> I have scripted this demo, but this type of exploration, sorting, searching is the first thing I did when playing with the data.  You always have to look at the data.

### Beat 3 — ask why, watch it get fixed (1:40–2:35)

**Type:**

```
Tua Tagovailoa's row can't be right — wrong team, and a contract signed in the future. Find out why the join picked the wrong contract for him and fix it so it always finds each player's real, current deal.  He didn't play for Atalanta until 2026, and he signed two contracts with Miami
```

**On screen:** the agent's diagnosis, the fix, and a new version of `qb_epa`.

> "I don't have to know *why* it's wrong — I just point at the row that's
> obviously broken and ask. And I can only catch it in the first place
> because I'm looking at the actual data, not a chat summary of it —
> that's the tool doing its job before I even type anything. [Read the
> agent's diagnosis aloud if it's short. Expected: the `is_active` flag
> isn't reliable, so the fix takes the highest-value contract instead.
> If it says something else, say what it says.]
>
> One instruction, and it goes and re-derives the join logic itself."

### Beat 4 — the diff proves it (2:35–3:25)

**On screen:** diff `qb_epa` v1 against v2. Tua's row changes. Scroll to show most rows don't.

> "Here's the diff I mentioned a minute ago. Tua's row now shows his real
> deal — Miami, 2024, over fifty million a year — and his bang-for-buck
> number drops from something impossible to something sane. [Name one or
> two other corrected starters if they're visible.] Everything else
> in this table — hundreds of other player-seasons — doesn't move. Not
> re-rendered, not re-guessed. The diff shows you the exact rows that
> changed and proves everything else didn't."

### Beat 5 — the loop, named (3:25–3:45)

**On screen:** the corrected `qb_epa` grid.

> "That's the loop: ask a real question in plain English, catch the answer
> that doesn't add up, ask why, and get proof — not a promise — that the
> fix was exactly as big as it needed to be and no bigger.
>
> Notice what I didn't do: I didn't ask the LLM to sort a column or scroll
> through rows — that's what the UI is for, and it's faster and more
> reliable than typing a question about it. The agent only gets pulled in
> for the part a UI can't do on its own: noticing something looks wrong
> and figuring out why."

### Close (3:45–4:50)

**On screen:** `tallyman pack nfl-demo`, then the Marimo export from the Notebook view, opening the exported file.

> "This is the core value prop of tallyman, a notebook like environment custom built for agents.  I will be making a series of videos showing 

> * tallyman features like diffs to see what changed
> * LLM driven chart authoring
> * Caching built into the core so you never have to run the same expensive computation twice
> * Dag based recomputation.
> * Computations and code backed by git
> * Export to notebooks so you can drop into your familiar jupyter ecosystem

> Checkout tallyman at https://github.com/buckaroo-data/tallyman and get in touch with any questions

**End card.**
