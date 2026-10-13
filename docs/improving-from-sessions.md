# Improving tallyman from sessions and evals

Tallyman is used through an LLM: Claude Code drives the MCP server, and the
person watches the catalog page and the Buckaroo grid. Most of what we learn
about tallyman's gaps comes from watching a model use it, so we have three ways
of turning that into changes. This doc is for a person or an agent picking up
that work.

1. **Session review.** Someone runs a real analysis through tallyman in a fresh
   Claude Code session. Afterwards a second session reads that transcript, finds
   where the model worked around tallyman or was misled by it, and turns what it
   finds into PRs.
2. **The error corpus and hints.** Every error tallyman has recorded on this
   machine is run through the hint code, to see which hints fire, which fire
   wrongly, and which recurring errors have none.
3. **The eval suite.** Recorded sessions become scripted multi-step scenarios.
   Their MCP calls are replayed with resets, grid opens, cache wipes and
   restarts in between, and an oracle checks the whole project after every
   step.

An older fourth piece, the prompt packs, lives in its own repo and is described
[at the end](#prompt-packs).

## Terms

Catalog terms (entry, alias, snapshot, worthy and cheap entries) follow
[architecture.md](architecture.md#terms). The terms below are specific to this
doc.

- **Driver session:** a Claude Code session that uses tallyman to do an
  analysis, the way a user would. It is the thing being studied.
- **Review session:** a later Claude Code session asked to read a driver
  session's transcript and say what tallyman should change.
- **Transcript:** the JSONL file Claude Code writes for each session, one JSON
  object per line.
- **Workaround call:** a tool call in a driver session that did tallyman's job
  outside tallyman, such as pandas over the raw parquet, `find /` to locate the
  project directory, or a raw websocket to Buckaroo.
- **Hint:** text tallyman appends to a build error when the error matches a
  known mistake, telling the model how to fix it.
- **Error corpus:** every tallyman error message recorded on this machine, from
  the per-project logs and from transcripts.
- **Scenario:** in the eval suite, a Python function that replays one recorded
  session's MCP calls, plus what the user did around them.
- **Oracle:** the checks the eval suite runs after every scenario step.
- **Finding:** one thing the oracle saw, at severity `error` (it breaks a rule
  in [system-contract.md](system-contract.md)) or `warn`.

## Where the evidence is

### Claude Code transcripts

Each session is `~/.claude/projects/<dir>/<session-id>.jsonl`, where `<dir>` is
the session's working directory with every character other than a letter or
digit replaced by `-`. Subagent transcripts are in
`<session-id>/subagents/agent-*.jsonl`, and large tool outputs in
`<session-id>/tool-results/`.

| Directory | What is in it |
|---|---|
| `-Users-paddy-code-tallyman-nfl-demo` | NFL demo driver sessions (`63721a56`, `0fcac6bb`, `b9d2057f`, `4a93fb4e`) and the review sessions that read them (`9fd208f4`, `c3f6d702`, `493d7512`) |
| `-Users-paddy-tallyman` | older driver sessions that eval scenarios were built from (`f3a97dc8`, `6180b849`, `96f1817c`) |
| `-Users-paddy-code-tallyman2` | development sessions, including the one that designed the eval suite (`28819403`) and the one that first ran it (`321e09c2`) |

In a transcript, each line has `"type": "user"` or `"type": "assistant"`. A tool
call is a `tool_use` block in an assistant message's `content`, and its result
is a `tool_result` block in the following user message, matched by
`tool_use_id`. Tallyman's tools are named `mcp__tallyman__<tool>`.

Tallyman reports a failure inside the reply body as `{"error": ...}`, and the
tool result's `is_error` flag stays false. To find tallyman errors in a
transcript, parse the body. This script counts tool calls by name and prints
each tallyman error:

```python
import collections, json, sys

calls, names = collections.Counter(), {}
for line in open(sys.argv[1]):
    content = (json.loads(line).get("message") or {}).get("content")
    if not isinstance(content, list):
        continue
    for b in content:
        if b.get("type") == "tool_use":
            names[b["id"]] = b["name"]
            calls[b["name"]] += 1
        elif b.get("type") == "tool_result" and names.get(b["tool_use_id"], "").startswith("mcp__tallyman__"):
            body = b["content"] if isinstance(b["content"], str) else "".join(c.get("text", "") for c in b["content"])
            try:
                reply = json.loads(body)
            except ValueError:
                continue
            if isinstance(reply, dict) and reply.get("error"):
                print("error", names[b["tool_use_id"]], str(reply["error"]).replace("\n", " ")[:120])
for name, n in calls.most_common():
    print(f"{n:4} {name}")
```

Saved as `tally.py` and run with `uv run --no-project python tally.py
<transcript>` on session `4a93fb4e`, it reports 48 `Bash` calls against 15
tallyman calls, and the one `SanityCheckPlan` build error. Both were starting
points for that session's review.

### Tallyman's per-project logs

These live in `$TALLYMAN_HOME/projects/<project>/artifacts/` (the home defaults
to `~/.tallyman-notebooks`). They are outside the catalog git repo, so a reset
leaves them alone.

- `errors.jsonl`: one record per failure, with the `message`, the recipe
  `code`, the `prompt`, the `tool`, the entry `hash` when there is one, and the
  full `traceback`. The companion serves it at `/{project}/api/errors`.
- `events.jsonl`: `build_ok`, `build_error`, `after_import_error`, `alias_set`
  and Buckaroo grid loads with timings. Each MCP process stamps its own
  `session` id, so several Claude sessions writing to one project can be told
  apart. The companion serves it at `/{project}/api/log`.
- `telemetry.jsonl`: timing spans for each Buckaroo grid load.

These logs hold errors the model never saw. In `4a93fb4e`, `errors.jsonl` held
six `unfaithful_heal` records, one for each version of `qb_epa`. (An unfaithful
heal is a snapshot rebuilt after eviction whose content digest differs from the
one recorded at build time.) The model saw only the first, guessed at its cause,
and guessed wrong.

Pytest and scratch runs write these files too, under the system temp
directory. Leave those out of a corpus: in the first corpus, 24 of the 135
distinct errors were test fixture strings.

## 1. Session review

### Running a driver session

- Use a fresh Claude Code session in a directory set up for the demo, and a new
  tallyman project for each run. For the NFL demo that directory is
  `~/code/tallyman_nfl_demo`, whose `.mcp.json` attaches tallyman.
- Tell the model not to use memory or earlier sessions about the data. A model
  that already knows tallyman's API from memory won't hit the first-attempt
  errors, and those are what we are looking for.
- Prompt the way a user would, with the outcome rather than the
  implementation. Don't coach the model when it goes wrong.
- **Check which tallyman the MCP server runs.** `.mcp.json` starts it with
  `uv run --directory <checkout> tallyman mcp`, and that checkout is what the
  session tests. Both NFL reviews found the driver on an old checkout:
  `~/code/tallyman_nfl_demo/.mcp.json` points at `/Users/paddy/tallyman`, which
  on 2026-09-29 was at `22f6a3e`, 136 commits behind `origin/main`. Part of each
  review therefore described tools that had already been replaced. The model
  ran `find /` and `cp` to get files into `data/` for `catalog_load_parquet`,
  while `main` already had `catalog_import_source`, which takes any path. In
  `b9d2057f` the companion also ran from a different checkout than the MCP
  server. Before a run, point `.mcp.json` at the checkout you mean to test and
  record its SHA with the run. Claude Code owns the MCP process, so the change
  takes effect only in a new session.

### Asking for a review

These prompts worked:

- "look at session 4a93fb4e-279f-4dcc-970d-7f77bdfa3019. Where can we help the
  LLM use tallyman better?"
- "review session b9d2057f-562e-4eca-9948-20c6c3d03866. why is it using bash and
  pandas so much. what tallyman tools are missing."

A useful review does the following, in order.

1. **Count tool calls by kind** (tallyman, Bash, other), and group the
   non-tallyman calls by what the model was trying to do. Workaround calls are
   the main signal. In `b9d2057f` (42 Bash calls, 10 tallyman):

   | Bash calls | What the model was doing | Why tallyman didn't cover it |
   |---|---|---|
   | 25 | exploring and checking data in pandas | no tool returned rows to the model |
   | 4 | `find /`, `ls` and `cp` to get files into the project | the load tool only took a path under `data/` |
   | 3 | reading the entry directory, `curl` against guessed routes | nothing returned a built result's rows |

   In `4a93fb4e` (48 Bash, 15 tallyman), about 13 calls grepped Buckaroo's
   minified `widget.js` to learn which display formatters exist, and about 7
   chased an error the model found only by tailing `errors.jsonl`.

2. **Compare the logs with what the model saw.** Read the project's
   `errors.jsonl` and `events.jsonl` for the session's time range. Errors the
   model never saw, and its guesses about them, are findings.

3. **Separate tooling gaps from model judgment.** In `4a93fb4e` the model
   noticed that a player had been joined to the wrong team's contract, mentioned
   it in the fourth paragraph of its summary, and kept the join because the
   prompt had asked for it. No tool change fixes that.

4. **Check every proposal against current `origin/main`**, since the driver may
   have run old code. In `c3f6d702` one proposal (let the load tool take any
   path) was dropped because `catalog_import_source` already did it, and another
   (a stale demo doc) was withdrawn after a second look.

5. **Rank the proposals by how many calls each would have saved**, and stop
   there. Paddy picks which ones become PRs.

### What a finding turns into

Pick the lightest change that would have removed the problem.

| What the model did | Change | Example |
|---|---|---|
| did tallyman's job in pandas or Bash because no tool could | a new MCP tool | `catalog_query` and `catalog_peek` return rows without saving an entry (#287) |
| never saw an error tallyman recorded | put it in the tool reply | every reply carries `new_errors`, the errors recorded since the last reply (#288) |
| reverse-engineered something on a common path | a short docstring addition | the formatter table in `catalog_add_display_klass` (#289) |
| hit a rare error whose fix is known | a hint on that error | a window keyed on `.contains()` fails DataFusion's plan check, and the hint says to mutate the key into a column first (#289) |
| wrote code that runs but is wrong in a way a check can see | a build-time lint | `_nondeterminism_warnings` in `build.py` (#93) |
| hit a tallyman bug | an issue, then a test-first fix | #261 |
| hit an xorq, ibis or DataFusion bug | a tallyman-side workaround or hint | #265; xorq is frozen, so nothing is filed upstream |
| made a judgment error | nothing in tallyman | the wrong-contract join above |

A tool's docstring is in the model's context whenever the model uses that
tool, so each added line costs context even when it doesn't apply. Paddy's rule (2026-09-28): rare cases go in hints, and
docstring additions stay short and cover common paths. #289 lists existing
`catalog_run` docstring gotchas that could move into hints for this reason.

The model should learn about the data through tallyman's tools, in a way the
person can see, rather than by reading raw files with pandas. A finding of the
form "the model used pandas" points to a missing tool, not to a prompt telling
the model to stop.

### Turning findings into PRs

- One PR per finding group, each in its own worktree off `origin/main`. A review
  session can hand each PR to a background agent: `c3f6d702` ran #287, #288 and
  #289 in parallel that way.
- Tests first. The failing tests go in one commit, CI goes red on exactly those
  tests, and then the fix commit turns it green. The PR body reports both runs.
- Reproduce on `main` before writing a hint or a fix. #289 confirmed that the
  window error still fires and narrowed which keys trigger it: `.contains()`
  and `.re_search()` fail, while arithmetic, casts and other string methods
  build.
- Examples in docstrings, hints and tests use neutral column names (`year`,
  `revenue`, `name`, `a`, `k`), not the demo data's (`season`, `payroll`,
  `team`). Paddy asked for this on #289. Quoting the transcript's own code in a
  PR's Problem section is fine.
- Review the PRs afterwards (`493d7512` ran `/code-review 287 288 289`), and
  check each review claim by running code. That review's worry about the cost
  of reading `errors.jsonl` was dropped once the first read measured 0.3 ms,
  and its worry about the window hint's regex was confirmed by building the
  failing plan by hand.

## 2. The error corpus and hints

### Where hints live

`_ibis_import_hint(exc_msg, code)` in `src/tallyman_xorq/build.py`. Every build
failure site calls it and appends the result to the `BuildError` message as
`\n\nHint: ...`. Because the hint is part of the message, it reaches the MCP
reply, `errors.jsonl` and the companion's error banner. Some branches read the
recipe code as well as the message: a bare `import ibis` is found in the code.

PR #289 adds a table of `(regex, hint)` pairs. As of 2026-09-29 the decision is
that this table lives in `tallyman_xorq` and `_ibis_import_hint` consults it,
instead of the MCP layer adding a separate `hint` field. That way
`catalog_query`, the companion's builds and recalc failures get hints too. New
hints go in that table.

### Checking hints against the corpus

Session `493d7512` did this on 2026-09-29, in these steps.

1. **Collect.** Every `errors.jsonl`, and every `build_error` and
   `after_import_error` event, under each tallyman home on the machine. Add the
   tallyman error bodies from every transcript under `~/.claude/projects/`.
   Drop pytest and scratch homes.
2. **Group** by the pair (exception text, recipe code), since some hints depend
   on the code. Before calling the hint function, strip the
   `build execution failed:` prefix, the traceback and any hint already
   appended. Otherwise the text of an old hint can match.
3. **Run** the hint function on each group and record which branch fired.
4. **Read every firing.** One branch told the model to fix its ibis import
   when the recipe had set `expr` to a pandas DataFrame. That was the only
   recorded error the branch matched, so it had been wrong every time it fired.
5. **Cluster the errors that got no hint**, and set aside those that don't
   need one: test fixture strings, tallyman's own guard messages that already
   say what to do, infrastructure failures the model can't fix, APIs that no
   longer exist, and one-offs.
6. **Find the fix for each remaining cluster.** Pair each error with the next
   recipe in the same session that built; that is usually the fix the model
   found. Build the fix on `main` to confirm it. A cluster where the model never
   found a fix is worth the most: `Cast error: Cannot cast string` was one
   (#40).
7. **Run each new regex over the whole corpus**, shell output included, to find
   false positives.

The first run found 242 recorded errors in 135 distinct (message, code) groups.
Twelve groups got a hint, and 2 of those 12 hints were wrong or partly wrong.
Six new hints would cover 22 more groups. Six existing branches matched nothing
on disk. They were kept: the records that motivated them are gone, which says
nothing about whether the error still happens.

### Writing a hint

- State the fix in one or two sentences, with a two-line before/after when code
  helps.
- Anchor the regex on what failed, not on text that can appear anywhere in the
  message. #289's first regex, `SanityCheckPlan.*WindowAggExec`, matched any
  plan-check failure with a window anywhere in the printed plan, including one
  where an ordered `collect` above the window was the real cause. The anchored
  form `SanityCheckPlan.*?Plan: \["(?:Bounded)?WindowAggExec` matches only when
  the node that failed is the window.
- Test with the recorded message: one test that the hint appears, and one that
  a similar message without the problem gets no hint.
- If the hint names an API, check that the API exists. The prompt-pack ledger
  records a hint that pointed the model at `xo.from_catalog`, which doesn't
  exist.

## 3. The eval suite

### What it is

A single-prompt test sees one call. Many of tallyman's bugs show only after
several: a reset to an earlier revision while grids are open, a revise whose
recalc leaves a follower stale, a page that returns different rows the second
time it is read. The eval suite replays recorded sessions and checks the whole
project after every step.

As of 2026-09-29 it is in PR #258 (branch `test/eval-suite`, worktree
`~/code/tallyman2-eval`), not on `main`. The PR is test-only. Its base branch,
#216's, has since reached `main` through #189, so the PR needs retargeting
before it can merge. The untracked `tests/eval_*.py` files in the base checkout
`~/code/tallyman2` are the suite's first version (`513c32a`), superseded by the
PR.

- `tests/eval_scenarios.py`: the scenarios. Each is a function of a `Session`,
  registered with `@scenario(origin=..., summary=..., known=...)`. `origin`
  names the transcripts it came from.
- `tests/eval_harness.py`: `Session`. It calls the real MCP tool functions, so
  checkpoints and auto-recalc run. It runs the companion in-process behind a
  `TestClient`, and it simulates Buckaroo: each load loads the posted build
  fresh and pages it, so a grid that would fail or show different rows fails
  here too. The harness records and does not assert.
- `tests/eval_oracle.py`: the checks, with every finding code listed in its
  docstring. Its **ledger** keeps the rows each content hash served the first
  time it was read. Any later read of that hash that differs is a finding,
  whether it comes after a reset, a cache wipe or a restart, from a fresh
  process, or from the grid.
- `tests/eval_data.py`: seeded synthetic data shaped like the NFL files and the
  shoe orders.
- `tests/test_eval.py`: one test per scenario. A scenario fails on any `error`
  finding whose code is not in its `known` map, which maps a finding code to
  the issue that explains it.

### Running it

From the eval worktree:

```
uv run pytest -m eval tests/test_eval.py -v -s
uv run pytest -m eval tests/test_eval.py -k reset_roundtrip -s
```

The `eval` marker is deselected by default, so the normal suite and CI skip
these tests. Each run writes one JSON report per scenario to
`tests/eval_reports/<git-sha>-<timestamp>/` (gitignored) and prints the findings
grouped by code. Environment variables:

| Variable | Effect |
|---|---|
| `EVAL_SWEEP` | when the full check runs: `mutations` (default, after every step that can change state), `every`, or `manual` |
| `EVAL_NFL_DIR` | a directory with the real `nfl_contracts.parquet` and `player_stats_season.parquet`, used instead of synthetic data |
| `EVAL_SCALE` | multiplies the synthetic row counts (default 1) |
| `EVAL_STALL_SECONDS` | the per-request stall budget (default 20) |
| `EVAL_BUCKAROO_0156` | `1` simulates a grid that ignores the row-order hint, so a grid/API first-window mismatch is a warning |

### Adding a scenario from a session

1. Pick a session that went wrong, or a review finding tallyman should be held
   to.
2. Copy its MCP calls in order, with the recipe code verbatim and the user's
   prompt as `prompt`. Comment each recipe with the session id and call number,
   for example `# f3a97dc8 #5`.
3. Port it to the current API and say in the scenario's docstring what
   changed. The recorded sessions predate ADR-011 (a file enters the catalog
   only through `catalog_import_source`, and recipes read aliases), so the
   scenarios import each file and read its alias where the session used
   `catalog_load_parquet` and `read_project_file`.
4. Add what the user did, or plausibly would do, between calls: `s.open(ref)`
   for a grid, `s.reset(step)`, `s.edit_source(path, change)`, `s.evict()`,
   `s.restart()`, and `s.hammer(refs)` for concurrent requests. End with
   `s.finish()`, which restarts, reads everything warm and cold, verifies the
   snapshots and reads every entry from a fresh process.
5. Use `s.expect(condition, message)` for anything the scenario itself knows
   should hold.

### What to do with findings

The first run, on 2026-09-25, passed 2 of 10 scenarios. The way it was handled
is the pattern to follow.

- The eval PR stays test-only. Nothing under `src/` changes in it.
- Diagnose each finding down to the condition that triggers it. Eval finding 3
  ended as: a revise that changes only columns the follower never reads gives
  the follower the same content hash, so the build reuses the entry, drops the
  new parent edge, and the entry stays stale forever (#262).
- File one issue per finding (#259, #260, #262 to #267), and fix each in its
  own test-first PR. #261, for example, stops two read-only tools from adding
  a revision.
- Map the finding to its issue in the scenario's `known` map, so the suite
  fails only on new behaviour. `known` is keyed by finding code, and some codes
  are broad (`http_5xx`), so mapping one to an issue would hide every other
  failure with that code. Give the finding its own code first by adding its
  message to `KNOWN_MESSAGES` in `eval_harness.py`, which maps a message
  fragment to a code.

## Prompt packs

`buckaroo-data/tallyman-prompt-packs` (checked out at
`~/code/tallyman-prompt-packs`) predates the eval suite. A pack is a scripted
multi-turn analysis written in terse prompts. It aims at operations real
sessions have not used yet, such as cumulative windows, pivots and as-of joins,
and each run uses a fresh session. `ledger.md` lists known error signatures. A
turn that averages more than two build attempts points to a docs or hint gap,
even when it succeeds. The last run was on 2026-06-17, before ADR-011, so some
ledger entries are stale. Issues #40, #41 and #42 came from the packs and are
still open.

## Checklist for an agent asked to review a session

1. Find the transcript. Note the date, the tallyman project, and the checkout
   and SHA the MCP server ran from.
2. Count tool calls by name, and group the non-tallyman calls by what the model
   was doing.
3. Read the project's `errors.jsonl` and `events.jsonl` for the session's time
   range, and list the errors the model didn't see.
4. Check every proposal against `origin/main`.
5. Place each finding in a row of the table under
   [What a finding turns into](#what-a-finding-turns-into).
6. Report a table ranked by calls saved, and wait for Paddy to choose.
7. For each chosen finding: a worktree off `origin/main`, failing tests pushed
   and seen red on CI, then the fix and green CI.
8. Use neutral column names in anything committed.

## State on 2026-09-29

This section will go stale; the rest of the doc should not.

| What | Where | State |
|---|---|---|
| eval suite | #258 | open, test-only, needs retargeting to `main` |
| `catalog_query` and `catalog_peek` | #287 | open, review fixes in progress |
| `new_errors` in tool replies | #288 | open, review fixes in progress |
| display klass docs, hint table, new hints | #289 | open; the hint table is moving into `tallyman_xorq` |
| eval findings | #259, #260, #262 to #267 | open issues |
| prompt-pack hints | #40, #41, #42 | open issues |
