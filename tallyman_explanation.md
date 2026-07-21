# tallyman notebooks - Paddy feedstock


(the LLM should never edit this section)

Tallyman — a data science environment designed for coding agents. Stop squinting at slow tables in a terminal; turn on the lights and see your data.
  

Tallyman is my take on an AI native notebook system.  Jupyter notebooks have been the go to tool for data science for over a decade for many reasons.

1. They combine the code and results of data analysis into one UI.  This is iporant because data science programming is different in nature than typical software engineering.
2. Leverage the highly perfomant python data science tools
3. Also functions as a literate programming environment allowing you to write  rendered narrative descriptions of the process (at a data analysis level not a code comment level) that is combined with graphical outputs from cells.


Tallyman takes an AI native approach to interactive data science.  It does this by constraining the problem space of data analysis.  Jupyter is a generic programming environment that works well for data science but can be used for any type of code.  Tallyman is built specifically for tabular data analysis to be driven an LLM via an MCP.  
'

The primary object in tallyman notebooks are xorq expressions.  A xorq expression is similar to a pandas dataframe with method chained aggregations and operations applied to it.    Expressions can depend on other expressions,  joining multiple expressions into one named alias.  The important thing is the naming and the intent.  If you have an expression named `customers` and another expression named `best_customers` that depends on `customers`, updating the definition of `customers` also causes a recomputation of `best_customers`.  

All of these expressions are meant to be written by an LLM sent to tallyman via MCP, executed once, and then the results cached.  Tallyman is a system that has interactive views of these expressions, but unlike jupyter notebooks, it doesn't require the expressions/dataframes to be resident in memory.  Tallyman can view dataframes with millions of rows without blowing up memory.  

I would say that jupyter notebooks are the most used example of literate programming.  LIterate programming combines documentation with ocde and results.  Documentation and comments in notebooks explain why you are doing something.  This is tedious to write as a programmer and documentation is frequently out of date.  LLMs can make this much easier, not by easking the LLM to "write docs for this",  not by automatically asking the LLM to write "write docs for this", but instead by capturing the orignal prompt that you used to get the LLM to write an expression.  This prompt is the clearest version of your intent as expressed and understood by you the writer.  Tallyman keeps this next to each expression.  You can review intent of each expression this way.

Back to updating named expressions.  The named expression thing is really important.  In a regular notebook, it would be equivalent to `customers_df`, and hopefully you'd update it appropriately and dependent variables/cells as you update your notebooks.  Marimo puts more rigor around this Cell-DAG approach, but Marimo is putting structure around unstructured python code,  marimo doesn't cache intermediate results.  Everytime you reload a notebook, marimo goes and re-executes everything.  Furthermore marimo, can't diff between versions.

I got side tracked there talking about the dependency graph.  We also have versioned history of each named expresion, and it's prompt.  All of that is built into tallyman, and it's fast.

There is a notebook view that lets you organize named expressions into a coherent order and edit the prompt/markdown for each alias.  I think of this as kind of an outline view of what will be a presentation notebook.  ... Then because we don't want to reinvent an entire ecosystem, if you want to export, you can export this order to either a marimo notebook or jupyter (coming) and leverage their rich presentation ecosystem.


An important note about why tallyman is built how it is.  LLMs are good at writing code, I think they are less good at udnerstanding existing jupyter kernel state.  The dominant form of interacting with LLMs is a chat interface that produces side effects or artifacts like cluade code.  I could have put a chat box in  each notebook cell and gotten claude to write that code secgtion, but that misses gthe whole system.  Tallyman is built to let LLMs do what they are good at, and provide the best parts of the notebook expreince for data analyssi




This should go with the customizable styling that you can build into the buckaroo view, that lives with the catalog, and the customization is written by the LLM.

Build the intelligence into the tool: use the LLM to make the injection mold, not to carve each piece of plastic by hand. The agent should be shaping      *how* you look at data — writing a summary statistic you'll use on every table for the rest of the project — while the looking itself stays instant and free.



# Bullet point list of features
(Claude please fill in the features I have written about into an organized list, + features that I ahven't mentioned)

## Authoring — the LLM writes expressions, over MCP
- Every analysis step is a **xorq expression** submitted through an MCP tool
  call, not code typed into a cell. Claude Code (or any MCP client) is the
  editor.
- **Named aliases** (`orders`, `by_region`) are the unit of work. An alias is a
  mutable pointer to the latest content hash of a logical step.
- **Expressions depend on other expressions by name.** A child either *follows*
  an alias (it goes stale when the parent advances) or *pins* an exact version
  (it never moves). Both are recorded as edges at build time.
- **The prompt is the documentation.** The natural-language request that
  produced an expression is stored alongside it, versioned with it. No "write
  docs for this" round-trip, and the doc can't drift from the code because they
  were minted together.
- **Intelligent CSV/parquet import** with a deterministic reader and an error
  contract designed to hand the LLM a usable suggestion when a file doesn't
  parse cleanly.
- **Projects** — multiple independent catalogs, with `project_new`,
  `project_list`, `project_switch`.

## Identity and caching — execute once
- **Content-addressed entries.** An entry's identity is a hash of its
  expression structure plus its source-file digests. Same code + same inputs →
  same hash → same directory on disk. Resubmitting identical work is free.
- **Two-axis caching.** A source axis caches reads of input files; a result axis
  bakes a snapshot for entries judged expensive (aggregates, joins, sorts,
  windows, UDFs). Cheap row-preserving entries recompute rather than pay for
  storage.
- **Snapshots self-heal.** An evicted snapshot rematerializes transparently on
  the next read.
- **Result digest.** Baked entries record a hash of their executed bytes as a
  row multiset. If a rebuild disagrees with the recorded digest, the expression
  is nondeterministic (sampling, `now()`, an impure UDF, source drift) and
  tallyman says so instead of silently serving different numbers.
- **Nothing lives in a kernel.** All catalog state is on disk. The MCP server
  holds no in-memory state at all.

## Reactivity — revise one alias, the graph catches up
- **Staleness on two axes**: an alias axis (a followed parent advanced) and a
  source axis (an input file's content digest changed). Computing staleness is
  read-only; it executes nothing and reports reasons.
- **Recalc cone.** When an alias head moves, its dependents are recomputed in
  topological order, parents before children.
- **Auto-recalc on revise**, with preview-then-commit for explicit recalcs. Only
  the followers of the alias that moved are rebuilt; pre-existing staleness is
  left alone and logged rather than swept up.
- **Cycle and failure handling** — a failed rebuild is recorded against the
  entry, not lost.

## History and diff
- **Versioned aliases (V1, V2, …).** Every hash an alias has ever pointed at
  stays on disk as forensic lineage, each with its own prompt and code.
- **Diff two versions** across five surfaces: code diff, schema diff, per-column
  summary stats, key-joined row-level side-by-side, and `head()` side-by-side.
- **Promote a diff** to accept a version.
- **Git-backed checkpoints.** Each mutation (and each whole recalc walk) is one
  atomic commit under a per-project lock.
- **Reset to any revision**, with build artifacts reconciled back to the
  recorded pointers — restored from the bullpen instead of recomputed.

## Viewing — a grid over data that was never in memory
- **Buckaroo grid per entry.** The viewer is handed the entry's xorq *build
  directory*, so sort, search, and summary-stat computation push down into the
  xorq backend. Millions of rows without materializing them in the browser or
  in a kernel.
- **Automatic per-column summary stats and histograms** on every entry.
- **Charts.** Vega-Lite specs attached to an entry and rendered above its grid.
- **Live updates over SSE.** The browser reflects each tool call as it lands.
  You and the model watch the same screen.
- **Log tab** — a linear, filterable activity view, including build failures.
- **Cache tab** — per-entry disk footprint.

## The LLM extends the analysis UI, not just the data
- **Project-authored summary stats.** The LLM writes a `compute(col)` function;
  it then appears as a pinned row in *every* grid in the project. Validated by a
  dry run at tool-call time, so a bad stat fails at the MCP call rather than in
  a subprocess log.
- **Post-processing functions** — a `process(expr)` per file, surfaced as an
  option in the grid's post-processing dropdown.
- **Display klasses** — project-specific styling and formatting rules for the
  main and summary views.
- All three are soft-deletable and versioned with the project.

## Narrative and export
- **Catalog vs. notebook are different views.** The catalog is everything ever
  built. The notebook is the curated subset, ordered, with editable
  markdown/prompt per alias — the outline of the presentation.
- **Export to Marimo** today, Jupyter coming, so presentation rides on an
  existing ecosystem rather than a new one.

## Sharing
- **Portable builds.** Absolute paths in build artifacts are rewritten to
  `${TALLYMAN_PROJECT_ROOT}` on write and expanded on load, so a project runs
  from anywhere on disk.
- **`tallyman pack` → `.tgz` → `tallyman serve`.** A colleague gets the whole
  catalog, history, and grids as a read-only companion with no environment to
  reproduce and no kernel to start.

# Introduction to Tallyman — a walkthrough

You have two windows open. On the left, a terminal running Claude Code. On the
right, a browser at `localhost:7860`. That's the whole setup. There is no cell
to click into, no kernel to restart, and no `%matplotlib inline` at the top of
anything.

You start the way you always do:

> Load `violations.csv` and show me what's in it.

The agent reads the file, writes an expression, and sends it to tallyman. A
second later the browser has an entry in it called `violations`, and clicking it
gives you a real grid: 4 million rows, every column, with a histogram and
summary statistics sitting above each column. Not `df.head()`. Not the first
five rows wrapped across your terminal at 80 characters. The whole result, and
you can scroll to row 3,000,000 if you want to.

Speed is the part that surprises people. Once the grid is up, sorting it by a
column or searching it returns results in less time than an agent takes to
print a 10-by-10 table into your terminal. Generating a hundred cells of text
is slower than sorting four million rows, and at the end of it you have a
hundred cells.

This is the first thing tallyman changes, and it's the least clever one. Coding
agents are good at writing dataframe code and bad at showing you the result,
because the terminal is a hostile display for tabular data. Tallyman gives the
agent somewhere to put the answer.

Looking at the data is the first step of any real analysis, and tooling should
make that step free. You shouldn't have to type code to sort by a column, and
you shouldn't have to wait while a model writes that code for you

## You ask for a transformation

> Group these by violation code and count them, sorted descending.

A new entry appears at the top of the catalog, named `by_code`. Above the grid
is the sentence you just typed. Below it is the expression the agent wrote. You
read both, in that order, and decide whether they agree.

That pairing is the second change. In a notebook you'd have a code cell and,
if you were disciplined, a markdown cell above it explaining why. Here the
"why" was already written — it's the thing you asked for — and tallyman keeps it
attached to the code it produced. You didn't write documentation. You have
documentation.

## You spot something wrong

The counts look too high, and scrolling the `violations` grid shows you why:
the file has duplicate rows.

> `violations` should drop exact duplicate rows.

Two things happen. The `violations` entry gets a **V2** chip; V1 is still in the
catalog, with its own prompt, its own code, and its own result. And `by_code`
recomputes on its own, because it was defined in terms of `violations` by name.
You didn't have to remember that `by_code` existed, or find the cell it lived
in, or rerun everything below cell 8 to be safe.

Now click **Diff** and compare `violations` V1 against V2. You get the code
diff, the schema diff, per-column statistics side by side, and the rows
themselves joined on a key. The count dropped by 41,000. You can see which rows
went away, not just that some did.

That's a thing you cannot do in Jupyter or Marimo, because neither one keeps
what the previous version produced. Once you rerun a cell, the old answer is
gone.

## You shape the review to your data

> Add a summary stat for the percent of nulls in each column.

The agent writes one small function. It appears as a new row in the statistics
block on *every* grid in the project, including the ones you built an hour ago.
The same works for formatting rules and for post-processing views. The agent
isn't just producing tables; it's customizing the tool you're reviewing them
with.

## You do a review pass

At some point you have twenty entries and you want to know whether the analysis
is any good. You go down the catalog and read the prompts — twenty sentences of
plain English, in your own words, in the order you asked them. When one looks
suspicious you open it, read the expression, look at the grid, and check its
version history.

Review is the human's job in this system, and it's the thing everything else is
arranged around: the prompts are there so you can read intent quickly, the
grids are there so you can check results without writing a query, and the diffs
are there so you can see exactly what a change did.

## You tell the story

The catalog holds everything you tried, including the dead ends. The **Notebook**
tab holds the subset that's worth showing: drag entries into an order, write
markdown between them, drop the ones that were detours. It's an outline of the
presentation you're about to give.

When you're ready, export that ordering to a Marimo notebook (Jupyter is
coming) and use their presentation ecosystem, or pack the project into a `.tgz`
and hand it to a colleague. They run one command and get the whole thing
read-only — catalog, history, grids, prompts — with no environment to reproduce
and no kernel to start.

## What you never did

You never restarted a kernel. You never wondered whether a variable was stale.
You never reran a cell to find out what it used to say. You never scrolled a
truncated table in a terminal. And you never wrote a line of code — but you
read every line the agent wrote, which is the part that actually needed you.

# For the CS-minded — what is actually happening

The user-facing behavior above comes out of a small number of design decisions.
None of them are novel on their own; the leverage is in constraining the
problem space enough that they can all hold at once.

**The unit is an expression, not a cell.** Every step is a xorq expression — a
method-chained relational transformation that compiles to a query plan (xorq is
built on Ibis and DataFusion). Constraining the language to tabular
transformations is what makes everything downstream tractable. You cannot
express arbitrary Python, so tallyman never has to reason about arbitrary
Python.

**Identity is content-addressed.** An entry's identity is a hash of its
expression structure plus the content digests of its source files. Same code
over the same inputs produces the same hash and lands in the same directory.
Resubmitting identical work is a no-op, which makes builds idempotent and makes
"did this change?" a pointer comparison rather than a diff.

**The graph is recorded, not inferred.** At build time each entry writes down
its direct parents as edges. An edge is either *following* (it names an alias
and tracks whatever that alias currently points at) or *pinned* (it names an
exact hash and never moves). Marimo infers its DAG by static analysis of Python
cells; tallyman doesn't have to, because the dependency was declared when the
expression was written.

**An alias is a mutable pointer with history.** `violations` is a name pointing
at a content hash, plus an ordered list of every hash it has ever pointed at.
Revising mints a new hash, advances the head, and appends to the history. Old
versions are never garbage; they're the lineage, and they're what makes diff
possible.

**Staleness is a read-only computation on two axes.** An entry is stale on the
alias axis when a following parent's head no longer matches the recorded hash,
and on the source axis when an input file's content digest no longer matches
what was recorded. Detecting staleness executes nothing — it compares the
manifest against the world and returns reasons. This is why the scan is instant
even on a large catalog.

**Recalc walks a cone in topological order.** When an alias head advances, its
dependents form a cone. Kahn's algorithm over the intra-cone edges orders the
rebuild so parents finish before children, and each entry is rebuilt exactly
once no matter how many paths reach it. Pre-existing staleness elsewhere in the
catalog is left alone and logged rather than swept into the walk.

**Caching runs on two axes with a worthiness rubric.** Reads of source files are
cached on one axis. On the other, results are baked to a snapshot only for
entries judged expensive — those whose plan contains an aggregate, join, sort,
window, or UDF. A cheap row-preserving projection recomputes on read instead of
paying storage, because recomputing it is cheaper than storing it. Evicted
snapshots self-heal: a cold read rematerializes transparently.

**Determinism is audited, not assumed.** Baked entries record a digest of their
executed bytes, computed as a row *multiset* so that scan-order nondeterminism
doesn't produce false alarms. If a rebuild disagrees with the recorded digest,
something in the expression is nondeterministic — sampling, `now()`, an impure
UDF, or source drift — and tallyman reports it rather than silently serving
different numbers under the same hash.

**State lives on disk; there is no kernel.** The MCP server holds no in-memory
state at all, and the companion holds only its list of SSE subscribers. This is
the design decision the whole system rests on, and the reason it suits agents:
an agent working in a live Jupyter kernel has to reason about invisible state it
cannot inspect — what's in memory, what's stale, what ran in what order. Here
there is nothing invisible to reason about. Every fact is a file, addressed by
content.

**The grid pushes computation down.** The viewer (Buckaroo) is handed the
entry's compiled build directory rather than a materialized dataframe, so sort,
search, and summary-statistic computation execute in the query engine against
the data on disk. Nothing needs to fit in browser memory, and nothing needs to
fit in a Python process either. This is why 4 million rows opens like 4
thousand.

**Mutations are git transactions.** Each operation — including an entire recalc
walk — is committed atomically under a per-project lock. Reset-to-revision does
a hard reset and then reconciles build artifacts back to the recorded pointers,
restoring them from a holding area rather than recomputing them. The history of
the analysis is a real history you can walk backwards.

**Builds are relocatable.** Compiled artifacts embed absolute paths; on write
these are rewritten to a `${TALLYMAN_PROJECT_ROOT}` placeholder and expanded on
load into a stable per-entry directory. That's what makes `pack` and `serve`
work: a project is a directory you can move to another machine and open.

**The browser is push-driven.** The companion holds one long-lived SSE stream
per client. Each MCP mutation notifies the companion, which fans out a named
event; the SPA increments a version counter and refetches only the affected
resource. No polling, and no stale view while you're watching the agent work.

## Appendix — the original framing

You already know the shape of the problem, because you've lived it in Jupyter.
Cell 3 reads a CSV into `orders`. Cell 8 filters it into `recent_orders`. Cell
14 groups that into `by_region`, and cells 15 through 40 build charts and
tables on top. Then you go back and change the filter in cell 8. Now you have a
choice: rerun everything and wait, or rerun the four cells you believe matter
and hope you got them all. The code on screen and the objects in the kernel
have drifted apart, and nothing in the system knows by how much. You do, sort
of, for about another twenty minutes.

That drift is the tax on the notebook format, and it's been worth paying for
over a decade, because the things Jupyter gets right are hard to give up: code
and results in one place, the full Python data stack underneath, and prose
woven through the analysis at the level of *why we did this* rather than *what
this line does*.

Marimo attacks the drift directly by inferring a DAG from the cells. That's
real progress. But it's structure imposed on unstructured Python, and it pays
for correctness by re-executing: reload the notebook and everything runs again.
It also can't tell you what changed between two versions of an analysis,
because there's no durable record of what the previous version produced.

Tallyman starts somewhere else. Instead of adding rigor to a general-purpose
programming environment, it narrows the environment until rigor is cheap. The
only thing you can express is a tabular transformation — a xorq expression,
which reads like a method-chained dataframe and compiles to a real query plan.
Give up arbitrary Python and you get, in exchange: a dependency graph that's
recorded rather than inferred, content-addressed results that are computed once
and kept, and a diff between any two versions of any step.

The second thing that changes is who's typing. Tallyman expects the LLM to
write the expressions and send them over MCP. This isn't a chat box bolted onto
a cell. An LLM asked to work inside a live Jupyter kernel has to reason about
invisible state it can't inspect — what's in memory, what's stale, what ran in
what order. An LLM asked to write a named, self-contained expression is being
asked to do the thing it's best at. Tallyman removes the state problem from the
model's job by not having any hidden state: everything is on disk, addressed by
content.

Once the model is the author, a few things fall out that don't have Jupyter
equivalents.

**Documentation stops rotting.** The prompt that produced an expression is the
cleanest statement of intent that exists, in your words, and tallyman stores it
next to the code and versions it alongside. You review intent, not comments.

**Naming becomes load-bearing.** `best_customers` follows `customers` by name.
Revise `customers` and the dependents recompute in dependency order, once.
Nothing else in the catalog is touched. And when a source *file* changes
underneath you, that counts as staleness too.

**History is real.** Every version an alias has ever had is still on disk, with
its prompt, its code, and its result. You can diff V2 against V3 by code, by
schema, by per-column statistics, and row-by-row on a join key. When you tell
someone "the numbers moved because I widened the date filter," you can show it.

**Size stops being a wall.** Because results are materialized to disk and the
grid pushes sort, filter, and summary statistics down into the query engine,
you can browse millions of rows without a kernel holding them in memory.

**Even the analysis UI is authorable.** Ask for a new column statistic and the
model writes it once; it then appears on every table in the project. Same for
post-processing views and formatting.

The last piece is the exit. The catalog holds everything you tried; the
notebook view holds the story you want to tell, ordered, with your prose
attached to each step. When it's time to present, export that ordering to
Marimo (Jupyter is coming) and use their ecosystem. Tallyman is trying to fix
how analysis gets built and kept, and has no interest in rebuilding how it gets
displayed.


