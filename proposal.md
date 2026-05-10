# The Future of Notebooks in a Claude Code World

PyData London 2026 — proposal

## Abstract

AI coding agents are changing how data professionals work. But an AI agent chat session is a stream, a long conversation that scrolls on and on. A good notebook is something different: a sequence of distinct, well-structured transformations, each with an explanation and a visible result. How do you get from the chat stream to that? And how do you see the visualizations, the tables, charts, and diffs that make data work legible?

We'll trace the historical reasons why the programming notebook style developed, what problems it solves, and what problems it creates. Notebooks intermingle three valuable concepts: a live execution environment, a long-running process that caches state in memory, and a narrative log of exploration steps. The long-running process is the key. It's why data scientists use notebooks instead of Python scripts. But this coupling is also why notebooks are fragile, unreproducible, and impossible to productionize. And the kernel's implicit mutable state is a poor fit for AI agents. Unlike databases (explicit state, declarative interface, introspectable), a notebook kernel degrades as implicit state accumulates across cells.

This talk introduces the Deconstructed Notebook: a system that gives AI-agent-driven data work the structure and visualization of a notebook without the notebook's baggage. Claude writes the instructions in the terminal. The PyData Arrow stack, driven by Ibis and xorq, handles the compute. A browser companion renders tables, charts, diffs, and lineage live as the work iterates, organized into distinct steps, not a scrolling chat log. The key architectural insight is that automatic caching of expression results to disk replaces the notebook kernel's in-memory state, letting each step execute as a self-contained script while preserving the interactive, incremental workflow data scientists depend on. The system is built on xorq, an open-source library built on Ibis and Apache Arrow, but the design principles generalize. We'll demo the full workflow live and share what we learned about building post-notebook tooling for the age of AI agents.

## Description

1. **The notebook's hidden contract** — Jupyter intermingles three valuable things: interactive execution, a long-running process that caches state in memory, and a narrative log of exploration steps. The coupling has real benefits — edit-in-place re-execution captures a clean story, not a noisy shell log, and the persistent kernel means you never have to reload expensive state. But the coupling is also why notebooks are fragile, unreproducible, and can't go to production. AI agents are fine with long-running stateful processes like databases (explicit state, declarative interface, introspectable). A notebook kernel is the opposite — implicit mutable state, imperative, execution-order-dependent — and the agent's model of it degrades as state accumulates. Self-contained steps with explicit inputs and cached outputs have much better failure modes.

2. **The display surface gap** — How data professionals actually use Claude Code today: saving PNGs, dumping ASCII tables, switching back and forth to Jupyter. The terminal is a fantastic interface for intent but a terrible interface for output.

3. **Prior art and adjacent solutions** — MCP Apps (renders UI inside Claude Desktop's chat window), chart-canvas (browser dashboard for Claude Desktop), Data Formulator (Microsoft's standalone viz tool). What each gets right, and why none of them solve the CLI agent case.

4. **The deconstructed notebook (live demo)** — Separate the three concerns. Terminal for intent. The PyData Arrow stack driven by Ibis/xorq for compute, with instructions written by Claude. Browser for display. Live walkthrough of the working system: the audience sees the browser update in real time as Claude iterates, with tables, charts, and diffs appearing in structured blocks, not a scrolling chat log. The default view shows the current result at each step — preserving the notebook's narrative quality — with iteration history available but not in your face.

5. **Iteration and diffing (live demo)** — Exploratory data analysis through model evaluation, driven by conversation. The audience watches the full loop live: prompt, compute, result, diff, refine. Interactive tables with sort/filter, Vega-Lite charts, side-by-side diffs showing exactly what changed between iterations, and expression lineage tracing the full dependency graph from raw data to final result.

6. **What the compute substrate needs to get right** — Why "just render HTML" isn't enough. The notebook kernel's real job is caching — keeping expensive intermediate results in memory so you can build on them. To decouple the notebook, you need a substrate that handles caching automatically: expression results stored as Parquet on local disk, streamed to the next step, no long-running process needed. Plus: content-addressing (so every iteration is retrievable), typed schemas (so composition errors are caught early), and separation of transform logic from visualization. Brief introduction of xorq's expression model as one approach to these requirements.

7. **Design principles for post-notebook tooling** — Expressions are append-only and immutable — every iteration is preserved. But the workflow and the final view are structured like a notebook: blocks that are iterated on, each showing its current result, with history accessible underneath. These blocks can be arranged into a traditional notebook-style narrative or a dashboard. The human controls the intent and reviews the display. Diffing is a first-class operation. Every intermediate result is addressable.

## Notes

This is a demo-driven talk. The concepts are illustrated with live demos of a working system, not static slides.
