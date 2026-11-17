# Tallyman

Tallyman — a data science environment designed for coding agents. Stop squinting at slow tables in a terminal; turn on the lights and see your data.

Watch this introduction to tallyman video to get a better sense of it:
[Tallyman introduction](https://youtu.be/1b2J7bcA_zE?si=dmJSqhRyAURW70Af)

Two windows: Claude Code in one, a browser in the other. You work by defining a set of named results that can depend on each other, then making sure those results live up to their name. You describe them; the agent writes the queries. You tell it what `likely_customers` should mean, and a moment later the answer is a table in the browser at full size, sortable and searchable, with statistics over every column. Your prompt sits above the query the agent wrote, so you read your intent, the code that came out of it, and the result together. You notice it's catching people who already churned, so you sharpen the sentence and the agent revises the query. Everything built on top of that name updates to match, fast enough that you don't lose your place. How big the data is, what has already been computed, and what needs recomputing never enter into it.

Those queries are expressions: self-contained programs that name, by alias, the imported data and the other results they read. Writing the expression is the agent's entire job. An expression is declarative and can be introspected, so tallyman reads its dependencies straight off it and builds a directed graph of the project's aliased expressions.  When a parent updates, its children are recomputed; importing a new version of a data file counts as an update too. Tallyman executes each expression once and writes the result and its summary statistics to disk. Reading it back is out of core: scrolling, sorting, and searching pull only the pieces of data needed to fill the screen, so nothing has to fit in memory and four million rows opens like four thousand.


You aren't typing `df.sort_values('lifetime_sales')` just to see which customers are at the top, you aren't waiting for the LLM to print a 10 row table like it's coming out of a 1200 baud modem.  You aren't running out of memory in the middle of a session.  You aren't building the ad-hoc cache that every long notebook grows, the pickle in /tmp behind an if not exists guard that you never quite trust. You aren't nursing a kernel along for days because one cell takes five minutes to rerun, or bracing yourself before you close the window. None of that is in your head while you work. What's in your head is the data and what it means.


# Tallyman notebooks

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



## Tests

```sh
uv run pytest                       # full suite
uv run pytest tests/test_pack.py    # the pack / portability proof
```

## License

Tallyman is licensed under the GNU Affero General Public License, version 3 only (`AGPL-3.0-only`). The full text is in [LICENSE](LICENSE).
