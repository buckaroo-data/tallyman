# tallyman notebooks

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

