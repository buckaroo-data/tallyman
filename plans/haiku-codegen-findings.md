# Haiku xorq codegen — test results

Generated 2026-05-30. The 5 DS demo-script step-intents were fed to a Haiku model (claude-haiku-4-5) with the real `catalog_run` docstring + dataset schema as context. Haiku wrote the xorq/process() code; every snippet was then executed through the real `build_and_persist` / `run_post_processing` / `write_post_processing` pipeline in isolated scratch projects (`haiku-ts`, `haiku-wine`, `haiku-titanic`, `haiku-diamonds`, `haiku-penguins`).


**Headline:** xorq codegen 13/20 steps executed; post-processing 4/11 (every failure imported scipy/sklearn/pandas, which the sandbox blocks).


## timeseries-window-functions  (project `haiku-ts`)

- ✓ **step 1** `catalog_load_parquet` — rows=560
- ✗ **step 2** `catalog_create` — BUILD_ERROR
  - `executing user code raised: module 'ibis' has no attribute 'to_date'. 
Traceback (most recent call last):
  File "/Users/paddy/code/tallyman-notebooks/src/tallyman_xorq/build.py", line 112, in _import_script`
- · **step 3** `catalog_chart` — 
- ✗ **step 4** `catalog_revise` — BUILD_ERROR
  - `executing user code raised: catalog entry 'stock_returns' not found in project 'haiku-ts'
Traceback (most recent call last):
  File "/Users/paddy/code/tallyman-notebooks/src/tallyman_xorq/build.py", line 112, i`
- · **step 5** `catalog_chart` — 
- · **step 6** `catalog_diff` — 
- ✗ **step 7** `catalog_run_post_processing` — PP_RUN_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `scipy` in process() (sandbox blocks __import__)
  - `no result.parquet found for entry 'stock_returns' (resolved hash: stock_returns)`
- ✗ **step 8** `catalog_add_post_processing` — PP_VALIDATE_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `scipy` in process() (sandbox blocks __import__)
  - `source raised at exec time: ImportError('__import__ not found')`

## hypothesis-testing-scipy  (project `haiku-wine`)

- ✓ **step 1** `catalog_load_parquet` — rows=1599
- ✓ **step 2** `catalog_load_parquet` — rows=4898
- ✓ **step 3** `catalog_create` — rows=6497
- ✗ **step 4** `catalog_create` — BUILD_ERROR
  - `executing user code raised: struct() got an unexpected keyword argument 'mean'
Traceback (most recent call last):
  File "/Users/paddy/code/tallyman-notebooks/src/tallyman_xorq/build.py", line 112, in _import_s`
- ✗ **step 5** `catalog_create` — OTHER_ERROR
  - `IsADirectoryError: [Errno 21] Is a directory: '/tmp/claude-501/tallyman_build_78hd_37o/8d7ab266d947/memtables'`
- ✗ **step 6** `catalog_run_post_processing` — PP_RUN_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `scipy` in process() (sandbox blocks __import__)
  - `source raised at exec time: ImportError('__import__ not found')`
- ✗ **step 7** `catalog_add_post_processing` — PP_VALIDATE_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `scipy` in process() (sandbox blocks __import__)
  - `source raised at exec time: ImportError('__import__ not found')`
- ✗ **step 8** `catalog_add_post_processing` — PP_VALIDATE_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `scipy` in process() (sandbox blocks __import__)
  - `source raised at exec time: ImportError('__import__ not found')`
- ✗ **step 9** `catalog_revise` — BUILD_ERROR
  - `executing user code raised: struct() got an unexpected keyword argument 'mean'
Traceback (most recent call last):
  File "/Users/paddy/code/tallyman-notebooks/src/tallyman_xorq/build.py", line 112, in _import_s`

## survival-cohort-analysis  (project `haiku-titanic`)

- ✓ **step 1** `catalog_load_parquet` — rows=891
- ✓ **step 2** `catalog_create` — rows=6
- ✓ **step 3** `catalog_create` — rows=6
- ✓ **step 4** `catalog_run` — rows=1
- ✓ **step 5** `catalog_create` — rows=891
- ✓ **step 6** `catalog_revise` — rows=891
- · **step 7** `catalog_diff` — 
- ✓ **step 8** `catalog_revise` — rows=891
- · **step 9** `catalog_chart` — 
- · **step 10** `catalog_chart` — 
- ✓ **step 11** `catalog_run_post_processing` — rows=6
- ✓ **step 12** `catalog_add_post_processing` — committed
- ✓ **step 13** `catalog_add_summary_stat` — committed

## regression-feature-engineering  (project `haiku-diamonds`)

- ✓ **step 1** `catalog_load_parquet` — rows=53940
- ✓ **step 2** `catalog_create` — rows=53940
- ✓ **step 3** `catalog_revise` — rows=53940
- · **step 4** `catalog_diff` — 
- ✓ **step 5** `catalog_create` — rows=1
- · **step 6** `catalog_chart` — 
- · **step 7** `catalog_chart` — 
- ✓ **step 8** `catalog_run_post_processing` — rows=53940
- ✓ **step 9** `catalog_add_post_processing` — committed

## clustering-unsupervised-prep  (project `haiku-penguins`)

- ✓ **step 1** `catalog_load_parquet` — rows=344
- ✓ **step 2** `catalog_create` — rows=342
- ✓ **step 3** `catalog_create` — rows=342
- ✗ **step 4** `catalog_run_post_processing` — PP_RUN_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `sklearn` in process() (sandbox blocks __import__)
  - `source raised at exec time: ImportError('__import__ not found')`
- ✗ **step 5** `catalog_add_post_processing` — PP_VALIDATE_ERROR  ⚠ imports `pandas` in process() (sandbox blocks __import__), imports `sklearn` in process() (sandbox blocks __import__)
  - `source raised at exec time: ImportError('__import__ not found')`
- ✗ **step 6** `catalog_create` — BUILD_ERROR
  - `executing user code raised: No module named 'sklearn'
Traceback (most recent call last):
  File "/Users/paddy/code/tallyman-notebooks/src/tallyman_xorq/build.py", line 112, in _import_script
    spec.loader.exe`
- ✗ **step 7** `catalog_create` — BUILD_ERROR
  - `executing user code raised: catalog entry 'penguins_labeled' not found in project 'haiku-penguins'
Traceback (most recent call last):
  File "/Users/paddy/code/tallyman-notebooks/src/tallyman_xorq/build.py", li`
- · **step 8** `catalog_chart` — 
- ✗ **step 9** `catalog_add_summary_stat` — STAT_ERROR
  - `source raised at exec time: ImportError('__import__ not found')`
- ✓ **step 10** `catalog_revise` — rows=342