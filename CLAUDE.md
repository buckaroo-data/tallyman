# tallyman — Project Rules

## Single-user dev project — no backward-compat / migration burden
This repo has exactly one user (Paddy), who runs it only to develop tallyman
itself. There is no installed base and no third-party data on disk.

- Do NOT worry about upgrading or migrating existing on-disk catalogs/projects,
  or backward compatibility for data written by older code. When a build,
  schema, or on-disk format changes, **rebuilding the corpus is always an
  acceptable fix** — prefer it over writing migration/compat code.
- Don't flag "this would break existing user catalogs on upgrade" as a concern.
  The only catalogs are mine, and I'll rebuild them.
- This is about not spending effort on migration/compat paths, not about
  lowering quality: still write correct code and tests for current behavior.
