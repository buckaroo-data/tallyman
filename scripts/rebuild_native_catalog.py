"""Rebuild a project's catalog into the native content-addressed store by
re-executing its recipes.

Why a rebuild (not a migration). The catalog on-disk format changed with the
native store (#52): the old xorq-catalog layout (``catalog.yaml`` + per-entry
``metadata/<hash>.zip.metadata.yaml`` sidecars + ``aliases.json`` /
``alias_history.json`` + in-entry ``prompts.jsonl``) is replaced by a decomposed
tracked surface (``aliases.jsonl``, ``notebook.jsonl``, ``prompts/<hash>.jsonl``,
relocated ``post_processing`` / ``stats``) plus native ``entries/<hash>.zip``
recipes. The project ``CLAUDE.md`` rule is "rebuild the corpus, don't write
migration code" — so this re-executes each entry's ``expr.py`` instead of
transcoding bytes.

The hash-drift fact that shapes everything. A re-exec does NOT reproduce the old
content hashes — empirically the hash drifts even re-running the identical recipe
against the identical data at the same path (the build pipeline + xorq
serialization have moved since the corpus was built; the hash is also sensitive
to the absolute source path). So the rebuild treats hashes as NOT preserved and
remaps every hash-keyed artifact through an ``old_hash -> new_hash`` table built
as it goes:

* a SOURCE entry (ADR-011: a version of a source alias, whose rows are an imported
  file) is not re-exec'd at all — its generated recipe reads the very snapshot the
  rebuild just wiped, and a raw file read is a build error anywhere else. It is
  replayed through ``update_and_depend``, from the provenance path when that still
  holds the imported bytes and from the clone under ``data/.cas`` when it does not,
  under the source alias that holds it now (a rename leaves ``provenance.alias``
  naming the alias it was imported as) and pinned to its version there.
  Its hash is a function of the bytes and the reader options, so it is preserved;
* recipes that chain by ALIAS (``tracked_expr_from_alias("citibike")``) survive untouched —
  the alias is re-pointed at the rebuilt parent before the child is built;
* recipes that chain by literal HASH (``tracked_expr_from_alias("763193211746")``) are
  rewritten in place (old hash -> new hash) before the child builds, which is why
  the build order is a topological sort of the tracked_expr_from_alias dependency graph;
* charts (``chart_specs/<hash>.vl.json``) and notebook cells are remapped;
* alias history is reconstructed with the remapped hashes;
* per-entry prompt history is carried across to the new ``prompts/<hash>.jsonl``.

Reads either the native ``aliases.jsonl`` or the old ``aliases.json`` +
``alias_history.json``, so it round-trips a native catalog and upgrades an old
one with the same code path.

Usage::

    TALLYMAN_HOME=~/.tallyman-notebooks uv run python scripts/rebuild_native_catalog.py first-project
    # inspect the plan without writing anything:
    TALLYMAN_HOME=~/.tallyman-notebooks uv run python scripts/rebuild_native_catalog.py first-project --dry-run

The rebuild is IN-PLACE: it reads every recipe into memory, wipes the catalog
metadata (keeping ``data/``), re-execs, and checkpoints. Destructive to the
catalog bookkeeping (recipes are regenerated); the single-user no-migration rule
permits it. Use ``--dry-run`` first.

Known limitation — build-time alias revision. A recipe chains a parent by ALIAS
with no version pin (``tracked_expr_from_alias("habitual_speeder_stats")``), so when that
alias was revised *after* a child was built and the revision changed the parent's
schema, the rebuild re-points the alias at its FINAL latest revision and the
child can fail to find a column the older revision had. The exact build-time
parent hash is recorded in ``manifest.parents`` (#84); pinning cross-alias refs
to it (rewriting the ref to the build-time parent hash, like the literal-hash
path already does) would close this — the next step for deeply-revised corpora.
Validated end-to-end on `first-project` (4 entries, alias chain) and the native
round-trip test; a 15-entry corpus with schema-evolving alias revisions
(`parking_ticket_analysis_1m`) reaches this limit.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

# 12-char lowercase-hex content hash (the entry-dir / pointer naming).
_HASH_RE = re.compile(r"\b[0-9a-f]{12}\b")
# tracked_expr_from_alias("ref") / tracked_expr_from_alias('ref', ...) — first positional arg only.
_FROM_CAT_RE = re.compile(r"tracked_expr_from_alias\(\s*['\"]([^'\"]+)['\"]")


@dataclass
class OldCatalog:
    """Everything the rebuild needs, read out of the old catalog before it is wiped."""

    recipes: dict[str, str] = field(default_factory=dict)  # old_hash -> expr.py text
    aliases: dict[str, str] = field(default_factory=dict)  # alias -> current old_hash
    history: dict[str, list[str]] = field(default_factory=dict)  # alias -> [old_hash, ...]
    kinds: dict[str, str] = field(default_factory=dict)  # alias -> "catalog" | "source" (ADR-011 D1)
    provenance: dict[str, dict] = field(default_factory=dict)  # old_hash -> manifest.provenance (source entries)
    charts: dict[str, str] = field(default_factory=dict)  # old_hash -> vega-lite spec text
    prompts: dict[str, list[dict]] = field(default_factory=dict)  # old_hash -> [{prompt, at}, ...]
    post_processing: dict[str, str] = field(default_factory=dict)  # name -> source
    stats: dict[str, str] = field(default_factory=dict)  # name -> source
    notebook_cells: list[dict] = field(default_factory=list)  # [{cell_id, alias, markdown}, ...]


def read_old_catalog(project: str) -> OldCatalog:
    """Read the pre-rebuild catalog across all three layouts this project has seen:

    * native (#52+): ``aliases.jsonl`` + ``chart_specs/`` files + ``prompts/`` +
      relocated ``post_processing`` / ``stats`` .py;
    * aliases.json-era: ``aliases.json`` + ``alias_history.json`` + ``chart_specs/``
      + ``artifacts/{post_processing,stats}``;
    * catalog.yaml-era (most of the real corpus): everything embedded in
      ``catalog.yaml`` — ``alias_map`` / ``alias_history`` / ``charts`` /
      ``post_processing`` / ``stats`` / ``notebook`` — with no decomposed files.

    Each section reads from its decomposed file if present, else falls back to
    ``catalog.yaml``.
    """
    from tallyman_core.aliases import CATALOG_KIND  # noqa: PLC0415
    from tallyman_core.paths import catalog_dir  # noqa: PLC0415

    cat = catalog_dir(project)
    oc = OldCatalog()

    cy = cat / "catalog.yaml"
    ydata: dict = {}
    if cy.is_file():
        import yaml  # noqa: PLC0415

        ydata = yaml.safe_load(cy.read_text()) or {}

    entries = cat / "entries"
    if entries.is_dir():
        for d in sorted(entries.iterdir()):
            if d.is_dir() and (d / "expr.py").is_file() and (d / "manifest.json").is_file():
                oc.recipes[d.name] = (d / "expr.py").read_text()
                try:
                    provenance = (json.loads((d / "manifest.json").read_text()) or {}).get("provenance")
                except (OSError, ValueError):
                    provenance = None
                if provenance:
                    oc.provenance[d.name] = provenance
                pj = d / "prompts.jsonl"
                if pj.is_file():
                    oc.prompts[d.name] = [json.loads(x) for x in pj.read_text().splitlines() if x.strip()]

    # prompts may already be decomposed (native layout) under prompts/<hash>.jsonl
    pdir = cat / "prompts"
    if pdir.is_dir():
        for f in pdir.glob("*.jsonl"):
            oc.prompts.setdefault(f.stem, [json.loads(x) for x in f.read_text().splitlines() if x.strip()])

    aliases_jsonl = cat / "aliases.jsonl"
    if aliases_jsonl.is_file():
        for line in aliases_jsonl.read_text().splitlines():
            if line.strip():
                rec = json.loads(line)
                oc.aliases[rec["alias"]] = rec["latest"]
                oc.history[rec["alias"]] = rec.get("history", [])
                oc.kinds[rec["alias"]] = rec.get("kind", CATALOG_KIND)
    elif (cat / "aliases.json").is_file():
        oc.aliases = json.loads((cat / "aliases.json").read_text())
        ah = cat / "alias_history.json"
        oc.history = json.loads(ah.read_text()) if ah.is_file() else {}
        for name, h in oc.aliases.items():  # default a historyless alias to its current hash
            oc.history.setdefault(name, [h])
    else:  # catalog.yaml-era: aliases embedded as alias_map / alias_history
        oc.aliases = dict(ydata.get("alias_map", {}) or {})
        oc.history = {k: list(v) for k, v in (ydata.get("alias_history", {}) or {}).items()}
        for name, h in oc.aliases.items():
            oc.history.setdefault(name, [h])

    cs = cat / "chart_specs"
    if cs.is_dir():
        for f in cs.glob("*.vl.json"):
            oc.charts[f.name[: -len(".vl.json")]] = f.read_text()
    if not oc.charts:  # catalog.yaml-era: charts as [{content_hash, spec}]
        for rec in ydata.get("charts", []) or []:
            oc.charts[rec["content_hash"]] = json.dumps(rec["spec"])

    # post_processing / stats: decomposed .py (old artifacts/ layout or native under
    # the repo), else the catalog.yaml-embedded [{name, source}] lists.
    for holder, dest in (("post_processing", oc.post_processing), ("stats", oc.stats)):
        for base in (cat / holder, cat.parent / holder):
            if base.is_dir():
                for f in base.glob("*.py"):
                    dest.setdefault(f.stem, f.read_text())
        if not dest:
            for rec in ydata.get(holder, []) or []:
                dest[rec["name"]] = rec["source"]

    # notebook cells (alias-keyed, so no hash remap): native notebook.jsonl else
    # the catalog.yaml-embedded {cells: [...]}.
    nb_jsonl = cat / "notebook.jsonl"
    if nb_jsonl.is_file():
        oc.notebook_cells = [json.loads(x) for x in nb_jsonl.read_text().splitlines() if x.strip()]
    else:
        oc.notebook_cells = list((ydata.get("notebook", {}) or {}).get("cells", []) or [])
    return oc


def parse_deps(
    expr_text: str,
    self_hash: str,
    aliases: dict[str, str],
    history: dict[str, list[str]],
    known: set[str],
    provenance: dict | None = None,
    kinds: dict[str, str] | None = None,
) -> set[str]:
    """The old hashes this recipe depends on, as the build resolved them.

    A literal hash ref is itself. An alias ref resolves to the alias's *build-time*
    target: if this entry is a revision of that alias (the documented self-chaining
    revise — ``tracked_expr_from_alias`` of one's own alias, #74), the dependency is the
    PREVIOUS revision in the alias history, not the current latest (which would be
    this very entry, a false self-cycle); otherwise it is the alias's current hash.

    A SOURCE entry reads nothing, but its versions are minted in order — ``update_and_depend``
    refuses to skip one — so it depends on the previous version of the source alias that holds it
    (``source_version_in``). That is not always ``provenance["alias"]``, the name it was imported as:
    a rename moves the history to a new name and leaves the provenance behind.
    """
    from tallyman_xorq.source_import import source_version_in  # noqa: PLC0415

    out: set[str] = set()
    if provenance:
        held = source_version_in(history, kinds, self_hash, provenance["alias"])
        if held is not None and held[1] > 1:
            out.add(history[held[0]][held[1] - 2])
    for ref in _FROM_CAT_RE.findall(expr_text):
        if ref in aliases:
            hist = history.get(ref, [])
            if self_hash in hist:
                i = hist.index(self_hash)
                dep = hist[i - 1] if i > 0 else None  # build-time parent = the prior revision
            else:
                dep = aliases[ref]  # a cross-alias chain resolves to the current latest
        else:
            dep = ref  # literal hash pin
        if dep:
            out.add(dep)
    return out & known


def toposort(
    recipes: dict[str, str],
    aliases: dict[str, str],
    history: dict[str, list[str]] | None = None,
    provenance: dict[str, dict] | None = None,
    kinds: dict[str, str] | None = None,
) -> list[str]:
    """Dependency order (parents before children) over the tracked_expr_from_alias graph."""
    history = history or {}
    provenance = provenance or {}
    known = set(recipes)
    dmap = {h: parse_deps(t, h, aliases, history, known, provenance.get(h), kinds) for h, t in recipes.items()}
    order: list[str] = []
    placed: set[str] = set()
    while len(placed) < len(recipes):
        ready = sorted(h for h in recipes if h not in placed and dmap[h] <= placed)
        if not ready:
            stuck = {h: sorted(dmap[h] - placed) for h in recipes if h not in placed}
            raise RuntimeError(f"unresolvable tracked_expr_from_alias dependencies (cycle or missing parent): {stuck}")
        order.extend(ready)
        placed.update(ready)
    return order


def rewrite_hash_refs(expr_text: str, remap: dict[str, str]) -> str:
    """Replace literal old-hash tracked_expr_from_alias refs with their rebuilt new hash.

    Only rewrites hashes already in *remap* (parents built earlier in topo order);
    alias refs are left alone (the alias is re-pointed before the child builds).
    """

    def sub(m: re.Match) -> str:
        return remap.get(m.group(0), m.group(0))

    return _HASH_RE.sub(sub, expr_text)


def replay_import(
    project: str,
    provenance: dict,
    *,
    into: tuple[str, int] | None = None,
    prompt: str | None = None,
    log=print,
) -> str:
    """Re-import a source version and return its content hash (ADR-011).

    A source entry cannot be re-exec'd: its generated recipe reads its own snapshot, which the rebuild
    has just wiped, and ``read_project_file`` is a build error outside that recipe. What it CAN do is
    the import again, which is deterministic — the entry hash is ``md5("source|<digest>|<reader>")``,
    so the same bytes under the same reader options mint the same entry, whichever file they are read
    from.

    Reads the provenance path when it still holds the imported bytes, and otherwise the clone under
    ``data/.cas``, which survives the rebuild because it lives under ``data/`` and only the catalog
    bookkeeping is wiped. Taking the clone changes the provenance path the rebuilt entry records (it
    then names the clone), which is logged; it changes nothing else.

    *into* is ``(alias, version)``: the source alias that holds the version in the old catalog and
    its place in that alias's history (``source_version_in``). The import goes under that alias,
    which after a rename is not ``provenance["alias"]`` (the name it was imported as), and is pinned
    to that version, so a replay out of order is an error instead of one version's bytes minted as
    another's number. None when no source alias holds it: it goes under the name it was imported
    as, unpinned, and the final alias write keeps that name only if the old catalog has it.
    """
    from tallyman_core.paths import data_dir  # noqa: PLC0415
    from tallyman_xorq import source_identity as si  # noqa: PLC0415
    from tallyman_xorq.ordered_copy import _spec_from_json  # noqa: PLC0415
    from tallyman_xorq.source_import import update_and_depend  # noqa: PLC0415

    imported = f"{provenance['alias']}-v{provenance['version']}"
    alias, version = into if into is not None else (provenance["alias"], None)
    if into is None:
        name = f"the version imported as {imported}, which no source alias holds"
    else:
        name = f"{alias}-v{version}" + ("" if f"{alias}-v{version}" == imported else f" (imported as {imported})")

    digest, suffix = provenance["digest"], provenance.get("suffix", "")
    original = Path(provenance["path"])
    clone = data_dir(project) / ".cas" / f"{digest}{suffix}"
    if original.is_file() and si._digest_file(original) == digest:
        src = original
    elif clone.is_file():
        src = clone
        log(f"  {name}: {original} no longer holds the imported bytes; re-importing from {clone.name}")
    else:
        raise RuntimeError(
            f"cannot rebuild {name}: {original} no longer has the imported bytes and their clone {clone} is "
            "missing, so there is nothing to import"
        )

    reader = provenance["reader"]
    schema = _spec_from_json(reader["schema"]) if reader["kind"] == "csv" else None
    options = dict(reader.get("scan_kwargs") or {}) if reader["kind"] == "csv" else {}
    out = update_and_depend(
        src,
        alias,
        version,
        project=project,
        prompt=prompt,
        schema=schema,
        **options,
    )
    return out["hash"]


def rebuild_project(project: str, *, dry_run: bool = False, log=print) -> dict[str, str]:
    """Re-exec a project's recipes into the native store. Returns old->new hash map.

    On ``dry_run`` it reads the old catalog, prints the topo plan, and returns the
    (empty) remap without writing anything.
    """
    from tallyman_core import aliases as al  # noqa: PLC0415
    from tallyman_core import (  # noqa: PLC0415
        catalog,
        catalog_state,
        set_active_project,  # noqa: PLC0415
    )
    from tallyman_core.charts import set_chart  # noqa: PLC0415
    from tallyman_core.paths import catalog_dir, ensure_project, project_dir, prompts_path  # noqa: PLC0415
    from tallyman_core.post_processing import write_post_processing  # noqa: PLC0415
    from tallyman_core.summary_stats import write_stat  # noqa: PLC0415
    from tallyman_xorq import build_and_persist  # noqa: PLC0415
    from tallyman_xorq.source_import import source_version_in  # noqa: PLC0415

    oc = read_old_catalog(project)
    if not oc.recipes:
        raise RuntimeError(f"no rebuildable entries (expr.py + manifest.json) found in project {project!r}")
    order = toposort(oc.recipes, oc.aliases, oc.history, oc.provenance, oc.kinds)
    log(
        f"project {project!r}: {len(oc.recipes)} entries ({len(oc.provenance)} imported sources), "
        f"{len(oc.aliases)} aliases, {len(oc.charts)} charts, "
        f"{len(oc.post_processing)} post-processing, {len(oc.stats)} stats"
    )
    log(f"build order (topological): {order}")

    if dry_run:
        log("dry-run: no changes written.")
        return {}

    cat = catalog_dir(project)
    shutil.rmtree(cat)  # wipe catalog bookkeeping; data/ is untouched
    ensure_project(project)
    set_active_project(project)  # recipes resolve project_path/tracked_expr_from_alias against the active project
    catalog_state.genesis(project)
    # The catalog just changed under the process-global result-expr memo; a stale
    # plan would make a tracked_expr_from_alias child re-exec against the pre-wipe parent.
    # (A fresh-process CLI run has an empty memo; an in-process rebuild does not.)
    from tallyman_xorq.result_cache import cached_result_expr  # noqa: PLC0415

    cached_result_expr.cache_clear()

    # Re-point an alias at each of its revisions AS that revision is rebuilt
    # (topo order replays history oldest-first), so a later self-chaining revise
    # resolves tracked_expr_from_alias(alias) to its build-time parent, and a cross-alias
    # child resolves to the latest-built revision. Keyed on history membership,
    # not just the final target.
    revises_at: dict[str, list[str]] = {}
    for name, hist in oc.history.items():
        for h in hist:
            revises_at.setdefault(h, []).append(name)
        if oc.aliases.get(name) not in hist and name in oc.aliases:
            revises_at.setdefault(oc.aliases[name], []).append(name)  # latest absent from history

    # The persisted recipe is portable: build.py rewrites the data path to a
    # ${TALLYMAN_PROJECT_ROOT} placeholder. Expand it back so the re-exec reads
    # the real source (recipes that read via project_path()/read_project_file() carry
    # no placeholder, so this is a no-op for them).
    root = str(project_dir(project))

    remap: dict[str, str] = {}
    for old_hash in order:
        prompts = oc.prompts.get(old_hash, [])
        first = prompts[0].get("prompt") if prompts else None
        provenance = oc.provenance.get(old_hash)
        imported_into = None  # the alias an import pointed at the entry itself
        if provenance is not None:
            held = source_version_in(oc.history, oc.kinds, old_hash, provenance["alias"])
            new_hash = replay_import(project, provenance, into=held, prompt=first, log=log)
            imported_into = held[0] if held is not None else provenance["alias"]
        else:
            recipe = oc.recipes[old_hash].replace("${TALLYMAN_PROJECT_ROOT}", root)
            recipe = rewrite_hash_refs(recipe, remap)  # fix literal parent-hash refs
            new_hash = build_and_persist(project, recipe, prompt=first).content_hash
        remap[old_hash] = new_hash
        tag = "" if new_hash == old_hash else f"  (rehashed -> {new_hash})"
        log(f"  built {old_hash}{tag}")
        for name in revises_at.get(old_hash, []):
            # The import already pointed the alias it went under. Any other alias that holds the entry
            # is pointed here, with the kind it had: a second source alias over the same bytes has to
            # be on this version before its own next version replays, pinned to follow it.
            if name != imported_into:
                al.set_alias(project, name, new_hash, kind=oc.kinds.get(name, al.CATALOG_KIND))
        if len(prompts) > 1:  # build wrote only the first; carry the rest
            p = prompts_path(project, new_hash)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("".join(json.dumps(x) + "\n" for x in prompts))

    # full alias map + history, remapped (overwrites the incremental sets above)
    al._write(
        project,
        {name: remap.get(h, h) for name, h in oc.aliases.items()},
        {name: [remap.get(x, x) for x in hs] for name, hs in oc.history.items()},
        {name: oc.kinds.get(name, al.CATALOG_KIND) for name in oc.aliases},
    )

    for old_h, spec in oc.charts.items():
        nh = remap.get(old_h)
        if nh:
            set_chart(project, nh, spec)
        else:
            log(f"  WARN chart for {old_h} dropped (no rebuilt entry)")

    for name, src in oc.post_processing.items():
        try:
            write_post_processing(project, name, src)
        except Exception as e:  # noqa: BLE001
            log(f"  WARN post-processing {name!r} failed re-validation: {e}")
    for name, src in oc.stats.items():
        try:
            write_stat(project, name, src)
        except Exception as e:  # noqa: BLE001
            log(f"  WARN stat {name!r} failed re-validation: {e}")

    # notebook cells are alias-keyed (no hash remap); re-append in order.
    from tallyman_core import notebook  # noqa: PLC0415

    for cell in oc.notebook_cells:
        alias = cell.get("alias")
        if alias:
            notebook.append(project, alias, markdown=cell.get("markdown"))

    step = catalog_state.checkpoint_catalog(project, "rebuild into native catalog format")
    log(f"  checkpoint -> step {step}")

    pointers = set(catalog_state.read_tallyman_state(project)["entry_hashes"])
    catalog.assert_catalog_consistent(project, pointers)
    log(f"  consistency OK: {len(pointers)} entries in the native store")
    return remap


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Rebuild a project's catalog into the native store by re-execing recipes.")
    ap.add_argument("project", help="project name under TALLYMAN_HOME/projects")
    ap.add_argument("--dry-run", action="store_true", help="read + print the build plan, write nothing")
    args = ap.parse_args(argv)
    try:
        rebuild_project(args.project, dry_run=args.dry_run)
    except Exception as e:  # noqa: BLE001
        print(f"rebuild failed: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
