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

* recipes that chain by ALIAS (``from_catalog("citibike")``) survive untouched —
  the alias is re-pointed at the rebuilt parent before the child is built;
* recipes that chain by literal HASH (``from_catalog("763193211746")``) are
  rewritten in place (old hash -> new hash) before the child builds, which is why
  the build order is a topological sort of the from_catalog dependency graph;
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
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from dataclasses import dataclass, field

# 12-char lowercase-hex content hash (the entry-dir / pointer naming).
_HASH_RE = re.compile(r"\b[0-9a-f]{12}\b")
# from_catalog("ref") / from_catalog('ref', ...) — first positional arg only.
_FROM_CAT_RE = re.compile(r"from_catalog\(\s*['\"]([^'\"]+)['\"]")


@dataclass
class OldCatalog:
    """Everything the rebuild needs, read out of the old catalog before it is wiped."""

    recipes: dict[str, str] = field(default_factory=dict)  # old_hash -> expr.py text
    aliases: dict[str, str] = field(default_factory=dict)  # alias -> current old_hash
    history: dict[str, list[str]] = field(default_factory=dict)  # alias -> [old_hash, ...]
    charts: dict[str, str] = field(default_factory=dict)  # old_hash -> vega-lite spec text
    prompts: dict[str, list[dict]] = field(default_factory=dict)  # old_hash -> [{prompt, at}, ...]
    post_processing: dict[str, str] = field(default_factory=dict)  # name -> source
    stats: dict[str, str] = field(default_factory=dict)  # name -> source


def read_old_catalog(project: str) -> OldCatalog:
    """Read the pre-rebuild catalog. Tolerates both the native (``aliases.jsonl``)
    and the old (``aliases.json`` + ``alias_history.json``) alias stores."""
    from tallyman_core.paths import catalog_dir  # noqa: PLC0415

    cat = catalog_dir(project)
    oc = OldCatalog()

    entries = cat / "entries"
    if entries.is_dir():
        for d in sorted(entries.iterdir()):
            if d.is_dir() and (d / "expr.py").is_file() and (d / "manifest.json").is_file():
                oc.recipes[d.name] = (d / "expr.py").read_text()
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
    else:
        aj = cat / "aliases.json"
        if aj.is_file():
            oc.aliases = json.loads(aj.read_text())
        ah = cat / "alias_history.json"
        if ah.is_file():
            oc.history = json.loads(ah.read_text())
        # an alias with no recorded history defaults to its single current hash
        for name, h in oc.aliases.items():
            oc.history.setdefault(name, [h])

    cs = cat / "chart_specs"
    if cs.is_dir():
        for f in cs.glob("*.vl.json"):
            oc.charts[f.name[: -len(".vl.json")]] = f.read_text()

    # post_processing / stats: old layout under artifacts/, native under the repo
    for holder, dest in (("post_processing", oc.post_processing), ("stats", oc.stats)):
        for base in (cat / holder, cat.parent / holder):
            if base.is_dir():
                for f in base.glob("*.py"):
                    dest.setdefault(f.stem, f.read_text())
    return oc


def parse_deps(expr_text: str, aliases: dict[str, str], known: set[str]) -> set[str]:
    """The set of old hashes this recipe depends on (alias refs resolved to their
    current hash, literal hash refs taken as-is), restricted to entries we have."""
    out: set[str] = set()
    for ref in _FROM_CAT_RE.findall(expr_text):
        out.add(aliases.get(ref, ref))
    return out & known


def toposort(recipes: dict[str, str], aliases: dict[str, str]) -> list[str]:
    """Dependency order (parents before children) over the from_catalog graph."""
    known = set(recipes)
    dmap = {h: parse_deps(t, aliases, known) for h, t in recipes.items()}
    order: list[str] = []
    placed: set[str] = set()
    while len(placed) < len(recipes):
        ready = sorted(h for h in recipes if h not in placed and dmap[h] <= placed)
        if not ready:
            stuck = {h: sorted(dmap[h] - placed) for h in recipes if h not in placed}
            raise RuntimeError(f"unresolvable from_catalog dependencies (cycle or missing parent): {stuck}")
        order.extend(ready)
        placed.update(ready)
    return order


def rewrite_hash_refs(expr_text: str, remap: dict[str, str]) -> str:
    """Replace literal old-hash from_catalog refs with their rebuilt new hash.

    Only rewrites hashes already in *remap* (parents built earlier in topo order);
    alias refs are left alone (the alias is re-pointed before the child builds).
    """

    def sub(m: re.Match) -> str:
        return remap.get(m.group(0), m.group(0))

    return _HASH_RE.sub(sub, expr_text)


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

    oc = read_old_catalog(project)
    if not oc.recipes:
        raise RuntimeError(f"no rebuildable entries (expr.py + manifest.json) found in project {project!r}")
    order = toposort(oc.recipes, oc.aliases)
    log(
        f"project {project!r}: {len(oc.recipes)} entries, {len(oc.aliases)} aliases, "
        f"{len(oc.charts)} charts, {len(oc.post_processing)} post-processing, {len(oc.stats)} stats"
    )
    log(f"build order (topological): {order}")

    if dry_run:
        log("dry-run: no changes written.")
        return {}

    cat = catalog_dir(project)
    shutil.rmtree(cat)  # wipe catalog bookkeeping; data/ is untouched
    ensure_project(project)
    set_active_project(project)  # recipes resolve project_path/from_catalog against the active project
    catalog_state.genesis(project)

    # alias whose CURRENT target is this old hash -> must be set right after it
    # builds so a child's from_catalog(alias) resolves at build time.
    cur_alias_for: dict[str, list[str]] = {}
    for name, h in oc.aliases.items():
        cur_alias_for.setdefault(h, []).append(name)

    # The persisted recipe is portable: build.py rewrites the data path to a
    # ${TALLYMAN_PROJECT_ROOT} placeholder. Expand it back so the re-exec reads
    # the real source (recipes that read via project_path()/from_project() carry
    # no placeholder, so this is a no-op for them).
    root = str(project_dir(project))

    remap: dict[str, str] = {}
    for old_hash in order:
        recipe = oc.recipes[old_hash].replace("${TALLYMAN_PROJECT_ROOT}", root)
        recipe = rewrite_hash_refs(recipe, remap)  # fix literal parent-hash refs
        prompts = oc.prompts.get(old_hash, [])
        first = prompts[0].get("prompt") if prompts else None
        res = build_and_persist(project, recipe, prompt=first)
        remap[old_hash] = res.content_hash
        tag = "" if res.content_hash == old_hash else f"  (rehashed -> {res.content_hash})"
        log(f"  built {old_hash}{tag}")
        for name in cur_alias_for.get(old_hash, []):
            al.set_alias(project, name, res.content_hash)
        if len(prompts) > 1:  # build wrote only the first; carry the rest
            p = prompts_path(project, res.content_hash)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("".join(json.dumps(x) + "\n" for x in prompts))

    # full alias map + history, remapped (overwrites the incremental sets above)
    al._write(
        project,
        {name: remap.get(h, h) for name, h in oc.aliases.items()},
        {name: [remap.get(x, x) for x in hs] for name, hs in oc.history.items()},
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
