"""Decide whether a catalog entry is stale relative to its recorded inputs (#89).

Everything the consumer needs is already captured at build: ``manifest.parents``, the resolved
``tracked_expr_from_alias`` edges and their read-intent. This is the read-only half — no recompute — of
the reactive consumer: compare the recorded inputs against the current world and report what moved.

**One axis** (ADR-011 D6). An entry is stale when a ``follow=True`` parent (an alias argument) resolves
to a different hash than the one recorded at build, and for no other reason. A ``follow=False`` parent
(a version pin) is never stale — it asked for that exact revision.

There used to be a second axis: a recorded ``(rel_path, digest)`` no longer matching the file on disk.
It asked a question the system could not answer, because ``manifest.sources`` did not record *how* the
entry came to depend on the file, so a child pinned to its parent by hash read as stale forever. With
every raw input an entry of its own (ADR-011 D1), the question is the first axis: a re-import advances
the source alias, and everything following it goes stale the way it does for any other parent. The scan
therefore touches no file at all — it reads ``aliases.jsonl`` and the manifests.

``result_digest`` is deliberately *not* a staleness input: a recompute-differs entry is
*nondeterministic*, not stale, and recompute cannot make it fresh — that is the #83/#121 path, surfaced
by the recompute action, not this scan.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tallyman_core import aliases
from tallyman_xorq.build import list_entries
from tallyman_xorq.dependents import descendant_cone, parents_of


@dataclass(frozen=True)
class StaleReason:
    axis: str  # always "alias"; the field is kept so a reason reads the same in the API and the UI
    ref: str  # the alias name
    was: str  # the parent hash recorded at build
    now: str | None  # the alias head now


@dataclass
class StaleVerdict:
    content_hash: str
    stale: bool  # actionably stale: a CURRENT alias head whose own recorded inputs moved
    reasons: list[StaleReason] = field(default_factory=list)
    unknown_axes: list[str] = field(default_factory=list)  # axes that can't be evaluated
    transitively_stale: bool = False  # set by scan(): a directly-stale ancestor exists
    live: bool = True  # #154: content_hash is a current alias head (set by scan)


def entry_staleness(project: str, content_hash: str) -> StaleVerdict:
    """Whether *content_hash*'s own recorded inputs have moved (read-only).

    Reads through the ``dependents`` manifest reader (``parents_of``) rather than the raw manifest, so
    that module stays the single seam over the recorded DAG. Nothing here opens a data file.
    """
    reasons: list[StaleReason] = []
    unknown: list[str] = []

    for parent in parents_of(project, content_hash):
        if not parent.follow:
            continue  # a version pin is never stale
        head = aliases.get_alias(project, parent.ref)
        if head is None:
            unknown.append(f"alias:{parent.ref}")  # alias gone — deletion is out of scope here
        elif head != parent.hash:
            reasons.append(StaleReason(axis="alias", ref=parent.ref, was=parent.hash, now=head))

    return StaleVerdict(
        content_hash=content_hash,
        stale=bool(reasons),
        reasons=reasons,
        unknown_axes=unknown,
    )


def verify_sweep(project: str) -> dict:
    """Opt-in corpus verification (ADR-006 D7): do materialized snapshots still match their recorded ``result_digest``?

    For every entry that recorded a digest, ``verify_result_faithful`` compares the content digest of the snapshot on
    disk with the recorded one. It READS AND NEVER WRITES (ADR-007 D5, D12): a snapshot that is missing stays missing,
    because a sweep that rewrote every deleted file would undo the Cache page's delete, and every file
    ``ensure_materialized`` writes is verified before it is served, so an absent file is checked at the moment it next
    exists. Returns ``{"results": {hash: bool|None}, "unfaithful": [...], "absent": [...], "errors": {hash: message}}``:
    ``None`` in ``results`` means nothing to check (the snapshot is absent), ``absent`` lists those hashes, and
    ``errors`` carries entries whose check failed (reported per-entry so one broken entry can't abort a corpus sweep).
    """
    from tallyman_xorq.materialize import snapshot_path
    from tallyman_xorq.result_cache import verify_result_faithful

    results: dict[str, bool | None] = {}
    absent: list[str] = []
    errors: dict[str, str] = {}
    for entry in list_entries(project):
        if not entry.get("result_digest"):
            continue
        h = entry["content_hash"]
        try:
            results[h] = verify_result_faithful(project, h)
            if not snapshot_path(project, h).exists():
                absent.append(h)
        except Exception as exc:
            errors[h] = str(exc)
    return {
        "results": results,
        "unfaithful": sorted(h for h, ok in results.items() if ok is False),
        "absent": sorted(absent),
        "errors": errors,
    }


def scan(project: str) -> dict[str, StaleVerdict]:
    """Actionable staleness for every live entry, tagging direct vs transitive.

    An entry is **directly** stale only when it is a *current alias head* whose own
    recorded input moved (#154). A superseded historical version pins alias heads
    that have since advanced, so its inputs read as moved forever — but it heads no
    alias, so recomputing it re-points nothing (and, for a recipe shape no current
    head uses, manufactures a junk entry), and it is not an invariant break to flag.
    Such a non-head is marked ``live=False`` and forced ``stale=False`` here (its
    ``reasons`` are kept for forensics). A **transitively** stale entry is a (not
    directly stale) descendant of a directly-stale head in the dependency cone.

    ``entry_staleness`` stays the raw per-entry primitive — did *this* entry's inputs
    move, regardless of headship; this function layers the head-reachability gate
    that makes ``stale`` mean *actionable*. The verdict dict still keys **every**
    entry (heads and husks alike), because the recalc replay indexes cone members
    that are not themselves heads (a hash-pinned child).
    """
    heads = set(aliases.load_aliases(project).values())  # current alias heads, read once
    verdicts = {
        entry["content_hash"]: entry_staleness(project, entry["content_hash"]) for entry in list_entries(project)
    }
    for content_hash, verdict in verdicts.items():
        verdict.live = content_hash in heads
        if not verdict.live:
            verdict.stale = False  # a non-head is never actionably stale (#154)
    directly_stale = [h for h, v in verdicts.items() if v.stale]
    if directly_stale:
        for content_hash in descendant_cone(project, directly_stale):
            verdict = verdicts.get(content_hash)
            if verdict is not None and verdict.live and not verdict.stale:  # a husk is dead history, not carried (#154)
                verdict.transitively_stale = True
    return verdicts
