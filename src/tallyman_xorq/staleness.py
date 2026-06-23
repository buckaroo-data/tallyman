"""Decide whether a catalog entry is stale relative to its recorded inputs (#89).

Everything the consumer needs is already captured at build: ``manifest.parents``
(the resolved ``tracked_expr_from_alias`` edges and their read-intent) and
``manifest.sources`` (each source's content digest). Nothing reads it. This is
the read-only half — no recompute — of the reactive consumer: compare the
recorded inputs against the current world and report what moved.

Two independent axes, both computable from already-recorded data:

1. **Upstream advanced.** A ``follow=True`` parent (an alias argument) is stale
   when its alias head now resolves to a different hash than the one recorded at
   build. A ``follow=False`` parent (a literal-hash pin) is never stale here — it
   asked for that exact revision.
2. **Source drifted.** A recorded ``(rel_path, digest)`` is stale when the file
   on disk no longer digests to the recorded value. Under ``off`` identity mode
   the manifest records no digests, so this axis is reported ``unknown`` — never
   silently ``fresh``.

``result_digest`` is deliberately *not* a staleness input: a recompute-differs
entry is *nondeterministic*, not stale, and recompute cannot make it fresh — that
is the #83/#121 path, surfaced by the recompute action, not this scan.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass, field

from tallyman_core import aliases
from tallyman_core.manifest import read_manifest
from tallyman_core.paths import entry_dir
from tallyman_xorq import source_identity
from tallyman_xorq.build import list_entries
from tallyman_xorq.dependents import descendant_cone
from tallyman_xorq.io import ProjectDataNotFound, project_path

# digest_for memoizes on (mtime_ns, size, inode); a same-stat in-place content
# swap would otherwise serve the cached digest and the source axis would
# false-negative. Force a faithful read for the staleness check (the env var is
# the documented bypass, source_identity.py).
_REHASH_ENV = "TALLYMAN_SOURCE_REHASH"


@dataclass(frozen=True)
class StaleReason:
    axis: str  # "alias" | "source"
    ref: str  # the alias name or the source's rel_path
    was: str  # the value recorded at build (parent hash / source digest)
    now: str | None  # the current value (alias head / current digest)


@dataclass
class StaleVerdict:
    content_hash: str
    stale: bool  # directly stale: this entry's own recorded inputs moved
    reasons: list[StaleReason] = field(default_factory=list)
    unknown_axes: list[str] = field(default_factory=list)  # axes that can't be evaluated
    transitively_stale: bool = False  # set by scan(): a directly-stale ancestor exists


@contextmanager
def _force_source_rehash():
    prev = os.environ.get(_REHASH_ENV)
    os.environ[_REHASH_ENV] = "1"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(_REHASH_ENV, None)
        else:
            os.environ[_REHASH_ENV] = prev


def entry_staleness(project: str, content_hash: str) -> StaleVerdict:
    """Whether *content_hash*'s own recorded inputs have moved (read-only)."""
    manifest = read_manifest(entry_dir(project, content_hash))
    reasons: list[StaleReason] = []
    unknown: list[str] = []

    # Axis 1: a followed alias advanced past the recorded head.
    for parent in manifest.parents or []:
        if not parent.follow:
            continue  # a hash pin is never stale on this axis
        head = aliases.get_alias(project, parent.ref)
        if head is None:
            unknown.append(f"alias:{parent.ref}")  # alias gone — deletion is out of scope here
        elif head != parent.hash:
            reasons.append(StaleReason(axis="alias", ref=parent.ref, was=parent.hash, now=head))

    # Axis 2: a recorded source drifted on disk.
    if manifest.sources is None:
        unknown.append("source")  # built under mode=off; the axis is unavailable
    else:
        with _force_source_rehash():
            for rel_path, recorded in manifest.sources.items():
                try:
                    current = source_identity.digest_for(project, project_path(rel_path, project))
                except ProjectDataNotFound:
                    unknown.append(f"source:{rel_path}")  # source file removed
                    continue
                if current != recorded:
                    reasons.append(StaleReason(axis="source", ref=rel_path, was=recorded, now=current))

    return StaleVerdict(
        content_hash=content_hash,
        stale=bool(reasons),
        reasons=reasons,
        unknown_axes=unknown,
    )


def scan(project: str) -> dict[str, StaleVerdict]:
    """Staleness for every live entry, tagging direct vs transitive staleness.

    A **directly** stale entry has its own recorded input moved. A
    **transitively** stale entry is a (not directly stale) descendant of one in
    the dependency cone. The recalc surfaces use this to flag both while
    distinguishing the entry the user must act on from the ones a cascade carries.
    """
    verdicts = {
        entry["content_hash"]: entry_staleness(project, entry["content_hash"]) for entry in list_entries(project)
    }
    directly_stale = [h for h, v in verdicts.items() if v.stale]
    if directly_stale:
        for content_hash in descendant_cone(project, directly_stale):
            verdict = verdicts.get(content_hash)
            if verdict is not None and not verdict.stale:
                verdict.transitively_stale = True
    return verdicts
