"""Every SSE kind the companion publishes has a listener in the SPA (#235).

The companion sends each event with its ``kind`` as the SSE event name (the ``/{project}/api/sse`` route in
``tallyman_companion/app.py``), and an ``EventSource`` hands a named event only to the listeners registered for that
name. A kind with no ``addEventListener`` in ``packages/app/src/SSEContext.tsx`` is dropped in the browser, and the
page that should have refetched stays stale until a reload.

The kinds come from the Python source: the companion's ``publish(...)`` calls, the MCP server's ``_notify(...)``
calls (the companion republishes what ``/internal/notify`` receives), and any other post to ``/internal/notify``
(``tallyman reset-to``). The test reads the source with ``ast`` and needs no running server.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "src"
_SSE_CONTEXT = _REPO / "packages" / "app" / "src" / "SSEContext.tsx"

# Kinds the companion publishes that deliberately have no listener in SSEContext.tsx, each with the reason. Empty:
# every kind published today refetches something. Add a kind here only when no page shows what it changes.
NO_LISTENER_NEEDED: dict[str, str] = {}

# The two sites that forward a kind they did not choose, so their kind is not a literal. The kinds they carry are
# collected at their callers: ``_notify``'s callers name theirs, and ``notify`` republishes what they posted.
_FORWARDERS = {
    ("tallyman_mcp/server.py", "_notify"),  # posts _notify's `kind` argument to /internal/notify
    ("tallyman_companion/app.py", "notify"),  # the /internal/notify route: publish(event) of the posted payload
}


def _kind_of_dict(node: ast.AST) -> str | None:
    """The literal ``"kind"`` value of a dict display, or None."""
    if not isinstance(node, ast.Dict):
        return None
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and key.value == "kind":
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value
    return None


def _returned_kinds(tree: ast.Module) -> dict[str, str]:
    """Module-level functions that return a dict with a literal kind, e.g. ``_recalc_sse_event`` -> ``recalc``."""
    out = {}
    for fn in tree.body:
        if isinstance(fn, ast.FunctionDef):
            for node in ast.walk(fn):
                if isinstance(node, ast.Return) and (kind := _kind_of_dict(node.value)):
                    out[fn.name] = kind
    return out


def _callee(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _posts_to_internal_notify(call: ast.Call) -> bool:
    for arg in call.args[:1]:
        for node in ast.walk(arg):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and "/internal/notify" in node.value:
                return True
    return False


def _collect() -> tuple[dict[str, list[str]], list[str]]:
    """Return ``{kind: [where it is sent]}`` and the sites whose kind could not be read."""
    published: dict[str, list[str]] = {}
    unresolved: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        rel = path.relative_to(_SRC).as_posix()
        tree = ast.parse(path.read_text(), filename=str(path))
        helpers = _returned_kinds(tree)
        # Map every call to its innermost enclosing function, to name forwarders and report sites.
        enclosing: dict[ast.AST, str] = {}
        for fn in ast.walk(tree):
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for node in ast.walk(fn):
                    enclosing[node] = fn.name  # walk order is outer first, so inner functions overwrite
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call):
                continue
            name = _callee(call)
            if name == "publish" and call.args:
                arg = call.args[0]
                kind = _kind_of_dict(arg)
                if kind is None and isinstance(arg, ast.Call) and _callee(arg) in helpers:
                    kind = helpers[_callee(arg)]
            elif name == "_notify" and call.args:
                arg = call.args[0]
                kind = arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str) else None
            elif _posts_to_internal_notify(call):
                json_kw = next((k.value for k in call.keywords if k.arg == "json"), None)
                kind = _kind_of_dict(json_kw) if json_kw is not None else None
            else:
                continue
            where = f"{rel}:{call.lineno}"
            if kind is not None:
                published.setdefault(kind, []).append(where)
            elif (rel, enclosing.get(call)) not in _FORWARDERS:
                unresolved.append(f"{where} in {enclosing.get(call)}()")
    for sites in published.values():
        sites.sort(key=lambda s: (s.split(":")[0], int(s.split(":")[1])))
    return published, unresolved


def _listened_kinds() -> set[str]:
    """Event names SSEContext.tsx registers with ``addEventListener``, ignoring commented-out lines."""
    text = re.sub(r"/\*.*?\*/", "", _SSE_CONTEXT.read_text(), flags=re.S)
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("//"))
    return set(re.findall(r"""\.addEventListener\(\s*["']([A-Za-z0-9_]+)["']""", code))


def test_every_publish_site_names_a_literal_kind():
    """A site whose kind the collector cannot read would slip past the listener check below."""
    published, unresolved = _collect()
    assert not unresolved, (
        "these sites publish a kind that is not a string literal, so the listener check cannot see it; "
        f"use a literal kind or add the site to _FORWARDERS with the reason: {unresolved}"
    )
    # The collector found the surfaces it reads: the companion's publish, the MCP _notify and the CLI's notify post.
    assert {"recalc", "project_switched", "new_entry", "project_reset"} <= published.keys(), sorted(published)


def test_every_published_kind_has_an_sse_listener():
    published, _ = _collect()
    listened = _listened_kinds()
    missing = {k: v for k, v in sorted(published.items()) if k not in listened and k not in NO_LISTENER_NEEDED}
    assert not missing, (
        "SSEContext.tsx has no addEventListener for these kinds, so the browser drops them "
        f"(kind: where it is sent): {missing}"
    )


def test_allowlist_names_only_published_kinds_without_a_listener():
    published, _ = _collect()
    listened = _listened_kinds()
    stale = {k for k in NO_LISTENER_NEEDED if k not in published or k in listened}
    assert not stale, f"NO_LISTENER_NEEDED lists kinds that are not published or now have a listener: {stale}"
