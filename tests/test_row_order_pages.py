"""ADR-008 D5 (plans/ADR-008-row-order-of-reads.md): every page request orders by ``__row_order``.

A page is a function of ``(content_hash, sort, offset, limit)``. Today ``/api/data`` serves
``cached_result_expr(...).limit(limit, offset=offset)`` with no ``ORDER BY``, and above DataFusion's scan-split
threshold (10,485,760 bytes) an unordered ``LIMIT/OFFSET`` returns different rows on each request, so paging through a
large entry repeats some rows and never shows others.

Asserting that eight identical requests agree with each other is not enough: one rejected engine setting returned the
same wrong page eight times out of eight (ADR-008 D5). Each response is compared with the rows that sit at those
positions in the file, computed independently with pandas from the source.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from tallyman_xorq.build import build_and_persist
from tallyman_xorq.source_import import update_and_depend
from tests.big_parquet import write_big_parquet

ROW_ORDER = "__row_order"
OFFSET, LIMIT, REQUESTS = 1_000_000, 50, 8
BIG_SRC = "big_src"

_PRELUDE = "from tallyman_xorq.io import tracked_expr_from_alias\n"


@pytest.fixture(scope="module")
def big_source(tmp_path_factory) -> Path:
    """A 26 MB parquet file, written once for the module: id (the file position), g (200 values), v (float)."""
    return write_big_parquet(tmp_path_factory.mktemp("big_source") / "big.parquet")


@pytest.fixture(scope="module")
def big_frame(big_source):
    return pq.read_table(big_source).to_pandas()


@pytest.fixture
def big_src(project, big_source) -> str:
    """The 26 MB file imported as a source alias (ADR-011 D1), and the alias name a recipe reads."""
    update_and_depend(big_source, BIG_SRC, project=project)
    return BIG_SRC


def _recipe(project: str, body: str) -> str:
    return f"{_PRELUDE}t = tracked_expr_from_alias({BIG_SRC!r}, project={project!r})\nexpr = {body}\n"


def _requests(client: TestClient, project: str, content_hash: str) -> list[list[dict]]:
    """Eight identical page requests at a deep offset; each response must carry the row-order column."""
    pages = []
    for i in range(REQUESTS):
        r = client.get(f"/{project}/api/data/{content_hash}?offset={OFFSET}&limit={LIMIT}")
        assert r.status_code == 200, r.text
        rows = r.json()["data"]
        assert len(rows) == LIMIT
        assert ROW_ORDER in rows[0], f"request {i}: a row must end in {ROW_ORDER}, got {list(rows[0])}"
        pages.append(rows)
    return pages


@pytest.mark.parametrize(
    ("body", "max_g"),
    [
        pytest.param("t.order_by('id')", None, id="worthy entry"),
        pytest.param("t.filter(t.g >= 0)", None, id="cheap entry that keeps every row"),
        pytest.param("t.filter(t.g < 150)", 150, id="cheap entry that drops rows"),
    ],
)
def test_a_deep_page_is_exactly_the_rows_at_those_positions(
    fresh_companion_app, project, big_src, big_frame, body, max_g
):
    """ADR-008 D5: eight identical requests each return the rows at positions offset..offset+limit-1.

    ``id`` is the file position, so for the entries that keep every row ``__row_order`` equals ``id``. A cheap entry
    that drops rows inherits its parent's positions, which then have gaps: its page is the surviving rows at
    ``OFFSET`` in ``__row_order`` order, and ``__row_order`` still holds each row's position in the source.
    """
    h = build_and_persist(project, _recipe(project, body)).content_hash
    kept = big_frame if max_g is None else big_frame[big_frame["g"] < max_g]
    expected = kept["id"].iloc[OFFSET : OFFSET + LIMIT].tolist()
    assert len(expected) == LIMIT, "the fixture must have rows at the requested offset"
    if max_g is None:
        assert expected == list(range(OFFSET, OFFSET + LIMIT))

    for i, rows in enumerate(_requests(TestClient(fresh_companion_app), project, h)):
        assert [r["id"] for r in rows] == expected, f"request {i} returned other rows than positions {OFFSET}.."
        assert [r[ROW_ORDER] for r in rows] == expected, f"request {i}: __row_order must be the position in the file"
