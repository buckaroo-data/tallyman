"""ADR-008 evidence (D2, D7) and #168: is a CSV root's ordered intermediate fixed under a content hash?

``tallyman_read_csv`` reads a CSV through an ordered parquet intermediate. That file's key is
``md5(absolute path | schema | reader options)``, not content, and it is overwritten in place when the CSV's mtime
changes. The script edits a CSV, re-runs the identical recipe, and reports the build hash before and after, and what
the first version's frozen build returns afterwards, for three shapes of the root expression:

1. today: a read of the intermediate with a trailing ``order_by("original_row_order")``;
2. ADR-008 D7 without the fix for #168: a plain read of the same intermediate (no sort, so no snapshot, no digest);
3. the fix for #168: digest the CSV, clone it to a content-named path, and key the intermediate on the clone.

Raw xorq builds, no tallyman build machinery, so case 1 reports the hash only: in a real build the root is worthy
because of its Sort, and its baked snapshot goes on serving the old rows. An unchanged hash also means
``build_and_persist`` returns the existing entry, so no new version is created at all.

    uv run python scripts/spike_csv_source_identity.py
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

HOME = Path(tempfile.mkdtemp(prefix="spike_csv_identity_"))
os.environ["TALLYMAN_HOME"] = str(HOME)  # the ordered intermediates live under it; set before tallyman is imported

import xorq.api as xo  # noqa: E402
from xorq.ibis_yaml.compiler import build_expr, load_expr  # noqa: E402

from tallyman_xorq import source_identity as si  # noqa: E402
from tallyman_xorq.io import _ordered_csv_parquet, tallyman_read_csv  # noqa: E402

BUILDS = HOME / "builds"
CAS = HOME / "cas"
ORIGINAL = "id,amount\n1,10\n2,20\n3,30\n"
EDITED = "id,amount\n1,10\n2,999\n3,30\n4,40\n"


def today(csv: Path):
    return tallyman_read_csv(str(csv))


def plain_read(csv: Path):
    return xo.deferred_read_parquet(str(_ordered_csv_parquet(str(csv), None, {})))


def keyed_on_clone(csv: Path):
    clone = CAS / f"{si._digest_file(csv)}{csv.suffix}"
    if not clone.exists():
        clone.write_bytes(csv.read_bytes())  # tallyman's ensure_cas_path makes a copy-on-write clone here
    return xo.deferred_read_parquet(str(_ordered_csv_parquet(str(clone), None, {})))


def main() -> None:
    CAS.mkdir()
    csv = HOME / "sales.csv"
    cases = (
        ("1. today (trailing order_by kept)", today, False),
        ("2. ADR-008 D7 without the #168 fix", plain_read, True),
        ("3. keyed on a content-named clone", keyed_on_clone, True),
    )
    for label, root, reread in cases:
        csv.write_text(ORIGINAL)
        v1 = Path(build_expr(root(csv), builds_dir=BUILDS))
        time.sleep(0.05)  # so the edit changes the CSV's mtime
        csv.write_text(EDITED)
        v2 = Path(build_expr(root(csv), builds_dir=BUILDS))
        print(f"{label}: hash before the edit {v1.name}, after {v2.name}, same hash: {v1.name == v2.name}")
        if reread:
            rows = load_expr(v1).execute().amount.tolist()
            print(f"   V1's frozen build, re-read after the edit: {rows} (built from {[10, 20, 30]})")


if __name__ == "__main__":
    main()
