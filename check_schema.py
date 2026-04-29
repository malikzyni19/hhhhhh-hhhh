"""
check_schema.py — Intelligence migration verifier
Run this script from the Koyeb shell or locally with DATABASE_URL set:

  DATABASE_URL=<neon-url> python3 check_schema.py

Reports whether signal_events exists and has all expected columns.
Safe: SELECT only, no writes.
"""

import os
import sys

db_url = os.environ.get("DATABASE_URL", "")
if not db_url:
    print("ERROR: DATABASE_URL not set.")
    sys.exit(1)

try:
    import sqlalchemy as sa
    engine = sa.create_engine(db_url)
except ImportError:
    print("ERROR: sqlalchemy not installed. Run: pip install sqlalchemy psycopg2-binary")
    sys.exit(1)

EXPECTED_TABLE   = "signal_events"
REQUIRED_COLUMNS = [
    "id", "signal_id", "pair", "module", "timeframe", "direction",
    "score", "zone_high", "zone_low", "detected_price", "detected_at",
    "exchange", "strategy_ver", "settings_json", "status", "source",
    "setup_type", "raw_setup", "raw_meta_json",
]
REQUIRED_INDEXES = [
    "ix_signal_events_pair",
    "ix_signal_events_module",
    "ix_signal_events_timeframe",
    "ix_signal_events_status",
    "ix_signal_events_detected_at",
    "ix_signal_events_signal_id_unique",
]

with engine.connect() as conn:
    # ── 1. Table exists? ──────────────────────────────────────
    tbl_row = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=:t"
    ), {"t": EXPECTED_TABLE}).fetchone()

    if not tbl_row:
        print(f"[FAIL] Table '{EXPECTED_TABLE}' does not exist.")
        sys.exit(1)
    print(f"[OK]   Table '{EXPECTED_TABLE}' exists.")

    # ── 2. Column check ───────────────────────────────────────
    col_rows = conn.execute(sa.text(
        "SELECT column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_name=:t ORDER BY ordinal_position"
    ), {"t": EXPECTED_TABLE}).fetchall()

    found_cols = {r[0]: (r[1], r[2]) for r in col_rows}
    print(f"\nColumns in {EXPECTED_TABLE} ({len(found_cols)} total):")
    for col in REQUIRED_COLUMNS:
        if col in found_cols:
            dtype, nullable = found_cols[col]
            print(f"  [OK]   {col:<22} {dtype} nullable={nullable}")
        else:
            print(f"  [MISS] {col:<22} ← MISSING")

    missing_cols = [c for c in REQUIRED_COLUMNS if c not in found_cols]
    new_col_check = all(c in found_cols for c in ("setup_type","raw_setup","raw_meta_json"))

    # ── 3. Index check ────────────────────────────────────────
    idx_rows = conn.execute(sa.text(
        "SELECT indexname FROM pg_indexes "
        "WHERE tablename=:t ORDER BY indexname"
    ), {"t": EXPECTED_TABLE}).fetchall()

    found_idxs = {r[0] for r in idx_rows}
    print(f"\nIndexes on {EXPECTED_TABLE} ({len(found_idxs)} total):")
    for idx in REQUIRED_INDEXES:
        if idx in found_idxs:
            print(f"  [OK]   {idx}")
        else:
            print(f"  [MISS] {idx} ← MISSING")

    missing_idxs = [i for i in REQUIRED_INDEXES if i not in found_idxs]

    # ── 4. Row count (informational) ──────────────────────────
    row_count = conn.execute(sa.text(
        f"SELECT COUNT(*) FROM {EXPECTED_TABLE}"
    )).scalar()
    print(f"\nRow count: {row_count}")

    # ── 5. Summary ────────────────────────────────────────────
    print("\n" + "=" * 52)
    if not missing_cols and not missing_idxs:
        print("RESULT: Migration fully applied — all columns and indexes present.")
    else:
        if missing_cols:
            print(f"RESULT: Missing columns: {missing_cols}")
        if missing_idxs:
            print(f"RESULT: Missing indexes:  {missing_idxs}")
    print(f"  setup_type / raw_setup / raw_meta_json present: {new_col_check}")
    print("=" * 52)
