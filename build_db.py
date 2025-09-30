#!/usr/bin/env python3
"""Build a SQLite database from the BAL_Ravens_2024.csv file.

Usage:
  python build_db.py                # builds ravens_2024.db (default table name ravens_2024)
  python build_db.py --db file.db    # custom DB filename
  python build_db.py --table plays   # custom table name
  python build_db.py --if-exists replace|append|fail

Notes:
  - Adds an auto-increment integer primary key column named 'row_id'.
  - Uses pandas for fast CSV ingestion.
  - Infers dtypes; extremely wide numeric columns may be stored as TEXT if mixed.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).parent
DEFAULT_CSV = ROOT / "BAL_Ravens_2024.csv"
DEFAULT_DB = ROOT / "ravens_2024.db"
DEFAULT_TABLE = "ravens_2024"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build SQLite DB from BAL_Ravens_2024.csv")
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Path to source CSV file")
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite database output path")
    p.add_argument("--table", default=DEFAULT_TABLE, help="Destination table name")
    p.add_argument("--if-exists", dest="if_exists", choices=["fail", "replace", "append"], default="replace", help="Behavior if table already exists (pandas to_sql parameter)")
    p.add_argument("--chunksize", type=int, default=None, help="Optional chunksize for incremental write (auto decides if not set)")
    return p.parse_args(argv)


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    # Let pandas infer; specify low_memory=False due to very wide columns.
    df = pd.read_csv(path, low_memory=False)
    # Remove unnamed index-like column if present.
    for col in list(df.columns):
        if col.lower().startswith("unnamed"):
            # Keep only if it has non-sequential values; else drop.
            try:
                as_int = pd.to_numeric(df[col], errors="coerce")
                if as_int.notna().all():
                    # Check if sequential 0..n-1
                    if (as_int.dropna().reset_index(drop=True) == range(len(df))).all():
                        df = df.drop(columns=[col])
                        continue
            except Exception:
                pass
            # If not dropped above, just keep it with a normalized name.
    return df


def add_row_id(df: pd.DataFrame) -> pd.DataFrame:
    if 'row_id' in df.columns:
        return df
    df = df.copy()
    df.insert(0, 'row_id', range(1, len(df) + 1))
    return df


def write_sqlite(df: pd.DataFrame, db_path: Path, table: str, if_exists: str, chunksize: int | None = None) -> int:
    # Ensure parent dirs
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Use sqlite3 connection so we can add PK after write if needed.
    with sqlite3.connect(db_path) as conn:
        # If replacing, we can drop existing table explicitly to control schema.
        if if_exists == 'replace':
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            except sqlite3.Error:
                pass
        # Write without the row_id first then add PK? Simpler: include row_id and set as PRIMARY KEY afterwards if append not used.
        df.to_sql(table, conn, if_exists='append' if if_exists == 'append' else 'replace', index=False, chunksize=chunksize)
        # Ensure primary key constraint (only safe if we replaced table)
        if if_exists != 'append':
            # SQLite doesn't allow altering primary key directly; recreate if needed.
            # We'll check schema; if row_id not primary key, rebuild.
            cur = conn.execute(f"PRAGMA table_info({table})")
            cols = cur.fetchall()
            # cols: cid, name, type, notnull, dflt_value, pk
            pk_status = {c[1]: c[5] for c in cols}
            if pk_status.get('row_id', 0) == 0:
                # Rebuild table with primary key.
                col_defs = []
                for c in cols:
                    name = c[1]
                    ctype = c[2] or 'TEXT'
                    if name == 'row_id':
                        col_defs.append('row_id INTEGER PRIMARY KEY')
                    else:
                        col_defs.append(f'"{name}" {ctype}')
                tmp_table = f"{table}__tmp_rebuild" 
                conn.execute(f"CREATE TABLE {tmp_table} ({', '.join(col_defs)})")
                col_names = [c[1] for c in cols]
                col_list = ', '.join(f'"{c}"' for c in col_names)
                conn.execute(f"INSERT INTO {tmp_table} ({col_list}) SELECT {col_list} FROM {table}")
                conn.execute(f"DROP TABLE {table}")
                conn.execute(f"ALTER TABLE {tmp_table} RENAME TO {table}")
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return count


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    df = load_csv(args.csv)
    df = add_row_id(df)
    # Auto determine chunksize if not specified and dataframe large (>200k rows maybe). We'll just set if memory might be high.
    if args.chunksize is None and len(df) > 150_000:
        args.chunksize = 25_000
    count = write_sqlite(df, args.db, args.table, args.if_exists, args.chunksize)
    print(f"Wrote {count} rows to {args.db} table '{args.table}'")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
