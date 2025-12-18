"""
City-based Customer Subsetting Script
Keeps: Only customers from selected cities + all related rows (orders, order_details, shipments, employees, products, suppliers, purchase orders)
Maintains: Full referential integrity
Masking: Disabled
"""

import yaml
import mysql.connector
from collections import defaultdict, deque
import os
import sys

# -----------------------------------
# Load config
# -----------------------------------
def load_config(path="config.yaml"):
    if not os.path.exists(path):
        print("[error] config.yaml not found")
        sys.exit(1)
    with open(path, "r") as f:
        return yaml.safe_load(f)

# -----------------------------------
# Connect to database
# -----------------------------------
def connect(cfg):
    return mysql.connector.connect(
        host=cfg["host"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        autocommit=False
    )

# -----------------------------------
# Primary key fetcher
# -----------------------------------
def get_pk(cursor, db, table):
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA=%s
          AND TABLE_NAME=%s
          AND CONSTRAINT_NAME='PRIMARY'
        ORDER BY ORDINAL_POSITION
    """, (db, table))
    rows = cursor.fetchall()
    return [r["COLUMN_NAME"] for r in rows]

# -----------------------------------
# FK discovery (auto)
# -----------------------------------
def discover_fk(cursor, db):
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA=%s
          AND REFERENCED_TABLE_NAME IS NOT NULL
    """, (db,))
    rows = cursor.fetchall()

    references = defaultdict(list)
    referenced_by = defaultdict(list)

    for r in rows:
        tbl = r["TABLE_NAME"]
        col = r["COLUMN_NAME"]
        ref_tbl = r["REFERENCED_TABLE_NAME"]
        ref_col = r["REFERENCED_COLUMN_NAME"]

        references[tbl].append((ref_tbl, col, ref_col))
        referenced_by[ref_tbl].append((tbl, col, ref_col))

    return references, referenced_by

# -----------------------------------
# Insert row to target
# -----------------------------------
def insert_row(cursor, table, row):
    cols = list(row.keys())
    q = f"INSERT INTO `{table}` ({','.join('`'+c+'`' for c in cols)}) VALUES ({','.join(['%s']*len(cols))})"
    cursor.execute(q, tuple(row[c] for c in cols))


# -----------------------------------
# Clear target database safely
# -----------------------------------
def clear_target_database(tgt_cur, tgt_cfg):
    print("[info] clearing target database...")

    # Get all table names
    tgt_cur.execute("""
        SELECT TABLE_NAME 
        FROM information_schema.tables
        WHERE table_schema=%s
    """, (tgt_cfg["database"],))

    tables = [row[0] for row in tgt_cur.fetchall()]

    # Disable FK checks
    tgt_cur.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Truncate all tables
    for t in tables:
        print(f"  truncating {t} ...")
        try:
            tgt_cur.execute(f"TRUNCATE TABLE `{t}`")
        except Exception as e:
            print(f"   [warn] could not truncate {t}: {e}")

    # Enable FK checks
    tgt_cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    print("[info] target database cleared.")


# -----------------------------------
# MAIN
# -----------------------------------
def main():
    cfg = load_config()
    src_cfg = cfg["source"]
    tgt_cfg = cfg["target"]

    city_filter = cfg.get("city_filter")
    root_table = cfg["root_table"]

    print("[info] connecting...")
    src = connect(src_cfg)
    tgt = connect(tgt_cfg)

    src_cur = src.cursor(dictionary=True)
    tgt_cur = tgt.cursor()

    clear_target_database(tgt_cur, tgt_cfg)
    tgt.commit()

    dbname = src_cfg["database"]

    # Build FK graph
    references, referenced_by = discover_fk(src_cur, dbname)
    print("[info] discovered FK relationships:", len(references))

    # Get PK of root table
    root_pk = get_pk(src_cur, dbname, root_table)

    # -----------------------------------
    # Fetch customers by city
    # -----------------------------------
    print("[info] selecting customers from selected cities...")

    if isinstance(city_filter, list):
        placeholders = ",".join(["%s"] * len(city_filter))
        query = f"SELECT * FROM {root_table} WHERE city IN ({placeholders})"
        src_cur.execute(query, tuple(city_filter))
    else:
        query = f"SELECT * FROM {root_table} WHERE city=%s"
        src_cur.execute(query, (city_filter,))

    seeds = src_cur.fetchall()
    print(f"[info] found {len(seeds)} customers.")

    if not seeds:
        print("[done] No customers found for given city filter.")
        return

    # collected rows
    collected = defaultdict(dict)
    pk_cache = {}

    def ensure_pk(table):
        if table not in pk_cache:
            pk_cache[table] = get_pk(src_cur, dbname, table)
        return pk_cache[table]

    # BFS queue
    q = deque()

    for row in seeds:
        key = tuple(row[c] for c in root_pk)
        collected[root_table][key] = row
        q.append((root_table, root_pk, key))

    # -----------------------------------
    # BFS to collect related rows
    # -----------------------------------
    print("[info] collecting related rows...")
    while q:
        table, pk_cols, pk_key = q.popleft()
        row = collected[table][pk_key]

        # follow parents
        for (parent_tbl, fk_col, parent_pk_col) in references.get(table, []):
            if row.get(fk_col) is None:
                continue

            src_cur.execute(
                f"SELECT * FROM `{parent_tbl}` WHERE `{parent_pk_col}`=%s LIMIT 1",
                (row[fk_col],)
            )
            parent = src_cur.fetchone()
            if parent:
                pkp = ensure_pk(parent_tbl)
                keyp = tuple(parent[c] for c in pkp)
                if keyp not in collected[parent_tbl]:
                    collected[parent_tbl][keyp] = parent
                    q.append((parent_tbl, pkp, keyp))

        # follow children
        for (child_tbl, child_fk_col, child_ref_col) in referenced_by.get(table, []):
            val = row.get(child_ref_col)
            if val is None:
                continue

            src_cur.execute(
                f"SELECT * FROM `{child_tbl}` WHERE `{child_fk_col}`=%s",
                (val,)
            )
            for child in src_cur.fetchall():
                pkc = ensure_pk(child_tbl)
                keyc = tuple(child[c] for c in pkc)
                if keyc not in collected[child_tbl]:
                    collected[child_tbl][keyc] = child
                    q.append((child_tbl, pkc, keyc))

    print("[info] summary of collected rows:")
    for t, d in collected.items():
        print(f"  {t}: {len(d)}")

    # Insert in order — parents first
    ordered_tables = list(collected.keys())
    ordered_tables.sort(key=lambda t: len(references.get(t, [])))

    print("[info] inserting into target DB...")
    for t in ordered_tables:
        rows = list(collected[t].values())
        print(f"  inserting {len(rows)} rows into {t} ...")
        for r in rows:
            try:
                insert_row(tgt_cur, t, r)
            except Exception as e:
                print("   [warn] skipping:", e)

        tgt.commit()

    print("[done] City-based subsetting complete!")

if __name__ == "__main__":
    main()
