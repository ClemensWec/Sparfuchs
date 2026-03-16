"""Analyze search logs to find quality issues in category suggest.

Reports:
1. Top queries without category clicks (user searched but didn't select -> bad suggest?)
2. Queries with spell corrections (frequent misspellings -> add to dictionary?)
3. Most selected categories (popular -> boost in ranking?)
4. Queries with 0 results (gaps in coverage)
5. Click-through rate by query (search -> selection conversion)
6. Location distribution (where are users searching from?)

Usage:
    python scripts/analyze_search_logs.py [--days N] [--min-count N]
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path("data/kaufda_dataset/offers.sqlite3")


def analyze(days: int = 30, min_count: int = 2) -> None:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Check if table exists
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='search_log'"
        ).fetchall()
    ]
    if not tables:
        print("search_log table does not exist yet.")
        print("It will be created automatically when the app starts.")
        return

    total = conn.execute("SELECT COUNT(*) FROM search_log").fetchone()[0]
    print(f"=== Search Log Analysis ({total} total entries) ===\n")

    if total == 0:
        print("No data yet. Use the app to generate search logs.")
        return

    date_filter = ""
    params: tuple = ()
    if days > 0:
        date_filter = "WHERE timestamp >= datetime('now', ?)"
        params = (f"-{days} days",)
        recent = conn.execute(
            f"SELECT COUNT(*) FROM search_log {date_filter}", params
        ).fetchone()[0]
        print(f"Entries in last {days} days: {recent}\n")

    # --- 1. Top queries (all) ---
    print("--- Top 20 Queries ---")
    rows = conn.execute(
        f"""
        SELECT query, COUNT(*) as cnt
        FROM search_log
        {date_filter}
        GROUP BY query
        ORDER BY cnt DESC
        LIMIT 20
        """,
        params,
    ).fetchall()
    for r in rows:
        print(f"  {r['query']:35s} {r['cnt']:>4d}x")

    # --- 2. Queries without any category click ---
    print("\n--- Queries Without Category Selection (potential suggest gaps) ---")
    rows = conn.execute(
        f"""
        SELECT s.query, s.cnt as search_count
        FROM (
            SELECT query, COUNT(*) as cnt
            FROM search_log
            {date_filter}
            GROUP BY query
        ) s
        LEFT JOIN (
            SELECT query
            FROM search_log
            WHERE selected_category_id IS NOT NULL
            {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
            GROUP BY query
        ) c ON s.query = c.query
        WHERE c.query IS NULL AND s.cnt >= ?
        ORDER BY s.cnt DESC
        LIMIT 20
        """,
        params + params + (min_count,) if date_filter else params + (min_count,),
    ).fetchall()
    if rows:
        for r in rows:
            print(f"  {r['query']:35s} {r['search_count']:>4d}x (no click)")
    else:
        print("  (none)")

    # --- 3. Most selected categories ---
    print("\n--- Most Selected Categories ---")
    rows = conn.execute(
        f"""
        SELECT selected_category_name, selected_category_id, COUNT(*) as cnt
        FROM search_log
        WHERE selected_category_id IS NOT NULL
        {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
        GROUP BY selected_category_id
        ORDER BY cnt DESC
        LIMIT 20
        """,
        params,
    ).fetchall()
    if rows:
        for r in rows:
            print(
                f"  {r['selected_category_name'] or '?':35s} (id={r['selected_category_id']}) {r['cnt']:>4d}x"
            )
    else:
        print("  (none)")

    # --- 4. Queries with 0 results ---
    print("\n--- Queries With 0 Results (coverage gaps) ---")
    rows = conn.execute(
        f"""
        SELECT query, COUNT(*) as cnt
        FROM search_log
        WHERE result_count = 0
        {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
        GROUP BY query
        ORDER BY cnt DESC
        LIMIT 20
        """,
        params,
    ).fetchall()
    if rows:
        for r in rows:
            print(f"  {r['query']:35s} {r['cnt']:>4d}x (0 results)")
    else:
        print("  (none)")

    # --- 5. Click-through rate ---
    print("\n--- Click-Through Rate (searches -> selections) ---")
    total_searches = conn.execute(
        f"""
        SELECT COUNT(*) FROM search_log
        WHERE result_count IS NOT NULL
        {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
        """,
        params,
    ).fetchone()[0]
    total_clicks = conn.execute(
        f"""
        SELECT COUNT(*) FROM search_log
        WHERE selected_category_id IS NOT NULL
        {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
        """,
        params,
    ).fetchone()[0]
    if total_searches > 0:
        ctr = total_clicks / total_searches * 100
        print(f"  Searches (with results): {total_searches}")
        print(f"  Category selections:     {total_clicks}")
        print(f"  Click-through rate:      {ctr:.1f}%")
    else:
        print("  No search data yet.")

    # --- 6. Spell corrections ---
    print("\n--- Spell Corrections (frequent misspellings) ---")
    rows = conn.execute(
        f"""
        SELECT corrected_from, query, COUNT(*) as cnt
        FROM search_log
        WHERE corrected_from IS NOT NULL
        {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
        GROUP BY corrected_from
        ORDER BY cnt DESC
        LIMIT 15
        """,
        params,
    ).fetchall()
    if rows:
        for r in rows:
            print(f"  {r['corrected_from']:20s} -> {r['query']:20s} {r['cnt']:>4d}x")
    else:
        print("  (none)")

    # --- 7. Location distribution ---
    print("\n--- Top Locations ---")
    rows = conn.execute(
        f"""
        SELECT location, COUNT(*) as cnt
        FROM search_log
        WHERE location IS NOT NULL AND location != ''
        {"AND " + date_filter.replace("WHERE ", "") if date_filter else ""}
        GROUP BY location
        ORDER BY cnt DESC
        LIMIT 10
        """,
        params,
    ).fetchall()
    if rows:
        for r in rows:
            print(f"  {r['location']:30s} {r['cnt']:>4d}x")
    else:
        print("  (no location data yet)")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze Sparfuchs search logs")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Only analyze last N days (0 = all time)",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=2,
        help="Minimum query count to report (default: 2)",
    )
    args = parser.parse_args()
    analyze(days=args.days, min_count=args.min_count)
