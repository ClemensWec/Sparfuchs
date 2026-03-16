"""Export ranking features for Learning-to-Rank training.

Joins search_log (queries + clicks) with ranking features to create
training data for XGBoost/LightGBM ranking models.

Usage: python scripts/export_ranking_features.py [--min-queries 100]
"""
import json
import sqlite3
from pathlib import Path

from app.services.category_search import CategorySearchService

DB_PATH = Path("data/kaufda_dataset/offers.sqlite3")

def export():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cs = CategorySearchService(DB_PATH)

    # Get top queries from search log
    queries = conn.execute("""
        SELECT query, COUNT(*) as search_count,
               COUNT(selected_category_id) as click_count
        FROM search_log
        WHERE query IS NOT NULL AND LENGTH(query) >= 2
        GROUP BY query
        HAVING search_count >= 3
        ORDER BY search_count DESC
        LIMIT 500
    """).fetchall()

    if not queries:
        print("Not enough search log data yet. Need at least 3 searches per query.")
        print("Use the app to generate search data, then re-run this script.")
        return

    # For each query: get ranking results + check if they were clicked
    training_data = []
    for q_row in queries:
        query = q_row["query"]

        # Get click data for this query
        clicks = conn.execute("""
            SELECT selected_category_id, COUNT(*) as click_count
            FROM search_log
            WHERE query = ? AND selected_category_id IS NOT NULL
            GROUP BY selected_category_id
        """, (query,)).fetchall()
        click_ids = {r["selected_category_id"]: r["click_count"] for r in clicks}

        # Get ranking results with features
        results = cs.search(query, limit=20)

        for rank, result in enumerate(results):
            cat_id = result["id"]
            training_data.append({
                "query": query,
                "category_id": cat_id,
                "category_name": result["name"],
                "rank": rank,
                "clicked": 1 if cat_id in click_ids else 0,
                "click_count": click_ids.get(cat_id, 0),
                "offer_count": result["offer_count"],
                "search_count": q_row["search_count"],
            })

    # Export as JSON Lines
    output_path = Path("data/ranking_training_data.jsonl")
    with open(output_path, "w", encoding="utf-8") as f:
        for item in training_data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"Exported {len(training_data)} training examples for {len(queries)} queries")
    print(f"Output: {output_path}")
    print(f"Queries with clicks: {sum(1 for q in queries if q['click_count'] > 0)}")
    print(f"Total clicks: {sum(q['click_count'] for q in queries)}")

if __name__ == "__main__":
    export()
