"""Profile search: measure performance + compare against baseline."""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.catalog_search import CatalogSearchService

DB = Path("data/kaufda_dataset/offers.sqlite3")
LAT, LON, RADIUS = 50.7194, 7.1221, 15

svc = CatalogSearchService(DB)

test_terms = [
    "milch", "butter", "käse", "joghurt", "bananen",
    "orangensaft", "hähnchen", "marmelade", "zahnpasta",
    "konfitüre extra pfirsich", "haltbare milch", "tiefkühlgemüse",
    "aufbackbrötchen", "katzenfutter", "weintrauben",
]

# Load baseline
baseline_path = Path("data/search_baseline.json")
baseline = json.loads(baseline_path.read_text(encoding="utf-8")) if baseline_path.exists() else {}

# Warm up DB cache
svc.search("test", lat=LAT, lon=LON, radius_km=RADIUS)

print(f"{'Term':<30} {'Results':>8} {'Baseline':>8} {'Diff':>6} {'Time(ms)':>10}")
print("-" * 66)

all_results = {}
total_time = 0
quality_ok = True
for term in test_terms:
    t0 = time.perf_counter()
    results = svc.search(term, lat=LAT, lon=LON, radius_km=RADIUS)
    dt = (time.perf_counter() - t0) * 1000
    total_time += dt

    bl_count = len(baseline.get(term, []))
    diff = len(results) - bl_count
    marker = "" if diff == 0 else ("+" if diff > 0 else "LOST!")
    if diff < 0:
        quality_ok = False

    print(f"{term:<30} {len(results):>8} {bl_count:>8} {diff:>+5d} {marker:<5} {dt:>8.0f}")
    all_results[term] = [
        {"title": r.title, "chain": r.chain, "score": round(r.score, 1)}
        for r in results
    ]

print("-" * 66)
print(f"{'TOTAL':<30} {'':>8} {'':>8} {'':>6} {total_time:>10.0f}")
print(f"{'AVG':<30} {'':>8} {'':>8} {'':>6} {total_time/len(test_terms):>10.0f}")

if quality_ok:
    print("\nQUALITY CHECK: PASS - no results lost")
else:
    print("\nQUALITY CHECK: FAIL")
