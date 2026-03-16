"""Benchmark 100 random search terms: performance + quality comparison."""
import json
import random
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.catalog_search import CatalogSearchService

DB = Path("data/kaufda_dataset/offers.sqlite3")
LAT, LON, RADIUS = 50.7194, 7.1221, 15
SEED = 42
N = 100

def load_terms() -> list[str]:
    conn = sqlite3.connect(str(DB))
    rows = conn.execute(
        "SELECT name FROM product_categories WHERE offer_count >= 3 ORDER BY offer_count DESC"
    ).fetchall()
    terms = [r[0] for r in rows if len(r[0].split()) <= 3]
    manual = [
        "milch", "butter", "eier", "brot", "käse", "joghurt", "wurst",
        "reis", "nudeln", "kaffee", "bier", "cola", "wasser", "pizza",
        "hähnchen", "hackfleisch", "lachs", "kartoffeln", "tomaten",
        "bananen", "äpfel", "erdbeeren", "gurke", "paprika", "zwiebeln",
        "müsli", "nutella", "marmelade", "honig", "senf", "ketchup",
        "toilettenpapier", "spülmittel", "waschmittel", "zahnpasta",
        "orangensaft", "apfelsaft", "mineralwasser", "sahne", "quark",
        "salami", "mozzarella", "gouda", "weintrauben", "zitronen",
        "champignons", "kekse", "croissant", "brötchen", "katzenfutter",
        "aufbackbrötchen", "tiefkühlgemüse", "haltbare milch",
    ]
    terms.extend(manual)
    conn.close()
    seen: set[str] = set()
    unique: list[str] = []
    for t in terms:
        key = t.lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique

def run_benchmark(output_path: str):
    svc = CatalogSearchService(DB)
    all_terms = load_terms()
    random.seed(SEED)
    terms = random.sample(all_terms, min(N, len(all_terms)))

    # Warm up
    svc.search("test", lat=LAT, lon=LON, radius_km=RADIUS)

    results = {}
    times = []
    total_results = 0
    zero_count = 0

    for i, term in enumerate(terms):
        t0 = time.perf_counter()
        hits = svc.search(term, lat=LAT, lon=LON, radius_km=RADIUS)
        dt = (time.perf_counter() - t0) * 1000
        times.append(dt)
        total_results += len(hits)
        if len(hits) == 0:
            zero_count += 1

        results[term] = {
            "count": len(hits),
            "time_ms": round(dt, 1),
            "top5": [{"title": r.title, "chain": r.chain, "score": round(r.score, 1)} for r in hits[:5]],
        }

        if (i + 1) % 25 == 0:
            elapsed = sum(times)
            print(f"  {i+1}/{len(terms)} ({elapsed:.0f}ms total, {elapsed/(i+1):.0f}ms avg)")

    # Summary
    times_sorted = sorted(times)
    print(f"\n{'='*60}")
    print(f"  ERGEBNISSE ({len(terms)} Suchbegriffe)")
    print(f"{'='*60}")
    print(f"  Total time:    {sum(times):.0f}ms")
    print(f"  Avg time:      {sum(times)/len(times):.0f}ms")
    print(f"  Median time:   {times_sorted[len(times)//2]:.0f}ms")
    print(f"  P95 time:      {times_sorted[int(len(times)*0.95)]:.0f}ms")
    print(f"  Max time:      {max(times):.0f}ms")
    print(f"  Min time:      {min(times):.0f}ms")
    print(f"  Total results: {total_results}")
    print(f"  Avg results:   {total_results/len(terms):.1f}")
    print(f"  Zero results:  {zero_count} ({zero_count*100//len(terms)}%)")
    print(f"  With results:  {len(terms)-zero_count} ({(len(terms)-zero_count)*100//len(terms)}%)")

    # Slowest terms
    slowest = sorted(results.items(), key=lambda x: -x[1]["time_ms"])[:10]
    print(f"\n  Langsamste:")
    for term, data in slowest:
        print(f"    {term:<35} {data['time_ms']:>8.0f}ms  ({data['count']} Treffer)")

    # Save
    Path(output_path).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  Gespeichert: {output_path}")

if __name__ == "__main__":
    label = sys.argv[1] if len(sys.argv) > 1 else "current"
    run_benchmark(f"data/bench100_{label}.json")
