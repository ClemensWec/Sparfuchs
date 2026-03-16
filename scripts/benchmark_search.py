"""
Benchmark: Sparfuchs vs KaufDA search quality.

Runs all product category names against our local search,
then samples a subset against KaufDA's live API for comparison.
"""
from __future__ import annotations

import asyncio
import json
import random
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.connectors.kaufda import KaufdaOffersSeoConnector, KaufdaLocation
from app.services.catalog_search import CatalogSearchService

DB = Path("data/kaufda_dataset/offers.sqlite3")
LAT, LON = 50.7194, 7.1221  # Bonn 53113
RADIUS = 15

# KaufDA config
KAUFDA_LOCATION = KaufdaLocation(lat=LAT, lng=LON, city="Bonn", zip="53113")
KAUFDA_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sparfuchs-Benchmark/1.0"
LOCAL_SAMPLE_SIZE = 500   # How many terms to test locally (0 = all)
KAUFDA_SAMPLE_SIZE = 250  # How many terms to test against KaufDA
KAUFDA_CONCURRENCY = 3    # Parallel requests
KAUFDA_DELAY = 0.5        # Seconds between batches


@dataclass
class SearchResult:
    term: str
    sparfuchs_count: int = 0
    sparfuchs_top: str = ""
    kaufda_count: int = -1  # -1 = not tested
    kaufda_top: str = ""


def load_search_terms() -> list[str]:
    """Load category names + common grocery terms. Use single-word or short terms for speed."""
    conn = sqlite3.connect(str(DB))

    # Category names: prefer shorter names (faster search, more realistic queries)
    rows = conn.execute(
        "SELECT name FROM product_categories WHERE offer_count >= 3 ORDER BY offer_count DESC"
    ).fetchall()
    # Filter to max 3 words (long product names are slow and unrealistic queries)
    terms = [r[0] for r in rows if len(r[0].split()) <= 3]

    # Add common manual search terms
    manual = [
        "milch", "butter", "eier", "brot", "käse", "joghurt", "wurst", "schinken",
        "reis", "nudeln", "mehl", "zucker", "kaffee", "tee", "bier", "wein",
        "cola", "wasser", "saft", "pizza", "chips", "schokolade", "eis",
        "hähnchen", "hackfleisch", "lachs", "thunfisch", "kartoffeln", "tomaten",
        "bananen", "äpfel", "erdbeeren", "gurke", "paprika", "zwiebeln",
        "müsli", "cornflakes", "nutella", "marmelade", "honig", "senf", "ketchup",
        "toilettenpapier", "spülmittel", "waschmittel", "zahnpasta", "shampoo",
        "haltbare milch", "h-milch", "haferflocken", "aufschnitt",
        "sahne", "quark", "öl", "essig", "salz", "pfeffer",
        "orangensaft", "apfelsaft", "mineralwasser", "milchreis",
        "salami", "leberwurst", "fleischkäse",
        "mozzarella", "gouda", "emmentaler", "camembert",
        "weintrauben", "zitronen", "orangen", "möhren", "brokkoli", "spinat",
        "champignons", "zucchini", "kekse", "kuchen", "croissant", "brötchen",
        "windeln", "katzenfutter", "hundefutter", "duschgel", "deo",
        "fischstäbchen", "aufbackbrötchen", "tiefkühlgemüse",
    ]
    terms.extend(manual)
    conn.close()

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in terms:
        key = t.lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique


def run_sparfuchs_search(terms: list[str]) -> dict[str, SearchResult]:
    """Run all terms against our local search."""
    svc = CatalogSearchService(DB)
    results: dict[str, SearchResult] = {}

    t0 = time.time()
    for i, term in enumerate(terms):
        suggestions = svc.search(term, lat=LAT, lon=LON, radius_km=RADIUS)
        n = len(suggestions)
        top = suggestions[0].title[:50] if suggestions else ""
        results[term] = SearchResult(term=term, sparfuchs_count=n, sparfuchs_top=top)

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            print(f"  Sparfuchs: {i+1}/{len(terms)} ({elapsed:.1f}s)")

    elapsed = time.time() - t0
    print(f"  Sparfuchs fertig: {len(terms)} Suchen in {elapsed:.1f}s ({len(terms)/elapsed:.0f} Suchen/s)")
    return results


async def run_kaufda_search(terms: list[str], results: dict[str, SearchResult]) -> None:
    """Run a sample of terms against KaufDA."""
    connector = KaufdaOffersSeoConnector(
        user_agent=KAUFDA_UA,
        location=KAUFDA_LOCATION,
    )

    sample = random.sample(terms, min(KAUFDA_SAMPLE_SIZE, len(terms)))
    print(f"\n  KaufDA: Teste {len(sample)} Terms (von {len(terms)} gesamt)...")

    sem = asyncio.Semaphore(KAUFDA_CONCURRENCY)
    tested = 0
    errors = 0
    t0 = time.time()

    async def search_one(term: str) -> None:
        nonlocal tested, errors
        async with sem:
            try:
                offers = await connector.fetch_search_offers(keyword=term)
                n = len(offers)
                top = offers[0].title[:50] if offers else ""
                if term in results:
                    results[term].kaufda_count = n
                    results[term].kaufda_top = top
                tested += 1
            except Exception as e:
                errors += 1
                if term in results:
                    results[term].kaufda_count = -2  # error
            await asyncio.sleep(KAUFDA_DELAY)

            if (tested + errors) % 25 == 0:
                elapsed = time.time() - t0
                print(f"    KaufDA: {tested+errors}/{len(sample)} ({elapsed:.1f}s, {errors} Fehler)")

    # Run in batches
    tasks = [search_one(t) for t in sample]
    await asyncio.gather(*tasks)

    elapsed = time.time() - t0
    print(f"  KaufDA fertig: {tested} OK, {errors} Fehler in {elapsed:.1f}s")


def print_report(results: dict[str, SearchResult]) -> None:
    """Print comparison report."""
    all_results = list(results.values())
    total = len(all_results)

    # Sparfuchs stats
    sf_zero = sum(1 for r in all_results if r.sparfuchs_count == 0)
    sf_low = sum(1 for r in all_results if 0 < r.sparfuchs_count < 3)
    sf_ok = sum(1 for r in all_results if r.sparfuchs_count >= 3)

    print(f"\n{'='*70}")
    print(f"  SPARFUCHS ERGEBNISSE ({total} Suchbegriffe)")
    print(f"{'='*70}")
    print(f"  OK (>=3 Treffer):  {sf_ok:5d} ({sf_ok*100//total}%)")
    print(f"  Low (1-2 Treffer): {sf_low:5d} ({sf_low*100//total}%)")
    print(f"  Zero (0 Treffer):  {sf_zero:5d} ({sf_zero*100//total}%)")
    print(f"  Mit Ergebnis:      {sf_ok+sf_low:5d} ({(sf_ok+sf_low)*100//total}%)")

    # KaufDA comparison (only tested terms)
    kd_tested = [r for r in all_results if r.kaufda_count >= 0]
    if kd_tested:
        kd_total = len(kd_tested)
        kd_zero = sum(1 for r in kd_tested if r.kaufda_count == 0)
        kd_low = sum(1 for r in kd_tested if 0 < r.kaufda_count < 3)
        kd_ok = sum(1 for r in kd_tested if r.kaufda_count >= 3)

        sf_sub_zero = sum(1 for r in kd_tested if r.sparfuchs_count == 0)
        sf_sub_low = sum(1 for r in kd_tested if 0 < r.sparfuchs_count < 3)
        sf_sub_ok = sum(1 for r in kd_tested if r.sparfuchs_count >= 3)

        print(f"\n{'='*70}")
        print(f"  VERGLEICH: SPARFUCHS vs KAUFDA ({kd_total} Suchbegriffe)")
        print(f"{'='*70}")
        print(f"  {'Metrik':<25} {'Sparfuchs':>12} {'KaufDA':>12} {'Differenz':>12}")
        print(f"  {'-'*61}")
        print(f"  {'OK (>=3 Treffer)':<25} {sf_sub_ok:>11d}  {kd_ok:>11d}  {sf_sub_ok-kd_ok:>+11d}")
        print(f"  {'Low (1-2 Treffer)':<25} {sf_sub_low:>11d}  {kd_low:>11d}  {sf_sub_low-kd_low:>+11d}")
        print(f"  {'Zero (0 Treffer)':<25} {sf_sub_zero:>11d}  {kd_zero:>11d}  {sf_sub_zero-kd_zero:>+11d}")
        print(f"  {'Mit Ergebnis':<25} {sf_sub_ok+sf_sub_low:>11d}  {kd_ok+kd_low:>11d}  {(sf_sub_ok+sf_sub_low)-(kd_ok+kd_low):>+11d}")
        print(f"  {'Erfolgsrate':<25} {(sf_sub_ok+sf_sub_low)*100//kd_total:>10d}%  {(kd_ok+kd_low)*100//kd_total:>10d}%")

        # Find terms where KaufDA has results but we don't
        kd_wins = sorted(
            [r for r in kd_tested if r.kaufda_count > 0 and r.sparfuchs_count == 0],
            key=lambda r: -r.kaufda_count,
        )
        if kd_wins:
            print(f"\n  KaufDA hat Treffer, wir nicht ({len(kd_wins)}):")
            for r in kd_wins[:20]:
                print(f"    {r.term:40s}  KD={r.kaufda_count:3d}  SF=0")

        # Find terms where we have results but KaufDA doesn't
        sf_wins = sorted(
            [r for r in kd_tested if r.sparfuchs_count > 0 and r.kaufda_count == 0],
            key=lambda r: -r.sparfuchs_count,
        )
        if sf_wins:
            print(f"\n  Wir haben Treffer, KaufDA nicht ({len(sf_wins)}):")
            for r in sf_wins[:20]:
                print(f"    {r.term:40s}  SF={r.sparfuchs_count:3d}  KD=0")

        # Average result count comparison
        both_have = [r for r in kd_tested if r.sparfuchs_count > 0 and r.kaufda_count > 0]
        if both_have:
            sf_avg = sum(r.sparfuchs_count for r in both_have) / len(both_have)
            kd_avg = sum(r.kaufda_count for r in both_have) / len(both_have)
            print(f"\n  Durchschn. Treffer (beide >0, n={len(both_have)}):")
            print(f"    Sparfuchs: {sf_avg:.1f}")
            print(f"    KaufDA:    {kd_avg:.1f}")

    # Save detailed results
    out_path = Path("data/benchmark_results.json")
    out_data = []
    for r in sorted(all_results, key=lambda x: x.term.lower()):
        out_data.append({
            "term": r.term,
            "sparfuchs": r.sparfuchs_count,
            "sparfuchs_top": r.sparfuchs_top,
            "kaufda": r.kaufda_count,
            "kaufda_top": r.kaufda_top,
        })
    out_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  Detail-Ergebnisse gespeichert: {out_path}")


async def main():
    print("Lade Suchbegriffe...")
    terms = load_search_terms()
    print(f"  {len(terms)} eindeutige Suchbegriffe geladen\n")

    # Sample for local search if needed
    if LOCAL_SAMPLE_SIZE > 0 and len(terms) > LOCAL_SAMPLE_SIZE:
        random.seed(42)
        local_terms = random.sample(terms, LOCAL_SAMPLE_SIZE)
        print(f"  Lokal-Sample: {len(local_terms)} von {len(terms)}")
    else:
        local_terms = terms

    print("Phase 1: Sparfuchs (lokal)...")
    results = run_sparfuchs_search(local_terms)

    print("\nPhase 2: KaufDA (live API)...")
    await run_kaufda_search(local_terms, results)

    print_report(results)


if __name__ == "__main__":
    asyncio.run(main())
