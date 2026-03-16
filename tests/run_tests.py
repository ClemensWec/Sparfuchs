#!/usr/bin/env python3
"""
Test-Runner für Sparfuchs.

Führt alle Tests aus und erstellt einen Report.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# Füge app zum Path hinzu
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str = ""
    details: str = ""


async def run_keyword_tests() -> list[TestResult]:
    """Teste Keyword-Generierung."""
    from app.utils.keywords import generate_keyword_variants

    results = []

    # Test 1: Tomate -> Tomaten
    variants = generate_keyword_variants("Tomate")
    results.append(TestResult(
        name="KW-01: Tomate -> Tomaten",
        passed="Tomaten" in variants,
        message="OK" if "Tomaten" in variants else f"FAIL: {variants}",
    ))

    # Test 2: tomate (lowercase) -> Tomate
    variants = generate_keyword_variants("tomate")
    results.append(TestResult(
        name="KW-02: tomate -> Tomate (capitalize)",
        passed="Tomate" in variants,
        message="OK" if "Tomate" in variants else f"FAIL: {variants}",
    ))

    # Test 3: Banane -> Bananen
    variants = generate_keyword_variants("Banane")
    results.append(TestResult(
        name="KW-03: Banane -> Bananen",
        passed="Bananen" in variants,
        message="OK" if "Bananen" in variants else f"FAIL: {variants}",
    ))

    # Test 4: Hähnchen -> Haehnchen
    variants = generate_keyword_variants("Hähnchen")
    results.append(TestResult(
        name="KW-04: Hähnchen -> Haehnchen",
        passed="Haehnchen" in variants,
        message="OK" if "Haehnchen" in variants else f"FAIL: {variants}",
    ))

    return results


async def run_chain_tests() -> list[TestResult]:
    """Teste Chain-Normalisierung."""
    from app.utils.chains import normalize_chain, normalize_chain_from_osm_tags

    results = []

    test_cases = [
        ("ALDI SÜD", "Aldi"),
        ("REWE City", "Rewe"),
        ("Nahkauf", "Rewe"),
        ("EDEKA Center", "Edeka"),
        ("Penny-Markt", "Penny"),
        ("Netto Marken-Discount", "Netto"),
    ]

    for input_val, expected in test_cases:
        result = normalize_chain(input_val)
        results.append(TestResult(
            name=f"CH: {input_val} -> {expected}",
            passed=result == expected,
            message="OK" if result == expected else f"FAIL: got {result}",
        ))

    # OSM Test
    tags = {"brand": "Nahkauf"}
    result = normalize_chain_from_osm_tags(tags)
    results.append(TestResult(
        name="CH-OSM: Nahkauf -> Rewe",
        passed=result == "Rewe",
        message="OK" if result == "Rewe" else f"FAIL: got {result}",
    ))

    return results


async def run_matching_tests() -> list[TestResult]:
    """Teste Fuzzy-Matching."""
    from app.utils.matching import calculate_match_score, MIN_SCORE_WITH_PRICE

    results = []

    test_cases = [
        ("Milch", "Milbona Haltbare Milch", True),
        ("Tomaten", "Rispentomaten", True),
        ("Bananen", "Chiquita Bananen", True),
        ("Butter", "Kerrygold Butter", True),
        ("Milch", "Schokolade", False),
    ]

    for query, offer, should_match in test_cases:
        score = calculate_match_score(query, offer)
        actual_match = score >= MIN_SCORE_WITH_PRICE
        passed = actual_match == should_match

        results.append(TestResult(
            name=f"FM: {query} vs {offer[:20]}...",
            passed=passed,
            message=f"Score: {score:.1f}, {'matched' if actual_match else 'no match'}",
        ))

    return results


async def run_kaufda_tests() -> list[TestResult]:
    """Teste KaufDA API (erfordert Netzwerk)."""
    import httpx

    from app.connectors.kaufda import KaufdaOffersSeoConnector, KaufdaLocation

    results = []

    connector = KaufdaOffersSeoConnector(
        user_agent="Mozilla/5.0 Sparfuchs-Test/1.0",
        location=KaufdaLocation(lat=50.7374, lng=7.0982, city="Bonn", zip="53111"),
    )

    test_keywords = [
        ("Milch", 5),
        ("Tomate", 3),  # Sollte via Varianten funktionieren
        ("Banane", 1),  # Sollte via Varianten funktionieren
        ("Hähnchen", 5),  # Sollte via Umlaut-Variante funktionieren
        ("Butter", 3),
        ("Brot", 3),
    ]

    for keyword, min_expected in test_keywords:
        try:
            offers = await connector.fetch_search_offers(keyword=keyword)
            passed = len(offers) >= min_expected
            chains = sorted(set(o.chain for o in offers))
            results.append(TestResult(
                name=f"KD: {keyword}",
                passed=passed,
                message=f"{len(offers)} Angebote von {chains}",
            ))
        except Exception as e:
            results.append(TestResult(
                name=f"KD: {keyword}",
                passed=False,
                message=f"ERROR: {e}",
            ))

    coverage_connector = KaufdaOffersSeoConnector(
        user_agent="Mozilla/5.0 Sparfuchs-Test/1.0",
        location=KaufdaLocation(lat=50.7206, lng=7.1187, city="Bonn", zip="53113"),
    )
    coverage_url = "https://www.kaufda.de/angebote/Milch"
    coverage_headers = {"User-Agent": "Mozilla/5.0 Sparfuchs-Test/1.0"}
    coverage_cookies = {"location": coverage_connector._location.to_cookie_value()}

    try:
        async with httpx.AsyncClient(timeout=30.0, headers=coverage_headers, follow_redirects=True) as client:
            next_data = await coverage_connector._fetch_next_data(
                client=client,
                url=coverage_url,
                cookies=coverage_cookies,
            )

        if next_data is None:
            results.append(TestResult(
                name="KD-COV: Milch 53113",
                passed=False,
                message="FAIL: __NEXT_DATA__ fehlt",
            ))
        else:
            seed_embedded, seed_total = coverage_connector._count_embedded_offer_items(next_data)
            recovered_offers = await coverage_connector.fetch_search_offers(keyword="Milch")
            passed = seed_total > seed_embedded and len(recovered_offers) > seed_embedded and len(recovered_offers) >= 30
            results.append(TestResult(
                name="KD-COV: Milch 53113",
                passed=passed,
                message=(
                    f"Seed eingebettet: {seed_embedded}, KaufDA totalItems: {seed_total}, "
                    f"Sparfuchs: {len(recovered_offers)}"
                ),
            ))
    except Exception as e:
        results.append(TestResult(
            name="KD-COV: Milch 53113",
            passed=False,
            message=f"ERROR: {e}",
        ))

    return results


async def run_web_validation_tests() -> list[TestResult]:
    """Teste wichtige Web-Validierungen (ohne externe Netzwerke)."""
    from fastapi.testclient import TestClient

    from app.main import app

    results = []

    client = TestClient(app)

    resp = client.post(
        "/results",
        data={
            "location": "Bonn",
            "radius_km": "5",
            "basket_json": "[]",
        },
    )
    results.append(
        TestResult(
            name="WEB-01: /results ohne basket -> Fehler",
            passed=(resp.status_code == 400) and ("mindestens einen Artikel" in (resp.text or "")),
            message=f"Status {resp.status_code}",
        )
    )

    resp = client.get(
        "/api/suggest",
        params={
            "q": "Milch",
            "location": "Bonn",
        },
    )
    data = resp.json()
    hits = data.get("hits") or []
    results.append(
        TestResult(
            name="WEB-02: /api/suggest Milch liefert Treffer",
            passed=(resp.status_code == 200) and (len(hits) > 0),
            message=f"Status {resp.status_code}, Hits {len(hits)}",
        )
    )

    resp = client.get(
        "/api/suggest",
        params={
            "q": "Haehnchen",
            "location": "Bonn",
        },
    )
    data = resp.json()
    hits = data.get("hits") or []
    results.append(
        TestResult(
            name="WEB-03: /api/suggest Hähnchen liefert Treffer",
            passed=(resp.status_code == 200) and (len(hits) > 0),
            message=f"Status {resp.status_code}, Hits {len(hits)}",
        )
    )

    return results


def print_results(category: str, results: list[TestResult]) -> int:
    """Druckt Ergebnisse und gibt Anzahl Fehler zurück."""
    print(f"\n{'='*60}")
    print(f"  {category}")
    print(f"{'='*60}")

    failed = 0
    for r in results:
        status = "[OK]" if r.passed else "[FAIL]"
        print(f"  {status} {r.name}")
        if r.message:
            print(f"        {r.message}")
        if not r.passed:
            failed += 1

    return failed


async def main():
    print(f"\n{'#'*60}")
    print(f"  SPARFUCHS TEST SUITE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    total_failed = 0

    # Unit Tests
    kw_results = await run_keyword_tests()
    total_failed += print_results("KEYWORD-NORMALISIERUNG", kw_results)

    ch_results = await run_chain_tests()
    total_failed += print_results("CHAIN-NORMALISIERUNG", ch_results)

    fm_results = await run_matching_tests()
    total_failed += print_results("FUZZY-MATCHING", fm_results)

    web_results = await run_web_validation_tests()
    total_failed += print_results("WEB-VALIDIERUNG", web_results)

    # Integration Tests
    print("\n[Führe KaufDA API Tests aus...]")
    kd_results = await run_kaufda_tests()
    total_failed += print_results("KAUFDA API", kd_results)

    # Summary
    total_tests = len(kw_results) + len(ch_results) + len(fm_results) + len(web_results) + len(kd_results)
    total_passed = total_tests - total_failed

    print(f"\n{'='*60}")
    print(f"  ZUSAMMENFASSUNG")
    print(f"{'='*60}")
    print(f"  Gesamt:    {total_tests} Tests")
    print(f"  Bestanden: {total_passed} Tests")
    print(f"  Fehler:    {total_failed} Tests")
    print(f"{'='*60}")

    if total_failed == 0:
        print("\n  [OK] ALLE TESTS BESTANDEN\n")
    else:
        print(f"\n  [FAIL] {total_failed} TESTS FEHLGESCHLAGEN\n")

    return total_failed


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
