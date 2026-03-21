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


async def run_category_api_tests() -> list[TestResult]:
    """Teste Category Browse API Endpoints."""
    from fastapi.testclient import TestClient

    from app.main import app

    results = []

    client = TestClient(app)

    # CAT-01: /api/category-tiles liefert Tiles
    resp = client.get("/api/category-tiles")
    data = resp.json()
    tiles = data.get("tiles", [])
    results.append(TestResult(
        name="CAT-01: /api/category-tiles liefert Tiles",
        passed=(resp.status_code == 200) and (len(tiles) > 0),
        message=f"Status {resp.status_code}, {len(tiles)} Tiles",
    ))

    # CAT-02: /api/category-tiles keine Duplikate
    tile_names = [t["name"].lower() for t in tiles]
    has_dupes = len(tile_names) != len(set(tile_names))
    results.append(TestResult(
        name="CAT-02: /api/category-tiles keine Duplikate",
        passed=not has_dupes,
        message=f"OK, {len(tiles)} unique" if not has_dupes else f"FAIL: Duplikate in {tile_names}",
    ))

    # CAT-03: /api/offers-by-category mit gültiger ID liefert Offers
    # Verwende erste Tile-ID als gültige Kategorie
    valid_cat_id = tiles[0]["id"] if tiles else 1
    resp = client.get("/api/offers-by-category", params={"category_id": valid_cat_id})
    data = resp.json()
    offers = data.get("offers", [])
    results.append(TestResult(
        name="CAT-03: /api/offers-by-category mit gültiger ID",
        passed=(resp.status_code == 200) and (len(offers) > 0),
        message=f"Status {resp.status_code}, {len(offers)} Offers für ID {valid_cat_id}",
    ))

    # CAT-04: /api/offers-by-category mit Location filtert
    resp = client.get("/api/offers-by-category", params={
        "category_id": valid_cat_id,
        "location": "Bonn",
        "radius_km": 5,
    })
    data = resp.json()
    offers_loc = data.get("offers", [])
    results.append(TestResult(
        name="CAT-04: /api/offers-by-category mit Location filtert",
        passed=(resp.status_code == 200),
        message=f"Status {resp.status_code}, {len(offers_loc)} Offers (mit Location)",
    ))

    # CAT-05: /api/offers-by-category ungültige ID -> 404
    resp = client.get("/api/offers-by-category", params={"category_id": 999999})
    results.append(TestResult(
        name="CAT-05: /api/offers-by-category ungültige ID -> 404",
        passed=(resp.status_code == 404),
        message=f"Status {resp.status_code}",
    ))

    return results


async def run_compare_tests() -> list[TestResult]:
    """Teste /api/compare Endpoint mit verschiedenen Basket-Konfigurationen."""
    from fastapi.testclient import TestClient

    from app.main import app

    results = []

    client = TestClient(app)

    # CMP-01: /api/compare mit category_id Basket Item
    resp = client.post("/api/compare", json={
        "location": "Bonn",
        "radius_km": 5,
        "basket": [{"category_id": 1, "category_name": "Milch"}],
    })
    data = resp.json()
    results.append(TestResult(
        name="CMP-01: /api/compare mit category_id Basket Item",
        passed=(resp.status_code == 200) and ("rows" in data or "error" not in data or "Datenbank" not in data.get("error", "")),
        message=f"Status {resp.status_code}, Keys: {list(data.keys())[:5]}",
    ))

    # CMP-02: /api/compare mit gemischtem Basket
    resp = client.post("/api/compare", json={
        "location": "Bonn",
        "radius_km": 5,
        "basket": [
            {"category_id": 1, "category_name": "Milch"},
            {"q": "Butter"},
        ],
    })
    data = resp.json()
    results.append(TestResult(
        name="CMP-02: /api/compare mit gemischtem Basket",
        passed=(resp.status_code == 200) and ("rows" in data),
        message=f"Status {resp.status_code}, Keys: {list(data.keys())[:5]}",
    ))

    # CMP-03: /api/compare ohne Location -> Fehler
    resp = client.post("/api/compare", json={
        "location": "",
        "radius_km": 5,
        "basket": [{"q": "Milch"}],
    })
    data = resp.json()
    results.append(TestResult(
        name="CMP-03: /api/compare ohne Location -> Fehler",
        passed=(resp.status_code == 400) and ("error" in data),
        message=f"Status {resp.status_code}, Error: {data.get('error', 'N/A')}",
    ))

    # CMP-04: /api/compare leerer Basket -> Fehler
    resp = client.post("/api/compare", json={
        "location": "Bonn",
        "radius_km": 5,
        "basket": [],
    })
    data = resp.json()
    results.append(TestResult(
        name="CMP-04: /api/compare leerer Basket -> Fehler",
        passed=(resp.status_code == 400) and ("error" in data),
        message=f"Status {resp.status_code}, Error: {data.get('error', 'N/A')}",
    ))

    return results


async def run_unit_parser_tests() -> list[TestResult]:
    """Teste Grundpreis-Normierung (unit_parser.py)."""
    from app.utils.unit_parser import parse_base_price

    results = []

    # UP-01: "1,99 €/kg" -> korrekt geparst
    bp = parse_base_price("1,99 €/kg")
    results.append(TestResult(
        name="UP-01: '1,99 €/kg' -> korrekt geparst",
        passed=(
            bp.unit == "kg"
            and bp.unit_group == "weight"
            and bp.price_eur is not None
            and abs(bp.price_eur - 1.99) < 0.01
            and bp.normalized_unit == "g"
        ),
        message=f"unit={bp.unit}, group={bp.unit_group}, price={bp.price_eur}, norm={bp.normalized_unit}",
    ))

    # UP-02: "2,49€/100g" -> auf kg normalisiert (price_per_normalized = Preis pro 1g)
    bp = parse_base_price("2,49€/100g")
    expected_ppn = 2.49 / 100.0  # 0.0249 €/g
    results.append(TestResult(
        name="UP-02: '2,49€/100g' -> auf g normalisiert",
        passed=(
            bp.unit == "g"
            and bp.unit_group == "weight"
            and bp.price_per_normalized is not None
            and abs(bp.price_per_normalized - expected_ppn) < 0.001
        ),
        message=f"unit={bp.unit}, ppn={bp.price_per_normalized}, expected={expected_ppn:.4f}",
    ))

    # UP-03: Leerer String -> None
    bp = parse_base_price("")
    results.append(TestResult(
        name="UP-03: Leerer String -> None",
        passed=(
            bp.unit is None
            and bp.price_eur is None
            and bp.price_per_normalized is None
        ),
        message=f"unit={bp.unit}, price={bp.price_eur}",
    ))

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

    up_results = await run_unit_parser_tests()
    total_failed += print_results("GRUNDPREIS-NORMIERUNG", up_results)

    cat_results = await run_category_api_tests()
    total_failed += print_results("CATEGORY BROWSE API", cat_results)

    cmp_results = await run_compare_tests()
    total_failed += print_results("PREISVERGLEICH API", cmp_results)

    # Integration Tests
    print("\n[Führe KaufDA API Tests aus...]")
    kd_results = await run_kaufda_tests()
    total_failed += print_results("KAUFDA API", kd_results)

    # Summary
    all_results = [kw_results, ch_results, fm_results, web_results, up_results, cat_results, cmp_results, kd_results]
    total_tests = sum(len(r) for r in all_results)
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
