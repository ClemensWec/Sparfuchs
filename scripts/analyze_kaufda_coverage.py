"""Analyse: Alle Ketten auf KaufDA durchgehen, Standorte + Prospekte erfassen."""
import json
import re
import sys
import time
from collections import defaultdict

import requests

CHAINS = {
    "Aldi-Nord": "ALDI Nord",
    "Aldi-Sued": "ALDI Sued",
    "Lidl": "Lidl",
    "REWE": "REWE",
    "Edeka": "Edeka",
    "Kaufland": "Kaufland",
    "Penny-Markt": "Penny",
    "Netto-Marken-Discount": "Netto",
    "Norma": "Norma",
    "Globus": "Globus",
    "Marktkauf": "Marktkauf",
}

KAUFDA_BASE = "https://www.kaufda.de"
CONTENT_VIEWER_BASE = "https://content-viewer-be.kaufda.de/v1/brochures"
VIEWER_HEADERS = {"Bonial-Api-Consumer": "web-content-viewer-fe"}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Sparfuchs-Analyse/1.0"
})


def fetch_next_data(url: str) -> dict | None:
    """Fetch __NEXT_DATA__ from a KaufDA page."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
        if match:
            return json.loads(match.group(1))
    except Exception as e:
        print(f"  ERROR fetching {url}: {e}", file=sys.stderr)
    return None


def get_brochure_details(content_id: str, lat: float, lng: float) -> dict | None:
    """Fetch brochure metadata from content-viewer API."""
    try:
        resp = session.get(
            f"{CONTENT_VIEWER_BASE}/{content_id}",
            params={"partner": "kaufda_web", "lat": str(lat), "lng": str(lng)},
            headers=VIEWER_HEADERS,
            timeout=15,
        )
        if resp.ok:
            return resp.json()
    except Exception:
        pass
    return None


def get_brochure_offer_count(content_id: str, lat: float, lng: float) -> int:
    """Count offers in a brochure via pages API."""
    try:
        resp = session.get(
            f"{CONTENT_VIEWER_BASE}/{content_id}/pages",
            params={"partner": "kaufda_web", "lat": str(lat), "lng": str(lng)},
            headers=VIEWER_HEADERS,
            timeout=15,
        )
        if resp.ok:
            return sum(len(p.get("offers", [])) for p in resp.json().get("contents", []))
    except Exception:
        pass
    return 0


def analyze_chain(slug: str, display_name: str) -> dict:
    """Analyze a chain: find all locations and brochures."""
    print(f"\n{'='*60}")
    print(f"Analysiere: {display_name} ({slug})")
    print(f"{'='*60}")

    # Step 1: Fetch main chain page
    url = f"{KAUFDA_BASE}/Geschaefte/{slug}"
    data = fetch_next_data(url)
    if not data:
        print(f"  Keine Daten gefunden!")
        return {"chain": display_name, "slug": slug, "error": "no data", "locations": [], "brochures": []}

    page_props = data.get("props", {}).get("pageProps", {})
    page_info = page_props.get("pageInformation", {})
    publisher = page_info.get("publisher", {})

    # Get default location
    default_city = publisher.get("defaultCity", {})
    default_lat = default_city.get("lat", 52.52)
    default_lng = default_city.get("lng", 13.405)
    default_city_name = default_city.get("displayName", "?")

    print(f"  Default-Stadt: {default_city_name} ({default_lat}, {default_lng})")

    # Step 2: Get brochures from main page
    brochures_viewer = page_info.get("brochures", {}).get("viewer", [])
    brochures_publisher = page_info.get("brochures", {}).get("publisher", [])
    all_brochures = brochures_viewer + brochures_publisher

    print(f"  Prospekte auf Hauptseite: {len(all_brochures)}")

    # Step 3: Find location links (cities) from the page
    # KaufDA shows "Angebote in anderen Orten" - these are in the page HTML
    # Let's also check the build_id for Next.js data fetching
    build_id = data.get("buildId", "")
    local_url = publisher.get("localUrl", "")
    local_id = str(publisher.get("localId", ""))

    # Extract city links from page
    locations = []
    seen_cities = set()

    # The main page has links to city-specific pages
    # Pattern: /Geschaefte/{city}/{slug}/...
    resp = session.get(url, timeout=30)
    city_pattern = re.compile(
        rf'/Geschaefte/([^/"]+)/{re.escape(slug)}(?:/|")',
        re.IGNORECASE,
    )
    city_matches = city_pattern.findall(resp.text)
    for city_slug in city_matches:
        if city_slug not in seen_cities:
            seen_cities.add(city_slug)
            locations.append(city_slug)

    # Also try to find from SEO links
    seo_pattern = re.compile(
        rf'href="/Geschaefte/([^/"]+/{re.escape(local_url)}/[^"]*)"',
    )
    seo_matches = seo_pattern.findall(resp.text)

    # Also extract from Next.js route data if available
    # Look for city list in pageProps
    cities_data = page_props.get("cities") or page_props.get("relatedCities") or []
    if not cities_data:
        # Try to find in sidebar/SEO content
        city_link_pattern = re.compile(
            rf'{re.escape(display_name)}\s+([A-Z][a-z\-]+(?:\s[A-Z][a-z\-]+)*)',
        )
        city_text_matches = city_link_pattern.findall(resp.text)
        for city_name in city_text_matches:
            if city_name not in seen_cities and len(city_name) > 2:
                seen_cities.add(city_name)

    print(f"  Gefundene Stadt-Links: {len(locations)}")

    # Step 4: Analyze each brochure
    brochure_details = []
    total_offers = 0

    for b in all_brochures:
        cid = b.get("contentId", "?")
        title = (b.get("title") or "?")[:60]
        page_count = b.get("pageCount", 0)

        # Get type from API
        meta = get_brochure_details(cid, default_lat, default_lng)
        brochure_type = "?"
        if meta:
            brochure_type = meta.get("content", {}).get("type", "?")

        # Count offers
        offers = get_brochure_offer_count(cid, default_lat, default_lng)
        total_offers += offers

        brochure_details.append({
            "content_id": cid,
            "title": title,
            "type": brochure_type,
            "pages": page_count,
            "offers": offers,
        })

        print(f"  [{brochure_type}] {title}")
        print(f"    {page_count} Seiten, {offers} Angebote")

        time.sleep(0.3)  # Rate limit

    # Step 5: Crawl city-specific pages to find region-specific brochures
    # Use the store-crawl approach from kaufda_brochures.py
    all_brochure_ids = {b.get("contentId") for b in all_brochures}
    regional_brochures = []

    # Sample a few cities to check for different brochures
    store_paths_to_check = []
    seed = f"Filialen/{default_city.get('url', '')}/{local_url}/v-r{local_id}" if default_city.get("url") else None

    if seed and build_id:
        # Fetch the seed store page to discover more stores
        try:
            store_resp = session.get(
                f"{KAUFDA_BASE}/_next/data/{build_id}/{seed}.json",
                timeout=15,
            )
            if store_resp.ok:
                store_data = store_resp.json()
                store_pi = store_data.get("pageProps", {}).get("pageInformation", {})
                store_brochures = store_pi.get("brochures", {})
                for bucket in ("viewer", "publisher"):
                    for sb in store_brochures.get(bucket, []):
                        sbid = sb.get("contentId")
                        if sbid and sbid not in all_brochure_ids:
                            all_brochure_ids.add(sbid)
                            regional_brochures.append({
                                "content_id": sbid,
                                "title": (sb.get("title") or "?")[:60],
                                "source": f"store_page_{default_city_name}",
                            })

                # Find more store paths
                path_pattern = re.compile(rf"/Filialen/([^\"\\]+/{re.escape(local_url)}/v-r{local_id})")
                for match in path_pattern.findall(store_resp.text):
                    store_paths_to_check.append(f"Filialen/{match}")
        except Exception as e:
            print(f"  Store-Crawl Fehler: {e}", file=sys.stderr)

    # Check a few more store pages for regional brochures
    checked = 0
    for store_path in store_paths_to_check[:5]:  # Max 5 additional stores
        try:
            sr = session.get(
                f"{KAUFDA_BASE}/_next/data/{build_id}/{store_path}.json",
                timeout=15,
            )
            if sr.ok:
                sp = sr.json().get("pageProps", {}).get("pageInformation", {})
                loc = sp.get("location", {})
                city = loc.get("city", "?")
                for bucket in ("viewer", "publisher"):
                    for sb in sp.get("brochures", {}).get(bucket, []):
                        sbid = sb.get("contentId")
                        if sbid and sbid not in all_brochure_ids:
                            all_brochure_ids.add(sbid)
                            regional_brochures.append({
                                "content_id": sbid,
                                "title": (sb.get("title") or "?")[:60],
                                "source": f"store_page_{city}",
                            })
            checked += 1
            time.sleep(0.3)
        except Exception:
            pass

    if regional_brochures:
        print(f"\n  Zusaetzliche regionale Prospekte ({len(regional_brochures)}):")
        for rb in regional_brochures:
            print(f"    {rb['title']} (gefunden via {rb['source']})")

    print(f"\n  ZUSAMMENFASSUNG {display_name}:")
    print(f"    Prospekte: {len(brochure_details)} (Hauptseite) + {len(regional_brochures)} (regional)")
    print(f"    Gesamt-Angebote (Hauptseite): {total_offers}")
    print(f"    Alle Prospekt-IDs: {len(all_brochure_ids)}")

    return {
        "chain": display_name,
        "slug": slug,
        "default_city": default_city_name,
        "brochures_main": brochure_details,
        "brochures_regional": regional_brochures,
        "total_brochure_ids": len(all_brochure_ids),
        "total_offers_main": total_offers,
        "city_links": len(locations),
    }


def main():
    print("=" * 60)
    print("KaufDA Prospekt-Analyse - Alle Ketten")
    print("=" * 60)

    results = []
    for slug, name in CHAINS.items():
        result = analyze_chain(slug, name)
        results.append(result)
        time.sleep(1)  # Rate limit between chains

    # Final summary
    print("\n\n" + "=" * 80)
    print("GESAMTUEBERSICHT")
    print("=" * 80)
    print(f"{'Kette':<20} {'Prospekte':>10} {'Regional':>10} {'Angebote':>10} {'Standorte':>10}")
    print("-" * 70)

    total_brochures = 0
    total_offers = 0
    for r in results:
        if "error" in r:
            print(f"{r['chain']:<20} {'FEHLER':>10}")
            continue
        main_b = len(r["brochures_main"])
        reg_b = len(r["brochures_regional"])
        offers = r["total_offers_main"]
        cities = r["city_links"]
        total_brochures += main_b + reg_b
        total_offers += offers
        print(f"{r['chain']:<20} {main_b:>10} {reg_b:>10} {offers:>10} {cities:>10}")

    print("-" * 70)
    print(f"{'TOTAL':<20} {total_brochures:>10} {'':>10} {total_offers:>10}")

    # Save detailed results
    with open("data/kaufda_coverage_analysis.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nDetaillierte Ergebnisse gespeichert: data/kaufda_coverage_analysis.json")


if __name__ == "__main__":
    main()
