"""Vollstaendige KaufDA-Analyse: Alle Ketten, Standorte, Prospekte."""
import json
import re
import sys
import time

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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Sparfuchs/1.0"
})


def fetch_next_data(url: str) -> dict | None:
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
        if match:
            return json.loads(match.group(1))
    except Exception as e:
        print(f"  ERROR: {url} -> {e}", file=sys.stderr)
    return None


def get_offer_count(content_id: str, lat: float, lng: float) -> int:
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
    return -1


def get_brochure_type(content_id: str, lat: float, lng: float) -> str:
    try:
        resp = session.get(
            f"{CONTENT_VIEWER_BASE}/{content_id}",
            params={"partner": "kaufda_web", "lat": str(lat), "lng": str(lng)},
            headers=VIEWER_HEADERS,
            timeout=15,
        )
        if resp.ok:
            return resp.json().get("content", {}).get("type", "unknown")
    except Exception:
        pass
    return "error"


def analyze_chain(slug: str, display_name: str) -> dict:
    print(f"\n{'='*60}")
    print(f"{display_name} ({slug})")
    print(f"{'='*60}")

    # Fetch main chain page
    url = f"{KAUFDA_BASE}/Geschaefte/{slug}"
    resp = session.get(url, timeout=30)
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
    if not match:
        print(f"  Keine __NEXT_DATA__!")
        return {"chain": display_name, "slug": slug, "error": True}

    data = json.loads(match.group(1))
    pi = data["props"]["pageProps"]["pageInformation"]
    pub = pi["publisher"]
    local_url = pub.get("localUrl", "")
    local_id = str(pub.get("localId", ""))
    default_city = pub.get("defaultCity", {})
    default_lat = default_city.get("lat", 52.52)
    default_lng = default_city.get("lng", 13.405)

    # 1. Get brochures from main page
    all_brochures_raw = pi.get("brochures", {}).get("viewer", []) + pi.get("brochures", {}).get("publisher", [])
    print(f"  Hauptseite: {len(all_brochures_raw)} Prospekte")

    # 2. Find city links in sidebar: pattern /{City}/{localUrl}/p-r{localId}
    city_pattern = re.compile(rf'href="(https://www\.kaufda\.de/([^/]+)/{re.escape(local_url)}/p-r{local_id})"')
    city_matches = city_pattern.findall(resp.text)
    cities = {}
    for full_url, city_slug in city_matches:
        if city_slug not in cities:
            cities[city_slug] = full_url

    print(f"  Standort-Links in Sidebar: {len(cities)}")
    if cities:
        city_names = list(cities.keys())[:10]
        more = f" +{len(cities)-10} weitere" if len(cities) > 10 else ""
        print(f"    {', '.join(city_names)}{more}")

    # 3. For each city, fetch brochures
    all_brochure_ids = {}  # content_id -> info
    for b in all_brochures_raw:
        cid = b.get("contentId")
        if cid:
            all_brochure_ids[cid] = {
                "title": (b.get("title") or "?")[:60],
                "pages": b.get("pageCount", 0),
                "source": "hauptseite",
            }

    # Sample city pages to discover regional brochures
    cities_checked = 0
    for city_slug, city_url in list(cities.items()):
        try:
            city_data = fetch_next_data(city_url)
            if city_data:
                city_pi = city_data.get("props", {}).get("pageProps", {}).get("pageInformation", {})
                city_brochures = city_pi.get("brochures", {})
                for bucket in ("viewer", "publisher"):
                    for b in city_brochures.get(bucket, []):
                        cid = b.get("contentId")
                        if cid and cid not in all_brochure_ids:
                            all_brochure_ids[cid] = {
                                "title": (b.get("title") or "?")[:60],
                                "pages": b.get("pageCount", 0),
                                "source": f"stadt_{city_slug}",
                            }
            cities_checked += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"    Fehler bei {city_slug}: {e}", file=sys.stderr)

        if cities_checked >= 5:  # Sample max 5 cities for speed
            break

    print(f"  Unique Prospekte nach {cities_checked} Stadt-Checks: {len(all_brochure_ids)}")

    # 4. Get offer counts for each unique brochure
    total_offers = 0
    brochure_details = []
    for cid, info in all_brochure_ids.items():
        btype = get_brochure_type(cid, default_lat, default_lng)
        offers = get_offer_count(cid, default_lat, default_lng) if btype == "static_brochure" else 0
        total_offers += max(offers, 0)
        brochure_details.append({
            "content_id": cid,
            "title": info["title"],
            "type": btype,
            "pages": info["pages"],
            "offers": offers,
            "source": info["source"],
        })
        type_label = "static" if btype == "static_brochure" else btype
        print(f"    [{type_label}] {info['title'][:45]} ({info['pages']}S, {offers} Ang.) via {info['source']}")
        time.sleep(0.2)

    print(f"  TOTAL: {len(all_brochure_ids)} Prospekte, {total_offers} Angebote, {len(cities)} Standorte")

    return {
        "chain": display_name,
        "slug": slug,
        "cities_total": len(cities),
        "cities_checked": cities_checked,
        "city_names": list(cities.keys()),
        "brochures": brochure_details,
        "unique_brochures": len(all_brochure_ids),
        "total_offers": total_offers,
    }


def main():
    print("KaufDA Vollanalyse - Alle Ketten, Standorte, Prospekte")
    print("=" * 60)

    results = []
    for slug, name in CHAINS.items():
        result = analyze_chain(slug, name)
        results.append(result)
        time.sleep(0.5)

    # Summary
    print("\n\n" + "=" * 80)
    print("GESAMTUEBERSICHT")
    print("=" * 80)
    print(f"{'Kette':<15} {'Standorte':>10} {'Prospekte':>10} {'Angebote':>10} {'In DB':>8}")
    print("-" * 60)

    # Load current DB counts
    import sqlite3, os
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "kaufda_dataset", "offers.sqlite3")
    db_counts = {}
    try:
        conn = sqlite3.connect(db_path)
        for row in conn.execute("SELECT chain, COUNT(*) FROM offers GROUP BY chain"):
            db_counts[row[0]] = row[1]
        conn.close()
    except Exception:
        pass

    grand_total_offers = 0
    grand_total_db = 0
    for r in results:
        if r.get("error"):
            print(f"{r['chain']:<15} {'FEHLER':>10}")
            continue
        db_count = db_counts.get(r["chain"], db_counts.get(r["chain"].replace(" ", ""), 0))
        grand_total_offers += r["total_offers"]
        grand_total_db += db_count
        delta = r["total_offers"] - db_count
        delta_str = f"+{delta}" if delta > 0 else str(delta)
        print(f"{r['chain']:<15} {r['cities_total']:>10} {r['unique_brochures']:>10} {r['total_offers']:>10} {db_count:>8} ({delta_str})")

    print("-" * 60)
    print(f"{'TOTAL':<15} {'':>10} {'':>10} {grand_total_offers:>10} {grand_total_db:>8} (+{grand_total_offers - grand_total_db})")

    # Save
    with open("data/kaufda_full_analysis.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nGespeichert: data/kaufda_full_analysis.json")


if __name__ == "__main__":
    main()
