"""Full KaufDA scrape: alle Ketten, alle Standorte, alle Prospekte, alle Angebote.

Speichert im Format fuer build_kaufda_offers_db.py:
  downloads/{chain_key}/{content_id}/metadata.json
  downloads/{chain_key}/{content_id}/pages.json
Dazu brochures.json Katalog fuer brochure_locations.
"""
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "downloads" / "kaufda_fresh"
DOWNLOADS_DIR = OUTPUT_DIR / "downloads"

CHAINS = {
    "aldi-nord":  {"slug": "Aldi-Nord",              "display": "ALDI Nord"},
    "aldi-sued":  {"slug": "Aldi-Sued",              "display": "ALDI Sued"},
    "lidl":       {"slug": "Lidl",                    "display": "Lidl"},
    "rewe":       {"slug": "REWE",                    "display": "REWE"},
    "edeka":      {"slug": "Edeka",                   "display": "EDEKA"},
    "kaufland":   {"slug": "Kaufland",                "display": "Kaufland"},
    "penny":      {"slug": "Penny-Markt",             "display": "Penny"},
    "netto":      {"slug": "Netto-Marken-Discount",   "display": "Netto Marken-Discount"},
    "norma":      {"slug": "Norma",                   "display": "Norma"},
    "globus":     {"slug": "Globus",                  "display": "Globus"},
    "marktkauf":  {"slug": "Marktkauf",               "display": "Marktkauf"},
}

KAUFDA = "https://www.kaufda.de"
VIEWER = "https://content-viewer-be.kaufda.de/v1/brochures"
VIEWER_HEADERS = {"Bonial-Api-Consumer": "web-content-viewer-fe"}

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Sparfuchs/1.0"})

# Stats
stats: dict[str, Any] = {"chains": {}, "total_brochures": 0, "total_offers": 0, "total_pages": 0}


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s.strip()).strip("_") or "item"


def fetch_next_data(url: str) -> dict | None:
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
            if m:
                return json.loads(m.group(1))
            return None
        except Exception as e:
            if attempt == 2:
                print(f"    FAIL {url}: {e}")
            time.sleep(1 + attempt)
    return None


def fetch_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict | None:
    for attempt in range(3):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 2:
                print(f"    FAIL {url}: {e}")
            time.sleep(1 + attempt)
    return None


def discover_brochures_from_page(page_data: dict, source: str) -> list[dict]:
    """Extract brochure info from a KaufDA __NEXT_DATA__ page."""
    results = []
    try:
        pi = page_data["props"]["pageProps"]["pageInformation"]
        location = pi.get("location", {}) or {}
        lat = location.get("lat")
        lng = location.get("lng")
        city = location.get("city", "")

        for bucket in ("viewer", "publisher"):
            for b in pi.get("brochures", {}).get(bucket, []):
                cid = b.get("contentId")
                if not cid:
                    continue
                results.append({
                    "content_id": cid,
                    "legacy_id": b.get("id"),
                    "title": b.get("title", ""),
                    "page_count": b.get("pageCount", 0),
                    "bucket": bucket,
                    "source": source,
                    "lat": lat,
                    "lng": lng,
                    "city": city,
                })
    except (KeyError, TypeError):
        pass
    return results


def scrape_chain(chain_key: str, chain_cfg: dict) -> dict:
    slug = chain_cfg["slug"]
    display = chain_cfg["display"]
    print(f"\n{'='*70}")
    print(f"  {display} ({chain_key})")
    print(f"{'='*70}")

    chain_stats = {"brochures": 0, "offers": 0, "pages": 0, "cities": 0}

    # 1. Fetch main page
    main_url = f"{KAUFDA}/Geschaefte/{slug}"
    resp = session.get(main_url, timeout=30)
    main_data = None
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
    if m:
        main_data = json.loads(m.group(1))

    if not main_data:
        print(f"  SKIP: No __NEXT_DATA__ on main page")
        return chain_stats

    pi = main_data["props"]["pageProps"]["pageInformation"]
    pub = pi["publisher"]
    local_url = pub.get("localUrl", "")
    local_id = str(pub.get("localId", ""))
    default_city = pub.get("defaultCity", {})
    build_id = main_data.get("buildId", "")

    # 2. Find sidebar city links
    city_pattern = re.compile(
        rf'href="(https://www\.kaufda\.de/([^/]+)/{re.escape(local_url)}/p-r{local_id})"'
    )
    city_matches = city_pattern.findall(resp.text)
    cities = {}
    for full_url, city_slug in city_matches:
        if city_slug not in cities:
            cities[city_slug] = full_url

    print(f"  Sidebar-Staedte: {len(cities)}")

    # 3. Also crawl store pages via Next.js data API for deeper discovery
    store_seed = None
    if default_city.get("url") and build_id:
        store_seed = f"Filialen/{default_city['url']}/{local_url}/v-r{local_id}"

    # Collect all pages to scan for brochures
    pages_to_scan: list[tuple[str, str]] = []  # (url, source_label)

    # Add main page
    pages_to_scan.append((main_url, "hauptseite"))

    # Add all city pages
    for city_slug, city_url in cities.items():
        pages_to_scan.append((city_url, f"stadt_{city_slug}"))

    # 4. Crawl store pages for deeper brochure discovery
    store_paths_found: list[str] = []
    if store_seed and build_id:
        seed_url = f"{KAUFDA}/_next/data/{build_id}/{store_seed}.json"
        try:
            seed_resp = session.get(seed_url, timeout=15)
            if seed_resp.ok:
                seed_data = seed_resp.json()
                # Discover more store paths
                path_pat = re.compile(rf"/Filialen/([^\"\\]+/{re.escape(local_url)}/v-r{local_id})")
                for match in path_pat.findall(seed_resp.text):
                    store_paths_found.append(f"Filialen/{match}")
        except Exception:
            pass

    print(f"  Store-Pfade entdeckt: {len(store_paths_found)}")

    # 5. Scan all pages + store pages for brochures
    all_brochures: dict[str, dict] = {}  # content_id -> info
    catalog_discoveries: dict[str, list[dict]] = {}  # content_id -> list of discoveries

    # Scan city pages
    for page_url, source in pages_to_scan:
        data = fetch_next_data(page_url) if page_url != main_url else main_data
        if not data:
            continue
        found = discover_brochures_from_page(data, source)
        for b in found:
            cid = b["content_id"]
            if cid not in all_brochures:
                all_brochures[cid] = b
            catalog_discoveries.setdefault(cid, []).append({
                "source": source,
                "lat": b.get("lat") or default_city.get("lat"),
                "lng": b.get("lng") or default_city.get("lng"),
                "city": b.get("city", ""),
            })
        time.sleep(0.2)

    # Scan store pages (sample up to 20 for deeper coverage)
    store_checked = 0
    for store_path in store_paths_found[:20]:
        try:
            sr = session.get(f"{KAUFDA}/_next/data/{build_id}/{store_path}.json", timeout=15)
            if sr.ok:
                store_data = sr.json()
                # Wrap in expected structure
                wrapped = {"props": {"pageProps": store_data.get("pageProps", store_data)}}
                if "pageInformation" not in wrapped["props"]["pageProps"]:
                    wrapped["props"]["pageProps"]["pageInformation"] = store_data.get("pageProps", {}).get("pageInformation", {})
                found = discover_brochures_from_page(wrapped, f"store_{store_path[-20:]}")
                for b in found:
                    cid = b["content_id"]
                    if cid not in all_brochures:
                        all_brochures[cid] = b
                    catalog_discoveries.setdefault(cid, []).append({
                        "source": f"store",
                        "lat": b.get("lat") or default_city.get("lat"),
                        "lng": b.get("lng") or default_city.get("lng"),
                        "city": b.get("city", ""),
                    })
            store_checked += 1
            time.sleep(0.2)
        except Exception:
            pass

    print(f"  Unique Prospekte gefunden: {len(all_brochures)}")
    chain_stats["cities"] = len(cities)

    # 6. Download metadata + pages for each brochure
    chain_dir = DOWNLOADS_DIR / safe_name(chain_key)
    chain_dir.mkdir(parents=True, exist_ok=True)

    catalog_brochures: list[dict] = []

    for cid, info in all_brochures.items():
        brochure_dir = chain_dir / safe_name(cid)
        brochure_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = brochure_dir / "metadata.json"
        pages_path = brochure_dir / "pages.json"

        query_params = {
            "partner": "kaufda_web",
            "lat": str(info.get("lat") or default_city.get("lat", 52.52)),
            "lng": str(info.get("lng") or default_city.get("lng", 13.405)),
        }

        # Fetch metadata
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        else:
            metadata = fetch_json(f"{VIEWER}/{cid}", params=query_params, headers=VIEWER_HEADERS)
            if metadata:
                metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
            time.sleep(0.15)

        if not metadata:
            print(f"    SKIP {cid}: no metadata")
            continue

        brochure_type = metadata.get("content", {}).get("type", "unknown")
        if brochure_type != "static_brochure":
            print(f"    SKIP {cid}: {brochure_type}")
            continue

        # Fetch pages
        if pages_path.exists():
            pages_data = json.loads(pages_path.read_text(encoding="utf-8"))
        else:
            pages_data = fetch_json(f"{VIEWER}/{cid}/pages", params=query_params, headers=VIEWER_HEADERS)
            if pages_data:
                pages_path.write_text(json.dumps(pages_data, ensure_ascii=False, indent=2), encoding="utf-8")
            time.sleep(0.15)

        if not pages_data:
            print(f"    SKIP {cid}: no pages")
            continue

        pages_list = pages_data.get("contents", [])
        offer_count = sum(len(p.get("offers", [])) for p in pages_list)
        title = (info.get("title") or metadata.get("content", {}).get("title", "?"))[:50]

        print(f"    [{len(pages_list)}S, {offer_count} Ang.] {title}")

        chain_stats["brochures"] += 1
        chain_stats["offers"] += offer_count
        chain_stats["pages"] += len(pages_list)

        # Build catalog entry for brochure_locations
        catalog_brochures.append({
            "content_id": cid,
            "legacy_id": info.get("legacy_id"),
            "title": info.get("title", ""),
            "page_count": len(pages_list),
            "chain_key": chain_key,
            "chain_name": display,
            "publisher_name": display,
            "valid_from": metadata.get("content", {}).get("validFrom"),
            "valid_until": metadata.get("content", {}).get("validUntil"),
            "discoveries": catalog_discoveries.get(cid, []),
            "query": query_params,
        })

    # Save chain catalog
    chain_catalog = {
        "chain_key": chain_key,
        "chain_name": display,
        "global_slug": slug,
        "publisher_name": display,
        "build_id": build_id,
        "local_url": local_url,
        "local_id": local_id,
        "seed_city": default_city.get("displayName", ""),
        "store_pages_crawled": store_checked,
        "brochure_count": len(catalog_brochures),
        "brochures": catalog_brochures,
        "failures": [],
    }

    print(f"  DONE: {chain_stats['brochures']} Prospekte, {chain_stats['offers']} Angebote, {chain_stats['pages']} Seiten")
    stats["chains"][chain_key] = chain_stats
    stats["total_brochures"] += chain_stats["brochures"]
    stats["total_offers"] += chain_stats["offers"]
    stats["total_pages"] += chain_stats["pages"]

    return chain_catalog


def main():
    print("=" * 70)
    print("  KaufDA FULL SCRAPE")
    print("  Alle Ketten x Alle Standorte x Alle Prospekte")
    print("=" * 70)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    all_catalogs = []

    for chain_key, chain_cfg in CHAINS.items():
        catalog = scrape_chain(chain_key, chain_cfg)
        if isinstance(catalog, dict) and "chain_key" in catalog:
            all_catalogs.append(catalog)
        time.sleep(0.5)

    # Save combined catalog (needed for build_kaufda_offers_db.py)
    catalog_path = OUTPUT_DIR / "brochures.json"
    catalog_path.write_text(json.dumps(all_catalogs, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary
    print("\n\n" + "=" * 70)
    print("  SCRAPE ERGEBNIS")
    print("=" * 70)
    print(f"{'Kette':<20} {'Prospekte':>10} {'Angebote':>10} {'Seiten':>8} {'Staedte':>8}")
    print("-" * 60)

    for chain_key, s in stats["chains"].items():
        name = CHAINS[chain_key]["display"]
        print(f"{name:<20} {s['brochures']:>10} {s['offers']:>10} {s['pages']:>8} {s['cities']:>8}")

    print("-" * 60)
    print(f"{'TOTAL':<20} {stats['total_brochures']:>10} {stats['total_offers']:>10} {stats['total_pages']:>8}")
    print(f"\nDaten gespeichert in: {OUTPUT_DIR}")
    print(f"Katalog: {catalog_path}")
    print(f"\nNaechster Schritt: DB bauen mit:")
    print(f"  python -m app.jobs.build_kaufda_offers_db \\")
    print(f"    --downloads-dir {DOWNLOADS_DIR} \\")
    print(f"    --output-path data/kaufda_dataset/offers.sqlite3 \\")
    print(f"    --catalog-path {catalog_path}")


if __name__ == "__main__":
    main()
