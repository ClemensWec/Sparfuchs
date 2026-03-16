"""Import ALL store locations from official chain APIs + KaufDA BFS crawl.

Sources:
  - Aldi Nord: Uberall API (single call, ~2200 stores)
  - Aldi Sued: Uberall API (single call, ~1950 stores)
  - Kaufland: filiale.kaufland.de JSON (single call, ~780 stores)
  - Globus: globus.de API (single POST, ~60 stores)
  - Lidl: Bing Maps Spatial Data (paginated, ~3255 stores)
  - Edeka: edeka.de API (paginated, ~6000 stores)
  - Norma: norma-online.de geo-search (grid, ~1350 stores)
  - Penny: penny.de API (partial, then KaufDA supplement)
  - REWE/Netto/Marktkauf: KaufDA BFS crawl fallback

Usage:
    python scripts/import_all_stores.py
    python scripts/import_all_stores.py --chains aldi-nord lidl
    python scripts/import_all_stores.py --skip-kaufda
"""
import argparse
import concurrent.futures
import json
import math
import re
import sqlite3
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "data" / "kaufda_dataset" / "offers.sqlite3"

KAUFDA_BASE = "https://www.kaufda.de"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Sparfuchs/1.0"


def make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16))
    return s


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def upsert_store(conn: sqlite3.Connection, *, osm_type: str, osm_id: int, name: str,
                 chain: str, lat: float, lon: float, address: str = "",
                 postcode: str = "", city_name: str = "", source: str = "api") -> None:
    now = utc_now()
    conn.execute("""
        INSERT INTO stores (osm_type, osm_id, name, chain, lat, lon, address, postcode, city_name, source, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(osm_type, osm_id) DO UPDATE SET
            name=excluded.name, chain=excluded.chain, lat=excluded.lat, lon=excluded.lon,
            address=excluded.address, postcode=excluded.postcode, city_name=excluded.city_name,
            source=excluded.source, updated_at=excluded.updated_at
    """, (osm_type, osm_id, name, chain, lat, lon, address, postcode, city_name, source, now, now))


# ============================================================
# ALDI NORD - Uberall API
# ============================================================
def import_aldi_nord(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Aldi Nord from Uberall API...")
    resp = session.get(
        "https://uberall.com/api/storefinders/ALDINORDDE_UimhY3MWJaxhjK9QdZo3Qa4chq1MAu/locations/all",
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    locations = data.get("response", {}).get("locations", [])
    count = 0
    for loc in locations:
        lat = loc.get("lat")
        lng = loc.get("lng")
        if not lat or not lng:
            continue
        store_id = loc.get("id") or loc.get("identifier", count)
        upsert_store(
            conn,
            osm_type="aldi_nord_api",
            osm_id=int(store_id) if str(store_id).isdigit() else hash(str(store_id)) & 0x7FFFFFFF,
            name=f"ALDI Nord {loc.get('city', '')}".strip(),
            chain="Aldi",
            lat=float(lat),
            lon=float(lng),
            address=loc.get("streetAndNumber", ""),
            postcode=loc.get("zip", ""),
            city_name=loc.get("city", ""),
            source="aldi_nord_api",
        )
        count += 1
    conn.commit()
    return count


# ============================================================
# ALDI SUED - Uberall API
# ============================================================
def import_aldi_sued(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Aldi Sued from Uberall API...")
    resp = session.get(
        "https://uberall.com/api/storefinders/gqNws2nRfBBlQJS9UrA8zV9txngvET/locations/all",
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    locations = data.get("response", {}).get("locations", [])
    count = 0
    for loc in locations:
        lat = loc.get("lat")
        lng = loc.get("lng")
        if not lat or not lng:
            continue
        store_id = loc.get("id") or loc.get("identifier", count)
        upsert_store(
            conn,
            osm_type="aldi_sued_api",
            osm_id=int(store_id) if str(store_id).isdigit() else hash(str(store_id)) & 0x7FFFFFFF,
            name=f"ALDI Sued {loc.get('city', '')}".strip(),
            chain="Aldi",
            lat=float(lat),
            lon=float(lng),
            address=loc.get("streetAndNumber", ""),
            postcode=loc.get("zip", ""),
            city_name=loc.get("city", ""),
            source="aldi_sued_api",
        )
        count += 1
    conn.commit()
    return count


# ============================================================
# KAUFLAND - filiale.kaufland.de
# ============================================================
def import_kaufland(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Kaufland from filiale.kaufland.de...")
    resp = session.get("https://filiale.kaufland.de/.klstorefinder.json", timeout=30)
    resp.raise_for_status()
    stores = resp.json()
    count = 0
    for s in stores:
        lat = s.get("lat")
        lng = s.get("lng")
        if not lat or not lng:
            continue
        store_num = s.get("n", str(count))
        sid = int(re.sub(r'\D', '', str(store_num)) or count) or count
        upsert_store(
            conn,
            osm_type="kaufland_api",
            osm_id=sid,
            name=f"Kaufland {s.get('t', '')}".strip(),
            chain="Kaufland",
            lat=float(lat),
            lon=float(lng),
            address=s.get("sn", ""),
            postcode=s.get("pc", ""),
            city_name=s.get("t", ""),
            source="kaufland_api",
        )
        count += 1
    conn.commit()
    return count


# ============================================================
# GLOBUS - globus.de API
# ============================================================
def import_globus(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Globus from globus.de API...")
    resp = session.post(
        "https://www.globus.de/api/open",
        data={"type": "maerkte"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    # Globus API returns {"success": true, "data": {"1001": {...}, "1002": {...}}}
    raw = data.get("data", data)
    if isinstance(raw, dict):
        markets = list(raw.values())
    elif isinstance(raw, list):
        markets = raw
    else:
        markets = []
    count = 0
    for m in markets:
        if not isinstance(m, dict):
            continue
        lat = m.get("breitengrad") or m.get("lat")
        lng = m.get("laengengrad") or m.get("lng")
        if not lat or not lng:
            continue
        sid = m.get("nummer") or m.get("id", count)
        upsert_store(
            conn,
            osm_type="globus_api",
            osm_id=int(sid) if str(sid).isdigit() else hash(str(sid)) & 0x7FFFFFFF,
            name=m.get("name", f"Globus {m.get('stadt', '')}").strip(),
            chain="Globus",
            lat=float(lat),
            lon=float(lng),
            address=m.get("strasse", ""),
            postcode=m.get("plz", ""),
            city_name=m.get("stadt", ""),
            source="globus_api",
        )
        count += 1
    conn.commit()
    return count


# ============================================================
# LIDL - Bing Maps Spatial Data
# ============================================================
def import_lidl(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Lidl from Bing Maps Spatial Data...")
    base = "https://spatial.virtualearth.net/REST/v1/data/ab055fcbaac04ec4bc563e65ffa07097/Filialdaten-SEC/Filialdaten-SEC"
    key = "AnTPGpOQpGHsC_ryx9LY3fRTI27dwcRWuPrfg93-WZR2m-1ax9e9ghlD4s1RaHOq"

    # Get total count
    count_resp = session.get(base, params={
        "key": key, "$filter": "Adresstyp Eq 1", "$format": "json",
        "$top": "1", "$inlinecount": "allpages",
    }, timeout=30)
    count_resp.raise_for_status()
    total = count_resp.json().get("d", {}).get("__count", 3500)
    total = int(total)
    print(f"    Total: {total} Lidl stores")

    count = 0
    page_size = 250
    for skip in range(0, total + page_size, page_size):
        resp = session.get(base, params={
            "key": key, "$filter": "Adresstyp Eq 1", "$format": "json",
            "$top": str(page_size), "$skip": str(skip),
        }, timeout=30)
        resp.raise_for_status()
        results = resp.json().get("d", {}).get("results", [])
        if not results:
            break
        for s in results:
            lat = s.get("Latitude")
            lng = s.get("Longitude")
            if not lat or not lng:
                continue
            eid = s.get("EntityID", count)
            sid = int(re.sub(r'\D', '', str(eid)) or count) or count
            upsert_store(
                conn,
                osm_type="lidl_api",
                osm_id=sid,
                name=f"Lidl {s.get('Locality', '')}".strip(),
                chain="Lidl",
                lat=float(lat),
                lon=float(lng),
                address=s.get("AddressLine", ""),
                postcode=s.get("PostalCode", ""),
                city_name=s.get("Locality", ""),
                source="lidl_bing_api",
            )
            count += 1
        time.sleep(0.2)
    conn.commit()
    return count


# ============================================================
# EDEKA - edeka.de API (paginated, limit=10)
# ============================================================
def import_edeka(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Edeka from edeka.de API (paginated, ~600 requests)...")
    base = "https://www.edeka.de/api/marketsearch/markets"
    # Use center-of-Germany coordinates
    lat, lon = 51.1657, 10.4515

    # Need browser-like headers for Edeka
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.edeka.de/marktsuche.jsp",
        "Accept-Language": "de-DE,de;q=0.9",
    }

    # First request to get total
    try:
        resp = session.get(base, params={"lat": str(lat), "lon": str(lon), "limit": "10", "offset": "0"},
                          headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        total = data.get("totalCount", 6000)
    except Exception as e:
        print(f"    First request failed ({e}), trying alternative approach...")
        # Edeka blocks? Fall back to KaufDA crawl later
        return 0

    print(f"    Total: {total} Edeka/Marktkauf stores")

    count = 0
    for offset in range(0, total + 10, 10):
        try:
            resp = session.get(base, params={
                "lat": str(lat), "lon": str(lon),
                "limit": "10", "offset": str(offset),
            }, headers=headers, timeout=30)
            resp.raise_for_status()
            markets = resp.json().get("markets", [])
            if not markets:
                break
            for m in markets:
                coords = m.get("coordinates", {})
                mlat = coords.get("lat")
                mlng = coords.get("lng")
                if not mlat or not mlng:
                    continue
                addr = m.get("address", {})
                mid = m.get("id", count)
                sid = int(re.sub(r'\D', '', str(mid)) or count) or count

                # Determine chain: Marktkauf vs Edeka
                dist_channel = (m.get("distributionChannel") or "").lower()
                market_name = m.get("name", "")
                if "marktkauf" in dist_channel or "marktkauf" in market_name.lower():
                    chain_name = "Marktkauf"
                else:
                    chain_name = "Edeka"

                upsert_store(
                    conn,
                    osm_type="edeka_api",
                    osm_id=sid,
                    name=market_name or f"{chain_name} {addr.get('city', '')}".strip(),
                    chain=chain_name,
                    lat=float(mlat),
                    lon=float(mlng),
                    address=addr.get("street", ""),
                    postcode=addr.get("zipCode", ""),
                    city_name=addr.get("city", ""),
                    source="edeka_api",
                )
                count += 1
        except Exception as e:
            print(f"    Error at offset {offset}: {e}", file=sys.stderr)

        if offset % 200 == 0 and offset > 0:
            conn.commit()
            print(f"    ... {count} stores at offset {offset}/{total}", end="\r")
            time.sleep(0.1)

    conn.commit()
    print()
    return count


# ============================================================
# NORMA - norma-online.de geo-grid search
# ============================================================
def import_norma(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Norma via geo-grid search...")
    # Germany bounding box: lat 47.2-55.1, lng 5.9-15.1
    # With 80km radius, ~25 grid points cover Germany
    grid_points = []
    for lat in [47.5, 48.5, 49.5, 50.5, 51.5, 52.5, 53.5, 54.5]:
        for lng in [6.5, 8.0, 9.5, 11.0, 12.5, 14.0]:
            grid_points.append((lat, lng))

    seen_stores: set[str] = set()  # Deduplicate by lat+lng
    count = 0

    for lat, lng in grid_points:
        try:
            resp = session.get(
                f"https://www.norma-online.de/de/filialfinder/suchergebnis",
                params={"lng": str(lng), "lat": str(lat), "r": "80000"},
                timeout=30,
            )
            if not resp.ok:
                continue

            # Parse HTML for store data - Norma returns HTML with lat/lng in data attributes or script
            text = resp.text

            # Try to find JSON data in the response
            json_match = re.search(r'var\s+stores\s*=\s*(\[.*?\]);', text, re.DOTALL)
            if json_match:
                stores = json.loads(json_match.group(1))
                for s in stores:
                    slat = s.get("lat") or s.get("latitude")
                    slng = s.get("lng") or s.get("longitude") or s.get("lon")
                    if not slat or not slng:
                        continue
                    key = f"{float(slat):.5f},{float(slng):.5f}"
                    if key in seen_stores:
                        continue
                    seen_stores.add(key)
                    upsert_store(
                        conn,
                        osm_type="norma_api",
                        osm_id=hash(key) & 0x7FFFFFFF,
                        name=s.get("name", f"Norma {s.get('city', '')}").strip(),
                        chain="Norma",
                        lat=float(slat),
                        lon=float(slng),
                        address=s.get("street", s.get("address", "")),
                        postcode=s.get("zip", s.get("postcode", "")),
                        city_name=s.get("city", s.get("ort", "")),
                        source="norma_api",
                    )
                    count += 1
                continue

            # Fallback: parse markers from HTML
            # Pattern: data-lat="..." data-lng="..."
            marker_pattern = re.compile(
                r'data-lat=["\']([^"\']+)["\'].*?data-lng=["\']([^"\']+)["\']',
                re.DOTALL,
            )
            for m in marker_pattern.finditer(text):
                slat, slng = m.group(1), m.group(2)
                key = f"{float(slat):.5f},{float(slng):.5f}"
                if key in seen_stores:
                    continue
                seen_stores.add(key)
                upsert_store(
                    conn,
                    osm_type="norma_api",
                    osm_id=hash(key) & 0x7FFFFFFF,
                    name="Norma",
                    chain="Norma",
                    lat=float(slat),
                    lon=float(slng),
                    source="norma_geo",
                )
                count += 1

            # Also try: lat/lng pairs in script tags
            coord_pattern = re.compile(r'"lat"\s*:\s*([\d.]+)\s*,\s*"lng"\s*:\s*([\d.]+)')
            for m in coord_pattern.finditer(text):
                slat, slng = m.group(1), m.group(2)
                if 47.0 < float(slat) < 56.0 and 5.0 < float(slng) < 16.0:
                    key = f"{float(slat):.5f},{float(slng):.5f}"
                    if key in seen_stores:
                        continue
                    seen_stores.add(key)
                    upsert_store(
                        conn,
                        osm_type="norma_api",
                        osm_id=hash(key) & 0x7FFFFFFF,
                        name="Norma",
                        chain="Norma",
                        lat=float(slat),
                        lon=float(slng),
                        source="norma_geo",
                    )
                    count += 1

            time.sleep(0.5)
        except Exception as e:
            print(f"    Grid point ({lat},{lng}) error: {e}", file=sys.stderr)

    conn.commit()
    return count


# ============================================================
# PENNY - penny.de API
# ============================================================
def import_penny(conn: sqlite3.Connection, session: requests.Session) -> int:
    print("  Fetching Penny from penny.de API...")
    count = 0
    try:
        resp = session.get("https://www.penny.de/.rest/market", timeout=30)
        if resp.ok:
            markets = resp.json()
            if isinstance(markets, list):
                for m in markets:
                    lat = m.get("latitude")
                    lng = m.get("longitude")
                    if not lat or not lng:
                        continue
                    mid = m.get("marketId", count)
                    sid = int(re.sub(r'\D', '', str(mid)) or count) or count
                    upsert_store(
                        conn,
                        osm_type="penny_api",
                        osm_id=sid,
                        name=m.get("marketName", f"Penny {m.get('city', '')}").strip(),
                        chain="Penny",
                        lat=float(lat),
                        lon=float(lng),
                        address=m.get("streetWithHouseNumber", ""),
                        postcode=m.get("zipCode", ""),
                        city_name=m.get("city", ""),
                        source="penny_api",
                    )
                    count += 1
    except Exception as e:
        print(f"    Penny API error: {e}", file=sys.stderr)

    # If we got too few, try geo-grid approach
    if count < 500:
        print(f"    Only {count} from main API, trying geo queries...")
        seen = set()
        for lat in [48.0, 49.5, 51.0, 52.5, 54.0]:
            for lng in [7.0, 9.0, 11.0, 13.0]:
                try:
                    resp = session.get(
                        "https://www.penny.de/.rest/market",
                        params={"latitude": str(lat), "longitude": str(lng)},
                        timeout=30,
                    )
                    if resp.ok:
                        markets = resp.json() if isinstance(resp.json(), list) else []
                        for m in markets:
                            mlat = m.get("latitude")
                            mlng = m.get("longitude")
                            if not mlat or not mlng:
                                continue
                            mid = m.get("marketId", "")
                            key = str(mid)
                            if key in seen:
                                continue
                            seen.add(key)
                            sid = int(re.sub(r'\D', '', str(mid)) or count) or count
                            upsert_store(
                                conn,
                                osm_type="penny_api",
                                osm_id=sid,
                                name=m.get("marketName", f"Penny {m.get('city', '')}").strip(),
                                chain="Penny",
                                lat=float(mlat),
                                lon=float(mlng),
                                address=m.get("streetWithHouseNumber", ""),
                                postcode=m.get("zipCode", ""),
                                city_name=m.get("city", ""),
                                source="penny_api",
                            )
                            count += 1
                    time.sleep(0.3)
                except Exception:
                    pass

    conn.commit()
    return count


# ============================================================
# KAUFDA BFS CRAWL - for REWE, Netto, Marktkauf, and supplementing others
# ============================================================
KAUFDA_CHAINS = {
    "rewe": {"display_name": "REWE", "global_slug": "REWE", "chain": "Rewe"},
    "netto": {"display_name": "Netto Marken-Discount", "global_slug": "Netto-Marken-Discount", "chain": "Netto"},
    "edeka-kd": {"display_name": "Edeka", "global_slug": "Edeka", "chain": "Edeka"},
    "marktkauf": {"display_name": "Marktkauf", "global_slug": "Marktkauf", "chain": "Marktkauf"},
    "penny-kd": {"display_name": "Penny", "global_slug": "Penny-Markt", "chain": "Penny"},
    "norma-kd": {"display_name": "Norma", "global_slug": "Norma", "chain": "Norma"},
    "aldi-nord-kd": {"display_name": "ALDI Nord", "global_slug": "Aldi-Nord", "chain": "Aldi"},
    "aldi-sued-kd": {"display_name": "ALDI Sued", "global_slug": "Aldi-Sued", "chain": "Aldi"},
    "lidl-kd": {"display_name": "Lidl", "global_slug": "Lidl", "chain": "Lidl"},
    "kaufland-kd": {"display_name": "Kaufland", "global_slug": "Kaufland", "chain": "Kaufland"},
    "globus-kd": {"display_name": "Globus", "global_slug": "Globus", "chain": "Globus"},
}


def kaufda_crawl_chain(conn: sqlite3.Connection, chain_key: str, cfg: dict, max_workers: int = 8) -> int:
    """BFS crawl through KaufDA store pages for a chain."""
    session = make_session()
    print(f"  KaufDA BFS crawl: {cfg['display_name']}...")

    # Get build_id and seed
    url = f"{KAUFDA_BASE}/Geschaefte/{cfg['global_slug']}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
    if not match:
        print(f"    No __NEXT_DATA__ found!", file=sys.stderr)
        return 0
    payload = json.loads(match.group(1))
    pi = payload["props"]["pageProps"]["pageInformation"]
    pub = pi["publisher"]
    dc = pub["defaultCity"]
    build_id = payload["buildId"]
    local_url = pub["localUrl"]
    local_id = str(pub["localId"])

    seed = f"Filialen/{dc['url']}/{local_url}/v-r{local_id}"
    path_pattern = re.compile(rf"/Filialen/([^\"\\]+/{re.escape(local_url)}/v-r{local_id})")

    queue: deque[str] = deque([seed])
    queued: set[str] = {seed}
    visited: set[str] = set()
    count = 0
    failures = 0
    started = time.time()

    def fetch_path(store_path: str) -> tuple[str, dict | None, str | None]:
        ws = make_session()
        try:
            r = ws.get(f"{KAUFDA_BASE}/_next/data/{build_id}/{store_path}.json", timeout=30)
            r.raise_for_status()
            return store_path, r.json(), r.text
        except Exception as exc:
            return store_path, None, str(exc)

    while queue:
        batch: list[str] = []
        while queue and len(batch) < max_workers:
            p = queue.popleft()
            queued.discard(p)
            if p in visited:
                continue
            visited.add(p)
            batch.append(p)

        if not batch:
            continue

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for store_path, data, raw_text in executor.map(fetch_path, batch):
                if data is None:
                    failures += 1
                    continue

                page_info = data.get("pageProps", {}).get("pageInformation", {})
                location = page_info.get("location") or {}
                city_info = page_info.get("city") or {}

                slat = location.get("lat")
                slng = location.get("lng")
                if slat and slng:
                    city = location.get("city") or city_info.get("displayName", "")
                    street = location.get("street", "")
                    zipcode = location.get("zip", "")
                    store_name = location.get("name") or f"{cfg['display_name']} {city}"

                    path_hash = hash(store_path) & 0x7FFFFFFF
                    upsert_store(
                        conn,
                        osm_type=f"kaufda_{chain_key}",
                        osm_id=path_hash,
                        name=store_name.strip(),
                        chain=cfg["chain"],
                        lat=float(slat),
                        lon=float(slng),
                        address=street,
                        postcode=zipcode,
                        city_name=city,
                        source="kaufda_crawl",
                    )
                    count += 1

                # Discover neighbors
                if isinstance(raw_text, str):
                    for m in path_pattern.findall(raw_text):
                        next_path = f"Filialen/{m}"
                        if next_path not in visited and next_path not in queued:
                            queue.append(next_path)
                            queued.add(next_path)

        conn.commit()
        elapsed = time.time() - started
        print(
            f"    [{cfg['display_name']}] visited={len(visited)} queue={len(queue)} "
            f"stores={count} fails={failures} {elapsed:.0f}s",
            end="\r",
        )

    print()
    conn.commit()
    return count


# ============================================================
# MAIN
# ============================================================
IMPORTERS = {
    "aldi-nord": ("Aldi Nord", import_aldi_nord),
    "aldi-sued": ("Aldi Sued", import_aldi_sued),
    "kaufland": ("Kaufland", import_kaufland),
    "globus": ("Globus", import_globus),
    "lidl": ("Lidl", import_lidl),
    "edeka": ("Edeka + Marktkauf", import_edeka),
    "norma": ("Norma", import_norma),
    "penny": ("Penny", import_penny),
}


def parse_args() -> argparse.Namespace:
    all_chains = list(IMPORTERS.keys()) + list(KAUFDA_CHAINS.keys())
    parser = argparse.ArgumentParser(description="Import ALL store locations from official APIs + KaufDA.")
    parser.add_argument("--chains", nargs="*", default=None, help="Specific chains to import.")
    parser.add_argument("--skip-kaufda", action="store_true", help="Skip KaufDA BFS crawl.")
    parser.add_argument("--kaufda-only", action="store_true", help="Only run KaufDA BFS crawl.")
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--max-workers", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.commit()

    # Count before
    before = conn.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    print(f"Stores in DB before: {before}")
    print("=" * 60)

    results = {}
    started = time.time()

    # Phase 1: Official APIs
    if not args.kaufda_only:
        print("\n=== PHASE 1: Official Store APIs ===\n")
        for key, (name, importer) in IMPORTERS.items():
            if args.chains and key not in args.chains:
                continue
            print(f"\n{name}:")
            try:
                count = importer(conn, make_session())
                results[key] = count
                print(f"  -> {count} stores imported")
            except Exception as e:
                print(f"  -> ERROR: {e}", file=sys.stderr)
                results[key] = f"ERROR: {e}"

    # Phase 2: KaufDA BFS crawl
    if not args.skip_kaufda:
        print("\n\n=== PHASE 2: KaufDA BFS Crawl ===\n")
        for key, cfg in KAUFDA_CHAINS.items():
            if args.chains and key not in args.chains:
                continue
            print(f"\n{cfg['display_name']}:")
            try:
                count = kaufda_crawl_chain(conn, key, cfg, max_workers=args.max_workers)
                results[f"kaufda_{key}"] = count
                print(f"  -> {count} stores from KaufDA crawl")
            except Exception as e:
                print(f"  -> ERROR: {e}", file=sys.stderr)
                results[f"kaufda_{key}"] = f"ERROR: {e}"

    # Update metadata
    total_stores = conn.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    try:
        conn.execute("INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)", ("stores", str(total_stores)))
        conn.execute("INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)", ("stores_last_import_at", utc_now()))
        conn.commit()
    except Exception:
        conn.commit()

    # Summary
    elapsed = time.time() - started
    print(f"\n\n{'='*60}")
    print("IMPORT SUMMARY")
    print(f"{'='*60}")
    print(f"\nImport results:")
    for key, val in results.items():
        print(f"  {key:<25} {val}")

    print(f"\n{'Chain':<25} {'Count':>8}")
    print("-" * 35)
    total = 0
    for row in conn.execute("SELECT chain, COUNT(*) FROM stores GROUP BY chain ORDER BY COUNT(*) DESC"):
        print(f"{row[0]:<25} {row[1]:>8}")
        total += row[1]
    print("-" * 35)
    print(f"{'TOTAL':<25} {total:>8}")
    print(f"\nBefore: {before} -> After: {total} (+{total - before})")
    print(f"Duration: {elapsed:.0f}s")

    conn.close()


if __name__ == "__main__":
    main()
