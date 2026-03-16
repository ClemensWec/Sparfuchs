"""Discover and download ALL KaufDA brochures using the FULL store registry (42K+ stores).

For each chain:
  1. Load ALL stores from the stores table (every single location)
  2. For stores without city_name: reverse-geocode from nearest known store
  3. Query KaufDA city page for EVERY store's location
  4. Also BFS-crawl KaufDA store pages for additional coverage
  5. Download metadata + pages for each unique brochure
  6. Save in format compatible with build_kaufda_offers_db.py

Usage:
    python -u scripts/scrape_all_brochures.py
    python -u scripts/scrape_all_brochures.py --chains lidl rewe
    python -u scripts/scrape_all_brochures.py --discover-only
    python -u scripts/scrape_all_brochures.py --resume
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
OUTPUT_DIR = ROOT_DIR / "data" / "kaufda_brochures" / "downloads"
STATE_PATH = ROOT_DIR / "data" / "kaufda_brochures" / "scrape_state.json"

KAUFDA_BASE = "https://www.kaufda.de"
CONTENT_VIEWER_BASE = "https://content-viewer-be.kaufda.de/v1/brochures"
VIEWER_HEADERS = {"Bonial-Api-Consumer": "web-content-viewer-fe"}
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Sparfuchs/1.0"

CHAIN_TARGETS = {
    "aldi-nord": {"display_name": "ALDI Nord", "global_slug": "Aldi-Nord", "chain_db": "Aldi"},
    "aldi-sued": {"display_name": "ALDI Sued", "global_slug": "Aldi-Sued", "chain_db": "Aldi"},
    "lidl": {"display_name": "Lidl", "global_slug": "Lidl", "chain_db": "Lidl"},
    "rewe": {"display_name": "REWE", "global_slug": "REWE", "chain_db": "Rewe"},
    "edeka": {"display_name": "Edeka", "global_slug": "Edeka", "chain_db": "Edeka"},
    "kaufland": {"display_name": "Kaufland", "global_slug": "Kaufland", "chain_db": "Kaufland"},
    "penny": {"display_name": "Penny", "global_slug": "Penny-Markt", "chain_db": "Penny"},
    "netto": {"display_name": "Netto Marken-Discount", "global_slug": "Netto-Marken-Discount", "chain_db": "Netto"},
    "norma": {"display_name": "Norma", "global_slug": "Norma", "chain_db": "Norma"},
    "globus": {"display_name": "Globus", "global_slug": "Globus", "chain_db": "Globus"},
    "marktkauf": {"display_name": "Marktkauf", "global_slug": "Marktkauf", "chain_db": "Marktkauf"},
}


def make_session() -> requests.Session:
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16))
    return s


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def city_to_slug(city_name: str) -> str:
    """Convert city name to KaufDA URL slug."""
    slug = re.sub(r'\s*\([^)]*\)', '', city_name)
    slug = slug.replace(" / ", "-").replace("/", "-").replace(" - ", "-")
    slug = slug.replace(" ", "-")
    slug = re.sub(r'[^\w\-äöüÄÖÜß]', '', slug)
    slug = re.sub(r'-+', '-', slug).strip('-')
    return slug


def get_chain_info(session: requests.Session, global_slug: str) -> dict | None:
    """Fetch build_id, local_url, local_id for a chain."""
    url = f"{KAUFDA_BASE}/Geschaefte/{global_slug}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
        if not match:
            return None
        payload = json.loads(match.group(1))
        pi = payload["props"]["pageProps"]["pageInformation"]
        pub = pi["publisher"]
        dc = pub.get("defaultCity", {})

        brochures = {}
        for bucket in ("viewer", "publisher"):
            for b in pi.get("brochures", {}).get(bucket, []):
                cid = b.get("contentId")
                if cid and cid not in brochures:
                    brochures[cid] = {
                        "content_id": cid,
                        "title": (b.get("title") or "?")[:80],
                        "page_count": b.get("pageCount", 0),
                        "valid_from": b.get("validFrom"),
                        "valid_until": b.get("validUntil"),
                        "discovered_at": "main_page",
                        "query_lat": str(dc.get("lat", 52.52)),
                        "query_lng": str(dc.get("lng", 13.405)),
                    }

        return {
            "build_id": payload["buildId"],
            "local_url": pub["localUrl"],
            "local_id": str(pub["localId"]),
            "default_lat": dc.get("lat", 52.52),
            "default_lng": dc.get("lng", 13.405),
            "brochures": brochures,
        }
    except Exception as e:
        print(f"  ERROR getting chain info for {global_slug}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Store loading: get ALL stores, resolve missing city_name via nearest neighbor
# ---------------------------------------------------------------------------

def load_all_stores_for_chain(db_path: Path, chain_db: str) -> list[dict]:
    """Load ALL stores for a chain from the stores table. Every single one."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, chain, lat, lon, postcode, city_name FROM stores WHERE chain = ?",
        (chain_db,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def resolve_missing_cities(stores: list[dict]) -> list[dict]:
    """For stores without city_name, assign city from nearest store that has one."""
    with_city = [s for s in stores if s.get("city_name")]
    without_city = [s for s in stores if not s.get("city_name")]

    if not without_city or not with_city:
        return stores

    print(f"    Resolving {len(without_city)} stores without city_name via nearest neighbor...")
    resolved = 0
    for store in without_city:
        best_dist = float("inf")
        best_city = None
        for ref in with_city:
            d = haversine_km(store["lat"], store["lon"], ref["lat"], ref["lon"])
            if d < best_dist:
                best_dist = d
                best_city = ref["city_name"]
        if best_city and best_dist < 20.0:  # max 20km radius
            store["city_name"] = best_city
            store["city_resolved"] = True
            resolved += 1

    print(f"    Resolved {resolved}/{len(without_city)} stores (within 20km)")
    return stores


# ---------------------------------------------------------------------------
# Phase 1a: Query KaufDA city pages from ALL store locations
# ---------------------------------------------------------------------------

def query_city_page(
    session: requests.Session,
    build_id: str,
    city_slug: str,
    local_url: str,
    local_id: str,
) -> list[dict]:
    """Query KaufDA Next.js data route for brochures at a city slug."""
    url = f"{KAUFDA_BASE}/_next/data/{build_id}/{city_slug}/{local_url}/p-r{local_id}.json"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code in (301, 404):
            return []
        if not resp.ok:
            return []
        data = resp.json()
        pi = data.get("pageProps", {}).get("pageInformation", {})
        location = pi.get("location") or pi.get("city") or {}
        lat = location.get("lat", 52.52)
        lng = location.get("lng", 13.405)

        results = []
        for bucket in ("viewer", "publisher"):
            for b in pi.get("brochures", {}).get(bucket, []):
                cid = b.get("contentId")
                if cid:
                    results.append({
                        "content_id": cid,
                        "title": (b.get("title") or "?")[:80],
                        "page_count": b.get("pageCount", 0),
                        "valid_from": b.get("validFrom"),
                        "valid_until": b.get("validUntil"),
                        "discovered_at": city_slug,
                        "query_lat": str(lat),
                        "query_lng": str(lng),
                    })
        return results
    except Exception:
        return []


def discover_brochures_from_all_stores(
    chain_key: str,
    cfg: dict,
    stores: list[dict],
    build_id: str,
    local_url: str,
    local_id: str,
    max_workers: int = 8,
) -> dict[str, dict]:
    """Process ALL stores → derive city slugs → query KaufDA for each unique slug.

    Every store is processed. HTTP responses are cached by city slug since
    KaufDA city pages are URL-determined. This means stores in the same city
    hit the cache, but every store is still individually accounted for.
    """
    all_brochures: dict[str, dict] = {}

    # Map every store to a city slug
    store_slugs: list[tuple[dict, str]] = []
    no_city_count = 0
    for store in stores:
        city = store.get("city_name", "")
        if not city:
            no_city_count += 1
            continue
        slug = city_to_slug(city)
        if slug:
            store_slugs.append((store, slug))

    # Get ALL unique slugs (preserving order of first appearance)
    unique_slugs = list(dict.fromkeys(slug for _, slug in store_slugs))

    print(f"    Total stores for chain: {len(stores)}")
    print(f"    Stores with city: {len(stores) - no_city_count}")
    print(f"    Stores without city (skipped): {no_city_count}")
    print(f"    Unique KaufDA city slugs to query: {len(unique_slugs)}")

    # HTTP cache: slug -> list of brochure dicts
    slug_cache: dict[str, list[dict]] = {}
    queried = 0
    started = time.time()

    def fetch_slug(slug: str) -> tuple[str, list[dict]]:
        worker_session = make_session()
        return slug, query_city_page(worker_session, build_id, slug, local_url, local_id)

    # Query all unique slugs concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_slug, slug): slug for slug in unique_slugs}
        for future in concurrent.futures.as_completed(futures):
            slug, results = future.result()
            slug_cache[slug] = results
            queried += 1

            for bro in results:
                cid = bro["content_id"]
                if cid not in all_brochures:
                    all_brochures[cid] = bro

            if queried % 200 == 0 or queried == len(unique_slugs):
                elapsed = time.time() - started
                print(
                    f"    [{cfg['display_name']}] {queried}/{len(unique_slugs)} slugs queried, "
                    f"{len(all_brochures)} unique brochures, {elapsed:.0f}s",
                )

    # Now process EVERY store and report coverage
    stores_matched = 0
    stores_no_brochures = 0
    for store, slug in store_slugs:
        cached = slug_cache.get(slug, [])
        if cached:
            stores_matched += 1
        else:
            stores_no_brochures += 1

    elapsed = time.time() - started
    print(f"    --- Store coverage ---")
    print(f"    Stores processed: {len(store_slugs)}/{len(stores)}")
    print(f"    Stores with KaufDA brochures: {stores_matched}")
    print(f"    Stores without KaufDA match: {stores_no_brochures}")
    print(f"    Unique brochures found: {len(all_brochures)}")
    print(f"    Duration: {elapsed:.0f}s")

    return all_brochures


# ---------------------------------------------------------------------------
# Phase 1b: BFS crawl KaufDA store pages for additional brochure discovery
# ---------------------------------------------------------------------------

def bfs_crawl_store_pages(
    chain_key: str,
    cfg: dict,
    build_id: str,
    local_url: str,
    local_id: str,
    max_workers: int = 8,
    page_limit: int | None = None,
) -> dict[str, dict]:
    """BFS-crawl KaufDA store pages starting from the default city seed.

    Each store page may list brochures specific to that store location,
    catching regional editions that city pages might not show.
    """
    session = make_session()

    # Fetch seed info
    info_url = f"{KAUFDA_BASE}/Geschaefte/{cfg['global_slug']}"
    resp = session.get(info_url, timeout=30)
    resp.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
    if not match:
        print(f"    BFS: Could not find __NEXT_DATA__ for {cfg['display_name']}")
        return {}
    payload = json.loads(match.group(1))
    pi = payload["props"]["pageProps"]["pageInformation"]
    pub = pi["publisher"]
    dc = pub.get("defaultCity", {})
    default_city_url = dc.get("url", "")

    seed = f"Filialen/{default_city_url}/{local_url}/v-r{local_id}"
    path_pattern = re.compile(rf'/Filialen/([^"\\]+/{re.escape(local_url)}/v-r{local_id})')

    queue: deque[str] = deque([seed])
    queued_set: set[str] = {seed}
    visited: set[str] = set()
    brochures: dict[str, dict] = {}
    failures = 0
    started = time.time()

    def fetch_path(store_path: str) -> tuple[str, dict | None, str | None]:
        worker_session = make_session()
        try:
            url = f"{KAUFDA_BASE}/_next/data/{build_id}/{store_path}.json"
            r = worker_session.get(url, timeout=30)
            r.raise_for_status()
            return store_path, r.json(), r.text
        except Exception as exc:
            return store_path, None, str(exc)

    while queue and (page_limit is None or len(visited) < page_limit):
        batch: list[str] = []
        while queue and len(batch) < max_workers and (page_limit is None or len(visited) + len(batch) < page_limit):
            path = queue.popleft()
            queued_set.discard(path)
            if path in visited:
                continue
            visited.add(path)
            batch.append(path)

        if not batch:
            continue

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for store_path, data, raw_text in executor.map(fetch_path, batch):
                if data is None:
                    failures += 1
                    continue

                page_info = data.get("pageProps", {}).get("pageInformation", {})
                location = page_info.get("location") or {}
                lat = location.get("lat", 52.52)
                lng = location.get("lng", 13.405)

                for bucket in ("viewer", "publisher"):
                    for b in page_info.get("brochures", {}).get(bucket, []):
                        cid = b.get("contentId")
                        if cid and cid not in brochures:
                            brochures[cid] = {
                                "content_id": cid,
                                "title": (b.get("title") or "?")[:80],
                                "page_count": b.get("pageCount", 0),
                                "valid_from": b.get("validFrom"),
                                "valid_until": b.get("validUntil"),
                                "discovered_at": f"bfs:{store_path[:60]}",
                                "query_lat": str(lat),
                                "query_lng": str(lng),
                            }

                if isinstance(raw_text, str):
                    for m in path_pattern.findall(raw_text):
                        next_path = f"Filialen/{m}"
                        if next_path not in visited and next_path not in queued_set:
                            queue.append(next_path)
                            queued_set.add(next_path)

        if len(visited) % 50 == 0:
            elapsed = time.time() - started
            print(
                f"    BFS [{cfg['display_name']}] visited={len(visited)} queue={len(queue)} "
                f"brochures={len(brochures)} fails={failures} {elapsed:.0f}s",
                end="\r",
            )

    print()  # newline after \r
    elapsed = time.time() - started
    print(
        f"    BFS done: {len(visited)} pages, {len(brochures)} brochures, "
        f"{failures} failures, {elapsed:.0f}s"
    )
    return brochures


# ---------------------------------------------------------------------------
# Phase 2: Download brochures
# ---------------------------------------------------------------------------

def download_brochure(
    session: requests.Session,
    content_id: str,
    query_lat: str,
    query_lng: str,
    chain_key: str,
    output_dir: Path,
) -> dict | None:
    """Download metadata + pages for a brochure."""
    brochure_dir = output_dir / chain_key / content_id
    metadata_path = brochure_dir / "metadata.json"
    pages_path = brochure_dir / "pages.json"

    if metadata_path.exists() and pages_path.exists():
        return {"status": "exists", "content_id": content_id}

    params = {"partner": "kaufda_web", "lat": query_lat, "lng": query_lng}

    try:
        resp = session.get(
            f"{CONTENT_VIEWER_BASE}/{content_id}",
            params=params,
            headers=VIEWER_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        metadata = resp.json()

        brochure_type = metadata.get("content", {}).get("type", "unknown")
        if brochure_type != "static_brochure":
            return {"status": "skipped", "content_id": content_id, "type": brochure_type}

        resp2 = session.get(
            f"{CONTENT_VIEWER_BASE}/{content_id}/pages",
            params=params,
            headers=VIEWER_HEADERS,
            timeout=30,
        )
        resp2.raise_for_status()
        pages = resp2.json()

        brochure_dir.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        pages_path.write_text(json.dumps(pages, ensure_ascii=False, indent=2), encoding="utf-8")

        offer_count = sum(len(p.get("offers", [])) for p in pages.get("contents", []))
        return {"status": "downloaded", "content_id": content_id, "offers": offer_count}

    except Exception as e:
        return {"status": "error", "content_id": content_id, "error": str(e)}


# ---------------------------------------------------------------------------
# State management for resume support
# ---------------------------------------------------------------------------

def load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover and download ALL KaufDA brochures using the full 42K store registry.")
    parser.add_argument("--chains", nargs="*", default=list(CHAIN_TARGETS.keys()))
    parser.add_argument("--discover-only", action="store_true", help="Only discover brochures, don't download.")
    parser.add_argument("--skip-bfs", action="store_true", help="Skip BFS store page crawl (Phase 1b).")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    parser.add_argument("--resume", action="store_true", help="Resume from saved state (skip completed chains).")
    parser.add_argument("--state-path", default=str(STATE_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    invalid = [c for c in args.chains if c not in CHAIN_TARGETS]
    if invalid:
        raise SystemExit(f"Unknown chains: {', '.join(invalid)}")

    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = Path(args.state_path)

    # Load or init state
    state = load_state(state_path) if args.resume else {}

    all_results: dict[str, dict[str, dict]] = {}
    grand_total_brochures = 0
    grand_total_offers = 0
    started = time.time()

    # ===== PHASE 1: Discover ALL brochures =====
    print("=" * 70)
    print("PHASE 1: Discover brochures from ALL 42K+ store locations")
    print("=" * 70)

    for chain_key in args.chains:
        cfg = CHAIN_TARGETS[chain_key]

        # Resume: skip if already discovered
        if args.resume and state.get(chain_key, {}).get("discovery_done"):
            prev = state[chain_key]
            brochure_ids = prev.get("brochure_ids", [])
            print(f"\n{cfg['display_name']}: already discovered ({len(brochure_ids)} brochures) — skipping")
            # Rebuild brochures dict from state
            all_results[chain_key] = {
                bid: {
                    "content_id": bid,
                    "title": "?",
                    "page_count": 0,
                    "discovered_at": "state",
                    "query_lat": prev.get("query_lat", "52.52"),
                    "query_lng": prev.get("query_lng", "13.405"),
                }
                for bid in brochure_ids
            }
            grand_total_brochures += len(brochure_ids)
            continue

        print(f"\n{'='*60}")
        print(f"{cfg['display_name']} ({chain_key})")
        print(f"{'='*60}")

        session = make_session()
        info = get_chain_info(session, cfg["global_slug"])
        if not info:
            print(f"  Could not get chain info for {cfg['display_name']}")
            continue

        build_id = info["build_id"]
        local_url = info["local_url"]
        local_id = info["local_id"]
        chain_brochures: dict[str, dict] = dict(info["brochures"])
        print(f"  Main page: {len(chain_brochures)} brochures")

        # --- Phase 1a: Query from ALL stores ---
        print(f"\n  Phase 1a: Query from ALL store locations")
        stores = load_all_stores_for_chain(db_path, cfg["chain_db"])
        stores = resolve_missing_cities(stores)

        store_brochures = discover_brochures_from_all_stores(
            chain_key, cfg, stores,
            build_id, local_url, local_id,
            max_workers=args.max_workers,
        )
        before = len(chain_brochures)
        chain_brochures.update(store_brochures)
        print(f"  Phase 1a result: +{len(chain_brochures) - before} new (total {len(chain_brochures)})")

        # --- Phase 1b: BFS store page crawl ---
        if not args.skip_bfs:
            print(f"\n  Phase 1b: BFS crawl KaufDA store pages")
            bfs_brochures = bfs_crawl_store_pages(
                chain_key, cfg,
                build_id, local_url, local_id,
                max_workers=args.max_workers,
            )
            before = len(chain_brochures)
            chain_brochures.update(bfs_brochures)
            print(f"  Phase 1b result: +{len(chain_brochures) - before} new (total {len(chain_brochures)})")

        all_results[chain_key] = chain_brochures
        grand_total_brochures += len(chain_brochures)

        # List discovered brochures
        for cid, bro in sorted(chain_brochures.items(), key=lambda x: x[1].get("title", "")):
            print(f"    {bro['title'][:55]:<55} ({bro.get('page_count', '?')}p) [{cid[:12]}...] via {bro['discovered_at']}")

        # Save state
        state[chain_key] = {
            "discovery_done": True,
            "brochure_ids": list(chain_brochures.keys()),
            "brochure_count": len(chain_brochures),
            "stores_total": len(stores),
            "query_lat": str(info["default_lat"]),
            "query_lng": str(info["default_lng"]),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        save_state(state_path, state)
        time.sleep(1.0)

    # Save discovery summary
    discovery_path = output_dir / "discovery.json"
    discovery_data = {
        chain_key: list(brochures.values())
        for chain_key, brochures in all_results.items()
    }
    discovery_path.parent.mkdir(parents=True, exist_ok=True)
    discovery_path.write_text(json.dumps(discovery_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nDiscovery saved: {discovery_path}")
    print(f"Total unique brochures across all chains: {grand_total_brochures}")

    if args.discover_only:
        return

    # ===== PHASE 2: Download all brochures =====
    print(f"\n{'='*70}")
    print("PHASE 2: Download metadata + pages for ALL brochures")
    print(f"{'='*70}")

    session = make_session()
    downloaded = 0
    skipped = 0
    errors = 0

    for chain_key, brochures in all_results.items():
        cfg = CHAIN_TARGETS[chain_key]
        print(f"\n{cfg['display_name']}: {len(brochures)} brochures to download")

        for i, (cid, bro) in enumerate(brochures.items(), 1):
            result = download_brochure(
                session, cid,
                bro.get("query_lat", "52.52"),
                bro.get("query_lng", "13.405"),
                chain_key, output_dir,
            )
            if result:
                status = result["status"]
                if status == "downloaded":
                    downloaded += 1
                    offers = result.get("offers", 0)
                    grand_total_offers += offers
                    print(f"  [{i}/{len(brochures)}] DL: {bro.get('title', '?')[:50]} -> {offers} offers")
                elif status == "exists":
                    skipped += 1
                elif status == "skipped":
                    skipped += 1
                    print(f"  [{i}/{len(brochures)}] SKIP: {bro.get('title', '?')[:50]} (type: {result.get('type')})")
                elif status == "error":
                    errors += 1
                    print(f"  [{i}/{len(brochures)}] ERR: {bro.get('title', '?')[:50]} -> {result.get('error')}", file=sys.stderr)

            if result and result["status"] in ("downloaded", "error"):
                time.sleep(0.15)

    # ===== SUMMARY =====
    elapsed = time.time() - started
    print(f"\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"Total stores in registry:    42,240+")
    print(f"Brochures discovered:        {grand_total_brochures}")
    print(f"Downloaded (new):            {downloaded}")
    print(f"Already existed:             {skipped}")
    print(f"Errors:                      {errors}")
    print(f"Total offers extracted:       {grand_total_offers}")
    print(f"Duration:                    {elapsed:.0f}s")
    print(f"Output:                      {output_dir}")
    print(f"State:                       {state_path}")

    print(f"\n{'Chain':<30} {'Stores':>8} {'Brochures':>10}")
    print("-" * 55)
    for chain_key, brochures in all_results.items():
        cfg = CHAIN_TARGETS[chain_key]
        store_count = state.get(chain_key, {}).get("stores_total", "?")
        print(f"{cfg['display_name']:<30} {store_count:>8} {len(brochures):>10}")
    print("-" * 55)
    print(f"{'TOTAL':<30} {'':>8} {grand_total_brochures:>10}")


if __name__ == "__main__":
    main()
