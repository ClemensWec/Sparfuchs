"""Build brochure-to-store mapping by querying KaufDA city pages.

For each chain:
  1. Load all stores from DB
  2. Compute KaufDA city slug for each store
  3. Query each unique city slug on KaufDA
  4. Record which brochure content_ids each slug returns
  5. Save mapping file

The mapping file is consumed by build_kaufda_offers_db.py to populate
the brochure_stores junction table.

Usage:
    python scripts/build_brochure_mapping.py
    python scripts/build_brochure_mapping.py --chains rewe kaufland
"""
import argparse
import concurrent.futures
import json
import math
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "data" / "kaufda_dataset" / "offers.sqlite3"
OUTPUT_PATH = ROOT_DIR / "data" / "kaufda_brochures" / "brochure_mapping.json"

KAUFDA_BASE = "https://www.kaufda.de"
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


def city_to_slug(city_name: str) -> str:
    slug = re.sub(r"\s*\([^)]*\)", "", city_name)
    slug = slug.replace(" / ", "-").replace("/", "-").replace(" - ", "-").replace(" ", "-")
    slug = re.sub(r"[^\w\-äöüÄÖÜß]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_chain_info(session: requests.Session, global_slug: str) -> dict | None:
    url = f"{KAUFDA_BASE}/Geschaefte/{global_slug}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
    if not match:
        return None
    payload = json.loads(match.group(1))
    pi = payload["props"]["pageProps"]["pageInformation"]
    pub = pi["publisher"]
    return {
        "build_id": payload["buildId"],
        "local_url": pub["localUrl"],
        "local_id": str(pub["localId"]),
    }


def query_city_page(session: requests.Session, build_id: str, city_slug: str, local_url: str, local_id: str) -> list[str]:
    url = f"{KAUFDA_BASE}/_next/data/{build_id}/{city_slug}/{local_url}/p-r{local_id}.json"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code in (301, 404) or not resp.ok:
            return []
        data = resp.json()
        pi = data.get("pageProps", {}).get("pageInformation", {})
        results = []
        for bucket in ("viewer", "publisher"):
            for b in pi.get("brochures", {}).get(bucket, []):
                cid = b.get("contentId")
                if cid and cid not in results:
                    results.append(cid)
        return results
    except Exception:
        return []


def load_stores_for_chain(db_path: Path, chain_db: str) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, chain, lat, lon, postcode, city_name FROM stores WHERE chain = ?",
        (chain_db,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def resolve_missing_cities(stores: list[dict]) -> list[dict]:
    with_city = [s for s in stores if s.get("city_name")]
    without_city = [s for s in stores if not s.get("city_name")]
    if not without_city or not with_city:
        return stores
    resolved = 0
    for store in without_city:
        best_dist = float("inf")
        best_city = None
        for ref in with_city:
            d = haversine_km(store["lat"], store["lon"], ref["lat"], ref["lon"])
            if d < best_dist:
                best_dist = d
                best_city = ref["city_name"]
        if best_city and best_dist < 20.0:
            store["city_name"] = best_city
            resolved += 1
    if resolved:
        print(f"    Resolved {resolved}/{len(without_city)} stores without city")
    return stores


def discover_mapping_for_chain(
    chain_key: str,
    cfg: dict,
    db_path: Path,
    max_workers: int = 8,
) -> dict | None:
    session = make_session()
    try:
        info = get_chain_info(session, cfg["global_slug"])
    except Exception as e:
        print(f"  ERROR getting chain info: {e}", file=sys.stderr)
        return None
    if not info:
        print(f"  Could not get chain info for {cfg['display_name']}")
        return None

    build_id = info["build_id"]
    local_url = info["local_url"]
    local_id = info["local_id"]

    stores = load_stores_for_chain(db_path, cfg["chain_db"])
    stores = resolve_missing_cities(stores)

    # Build store_id → city_slug mapping
    store_city_slugs: dict[str, str] = {}
    for store in stores:
        city = store.get("city_name", "")
        if city:
            slug = city_to_slug(city)
            if slug:
                store_city_slugs[str(store["id"])] = slug

    unique_slugs = sorted(set(store_city_slugs.values()))
    print(f"    {len(stores)} stores, {len(store_city_slugs)} with slug, {len(unique_slugs)} unique slugs")

    # Query all unique slugs concurrently
    slug_brochures: dict[str, list[str]] = {}
    queried = 0
    started = time.time()

    def fetch_slug(slug: str) -> tuple[str, list[str]]:
        worker_session = make_session()
        return slug, query_city_page(worker_session, build_id, slug, local_url, local_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_slug, slug): slug for slug in unique_slugs}
        for future in concurrent.futures.as_completed(futures):
            slug, content_ids = future.result()
            if content_ids:
                slug_brochures[slug] = content_ids
            queried += 1
            if queried % 200 == 0 or queried == len(unique_slugs):
                elapsed = time.time() - started
                total_bro = len({cid for cids in slug_brochures.values() for cid in cids})
                print(f"    {queried}/{len(unique_slugs)} slugs, {total_bro} brochures, {elapsed:.0f}s")

    all_brochure_ids = {cid for cids in slug_brochures.values() for cid in cids}
    elapsed = time.time() - started
    print(f"    Done: {len(slug_brochures)} slugs with brochures, {len(all_brochure_ids)} unique brochures, {elapsed:.0f}s")

    return {
        "slug_brochures": slug_brochures,
        "store_city_slugs": store_city_slugs,
        "unique_brochure_count": len(all_brochure_ids),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build brochure-to-store mapping via KaufDA city pages.")
    parser.add_argument("--chains", nargs="*", default=list(CHAIN_TARGETS.keys()))
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--output", default=str(OUTPUT_PATH))
    parser.add_argument("--max-workers", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    output_path = Path(args.output)

    invalid = [c for c in args.chains if c not in CHAIN_TARGETS]
    if invalid:
        raise SystemExit(f"Unknown chains: {', '.join(invalid)}")

    result = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "chains": {},
    }

    for chain_key in args.chains:
        cfg = CHAIN_TARGETS[chain_key]
        print(f"\n{cfg['display_name']} ({chain_key})")
        mapping = discover_mapping_for_chain(chain_key, cfg, db_path, max_workers=args.max_workers)
        if mapping:
            result["chains"][chain_key] = mapping

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    print(f"\nMapping saved: {output_path}")

    total_stores = sum(len(c["store_city_slugs"]) for c in result["chains"].values())
    total_brochures = sum(c["unique_brochure_count"] for c in result["chains"].values())
    print(f"Stores mapped: {total_stores}")
    print(f"Brochures covered: {total_brochures}")


if __name__ == "__main__":
    main()
