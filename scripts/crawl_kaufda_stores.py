"""Crawl KaufDA store pages via BFS to build a COMPLETE store registry.

For each chain, starts from a seed store page and follows neighbor links
to discover ALL stores across Germany. Saves to SQLite stores table.

Usage:
    python scripts/crawl_kaufda_stores.py
    python scripts/crawl_kaufda_stores.py --chains lidl rewe
    python scripts/crawl_kaufda_stores.py --resume  (continue from saved state)
"""
import argparse
import concurrent.futures
import json
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
STATE_PATH = ROOT_DIR / "data" / "kaufda_stores_crawl_state.json"

CHAIN_TARGETS = {
    "aldi-nord": {"display_name": "ALDI Nord", "global_slug": "Aldi-Nord"},
    "aldi-sued": {"display_name": "ALDI Sued", "global_slug": "Aldi-Sued"},
    "lidl": {"display_name": "Lidl", "global_slug": "Lidl"},
    "rewe": {"display_name": "REWE", "global_slug": "REWE"},
    "edeka": {"display_name": "EDEKA", "global_slug": "Edeka"},
    "kaufland": {"display_name": "Kaufland", "global_slug": "Kaufland"},
    "penny": {"display_name": "Penny", "global_slug": "Penny-Markt"},
    "netto": {"display_name": "Netto Marken-Discount", "global_slug": "Netto-Marken-Discount"},
    "norma": {"display_name": "Norma", "global_slug": "Norma"},
    "globus": {"display_name": "Globus", "global_slug": "Globus"},
    "marktkauf": {"display_name": "Marktkauf", "global_slug": "Marktkauf"},
}

KAUFDA_BASE = "https://www.kaufda.de"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Sparfuchs/1.0"


def make_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=2,
        read=2,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16))
    return session


def fetch_global_page_info(session: requests.Session, global_slug: str) -> dict[str, Any]:
    url = f"{KAUFDA_BASE}/Geschaefte/{global_slug}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text)
    if not match:
        raise RuntimeError(f"__NEXT_DATA__ missing on {url}")
    payload = json.loads(match.group(1))
    pi = payload["props"]["pageProps"]["pageInformation"]
    pub = pi["publisher"]
    dc = pub["defaultCity"]
    return {
        "build_id": payload["buildId"],
        "local_url": pub["localUrl"],
        "local_id": str(pub["localId"]),
        "default_city_url": dc["url"],
        "default_city_name": dc["displayName"],
        "default_lat": dc["lat"],
        "default_lng": dc["lng"],
    }


def ensure_kaufda_stores_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS kaufda_stores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_key TEXT NOT NULL,
            chain_name TEXT NOT NULL,
            store_path TEXT NOT NULL UNIQUE,
            name TEXT,
            street TEXT,
            zip TEXT,
            city TEXT,
            lat REAL,
            lng REAL,
            brochure_ids TEXT,
            crawled_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_kaufda_stores_chain ON kaufda_stores(chain_key);
        CREATE INDEX IF NOT EXISTS idx_kaufda_stores_zip ON kaufda_stores(zip);
        CREATE INDEX IF NOT EXISTS idx_kaufda_stores_lat_lng ON kaufda_stores(lat, lng);
    """)


def upsert_store(
    conn: sqlite3.Connection,
    chain_key: str,
    chain_name: str,
    store_path: str,
    location: dict,
    brochure_ids: list[str],
    now: str,
) -> None:
    conn.execute(
        """
        INSERT INTO kaufda_stores (chain_key, chain_name, store_path, name, street, zip, city, lat, lng, brochure_ids, crawled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(store_path) DO UPDATE SET
            name=excluded.name,
            street=excluded.street,
            zip=excluded.zip,
            city=excluded.city,
            lat=excluded.lat,
            lng=excluded.lng,
            brochure_ids=excluded.brochure_ids,
            crawled_at=excluded.crawled_at
        """,
        (
            chain_key,
            chain_name,
            store_path,
            location.get("name", ""),
            location.get("street", ""),
            location.get("zip", ""),
            location.get("city", ""),
            location.get("lat"),
            location.get("lng"),
            json.dumps(brochure_ids) if brochure_ids else "[]",
            now,
        ),
    )


def load_state(state_path: Path) -> dict:
    if state_path.exists():
        return json.loads(state_path.read_text(encoding="utf-8"))
    return {}


def save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def crawl_chain_stores(
    chain_key: str,
    chain_cfg: dict,
    conn: sqlite3.Connection,
    max_workers: int = 8,
    state: dict | None = None,
    state_path: Path | None = None,
) -> dict[str, Any]:
    """BFS crawl through ALL store pages for a chain. Extract + save store locations."""
    session = make_session()
    info = fetch_global_page_info(session, chain_cfg["global_slug"])
    build_id = info["build_id"]
    local_url = info["local_url"]
    local_id = info["local_id"]

    seed = f"Filialen/{info['default_city_url']}/{local_url}/v-r{local_id}"
    path_pattern = re.compile(rf"/Filialen/([^\"\\]+/{re.escape(local_url)}/v-r{local_id})")

    # Resume support: reload visited paths from previous state
    chain_state = (state or {}).get(chain_key, {})
    visited: set[str] = set(chain_state.get("visited", []))
    queued_list = chain_state.get("queue", [])

    if visited:
        # Resuming: use saved queue
        queue: deque[str] = deque(queued_list) if queued_list else deque()
        if not queue:
            print(f"  Resume: {len(visited)} visited, queue empty -> done")
            return {
                "chain_key": chain_key,
                "store_pages_crawled": len(visited),
                "stores_found": 0,
                "resumed": True,
            }
        print(f"  Resume: {len(visited)} visited, {len(queue)} in queue")
    else:
        queue = deque([seed])

    queued_set: set[str] = set(queue)
    stores_found = 0
    failures = 0
    started = time.time()
    last_save = time.time()

    def fetch_path(store_path: str) -> tuple[str, dict | None, str | None]:
        worker_session = make_session()
        try:
            url = f"{KAUFDA_BASE}/_next/data/{build_id}/{store_path}.json"
            resp = worker_session.get(url, timeout=30)
            resp.raise_for_status()
            return store_path, resp.json(), resp.text
        except Exception as exc:
            return store_path, None, str(exc)

    while queue:
        # Build batch
        batch: list[str] = []
        while queue and len(batch) < max_workers:
            path = queue.popleft()
            queued_set.discard(path)
            if path in visited:
                continue
            visited.add(path)
            batch.append(path)

        if not batch:
            continue

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for store_path, payload, raw_text in executor.map(fetch_path, batch):
                if payload is None:
                    failures += 1
                    continue

                page_info = payload.get("pageProps", {}).get("pageInformation", {})
                location = page_info.get("location") or {}
                city_info = page_info.get("city") or {}

                # Extract store location
                store_loc = {
                    "name": location.get("name") or page_info.get("publisher", {}).get("name", ""),
                    "street": location.get("street", ""),
                    "zip": location.get("zip", ""),
                    "city": location.get("city") or city_info.get("displayName", ""),
                    "lat": location.get("lat"),
                    "lng": location.get("lng"),
                }

                # Collect brochure IDs found at this store
                brochure_ids = []
                for bucket in ("viewer", "publisher"):
                    for b in page_info.get("brochures", {}).get(bucket, []):
                        cid = b.get("contentId")
                        if cid:
                            brochure_ids.append(cid)

                now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                upsert_store(conn, chain_key, chain_cfg["display_name"], store_path, store_loc, brochure_ids, now)
                stores_found += 1

                # Discover neighbor store pages
                if isinstance(raw_text, str):
                    for match in path_pattern.findall(raw_text):
                        next_path = f"Filialen/{match}"
                        if next_path not in visited and next_path not in queued_set:
                            queue.append(next_path)
                            queued_set.add(next_path)

        conn.commit()

        # Progress + periodic state save (every 30s)
        elapsed = time.time() - started
        if time.time() - last_save > 30 and state is not None and state_path:
            state[chain_key] = {
                "visited": list(visited),
                "queue": list(queue),
                "stores_found": stores_found,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            save_state(state_path, state)
            last_save = time.time()

        # Print progress every batch
        print(
            f"  [{chain_cfg['display_name']}] "
            f"visited={len(visited)} queue={len(queue)} stores={stores_found} "
            f"fails={failures} {elapsed:.0f}s",
            end="\r",
        )

    print()  # newline after \r progress

    # Final state save
    if state is not None and state_path:
        state[chain_key] = {
            "visited": list(visited),
            "queue": [],
            "stores_found": stores_found,
            "done": True,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        save_state(state_path, state)

    conn.commit()
    elapsed = time.time() - started

    return {
        "chain_key": chain_key,
        "chain_name": chain_cfg["display_name"],
        "store_pages_crawled": len(visited),
        "stores_found": stores_found,
        "failures": failures,
        "elapsed_seconds": round(elapsed, 1),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl KaufDA to build complete store registry.")
    parser.add_argument(
        "--chains",
        nargs="*",
        default=list(CHAIN_TARGETS.keys()),
        help=f"Chain keys: {', '.join(CHAIN_TARGETS.keys())}",
    )
    parser.add_argument("--max-workers", type=int, default=8, help="Concurrent requests per chain.")
    parser.add_argument("--resume", action="store_true", help="Resume from saved state.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="SQLite database path.")
    parser.add_argument("--state-path", default=str(STATE_PATH), help="Crawl state file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    invalid = [c for c in args.chains if c not in CHAIN_TARGETS]
    if invalid:
        raise SystemExit(f"Unknown chains: {', '.join(invalid)}")

    db_path = Path(args.db_path)
    state_path = Path(args.state_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    ensure_kaufda_stores_table(conn)
    conn.commit()

    # Load or init state
    if args.resume:
        state = load_state(state_path)
        print(f"Resuming from state: {state_path}")
    else:
        state = {}

    results = []
    for chain_key in args.chains:
        chain_cfg = CHAIN_TARGETS[chain_key]

        # Skip if already done in state
        if args.resume and state.get(chain_key, {}).get("done"):
            print(f"\n{chain_cfg['display_name']}: already done ({state[chain_key].get('stores_found', '?')} stores)")
            continue

        print(f"\n{'='*60}")
        print(f"Crawling: {chain_cfg['display_name']}")
        print(f"{'='*60}")

        try:
            result = crawl_chain_stores(
                chain_key=chain_key,
                chain_cfg=chain_cfg,
                conn=conn,
                max_workers=args.max_workers,
                state=state,
                state_path=state_path,
            )
            results.append(result)
            print(
                f"  DONE: {result['store_pages_crawled']} pages, "
                f"{result['stores_found']} stores, "
                f"{result.get('failures', 0)} failures, "
                f"{result['elapsed_seconds']}s"
            )
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            # Save state so we can resume
            save_state(state_path, state)

    # Final summary
    print(f"\n{'='*60}")
    print("STORE REGISTRY SUMMARY")
    print(f"{'='*60}")

    cursor = conn.execute("SELECT chain_key, chain_name, COUNT(*) as cnt FROM kaufda_stores GROUP BY chain_key ORDER BY cnt DESC")
    total = 0
    print(f"{'Chain':<25} {'Stores':>8}")
    print("-" * 35)
    for row in cursor:
        print(f"{row[1]:<25} {row[2]:>8}")
        total += row[2]
    print("-" * 35)
    print(f"{'TOTAL':<25} {total:>8}")

    # Also compare with OSM stores
    try:
        osm_cursor = conn.execute("SELECT chain, COUNT(*) FROM stores GROUP BY chain ORDER BY COUNT(*) DESC")
        print(f"\nVergleich mit OSM-Stores:")
        print(f"{'Chain':<25} {'KaufDA':>8} {'OSM':>8}")
        print("-" * 45)
        osm_counts = {row[0]: row[1] for row in osm_cursor}
        kd_cursor = conn.execute("SELECT chain_name, COUNT(*) FROM kaufda_stores GROUP BY chain_name")
        kd_counts = {row[0]: row[1] for row in kd_cursor}
        all_chains = sorted(set(list(osm_counts.keys()) + list(kd_counts.keys())))
        for chain in all_chains:
            kd = kd_counts.get(chain, 0)
            osm = osm_counts.get(chain, 0)
            print(f"{chain:<25} {kd:>8} {osm:>8}")
    except Exception:
        pass

    conn.close()
    print(f"\nState saved: {state_path}")
    print(f"Database: {db_path}")


if __name__ == "__main__":
    main()
