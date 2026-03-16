from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.catalog_data import CatalogDataService, LocalPlace
from app.services.overpass import OverpassClient, Store
from app.utils.chains import KNOWN_CHAINS
from app.utils.geo import haversine_km


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import OSM supermarket stores into the local Sparfuchs SQLite DB.")
    parser.add_argument(
        "--db-path",
        default="data/kaufda_dataset/offers.sqlite3",
        help="Path to the local SQLite database.",
    )
    parser.add_argument(
        "--query-radius-km",
        type=float,
        default=12.0,
        help="Overpass radius per seed place in kilometers.",
    )
    parser.add_argument(
        "--seed-min-distance-km",
        type=float,
        default=8.0,
        help="Optional minimum distance between seed places to reduce overlapping queries.",
    )
    parser.add_argument(
        "--limit-places",
        type=int,
        default=None,
        help="Optional limit for test runs.",
    )
    parser.add_argument(
        "--place-contains",
        nargs="*",
        default=None,
        help="Optional case-insensitive substrings to restrict seed places, e.g. Bonn Koeln.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="How many Overpass requests to run in parallel.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="How many seed places to process in a single run.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=1.5,
        help="Optional delay before each Overpass request to reduce rate limits.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry seed places that previously failed in the state file.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Discard any existing import state and rebuild it from the current seed selection.",
    )
    parser.add_argument(
        "--state-path",
        default="data/kaufda_dataset/stores_import.state.json",
        help="Where to persist batch progress and per-seed status.",
    )
    parser.add_argument(
        "--output-summary",
        default="data/kaufda_dataset/stores_import.summary.json",
        help="Where to write the import summary JSON.",
    )
    return parser.parse_args()


def _user_agent() -> str:
    return "Sparfuchs/0.1 (local store import)"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_stores_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS stores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            osm_type TEXT NOT NULL,
            osm_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            chain TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            address TEXT,
            postcode TEXT,
            city_name TEXT,
            source TEXT NOT NULL DEFAULT 'overpass',
            seed_place_name TEXT,
            seed_lat REAL,
            seed_lon REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(osm_type, osm_id)
        );

        CREATE INDEX IF NOT EXISTS stores_chain_idx ON stores(chain);
        CREATE INDEX IF NOT EXISTS stores_lat_lon_idx ON stores(lat, lon);
        """
    )


def choose_seed_places(places: list[LocalPlace], *, min_distance_km: float, limit: int | None) -> list[LocalPlace]:
    ordered = sorted(
        places,
        key=lambda place: (place.chain_count, place.region_count, len(place.display_name)),
        reverse=True,
    )
    selected: list[LocalPlace] = []
    for place in ordered:
        if min_distance_km > 0 and any(
            haversine_km(place.lat, place.lon, chosen.lat, chosen.lon) < min_distance_km
            for chosen in selected
        ):
            continue
        selected.append(place)
        if limit is not None and len(selected) >= limit:
            break
    return selected


def load_json(path: Path, default: object) -> object:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def seed_key(place: LocalPlace) -> str:
    return f"{place.display_name}|{place.lat:.5f}|{place.lon:.5f}"


def build_state(*, seeds: list[LocalPlace], config: dict[str, object]) -> dict[str, object]:
    return {
        "version": 1,
        "updated_at": utc_now(),
        "config": config,
        "seeds": [
            {
                "key": seed_key(place),
                "display_name": place.display_name,
                "lat": place.lat,
                "lon": place.lon,
                "chain_count": place.chain_count,
                "region_count": place.region_count,
                "status": "pending",
                "attempts": 0,
                "raw_hits": 0,
                "last_error": None,
                "updated_at": None,
            }
            for place in seeds
        ],
    }


def load_or_create_state(
    *,
    state_path: Path,
    seeds: list[LocalPlace],
    config: dict[str, object],
    reset_state: bool,
) -> dict[str, object]:
    if reset_state or not state_path.exists():
        state = build_state(seeds=seeds, config=config)
        save_json(state_path, state)
        return state

    existing = load_json(state_path, default={})
    if not isinstance(existing, dict) or existing.get("config") != config:
        state = build_state(seeds=seeds, config=config)
        save_json(state_path, state)
        return state

    existing_entries = {
        str(entry.get("key")): entry
        for entry in existing.get("seeds", [])
        if isinstance(entry, dict) and entry.get("key")
    }
    state = build_state(seeds=seeds, config=config)
    for entry in state["seeds"]:
        previous = existing_entries.get(str(entry["key"]))
        if previous is None:
            continue
        for field in ("status", "attempts", "raw_hits", "last_error", "updated_at"):
            entry[field] = previous.get(field)
    state["updated_at"] = utc_now()
    save_json(state_path, state)
    return state


def summarize_state(state: dict[str, object]) -> dict[str, int]:
    entries = [entry for entry in state.get("seeds", []) if isinstance(entry, dict)]
    return {
        "total": len(entries),
        "done": sum(1 for entry in entries if entry.get("status") == "done"),
        "failed": sum(1 for entry in entries if entry.get("status") == "failed"),
        "pending": sum(1 for entry in entries if entry.get("status") == "pending"),
    }


def upsert_store(conn: sqlite3.Connection, store: Store, *, seed_place: LocalPlace, now_iso: str) -> None:
    conn.execute(
        """
        INSERT INTO stores (
            osm_type,
            osm_id,
            name,
            chain,
            lat,
            lon,
            address,
            postcode,
            city_name,
            source,
            seed_place_name,
            seed_lat,
            seed_lon,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(osm_type, osm_id) DO UPDATE SET
            name=excluded.name,
            chain=excluded.chain,
            lat=excluded.lat,
            lon=excluded.lon,
            address=excluded.address,
            postcode=excluded.postcode,
            city_name=excluded.city_name,
            source=excluded.source,
            seed_place_name=excluded.seed_place_name,
            seed_lat=excluded.seed_lat,
            seed_lon=excluded.seed_lon,
            updated_at=excluded.updated_at
        """,
        (
            store.osm_type,
            store.osm_id,
            store.name,
            store.chain,
            store.lat,
            store.lon,
            store.address,
            store.postcode,
            store.city_name,
            "overpass",
            seed_place.display_name,
            seed_place.lat,
            seed_place.lon,
            now_iso,
            now_iso,
        ),
    )


async def import_stores(
    *,
    db_path: Path,
    query_radius_km: float,
    seed_min_distance_km: float,
    limit_places: int | None,
    place_contains: list[str] | None,
    concurrency: int,
    batch_size: int,
    delay_seconds: float,
    retry_failed: bool,
    reset_state: bool,
    state_path: Path,
) -> dict[str, object]:
    service = CatalogDataService(db_path=db_path)
    if not service.available():
        raise SystemExit(f"Database not found: {db_path}")

    places = service.list_local_places()
    if not places:
        raise SystemExit("No local places available in stores table.")

    filters = [part.strip().lower() for part in (place_contains or []) if part and part.strip()]
    if filters:
        places = [
            place
            for place in places
            if any(part in place.display_name.lower() for part in filters)
        ]
        if not places:
            raise SystemExit("No local places matched --place-contains.")

    seeds = choose_seed_places(
        places,
        min_distance_km=max(0.0, float(seed_min_distance_km)),
        limit=limit_places,
    )
    if not seeds:
        raise SystemExit("No seed places selected.")

    config = {
        "query_radius_km": float(query_radius_km),
        "seed_min_distance_km": float(seed_min_distance_km),
        "limit_places": limit_places,
        "place_contains": filters,
    }
    state = load_or_create_state(
        state_path=state_path,
        seeds=seeds,
        config=config,
        reset_state=reset_state,
    )
    seed_lookup = {seed_key(place): place for place in seeds}
    eligible_statuses = {"pending"} | ({"failed"} if retry_failed else set())
    pending_entries = [
        entry
        for entry in state.get("seeds", [])
        if isinstance(entry, dict) and entry.get("status") in eligible_statuses
    ]
    if batch_size > 0:
        pending_entries = pending_entries[:batch_size]
    batch_seeds = [seed_lookup[str(entry["key"])] for entry in pending_entries if str(entry["key"]) in seed_lookup]
    if not batch_seeds:
        state_summary = summarize_state(state)
        conn = connect_db(db_path)
        try:
            ensure_stores_table(conn)
            stores_in_db = int(conn.execute("SELECT COUNT(*) FROM stores").fetchone()[0])
        finally:
            conn.close()
        return {
            "db_path": str(db_path),
            "state_path": str(state_path),
            "seed_places_total": len(places),
            "seed_places_used": len(seeds),
            "batch_size": batch_size,
            "batch_processed": 0,
            "raw_store_hits": 0,
            "unique_store_hits": 0,
            "stores_in_db": stores_in_db,
            "failures": [],
            "state": state_summary,
            "duration_seconds": 0.0,
        }

    conn = connect_db(db_path)
    ensure_stores_table(conn)
    conn.commit()

    client = OverpassClient(user_agent=_user_agent())
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    radius_m = max(500, int(float(query_radius_km) * 1000.0))
    fetched_store_keys: set[tuple[str, int]] = set()
    total_raw_hits = 0
    failures: list[dict[str, str]] = []
    start = time.time()

    async def fetch_seed(place: LocalPlace) -> tuple[LocalPlace, list[Store] | None, str | None]:
        async with sem:
            try:
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                stores = await client.find_supermarkets(lat=place.lat, lon=place.lon, radius_m=radius_m)
                stores = [store for store in stores if store.chain in KNOWN_CHAINS]
                return place, stores, None
            except Exception as exc:
                return place, None, str(exc)

    try:
        state_entries_by_key = {
            str(entry["key"]): entry
            for entry in state.get("seeds", [])
            if isinstance(entry, dict) and entry.get("key")
        }

        for index, result in enumerate(asyncio.as_completed([fetch_seed(seed) for seed in batch_seeds]), start=1):
            place, stores, error = await result
            entry = state_entries_by_key[seed_key(place)]
            entry["attempts"] = int(entry.get("attempts") or 0) + 1
            entry["updated_at"] = utc_now()
            if error is not None:
                failures.append({"place": place.display_name, "error": error})
                entry["status"] = "failed"
                entry["last_error"] = error
                save_json(state_path, state)
                progress = summarize_state(state)
                print(
                    f"[{index}/{len(batch_seeds)}] fail {place.display_name}: {error} "
                    f"(done={progress['done']} failed={progress['failed']} pending={progress['pending']})"
                )
                continue

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            total_raw_hits += len(stores or [])
            for store in stores or []:
                fetched_store_keys.add((store.osm_type, store.osm_id))
                upsert_store(conn, store, seed_place=place, now_iso=now_iso)
            conn.commit()
            entry["status"] = "done"
            entry["raw_hits"] = len(stores or [])
            entry["last_error"] = None
            save_json(state_path, state)
            progress = summarize_state(state)
            print(
                f"[{index}/{len(batch_seeds)}] {place.display_name}: "
                f"raw={len(stores or [])} unique_total={len(fetched_store_keys)} "
                f"(done={progress['done']} failed={progress['failed']} pending={progress['pending']})"
            )

        count_row = conn.execute("SELECT COUNT(*) AS count FROM stores").fetchone()
        total_stored = int(count_row["count"]) if count_row is not None else 0
        conn.execute(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
            ("stores", str(total_stored)),
        )
        conn.execute(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
            ("stores_last_import_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        )
        conn.commit()
    finally:
        conn.close()

    state["updated_at"] = utc_now()
    save_json(state_path, state)

    return {
        "db_path": str(db_path),
        "state_path": str(state_path),
        "seed_places_total": len(places),
        "seed_places_used": len(seeds),
        "batch_size": batch_size,
        "batch_processed": len(batch_seeds),
        "query_radius_km": query_radius_km,
        "seed_min_distance_km": seed_min_distance_km,
        "concurrency": concurrency,
        "delay_seconds": delay_seconds,
        "raw_store_hits": total_raw_hits,
        "unique_store_hits": len(fetched_store_keys),
        "stores_in_db": total_stored,
        "failures": failures,
        "state": summarize_state(state),
        "duration_seconds": round(time.time() - start, 2),
    }


def main() -> None:
    args = parse_args()
    summary = asyncio.run(
        import_stores(
            db_path=Path(args.db_path),
            query_radius_km=args.query_radius_km,
            seed_min_distance_km=args.seed_min_distance_km,
            limit_places=args.limit_places,
            place_contains=args.place_contains,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            delay_seconds=args.delay_seconds,
            retry_failed=args.retry_failed,
            reset_state=args.reset_state,
            state_path=Path(args.state_path),
        )
    )
    output_path = Path(args.output_summary)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
