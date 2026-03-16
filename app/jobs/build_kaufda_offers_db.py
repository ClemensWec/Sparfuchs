"""Build a normalized SQLite search index from downloaded KaufDA brochure offers.

Schema (normalized):
- offers:           Deduplicated offers (~16K rows instead of ~687K)
- brochures:        Brochure metadata (~2.5K rows)
- offer_brochures:  Which offers appear in which brochures (~687K rows, just IDs)
- brochure_stores:  Which brochures are available at which stores (from mapping file)
- stores:           Store registry (preserved from existing DB)
- offers_fts:       Full-text search on deduplicated offers

Usage:
    python app/jobs/build_kaufda_offers_db.py \\
        --downloads-dir data/kaufda_brochures/downloads \\
        --output-path data/kaufda_dataset/offers.sqlite3 \\
        --mapping-path data/kaufda_brochures/brochure_mapping.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.jobs.kaufda_offer_dataset import extract_offer_records_from_brochure, iter_brochure_dirs
from app.utils.chains import normalize_chain
from app.utils.text import compact_text, normalize_search_text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a normalized SQLite search index from downloaded KaufDA brochure offers."
    )
    parser.add_argument("--downloads-dir", default="data/kaufda_brochures/downloads")
    parser.add_argument("--output-path", default="data/kaufda_dataset/offers.sqlite3")
    parser.add_argument("--mapping-path", default="data/kaufda_brochures/brochure_mapping.json",
                        help="Brochure mapping JSON (from build_brochure_mapping.py).")
    parser.add_argument("--limit-brochures", type=int, default=None)
    return parser.parse_args()


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-200000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS offers;
        DROP TABLE IF EXISTS brochures;
        DROP TABLE IF EXISTS offer_brochures;
        DROP TABLE IF EXISTS brochure_stores;
        DROP TABLE IF EXISTS brochure_locations;
        DROP TABLE IF EXISTS metadata;
        DROP TABLE IF EXISTS offers_fts;

        CREATE TABLE offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_key TEXT NOT NULL,
            chain TEXT NOT NULL,
            product_name TEXT,
            product_name_normalized TEXT,
            brand_name TEXT,
            brand_name_normalized TEXT,
            description_text TEXT,
            description_text_normalized TEXT,
            search_text TEXT,
            search_text_normalized TEXT,
            sales_price_eur REAL,
            regular_price_eur REAL,
            discount_amount_eur REAL,
            discount_percent REAL,
            currency_code TEXT,
            base_price_text TEXT,
            discount_label_type TEXT,
            discount_label_value REAL,
            offer_image_url TEXT,
            offer_type TEXT,
            valid_from TEXT,
            valid_until TEXT,
            raw_deals_json TEXT,
            category_id INTEGER,
            occurrence_count INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX offers_chain_idx ON offers(chain);
        CREATE INDEX offers_chain_key_idx ON offers(chain_key);
        CREATE INDEX offers_valid_idx ON offers(valid_from, valid_until);
        CREATE INDEX offers_product_idx ON offers(product_name_normalized, brand_name_normalized);
        CREATE INDEX offers_product_name_idx ON offers(product_name);

        CREATE TABLE brochures (
            content_id TEXT PRIMARY KEY,
            chain_key TEXT NOT NULL,
            chain TEXT NOT NULL,
            legacy_id TEXT,
            title TEXT,
            publisher_name TEXT,
            valid_from TEXT,
            valid_until TEXT,
            page_count INTEGER,
            brochure_type TEXT
        );

        CREATE INDEX brochures_chain_idx ON brochures(chain_key);

        CREATE TABLE offer_brochures (
            offer_id INTEGER NOT NULL,
            brochure_content_id TEXT NOT NULL,
            offer_content_id TEXT,
            page_number INTEGER,
            page_index INTEGER,
            placement TEXT,
            ad_format TEXT,
            bbox_top_left_x REAL,
            bbox_top_left_y REAL,
            bbox_bottom_right_x REAL,
            bbox_bottom_right_y REAL,
            UNIQUE(offer_id, brochure_content_id)
        );

        CREATE INDEX ob_offer_idx ON offer_brochures(offer_id);
        CREATE INDEX ob_brochure_idx ON offer_brochures(brochure_content_id);

        CREATE TABLE brochure_stores (
            brochure_content_id TEXT NOT NULL,
            store_id INTEGER NOT NULL,
            city_slug TEXT,
            PRIMARY KEY(brochure_content_id, store_id)
        );

        CREATE INDEX bs_store_idx ON brochure_stores(store_id);

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

        CREATE VIRTUAL TABLE offers_fts USING fts5(
            product_name_normalized,
            brand_name_normalized,
            description_text_normalized,
            search_text_normalized,
            chain,
            tokenize='unicode61'
        );

        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )


def load_existing_stores(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stores'").fetchone()
        if row is None:
            return []
        rows = list(conn.execute(
            "SELECT osm_type, osm_id, name, chain, lat, lon, address, postcode, "
            "city_name, source, seed_place_name, seed_lat, seed_lon, created_at, updated_at FROM stores"
        ))
        return [dict(r) for r in rows]
    finally:
        conn.close()


def restore_existing_stores(conn: sqlite3.Connection, stores: list[dict[str, Any]]) -> int:
    if not stores:
        return 0
    for store in stores:
        conn.execute(
            "INSERT OR REPLACE INTO stores (osm_type, osm_id, name, chain, lat, lon, address, "
            "postcode, city_name, source, seed_place_name, seed_lat, seed_lon, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                store.get("osm_type"), store.get("osm_id"), store.get("name"), store.get("chain"),
                store.get("lat"), store.get("lon"), store.get("address"), store.get("postcode"),
                store.get("city_name"), store.get("source") or "overpass",
                store.get("seed_place_name"), store.get("seed_lat"), store.get("seed_lon"),
                store.get("created_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                store.get("updated_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            ),
        )
    return len(stores)


def canonical_chain(row: dict[str, Any], brochure_dir: Path) -> str:
    for raw in (row.get("publisher_name"), row.get("chain_key"), brochure_dir.parent.name):
        chain = normalize_chain(raw)
        if chain:
            return chain
    return brochure_dir.parent.name.replace("-", " ").title()


def _offer_dedup_key(
    chain_key: str,
    product_name_normalized: str | None,
    brand_name_normalized: str | None,
    sales_price: float | None,
    valid_from: str | None,
    valid_until: str | None,
) -> tuple:
    return (
        chain_key,
        product_name_normalized or "",
        brand_name_normalized or "",
        sales_price,
        valid_from or "",
        valid_until or "",
    )


def _extract_brochure_metadata(brochure_dir: Path) -> dict[str, Any]:
    """Extract brochure metadata from metadata.json."""
    meta = json.loads((brochure_dir / "metadata.json").read_text(encoding="utf-8"))
    content = meta.get("content", {})
    return {
        "content_id": content.get("id") or brochure_dir.name,
        "legacy_id": content.get("legacyId"),
        "title": compact_text(content.get("title")),
        "publisher_name": compact_text((content.get("publisher") or {}).get("name")),
        "valid_from": content.get("validFrom"),
        "valid_until": content.get("validUntil"),
        "page_count": content.get("pageCount"),
        "brochure_type": content.get("type"),
    }


def _insert_brochure(conn: sqlite3.Connection, chain_key: str, chain: str, meta: dict[str, Any]) -> str:
    content_id = str(meta["content_id"])
    conn.execute(
        "INSERT OR IGNORE INTO brochures "
        "(content_id, chain_key, chain, legacy_id, title, publisher_name, valid_from, valid_until, page_count, brochure_type) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            content_id, chain_key, chain, meta.get("legacy_id"),
            meta.get("title"), meta.get("publisher_name"),
            meta.get("valid_from"), meta.get("valid_until"),
            meta.get("page_count"), meta.get("brochure_type"),
        ),
    )
    return content_id


def _insert_deduped_offer(conn: sqlite3.Connection, chain_key: str, chain: str, row: dict[str, Any]) -> int:
    product_name = compact_text(row.get("product_name"))
    brand_name = compact_text(row.get("brand_name"))
    description_text = compact_text(row.get("description_text"))
    search_text = compact_text(row.get("search_text"))
    product_name_normalized = normalize_search_text(product_name)
    brand_name_normalized = normalize_search_text(brand_name)
    description_text_normalized = normalize_search_text(description_text)
    search_text_normalized = normalize_search_text(search_text)

    cursor = conn.execute(
        "INSERT INTO offers ("
        "chain_key, chain, product_name, product_name_normalized, brand_name, brand_name_normalized, "
        "description_text, description_text_normalized, search_text, search_text_normalized, "
        "sales_price_eur, regular_price_eur, discount_amount_eur, discount_percent, "
        "currency_code, base_price_text, discount_label_type, discount_label_value, "
        "offer_image_url, offer_type, valid_from, valid_until, raw_deals_json"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            chain_key, chain,
            product_name, product_name_normalized,
            brand_name, brand_name_normalized,
            description_text, description_text_normalized,
            search_text, search_text_normalized,
            row.get("sales_price_eur"), row.get("regular_price_eur"),
            row.get("discount_amount_eur"), row.get("discount_percent"),
            row.get("currency_code"), compact_text(row.get("base_price_text")),
            row.get("discount_label_type"), row.get("discount_label_value"),
            row.get("offer_image_url"), row.get("offer_type"),
            row.get("valid_from"), row.get("valid_until"),
            json.dumps(row.get("raw_deals") or [], ensure_ascii=False),
        ),
    )
    return int(cursor.lastrowid)


def _insert_offer_brochure(conn: sqlite3.Connection, offer_id: int, brochure_content_id: str, row: dict[str, Any]) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO offer_brochures "
        "(offer_id, brochure_content_id, offer_content_id, page_number, page_index, "
        "placement, ad_format, bbox_top_left_x, bbox_top_left_y, bbox_bottom_right_x, bbox_bottom_right_y) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            offer_id, brochure_content_id,
            row.get("offer_content_id"), row.get("page_number"), row.get("page_index"),
            row.get("placement"), row.get("ad_format"),
            row.get("bbox_top_left_x"), row.get("bbox_top_left_y"),
            row.get("bbox_bottom_right_x"), row.get("bbox_bottom_right_y"),
        ),
    )


def _build_fts(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO offers_fts (rowid, product_name_normalized, brand_name_normalized, "
        "description_text_normalized, search_text_normalized, chain) "
        "SELECT id, product_name_normalized, brand_name_normalized, "
        "description_text_normalized, search_text_normalized, chain FROM offers"
    )


def _load_and_insert_mapping(conn: sqlite3.Connection, mapping_path: Path) -> int:
    """Load brochure mapping and populate brochure_stores table."""
    if not mapping_path.exists():
        print(f"  No mapping file at {mapping_path}, brochure_stores will be empty.")
        return 0

    mapping = json.loads(mapping_path.read_text(encoding="utf-8"))
    chains = mapping.get("chains", {})
    inserted = 0

    for chain_key, chain_data in chains.items():
        slug_brochures = chain_data.get("slug_brochures", {})
        store_city_slugs = chain_data.get("store_city_slugs", {})

        # Invert: store_id → city_slug is already stored,
        # and slug → [brochure_content_ids] is in slug_brochures.
        # We need: for each brochure, find all stores whose slug maps to it.

        # Build slug → [store_ids]
        slug_to_stores: dict[str, list[int]] = {}
        for store_id_str, slug in store_city_slugs.items():
            slug_to_stores.setdefault(slug, []).append(int(store_id_str))

        # For each slug → brochures, link brochures to stores
        for slug, content_ids in slug_brochures.items():
            store_ids = slug_to_stores.get(slug, [])
            if not store_ids:
                continue
            for content_id in content_ids:
                for store_id in store_ids:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO brochure_stores (brochure_content_id, store_id, city_slug) "
                            "VALUES (?, ?, ?)",
                            (content_id, store_id, slug),
                        )
                        inserted += 1
                    except Exception:
                        pass

        conn.commit()

    return inserted


def print_progress(*, current: int, total: int, chain_key: str, unique_offers: int, raw_offers: int) -> None:
    percent = (current / total) * 100.0 if total else 100.0
    sys.stdout.write(
        f"\r[{current}/{total} {percent:5.1f}%] "
        f"[{chain_key:15}] "
        f"[unique {unique_offers:>7,} / raw {raw_offers:>8,}]"
    )
    sys.stdout.flush()


def build_database(
    downloads_dir: Path,
    output_path: Path,
    *,
    mapping_path: Path,
    limit_brochures: int | None = None,
) -> dict[str, Any]:
    brochure_dirs = list(iter_brochure_dirs(downloads_dir))
    if limit_brochures is not None:
        brochure_dirs = brochure_dirs[:limit_brochures]
    if not brochure_dirs:
        raise SystemExit(f"No brochure folders found under {downloads_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    if temp_path.exists():
        temp_path.unlink()
    existing_stores = load_existing_stores(output_path)
    if output_path.exists():
        output_path.unlink()

    start = time.time()
    dedup_map: dict[tuple, int] = {}
    total_raw_offers = 0
    total_unique_offers = 0
    total_ob_links = 0
    total_pages = 0

    conn = connect_db(temp_path)
    try:
        init_db(conn)
        restored = restore_existing_stores(conn, existing_stores)
        if restored:
            conn.commit()

        for index, brochure_dir in enumerate(brochure_dirs, start=1):
            chain_key = brochure_dir.parent.name

            # Extract brochure metadata
            brochure_meta = _extract_brochure_metadata(brochure_dir)
            chain_sample: dict[str, Any] = {}

            # Extract offers
            page_manifest, offer_rows = extract_offer_records_from_brochure(brochure_dir)
            total_pages += len(page_manifest)

            if offer_rows:
                chain_sample = offer_rows[0]
            chain = canonical_chain(chain_sample, brochure_dir) if chain_sample else canonical_chain({}, brochure_dir)

            # Insert brochure
            brochure_content_id = _insert_brochure(conn, chain_key, chain, brochure_meta)

            # Process offers with dedup
            for row in offer_rows:
                product_name_normalized = normalize_search_text(compact_text(row.get("product_name")))
                brand_name_normalized = normalize_search_text(compact_text(row.get("brand_name")))

                key = _offer_dedup_key(
                    chain_key,
                    product_name_normalized,
                    brand_name_normalized,
                    row.get("sales_price_eur"),
                    row.get("valid_from"),
                    row.get("valid_until"),
                )

                if key not in dedup_map:
                    offer_id = _insert_deduped_offer(conn, chain_key, chain, row)
                    dedup_map[key] = offer_id
                    total_unique_offers += 1
                else:
                    offer_id = dedup_map[key]

                _insert_offer_brochure(conn, offer_id, brochure_content_id, row)
                total_raw_offers += 1
                total_ob_links += 1

            conn.commit()
            print_progress(
                current=index, total=len(brochure_dirs), chain_key=chain_key,
                unique_offers=total_unique_offers, raw_offers=total_raw_offers,
            )

        # Update occurrence counts from offer_brochures
        print("\nUpdating occurrence counts...")
        conn.execute(
            "UPDATE offers SET occurrence_count = "
            "(SELECT COUNT(*) FROM offer_brochures WHERE offer_brochures.offer_id = offers.id)"
        )
        conn.commit()

        # Build FTS
        print("Building FTS index...")
        _build_fts(conn)
        conn.commit()

        # Load mapping → brochure_stores
        print("Loading brochure-store mapping...")
        bs_count = _load_and_insert_mapping(conn, mapping_path)
        print(f"  Inserted {bs_count:,} brochure-store links.")

        # Metadata
        duration = round(time.time() - start, 2)
        for key, value in [
            ("downloads_dir", str(downloads_dir)),
            ("brochures", str(len(brochure_dirs))),
            ("pages", str(total_pages)),
            ("offers_unique", str(total_unique_offers)),
            ("offers_raw", str(total_raw_offers)),
            ("offer_brochure_links", str(total_ob_links)),
            ("brochure_store_links", str(bs_count)),
            ("stores", str(len(existing_stores))),
            ("generated_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            ("duration_seconds", str(duration)),
        ]:
            conn.execute("INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)", (key, value))
        conn.commit()
    finally:
        conn.close()

    temp_path.replace(output_path)
    summary = {
        "downloads_dir": str(downloads_dir),
        "output_path": str(output_path),
        "brochures": len(brochure_dirs),
        "pages": total_pages,
        "offers_unique": total_unique_offers,
        "offers_raw": total_raw_offers,
        "offer_brochure_links": total_ob_links,
        "brochure_store_links": bs_count,
        "stores_preserved": len(existing_stores),
        "dedup_ratio": f"{(1 - total_unique_offers / total_raw_offers) * 100:.1f}%" if total_raw_offers else "0%",
        "duration_seconds": round(time.time() - start, 2),
    }
    summary_path = output_path.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    summary = build_database(
        downloads_dir=Path(args.downloads_dir),
        output_path=Path(args.output_path),
        mapping_path=Path(args.mapping_path),
        limit_brochures=args.limit_brochures,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
