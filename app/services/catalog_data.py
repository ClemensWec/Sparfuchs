from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date
from math import cos, radians
from pathlib import Path

from app.connectors.base import Offer
from app.services.overpass import Store
from app.utils.geo import haversine_km
from app.utils.matching import calculate_match_score
from app.utils.text import compact_text, normalize_search_text
from app.utils.unit_parser import parse_base_price


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_kaufda_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = str(value).strip()
    if len(raw) >= 10:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
    return None


def _parse_base_price(value: str | None) -> tuple[float | None, str | None]:
    # Legacy helper kept for compatibility — new code uses parse_base_price() directly.
    parsed = parse_base_price(value)
    return (parsed.price_eur, parsed.unit)


def _parse_float(value: str | float | int | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None



@dataclass(frozen=True)
class LocalPlace:
    display_name: str
    normalized_name: str
    lat: float
    lon: float
    region_count: int
    chain_count: int


@dataclass(frozen=True)
class LocationScope:
    """Resolved set of offers available in a geographic radius."""
    brochure_content_ids: tuple[str, ...]
    full_chains: tuple[str, ...]
    chains_in_area: tuple[str, ...]
    local_offer_ids: frozenset[int]
    category_counts: dict[int, int]  # category_id → offer count in scope


class _LocationScopeCache:
    """In-memory TTL cache for LocationScope, keyed by rounded coordinates."""

    def __init__(self, ttl_seconds: int = 300) -> None:
        self._ttl = ttl_seconds
        self._cache: dict[tuple[float, float, int], tuple[float, LocationScope]] = {}

    @staticmethod
    def _key(lat: float, lon: float, radius_km: float) -> tuple[float, float, int]:
        return (round(lat, 2), round(lon, 2), int(radius_km))

    def get(self, lat: float, lon: float, radius_km: float) -> LocationScope | None:
        key = self._key(lat, lon, radius_km)
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, scope = entry
        if time.monotonic() - ts > self._ttl:
            del self._cache[key]
            return None
        return scope

    def put(self, lat: float, lon: float, radius_km: float, scope: LocationScope) -> None:
        key = self._key(lat, lon, radius_km)
        self._cache[key] = (time.monotonic(), scope)


_scope_cache = _LocationScopeCache(ttl_seconds=300)

# Module-level caches for CatalogDataService (frozen dataclass can't hold mutable attrs)
_stores_table_cache: dict[str, bool] = {}
_local_places_cache: dict[str, tuple[float, list[LocalPlace]]] = {}  # db_path → (timestamp, places)
_LOCAL_PLACES_TTL = 600  # 10 minutes
_all_chains_cache: dict[str, tuple[float, list[str]]] = {}  # db_path → (timestamp, chains)
_ALL_CHAINS_TTL = 600
_offers_cache: dict[str, tuple[float, list]] = {}  # cache_key → (timestamp, offers)
_OFFERS_CACHE_MAX = 20
_OFFERS_TTL = 300  # 5 minutes
_brochure_map_cache: dict[str, tuple[float, dict]] = {}  # chain_key → (timestamp, brochure_map)
_BROCHURE_MAP_TTL = 300


# ---------------------------------------------------------------------------
# Index page stats (cached)
# ---------------------------------------------------------------------------

_index_stats_cache: dict[str, object] = {}
_index_stats_ts: float = 0.0
_INDEX_STATS_TTL = 3600  # 1 hour


def _relative_time_de(dt: date) -> str:
    delta_days = (date.today() - dt).days
    if delta_days < 0:
        return "gerade eben"
    if delta_days == 0:
        return "heute"
    if delta_days == 1:
        return "vor 1 Tag"
    if delta_days < 7:
        return f"vor {delta_days} Tagen"
    weeks = delta_days // 7
    if weeks == 1:
        return "vor 1 Woche"
    return f"vor {weeks} Wochen"


def get_index_stats(db_path: Path) -> dict[str, str | int]:
    global _index_stats_ts

    now = time.monotonic()
    if _index_stats_cache and (now - _index_stats_ts) < _INDEX_STATS_TTL:
        return _index_stats_cache

    if not db_path.exists():
        return {"offer_count": "0", "chain_count": 0, "last_updated": ""}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) AS offer_count,
                COUNT(DISTINCT chain) AS chain_count,
                MAX(valid_from) AS last_valid_from
            FROM offers
        """).fetchone()
    finally:
        conn.close()

    offer_count = int(row["offer_count"])
    chain_count = int(row["chain_count"])
    last_valid_from = _parse_kaufda_date(row["last_valid_from"])

    result = {
        "offer_count": f"{offer_count:,}".replace(",", "."),
        "chain_count": chain_count,
        "last_updated": _relative_time_de(last_valid_from) if last_valid_from else "",
    }

    _index_stats_cache.clear()
    _index_stats_cache.update(result)
    _index_stats_ts = now

    return result


@dataclass(frozen=True)
class CatalogDataService:
    db_path: Path

    # Mutable caches on a frozen dataclass — stored as class-level defaults
    # and keyed by db_path internally, or we use object.__setattr__ on first call.
    # Simpler: use module-level caches keyed by db_path.

    def available(self) -> bool:
        return self.db_path.exists()

    def stores_table_available(self) -> bool:
        # Cache the result — stores table doesn't change at runtime
        cached = _stores_table_cache.get(str(self.db_path))
        if cached is not None:
            return cached
        if not self.available():
            _stores_table_cache[str(self.db_path)] = False
            return False
        with _connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='stores'"
            ).fetchone()
            if row is None:
                _stores_table_cache[str(self.db_path)] = False
                return False
            count_row = conn.execute("SELECT COUNT(*) AS count FROM stores").fetchone()
        result = bool(count_row and int(count_row["count"]) > 0)
        _stores_table_cache[str(self.db_path)] = result
        return result

    def resolve_by_postcode(self, postcode: str) -> LocalPlace | None:
        if not self.available() or not postcode:
            return None
        with _connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    MIN(city_name) AS display_name,
                    AVG(lat) AS avg_lat,
                    AVG(lon) AS avg_lng,
                    COUNT(*) AS store_count,
                    COUNT(DISTINCT chain) AS chain_count
                FROM stores
                WHERE postcode = ?
                  AND lat IS NOT NULL AND lon IS NOT NULL
                """,
                [postcode],
            ).fetchone()
            if row and row["avg_lat"] is not None and int(row["store_count"]) > 0:
                display = compact_text(row["display_name"]) or postcode
                return LocalPlace(
                    display_name=display,
                    normalized_name=normalize_search_text(display),
                    lat=float(row["avg_lat"]),
                    lon=float(row["avg_lng"]),
                    region_count=int(row["store_count"]),
                    chain_count=int(row["chain_count"]),
                )
        return None

    def list_local_places(self) -> list[LocalPlace]:
        if not self.available():
            return []
        return self._load_local_places()

    def resolve_local_place(self, query: str) -> LocalPlace | None:
        if not self.available():
            return None

        raw_query = compact_text(query)
        if not raw_query:
            return None

        simplified_query = _strip_leading_postcode(raw_query) or raw_query
        query_normalized = normalize_search_text(simplified_query)
        if not query_normalized:
            return None

        places = self._load_local_places()
        if not places:
            return None

        exact_matches = [place for place in places if place.normalized_name == query_normalized]
        if exact_matches:
            exact_matches.sort(key=lambda place: (place.chain_count, place.region_count, -len(place.display_name)), reverse=True)
            return exact_matches[0]

        scored: list[tuple[float, LocalPlace]] = []
        for place in places:
            score = calculate_match_score(simplified_query, place.display_name)
            if place.normalized_name.startswith(query_normalized):
                score += 8.0
            elif query_normalized in place.normalized_name:
                score += 4.0
            if score < 72.0:
                continue
            scored.append((score, place))

        if not scored:
            return None

        scored.sort(
            key=lambda item: (
                item[0],
                item[1].chain_count,
                item[1].region_count,
                -len(item[1].display_name),
            ),
            reverse=True,
        )
        return scored[0][1]

    def local_coverage_summary(self) -> dict[str, object]:
        with _connect(self.db_path) as conn:
            total_places = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT city_name) FROM stores WHERE city_name IS NOT NULL AND city_name != ''"
                ).fetchone()[0]
            )
            places_by_chain_rows = list(
                conn.execute(
                    """
                    SELECT chain, COUNT(DISTINCT city_name) AS place_count
                    FROM stores
                    WHERE city_name IS NOT NULL AND city_name != ''
                    GROUP BY chain
                    ORDER BY chain ASC
                    """
                )
            )
            top_places_rows = list(
                conn.execute(
                    """
                    SELECT
                        city_name,
                        COUNT(DISTINCT chain) AS chain_count,
                        COUNT(*) AS store_count
                    FROM stores
                    WHERE city_name IS NOT NULL AND city_name != ''
                    GROUP BY city_name
                    ORDER BY chain_count DESC, store_count DESC, city_name ASC
                    LIMIT 25
                    """
                )
            )

        return {
            "total_places": total_places,
            "places_by_chain": [
                {"chain": str(row["chain"]), "place_count": int(row["place_count"])}
                for row in places_by_chain_rows
            ],
            "top_places": [
                {
                    "city_name": compact_text(row["city_name"]),
                    "chain_count": int(row["chain_count"]),
                    "brochure_count": int(row["store_count"]),
                }
                for row in top_places_rows
            ],
        }

    def find_stores_in_radius(
        self,
        *,
        lat: float,
        lon: float,
        radius_km: float,
        chains: list[str],
    ) -> list[Store]:
        if not self.available() or not chains:
            return []
        return self._find_cached_stores_in_radius(lat=lat, lon=lon, radius_km=radius_km, chains=chains)

    def _find_cached_stores_in_radius(
        self,
        *,
        lat: float,
        lon: float,
        radius_km: float,
        chains: list[str],
    ) -> list[Store]:
        if not self.stores_table_available():
            return []

        radius_km = max(0.1, float(radius_km))
        lat_delta = radius_km / 111.0
        lon_divisor = max(0.1, cos(radians(lat)) * 111.0)
        lon_delta = radius_km / lon_divisor
        placeholders = ", ".join("?" for _ in chains)
        sql = f"""
            SELECT
                osm_type,
                osm_id,
                name,
                chain,
                lat,
                lon,
                address,
                postcode,
                city_name
            FROM stores
            WHERE chain IN ({placeholders})
              AND lat BETWEEN ? AND ?
              AND lon BETWEEN ? AND ?
            ORDER BY chain ASC, name ASC
        """
        params: list[object] = list(chains) + [
            lat - lat_delta,
            lat + lat_delta,
            lon - lon_delta,
            lon + lon_delta,
        ]
        with _connect(self.db_path) as conn:
            rows = list(conn.execute(sql, params))

        stores: list[Store] = []
        for row in rows:
            distance_km = haversine_km(lat, lon, float(row["lat"]), float(row["lon"]))
            if distance_km > radius_km:
                continue
            stores.append(
                Store(
                    osm_type=str(row["osm_type"]),
                    osm_id=int(row["osm_id"]),
                    name=compact_text(row["name"]) or str(row["chain"]),
                    chain=str(row["chain"]),
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    address=compact_text(row["address"]) or None,
                    postcode=compact_text(row["postcode"]) or None,
                    city_name=compact_text(row["city_name"]) or None,
                )
            )

        stores.sort(key=lambda store: haversine_km(lat, lon, store.lat, store.lon))
        return stores

    def _load_local_places(self) -> list[LocalPlace]:
        # Check cache first — local places rarely change
        db_key = str(self.db_path)
        cached = _local_places_cache.get(db_key)
        if cached is not None:
            ts, places = cached
            if time.monotonic() - ts < _LOCAL_PLACES_TTL:
                return places

        sql = """
            SELECT
                city_name AS display_name,
                AVG(lat) AS avg_lat,
                AVG(lon) AS avg_lng,
                COUNT(*) AS store_count,
                COUNT(DISTINCT chain) AS chain_count
            FROM stores
            WHERE city_name IS NOT NULL
              AND city_name != ''
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            GROUP BY city_name
            ORDER BY city_name ASC
        """
        with _connect(self.db_path) as conn:
            rows = list(conn.execute(sql))

        places = [
            LocalPlace(
                display_name=compact_text(row["display_name"]) or "",
                normalized_name=normalize_search_text(compact_text(row["display_name"]) or ""),
                lat=float(row["avg_lat"]),
                lon=float(row["avg_lng"]),
                region_count=int(row["store_count"]),
                chain_count=int(row["chain_count"]),
            )
            for row in rows
            if row["avg_lat"] is not None and row["avg_lng"] is not None
        ]
        _local_places_cache[db_key] = (time.monotonic(), places)
        return places

    def match_stores_to_regions(self, stores: list[Store]) -> list[Store]:
        if not stores or not self.available():
            return stores

        chains = sorted({store.chain for store in stores if store.chain})
        if not chains:
            return stores

        brochure_map = self._get_brochure_map(tuple(chains))
        if brochure_map is None:
            return stores

        matched: list[Store] = []
        for store in stores:
            brochure_ids = brochure_map.get((store.osm_type, store.osm_id), ())
            if brochure_ids:
                matched.append(
                    Store(
                        osm_type=store.osm_type,
                        osm_id=store.osm_id,
                        name=store.name,
                        chain=store.chain,
                        lat=store.lat,
                        lon=store.lon,
                        address=store.address,
                        postcode=store.postcode,
                        city_name=store.city_name,
                        brochure_content_ids=brochure_ids,
                    )
                )
            else:
                matched.append(store)

        return matched

    def _get_brochure_map(
        self, chains: tuple[str, ...]
    ) -> dict[tuple[str, int], tuple[str, ...]] | None:
        """Get brochure map for chains, cached in module-level dict."""
        cache_key = ",".join(chains)
        cached = _brochure_map_cache.get(cache_key)
        if cached is not None:
            ts, bmap = cached
            if time.monotonic() - ts < _BROCHURE_MAP_TTL:
                return bmap

        with _connect(self.db_path) as conn:
            table_check = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='brochure_stores'"
            ).fetchone()
            if not table_check:
                return None

            chain_ph = ", ".join("?" for _ in chains)
            sql = f"""
                SELECT s.osm_type, s.osm_id,
                       GROUP_CONCAT(DISTINCT bs.brochure_content_id) AS brochure_ids
                FROM stores s
                JOIN brochure_stores bs ON bs.store_id = s.id
                WHERE s.chain IN ({chain_ph})
                GROUP BY s.osm_type, s.osm_id
            """
            rows = list(conn.execute(sql, list(chains)))

        brochure_map: dict[tuple[str, int], tuple[str, ...]] = {}
        for row in rows:
            ids = tuple(
                sorted({part.strip() for part in str(row["brochure_ids"] or "").split(",") if part.strip()})
            )
            brochure_map[(str(row["osm_type"]), int(row["osm_id"]))] = ids

        if len(_brochure_map_cache) >= 20:
            oldest_key = next(iter(_brochure_map_cache))
            del _brochure_map_cache[oldest_key]
        _brochure_map_cache[cache_key] = (time.monotonic(), brochure_map)
        return brochure_map

    def resolve_location_scope(
        self,
        *,
        lat: float,
        lon: float,
        radius_km: float,
    ) -> LocationScope | None:
        """Resolve which offers are available within a geographic radius.

        Returns a LocationScope with local offer IDs and per-category counts,
        or None if location data is unavailable.
        """
        cached = _scope_cache.get(lat, lon, radius_km)
        if cached is not None:
            return cached

        if not self.available() or not self.stores_table_available():
            return None

        # Get all chains (no filter — we want full scope)
        all_chains = self._all_chains()
        if not all_chains:
            return None

        # Find stores in radius
        stores = self.find_stores_in_radius(
            lat=lat, lon=lon, radius_km=radius_km, chains=all_chains,
        )
        if not stores:
            return None

        # Attach brochure IDs
        stores = self.match_stores_to_regions(stores)

        chains_in_area = sorted({s.chain for s in stores})
        brochure_ids = sorted({
            bid for s in stores
            for bid in (s.brochure_content_ids or ())
            if bid
        })
        chains_with_brochures = {s.chain for s in stores if s.brochure_content_ids}
        full_chains = sorted(c for c in chains_in_area if c not in chains_with_brochures)

        # Resolve actual offer IDs in scope
        local_offer_ids, category_counts = self._resolve_offer_ids_and_counts(
            brochure_ids=brochure_ids,
            full_chains=full_chains,
            chains_in_area=chains_in_area,
        )

        scope = LocationScope(
            brochure_content_ids=tuple(brochure_ids),
            full_chains=tuple(full_chains),
            chains_in_area=tuple(chains_in_area),
            local_offer_ids=local_offer_ids,
            category_counts=category_counts,
        )
        _scope_cache.put(lat, lon, radius_km, scope)
        return scope

    def _all_chains(self) -> list[str]:
        db_key = str(self.db_path)
        cached = _all_chains_cache.get(db_key)
        if cached is not None:
            ts, chains = cached
            if time.monotonic() - ts < _ALL_CHAINS_TTL:
                return chains
        with _connect(self.db_path) as conn:
            rows = conn.execute("SELECT DISTINCT chain FROM stores ORDER BY chain").fetchall()
        chains = [row["chain"] for row in rows]
        _all_chains_cache[db_key] = (time.monotonic(), chains)
        return chains

    def _resolve_offer_ids_and_counts(
        self,
        *,
        brochure_ids: list[str],
        full_chains: list[str],
        chains_in_area: list[str],
    ) -> tuple[frozenset[int], dict[int, int]]:
        """Get offer IDs and per-category counts for a location scope."""
        offer_ids: set[int] = set()
        cat_counts: dict[int, int] = {}

        with _connect(self.db_path) as conn:
            # Offers linked to brochures in scope
            if brochure_ids:
                ph = ", ".join("?" for _ in brochure_ids)
                rows = conn.execute(
                    f"""
                    SELECT o.id, o.category_id
                    FROM offers o
                    JOIN offer_brochures ob ON ob.offer_id = o.id
                    WHERE ob.brochure_content_id IN ({ph})
                      AND o.chain IN ({", ".join("?" for _ in chains_in_area)})
                    """,
                    brochure_ids + chains_in_area,
                ).fetchall()
                for row in rows:
                    oid = int(row["id"])
                    offer_ids.add(oid)
                    cat_id = row["category_id"]
                    if cat_id is not None:
                        cat_counts[int(cat_id)] = cat_counts.get(int(cat_id), 0) + 1

            # All offers from chains without brochure data
            if full_chains:
                ph = ", ".join("?" for _ in full_chains)
                rows = conn.execute(
                    f"SELECT id, category_id FROM offers WHERE chain IN ({ph})",
                    full_chains,
                ).fetchall()
                for row in rows:
                    oid = int(row["id"])
                    if oid not in offer_ids:
                        offer_ids.add(oid)
                        cat_id = row["category_id"]
                        if cat_id is not None:
                            cat_counts[int(cat_id)] = cat_counts.get(int(cat_id), 0) + 1

            # Aggregate level-3 counts into their level-2 parent groups.
            # Only look up parents for category IDs that actually have counts.
            cat_ids_with_counts = [cid for cid in cat_counts if cat_counts[cid] > 0]
            if cat_ids_with_counts:
                ph = ",".join("?" for _ in cat_ids_with_counts)
                l3_parents = conn.execute(
                    f"""
                    SELECT id, parent_id FROM categories_v2
                    WHERE id IN ({ph}) AND level = 3 AND parent_id IS NOT NULL
                    """,
                    cat_ids_with_counts,
                ).fetchall()
                for row in l3_parents:
                    child_count = cat_counts[row["id"]]
                    parent_id = row["parent_id"]
                    cat_counts[parent_id] = cat_counts.get(parent_id, 0) + child_count

        return frozenset(offer_ids), cat_counts

    def load_current_offers(
        self,
        *,
        chains: list[str],
        brochure_content_ids: list[str] | None = None,
        full_chains: list[str] | None = None,
    ) -> list[Offer]:
        if not self.available() or not chains:
            return []

        brochure_content_ids = [item for item in (brochure_content_ids or []) if item]
        full_chain_set = {item for item in (full_chains or []) if item}

        # Build cache key from inputs
        cache_key = (
            ",".join(sorted(chains))
            + "|" + ",".join(sorted(brochure_content_ids))
            + "|" + ",".join(sorted(full_chain_set))
        )
        cached = _offers_cache.get(cache_key)
        if cached is not None:
            ts, offers = cached
            if time.monotonic() - ts < _OFFERS_TTL:
                return offers

        params: list[object] = []
        sql = """
            SELECT o.*, pl.category_v2_id
            FROM offers o
            LEFT JOIN product_labels pl ON o.product_name = pl.product_name
            WHERE 1=1
        """

        chain_placeholders = ", ".join("?" for _ in chains)
        sql += f" AND o.chain IN ({chain_placeholders})"
        params.extend(chains)

        scope_clauses: list[str] = []
        if brochure_content_ids:
            placeholders = ", ".join("?" for _ in brochure_content_ids)
            scope_clauses.append(
                f"o.id IN (SELECT offer_id FROM offer_brochures WHERE brochure_content_id IN ({placeholders}))"
            )
            params.extend(brochure_content_ids)
        if full_chain_set:
            placeholders = ", ".join("?" for _ in full_chain_set)
            scope_clauses.append(f"o.chain IN ({placeholders})")
            params.extend(sorted(full_chain_set))
        if scope_clauses:
            sql += " AND (" + " OR ".join(scope_clauses) + ")"

        sql += " ORDER BY o.chain ASC, o.product_name ASC"

        with _connect(self.db_path) as conn:
            rows = list(conn.execute(sql, params))
        result = [self._row_to_offer(row) for row in rows]

        # Cache with size limit
        if len(_offers_cache) >= _OFFERS_CACHE_MAX:
            # Evict oldest entry
            oldest_key = next(iter(_offers_cache))
            del _offers_cache[oldest_key]
        _offers_cache[cache_key] = (time.monotonic(), result)
        return result

    def _row_to_offer(self, row: sqlite3.Row) -> Offer:
        keys = row.keys()
        # Prefer pre-parsed columns from DB; fall back to live parsing for old DBs.
        if "price_per_normalized" in keys and row["price_per_normalized"] is not None:
            base_price_eur = _parse_float(row["base_price_eur"])
            base_unit = row["qty_unit"]
        else:
            base_price_eur, base_unit = _parse_base_price(row["base_price_text"])

        return Offer(
            id=str(row["id"]),
            title=compact_text(row["product_name"]),
            brand=(compact_text(row["brand_name"]) or None),
            chain=str(row["chain"]),
            price_eur=_parse_float(row["sales_price_eur"]),
            was_price_eur=_parse_float(row["regular_price_eur"]),
            is_offer=True,
            valid_from=_parse_kaufda_date(row["valid_from"]),
            valid_to=_parse_kaufda_date(row["valid_until"]),
            quantity=_parse_float(row["qty_value"]) if "qty_value" in keys else None,
            unit=row["qty_unit"] if "qty_unit" in keys else None,
            base_price_eur=base_price_eur,
            base_unit=base_unit,
            image_url=row["offer_image_url"],
            source="kaufda_catalog",
            extra={
                "category_id": (row["category_v2_id"] if "category_v2_id" in row.keys() and row["category_v2_id"] else None)
                    or (row["category_id"] if "category_id" in keys else None),
                "unit_group": row["qty_unit_group"] if "qty_unit_group" in keys else None,
                "normalized_unit": row["normalized_unit"] if "normalized_unit" in keys else None,
                "price_per_normalized": _parse_float(row["price_per_normalized"]) if "price_per_normalized" in keys else None,
            },
        )



def _strip_leading_postcode(query: str) -> str | None:
    parts = query.split()
    if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 5:
        return " ".join(parts[1:]).strip()
    return None
