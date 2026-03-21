from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.services.catalog_data import CatalogDataService, get_index_stats
from app.services.catalog_search import CatalogSearchService
from app.services.category_search import CategorySearchService
from app.services.geocode import GeoPoint, GeocodeError
from app.services.matching import Suggestion
from app.services.pricing import BasketPricer, SparMixPricer, SparMixResult, WantedItem
from app.utils.chains import KNOWN_CHAINS
from app.utils.text import compact_text


APP_NAME = "Sparfuchs"

# ---------------------------------------------------------------------------
# Geocode cache: avoids repeated DB/network lookups for same location string
# ---------------------------------------------------------------------------
_geocode_cache: dict[str, GeoPoint] = {}
_GEOCODE_CACHE_MAX = 200

# Intent detection for deal-seeking queries
_DEAL_INTENTS = {
    "rabatt", "angebot", "angebote", "günstig", "guenstig", "billig",
    "deal", "deals", "sale", "aktion", "aktionen", "sonderangebot",
    "sonderangebote", "schnäppchen", "schnaeppchen",
}


def _user_agent() -> str:
    return os.getenv("SPARFUCHS_USER_AGENT", "Sparfuchs/0.1 (set SPARFUCHS_USER_AGENT)")


def _try_parse_coords(value: str) -> tuple[float, float] | None:
    s = (value or "").strip()
    if not s:
        return None
    s = s.replace(";", ",")
    if "," in s:
        a, b = (part.strip() for part in s.split(",", 1))
        parts = [a, b]
    else:
        parts = [part for part in s.split() if part]
        if len(parts) != 2:
            return None
    try:
        lat = float(parts[0])
        lon = float(parts[1])
    except ValueError:
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return (lat, lon)


def _smart_store_limit(stores: list, max_count: int) -> list:
    """Keep at least 1 store per chain, then fill remaining slots by distance."""
    seen_chains: set[str] = set()
    guaranteed: list = []
    rest: list = []
    for store in stores:
        if store.chain not in seen_chains:
            seen_chains.add(store.chain)
            guaranteed.append(store)
        else:
            rest.append(store)
    if len(guaranteed) >= max_count:
        return guaranteed
    remaining = max_count - len(guaranteed)
    return guaranteed + rest[:remaining]


COMPARE_TIMEOUT_SECONDS = float(os.getenv("SPARFUCHS_COMPARE_TIMEOUT_SECONDS", "60"))
MAX_EVALUATED_STORES = int(os.getenv("SPARFUCHS_MAX_EVALUATED_STORES", "80"))


app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


@app.get("/sw.js")
async def service_worker():
    return FileResponse(
        "app/static/sw.js",
        media_type="application/javascript",
        headers={"Service-Worker-Allowed": "/"},
    )

catalog_db_path = Path(os.getenv("SPARFUCHS_CATALOG_DB_PATH", "data/kaufda_dataset/offers.sqlite3"))
catalog_search = CatalogSearchService(db_path=catalog_db_path)
catalog_data = CatalogDataService(db_path=catalog_db_path)
category_search = CategorySearchService(db_path=catalog_db_path)

# Ensure search_log table exists for analytics
import sqlite3 as _sqlite3
try:
    _conn = _sqlite3.connect(str(catalog_db_path))
    _conn.execute("""
        CREATE TABLE IF NOT EXISTS search_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            result_count INTEGER,
            selected_category_id INTEGER,
            selected_category_name TEXT,
            corrected_from TEXT,
            timestamp TEXT DEFAULT (datetime('now')),
            location TEXT,
            radius_km REAL
        )
    """)
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_search_log_query ON search_log(query)")
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_search_log_timestamp ON search_log(timestamp)")
    # Migrate: add location/radius_km columns if missing (existing installs)
    _existing_cols = {r[1] for r in _conn.execute("PRAGMA table_info(search_log)").fetchall()}
    if "location" not in _existing_cols:
        _conn.execute("ALTER TABLE search_log ADD COLUMN location TEXT")
    if "radius_km" not in _existing_cols:
        _conn.execute("ALTER TABLE search_log ADD COLUMN radius_km REAL")
    _conn.commit()
    _conn.close()
except Exception:
    pass

# Ensure price_history table exists for trend tracking
try:
    _conn = _sqlite3.connect(str(catalog_db_path))
    _conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER,
            category_name TEXT,
            chain TEXT,
            price_eur REAL,
            was_price_eur REAL,
            store_name TEXT,
            location TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )
    """)
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_category ON price_history(category_id, timestamp)")
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_timestamp ON price_history(timestamp)")
    _conn.commit()
    _conn.close()
except Exception:
    pass

# Ensure product_labels index for category lookups
try:
    _conn = _sqlite3.connect(str(catalog_db_path))
    _conn.execute("CREATE INDEX IF NOT EXISTS idx_product_labels_cat ON product_labels(category_v2_id)")
    _conn.commit()
    _conn.close()
except Exception:
    pass


async def _resolve_location(location: str) -> GeoPoint:
    # Fast path: check in-memory cache first
    cache_key = (location or "").strip().lower()
    if cache_key in _geocode_cache:
        return _geocode_cache[cache_key]

    result = await _resolve_location_uncached(location)

    # Store in cache (simple FIFO eviction)
    if len(_geocode_cache) >= _GEOCODE_CACHE_MAX:
        oldest = next(iter(_geocode_cache))
        del _geocode_cache[oldest]
    _geocode_cache[cache_key] = result
    return result


async def _resolve_location_uncached(location: str) -> GeoPoint:
    coords = _try_parse_coords(location)
    if coords is not None:
        return GeoPoint(lat=coords[0], lon=coords[1], display_name=f"{coords[0]}, {coords[1]}")

    raw_query = compact_text(location)
    if not raw_query:
        raise GeocodeError("Standort fehlt.")

    if raw_query.isdigit() and len(raw_query) == 5:
        plz_place = catalog_data.resolve_by_postcode(raw_query) if catalog_data.available() else None
        if plz_place is not None:
            return GeoPoint(lat=plz_place.lat, lon=plz_place.lon, display_name=plz_place.display_name)
        raise GeocodeError(
            "PLZ konnte lokal nicht aufgeloest werden. "
            "Bitte Stadt, 'PLZ Stadt' oder Koordinaten verwenden."
        )

    local_place = catalog_data.resolve_local_place(raw_query) if catalog_data.available() else None
    if local_place is not None:
        return GeoPoint(lat=local_place.lat, lon=local_place.lon, display_name=local_place.display_name)

    raise GeocodeError(
        "Standort lokal nicht gefunden. Bitte Stadt, 'PLZ Stadt' oder Koordinaten verwenden."
    )


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    stats = get_index_stats(catalog_db_path)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "chains": KNOWN_CHAINS,
            "default_radius_km": 5,
            "default_location": "Bonn",
            "error": None,
            "warnings": [],
            "offer_count": stats["offer_count"],
            "chain_count": stats["chain_count"],
            "last_updated": stats["last_updated"],
        },
    )


@app.get("/api/suggest")
async def api_suggest(
    q: str = "",
    chains: str | None = None,
    location: str = "",
) -> JSONResponse:
    query = (q or "").strip()
    if len(query) < 2:
        return JSONResponse({"q": q, "hits": []})

    selected_chains = None
    if chains:
        selected_chains = [item for item in (part.strip() for part in chains.split(",")) if item]
    chains_for_suggest = selected_chains or KNOWN_CHAINS

    lat, lon, radius = None, None, None
    loc = (location or "").strip()
    if loc:
        try:
            geo = await _resolve_location(loc)
            lat, lon, radius = geo.lat, geo.lon, 30.0
        except (GeocodeError, Exception):
            pass

    local_hits: list[Suggestion] = []
    if catalog_search.available():
        try:
            local_hits = catalog_search.search(
                query, chains=chains_for_suggest,
                lat=lat, lon=lon, radius_km=radius, limit=15,
            )
        except Exception:
            local_hits = []
    return JSONResponse({"q": q, "hits": [asdict(hit) for hit in local_hits]})


@app.get("/api/suggest-categories")
async def api_suggest_categories(
    q: str = "",
    location: str = "",
    radius_km: float = 10.0,
) -> JSONResponse:
    """Return matching product categories for autocomplete."""
    query = (q or "").strip()
    if len(query) < 2:
        return JSONResponse({"q": q, "categories": []})

    # Resolve location scope for local category counts
    local_counts: dict[int, int] | None = None
    loc = (location or "").strip()
    if loc:
        try:
            geo = await _resolve_location(loc)
            scope = catalog_data.resolve_location_scope(
                lat=geo.lat, lon=geo.lon, radius_km=radius_km,
            )
            if scope is not None:
                local_counts = scope.category_counts
        except Exception:
            pass

    # Intent detection for deal-seeking queries
    query_tokens = set(query.lower().split())
    is_deal_intent = bool(query_tokens & _DEAL_INTENTS)

    if is_deal_intent:
        deal_result: dict[str, Any] = {
            "id": None,
            "name": "Alle Angebote durchsuchen",
            "offer_count": 0,
            "display_offer_count": 0,
            "kind": "intent",
            "type": "deal_intent",
            "oberkategorie": "Angebote",
            "search_url": f"/search?q={query}",
        }
        try:
            import sqlite3 as _sq
            _dc = _sq.connect(str(catalog_db_path))
            total = _dc.execute("SELECT COUNT(*) FROM offers").fetchone()[0]
            _dc.close()
            deal_result["offer_count"] = total
            deal_result["display_offer_count"] = total
        except Exception:
            pass

        # Still show category results (remove deal tokens for category search)
        clean_tokens = [t for t in query.lower().split() if t not in _DEAL_INTENTS]
        if clean_tokens:
            clean_query = " ".join(clean_tokens)
            hits = category_search.search(clean_query, limit=6, local_category_counts=local_counts)
        else:
            hits = []

        # Extract spell correction info if present
        corrected_to = None
        if hits and "corrected_to" in hits[0]:
            corrected_to = hits[0]["corrected_to"]
            for h in hits:
                h.pop("corrected_from", None)
                h.pop("corrected_to", None)

        brand_hits = category_search.search_brands(clean_query, limit=3) if clean_tokens else []

        for h in hits:
            h.pop("_level", None)
        resp: dict[str, Any] = {"q": q, "categories": [deal_result] + hits}
        if brand_hits:
            resp["brands"] = brand_hits
        if corrected_to:
            resp["corrected_to"] = corrected_to
        return JSONResponse(resp)

    hits = category_search.search(query, limit=8, local_category_counts=local_counts)

    # Fallback: if location filter removes everything, show national results
    if not hits and local_counts is not None:
        national_hits = category_search.search(query, limit=5)
        for h in national_hits:
            h["not_local"] = True
            h["display_offer_count"] = h.get("offer_count", 0)
        hits = national_hits

    # Extract spell correction info if present
    corrected_to = None
    if hits and "corrected_to" in hits[0]:
        corrected_to = hits[0]["corrected_to"]
        # Clean correction markers from results
        for h in hits:
            h.pop("corrected_from", None)
            h.pop("corrected_to", None)
    # Search for matching brands
    brand_hits = category_search.search_brands(query, limit=3)

    # Remove internal fields before sending to client
    for h in hits:
        h.pop("_level", None)
    resp: dict[str, Any] = {"q": q, "categories": hits}
    if brand_hits:
        resp["brands"] = brand_hits
    if corrected_to:
        resp["corrected_to"] = corrected_to
    return JSONResponse(resp)


@app.get("/api/sibling-categories")
async def api_sibling_categories(
    category_id: int,
    location: str = "",
    radius_km: float = 10.0,
) -> JSONResponse:
    """Return sibling categories (same parent) for product swap feature."""
    if not catalog_data.available():
        return JSONResponse({"siblings": []})

    import sqlite3
    conn = sqlite3.connect(str(catalog_db_path))
    conn.row_factory = sqlite3.Row

    try:
        # Look up the given category in categories_v2
        row = conn.execute(
            "SELECT id, name, parent_id, level, product_count FROM categories_v2 WHERE id = ?",
            (category_id,),
        ).fetchone()

        if not row:
            # Fallback: try product_categories table
            row = conn.execute(
                "SELECT id, name, parent_id, kind, offer_count AS product_count FROM product_categories WHERE id = ?",
                (category_id,),
            ).fetchone()
            if not row:
                return JSONResponse({"siblings": []})
            # product_categories: kind='family' means group head
            parent_id = row["parent_id"]
            if parent_id is None:
                parent_id = category_id
            siblings = conn.execute(
                """SELECT id, name, offer_count AS product_count
                   FROM product_categories
                   WHERE parent_id = ? AND id != ?
                   ORDER BY offer_count DESC""",
                (parent_id, category_id),
            ).fetchall()
            # Include parent if it's not the queried category
            if parent_id != category_id:
                parent_row = conn.execute(
                    "SELECT id, name, offer_count AS product_count FROM product_categories WHERE id = ?",
                    (parent_id,),
                ).fetchone()
                if parent_row:
                    siblings = [parent_row] + list(siblings)
        else:
            # categories_v2: 3-level hierarchy
            level = int(row["level"])
            parent_id = row["parent_id"]

            if level == 3:
                # Specific item → siblings are other level-3 under same parent (level-2 group)
                if parent_id is None:
                    return JSONResponse({"siblings": []})
                siblings = conn.execute(
                    """SELECT id, name, product_count
                       FROM categories_v2
                       WHERE parent_id = ? AND id != ? AND level = 3
                       ORDER BY product_count DESC""",
                    (parent_id, category_id),
                ).fetchall()
                # Also include the parent group node if it has offers
                parent_row = conn.execute(
                    "SELECT id, name, product_count FROM categories_v2 WHERE id = ?",
                    (parent_id,),
                ).fetchone()
                if parent_row and parent_row["product_count"] and parent_row["product_count"] > 0:
                    siblings = [parent_row] + list(siblings)
            elif level == 2:
                # Group node → siblings are other level-2 under same parent (level-1)
                if parent_id is None:
                    return JSONResponse({"siblings": []})
                siblings = conn.execute(
                    """SELECT id, name, product_count
                       FROM categories_v2
                       WHERE parent_id = ? AND id != ? AND level = 2
                       ORDER BY product_count DESC""",
                    (parent_id, category_id),
                ).fetchall()
            else:
                # Level 1 (top-level) → no meaningful siblings
                return JSONResponse({"siblings": []})

        # Resolve local counts if location provided
        local_counts: dict[int, int] | None = None
        loc = (location or "").strip()
        if loc:
            try:
                geo = await _resolve_location(loc)
                scope = catalog_data.resolve_location_scope(
                    lat=geo.lat, lon=geo.lon, radius_km=radius_km,
                )
                if scope is not None:
                    local_counts = scope.category_counts
            except Exception:
                pass

        result = []
        for s in siblings:
            cat_id = s["id"]
            count = s["product_count"] or 0
            display_count = count

            if local_counts is not None:
                display_count = local_counts.get(cat_id, 0)
                if display_count == 0:
                    continue  # Skip siblings with no local offers

            result.append({
                "id": cat_id,
                "name": s["name"],
                "offer_count": count,
                "display_count": display_count,
            })

        # Sort by display count descending, limit to 6
        result.sort(key=lambda x: x["display_count"], reverse=True)
        result = result[:6]

        return JSONResponse({"siblings": result})
    finally:
        conn.close()


@app.get("/api/alternative-offers")
async def api_alternative_offers(
    category_id: int,
    location: str = "",
    radius_km: float = 10.0,
    chains: str = "",
) -> JSONResponse:
    """Return actual offers from sibling categories for the alternatives panel."""
    if not catalog_data.available():
        return JSONResponse({"groups": []})

    import sqlite3
    from app.services.catalog_data import _parse_base_price

    conn = sqlite3.connect(str(catalog_db_path))
    conn.row_factory = sqlite3.Row

    try:
        # 1) Find the category and its siblings (same logic as sibling-categories)
        row = conn.execute(
            "SELECT id, name, parent_id, level FROM categories_v2 WHERE id = ?",
            (category_id,),
        ).fetchone()

        if not row:
            return JSONResponse({"groups": []})

        level = int(row["level"])
        parent_id = row["parent_id"]

        if level == 3 and parent_id:
            # L3 → siblings are other L3 under same L2 parent
            sibling_rows = conn.execute(
                "SELECT id, name FROM categories_v2 WHERE parent_id = ? AND id != ? AND level = 3",
                (parent_id, category_id),
            ).fetchall()
        elif level == 2:
            # L2 → children are L3 under this node (show offers from children)
            sibling_rows = conn.execute(
                "SELECT id, name FROM categories_v2 WHERE parent_id = ? AND level = 3",
                (category_id,),
            ).fetchall()
        else:
            return JSONResponse({"groups": []})

        if not sibling_rows:
            return JSONResponse({"groups": []})

        # 2) Collect all sibling category IDs
        sibling_ids = [s["id"] for s in sibling_rows]
        sibling_names = {s["id"]: s["name"] for s in sibling_rows}

        # 3) Build location scope filter
        scope_where = ""
        scope_params: list = []

        loc = (location or "").strip()
        if loc:
            try:
                geo = await _resolve_location(loc)
                scope = catalog_data.resolve_location_scope(
                    lat=geo.lat, lon=geo.lon, radius_km=radius_km,
                )
                if scope is not None:
                    bids = scope.brochure_content_ids
                    fchains = scope.full_chains
                    clauses = []
                    if bids:
                        placeholders = ",".join("?" * len(bids))
                        clauses.append(f"o.id IN (SELECT offer_id FROM offer_brochures WHERE brochure_content_id IN ({placeholders}))")
                        scope_params.extend(bids)
                    if fchains:
                        placeholders = ",".join("?" * len(fchains))
                        clauses.append(f"o.chain IN ({placeholders})")
                        scope_params.extend(fchains)
                    if clauses:
                        scope_where = " AND (" + " OR ".join(clauses) + ")"
            except Exception:
                pass

        # Chain filter
        chain_where = ""
        chain_params: list = []
        if chains:
            chain_list = [c.strip() for c in chains.split(",") if c.strip()]
            if chain_list:
                placeholders = ",".join("?" * len(chain_list))
                chain_where = f" AND o.chain IN ({placeholders})"
                chain_params = chain_list

        # 4) Query actual offers grouped by sibling category
        cat_placeholders = ",".join("?" * len(sibling_ids))
        sql = f"""
            SELECT o.product_name, o.brand_name, o.chain, o.sales_price_eur,
                   o.regular_price_eur, o.offer_image_url, o.base_price_text,
                   pl.category_v2_id
            FROM offers o
            JOIN product_labels pl ON o.product_name = pl.product_name
            WHERE pl.category_v2_id IN ({cat_placeholders})
                  {scope_where}{chain_where}
            ORDER BY pl.category_v2_id, o.sales_price_eur IS NULL, o.sales_price_eur ASC
        """
        params = sibling_ids + scope_params + chain_params
        offer_rows = conn.execute(sql, params).fetchall()

        # 5) Group and limit: max 4 offers per category, max 6 categories
        from collections import defaultdict
        grouped: dict[int, list] = defaultdict(list)
        for orow in offer_rows:
            cat = orow["category_v2_id"]
            if len(grouped[cat]) >= 4:
                continue
            bp_eur, bp_unit = _parse_base_price(orow["base_price_text"])
            grouped[cat].append({
                "title": orow["product_name"],
                "brand": orow["brand_name"],
                "chain": orow["chain"],
                "price_eur": float(orow["sales_price_eur"]) if orow["sales_price_eur"] is not None else None,
                "was_price_eur": float(orow["regular_price_eur"]) if orow["regular_price_eur"] is not None else None,
                "image_url": orow["offer_image_url"],
                "base_price_eur": bp_eur,
                "base_unit": bp_unit,
                "category_id": cat,
                "category_name": sibling_names.get(cat, ""),
            })

        # 6) Build response — sort groups by offer count descending
        groups = []
        for cat_id in sorted(grouped.keys(), key=lambda c: len(grouped[c]), reverse=True):
            if len(groups) >= 6:
                break
            groups.append({
                "category_id": cat_id,
                "category_name": sibling_names.get(cat_id, ""),
                "offers": grouped[cat_id],
            })

        return JSONResponse({"groups": groups})
    finally:
        conn.close()


@app.post("/api/log-search")
async def api_log_search(request: Request) -> JSONResponse:
    """Log a search event for analytics."""
    try:
        body = await request.json()
        query = str(body.get("query", ""))[:500]
        result_count = body.get("result_count")
        if result_count is not None:
            result_count = int(result_count)
        cat_id = body.get("category_id")
        if cat_id is not None:
            cat_id = int(cat_id)
        cat_name = body.get("category_name")
        if cat_name is not None:
            cat_name = str(cat_name)[:200]
        corrected_from = body.get("corrected_from")
        if corrected_from is not None:
            corrected_from = str(corrected_from)[:500]
        location_val = body.get("location")
        if location_val is not None:
            location_val = str(location_val)[:200]
        radius_val = body.get("radius_km")
        if radius_val is not None:
            try:
                radius_val = float(radius_val)
            except (ValueError, TypeError):
                radius_val = None

        conn = _sqlite3.connect(str(catalog_db_path))
        try:
            conn.execute(
                "INSERT INTO search_log (query, result_count, selected_category_id, selected_category_name, corrected_from, location, radius_km) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (query, result_count, cat_id, cat_name, corrected_from, location_val, radius_val),
            )
            conn.commit()
        finally:
            conn.close()
        return JSONResponse({"ok": True})
    except Exception:
        return JSONResponse({"ok": False}, status_code=400)


@app.get("/api/search")
async def api_search(
    q: str = "",
    location: str = "",
    radius_km: float = 10.0,
    chains: str | None = None,
    limit: int = 60,
    offset: int = 0,
) -> JSONResponse:
    query = (q or "").strip()
    if len(query) < 2:
        return JSONResponse({"q": q, "hits": [], "total": 0, "location": location})

    selected_chains = None
    if chains:
        selected_chains = [item for item in (part.strip() for part in chains.split(",")) if item]

    lat, lon = None, None
    resolved_location = location
    loc = (location or "").strip()
    if loc:
        try:
            geo = await _resolve_location(loc)
            lat, lon = geo.lat, geo.lon
            resolved_location = geo.display_name
        except (GeocodeError, Exception):
            pass

    # Resolve location scope for offer filtering
    location_scope = None
    if lat is not None and lon is not None:
        try:
            location_scope = catalog_data.resolve_location_scope(
                lat=lat, lon=lon, radius_km=radius_km,
            )
        except Exception:
            pass

    all_hits: list[Suggestion] = []
    if catalog_search.available():
        try:
            all_hits = catalog_search.search(
                query,
                chains=selected_chains,
                lat=lat, lon=lon,
                radius_km=radius_km if lat is not None else None,
                limit=0,
                local_offer_ids=location_scope.local_offer_ids if location_scope else None,
            )
        except Exception:
            all_hits = []

    total = len(all_hits)
    page = all_hits[offset:offset + limit] if offset > 0 else all_hits[:limit]

    available_chains = sorted({h.chain for h in all_hits})
    available_brands = sorted({h.brand for h in all_hits if h.brand})

    return JSONResponse({
        "q": query,
        "hits": [asdict(hit) for hit in page],
        "total": total,
        "offset": offset,
        "limit": limit,
        "location": resolved_location,
        "available_chains": available_chains,
        "available_brands": available_brands,
    })


@app.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = "",
    location: str = "",
    radius_km: float = 10.0,
) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="search.html",
        context={
            "q": q,
            "location": location or "Bonn",
            "radius_km": radius_km,
            "chains": KNOWN_CHAINS,
        },
    )


@app.post("/api/compare")
async def api_compare(request: Request) -> JSONResponse:
    """Live comparison API — returns store ranking as JSON."""
    import time as _time
    import sqlite3 as _sqlite3

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Ungültige Anfrage."}, status_code=400)

    location = str(body.get("location", "")).strip()
    radius_km = float(body.get("radius_km", 5))
    basket_payload = body.get("basket", [])
    max_stores_val = int(body.get("max_stores", 0))

    if not location:
        return JSONResponse({"error": "Bitte Standort angeben."}, status_code=400)

    # Build wanted items
    wanted_items: list[WantedItem] = []
    for raw in basket_payload:
        if not isinstance(raw, dict):
            continue
        cat_id = raw.get("category_id")
        cat_name = raw.get("category_name", "")
        if cat_id is not None:
            expanded = category_search.expand_category(
                category_id=int(cat_id), category_name=str(cat_name),
            ) if category_search.available() else {"ids": [int(cat_id)]}
            wanted_items.append(WantedItem(
                q=str(cat_name), brand=None, any_brand=True,
                category_id=int(cat_id), category_name=str(cat_name),
                category_ids=tuple(int(i) for i in expanded.get("ids", [int(cat_id)])),
            ))
            continue
        query = str(raw.get("q", "")).strip()
        if not query:
            continue
        wanted_items.append(WantedItem(
            q=query,
            brand=(str(raw.get("brand")).strip() or None) if raw.get("brand") is not None else None,
            any_brand=bool(raw.get("any_brand", True)),
        ))

    if not wanted_items:
        return JSONResponse({"error": "Bitte mindestens einen Artikel hinzufügen."}, status_code=400)

    if not catalog_data.available():
        return JSONResponse({"error": "Datenbank nicht verfügbar."}, status_code=503)

    selected_chains = KNOWN_CHAINS

    try:
        async with asyncio.timeout(COMPARE_TIMEOUT_SECONDS):
            _t0 = _time.perf_counter()
            geo = await _resolve_location(location)

            stores = catalog_data.find_stores_in_radius(
                lat=geo.lat, lon=geo.lon, radius_km=radius_km, chains=selected_chains,
            )

            if not stores:
                return JSONResponse({"rows": [], "spar_mix": None, "warning": "Keine Märkte im Umkreis gefunden."})

            total_store_count = len(stores)
            if MAX_EVALUATED_STORES > 0 and total_store_count > MAX_EVALUATED_STORES:
                stores = _smart_store_limit(stores, MAX_EVALUATED_STORES)

            stores = catalog_data.match_stores_to_regions(stores)

            chains_in_area = sorted({s.chain for s in stores})
            matched_brochure_ids = sorted({
                bid for s in stores for bid in (s.brochure_content_ids or ()) if bid
            })
            chains_with_brochures = {s.chain for s in stores if s.brochure_content_ids}
            chains_without_brochures = [c for c in chains_in_area if c not in chains_with_brochures]

            offers = catalog_data.load_current_offers(
                chains=chains_in_area,
                brochure_content_ids=matched_brochure_ids,
                full_chains=chains_without_brochures,
            )

            basket_pricer = BasketPricer(offers) if offers else None
            rows = basket_pricer.price_basket_for_stores(
                stores=stores, wanted=wanted_items, origin=(geo.lat, geo.lon),
            ) if basket_pricer else []

            effective_max = max_stores_val if max_stores_val > 0 else None
            spar_mix = SparMixPricer(basket_pricer).compute(
                stores=stores, wanted=wanted_items, origin=(geo.lat, geo.lon),
                max_stores=effective_max, basket_rows=rows,
            ) if basket_pricer else SparMixResult(total_eur=None, lines=[], store_count=0, stores_used=[])

            _t1 = _time.perf_counter()
            print(f"[PERF] /api/compare: {(_t1-_t0)*1000:.0f}ms ({len(stores)} stores × {len(wanted_items)} items)")

            # Serialize rows
            def _serialize_row(r, idx):
                total_items = len(r.lines)
                found = total_items - r.missing_count
                lines_json = []
                for line in r.lines:
                    lj = {"wanted": line.wanted.q, "score": line.score}
                    if line.offer:
                        lj["offer"] = {
                            "title": line.offer.title,
                            "brand": line.offer.brand,
                            "price_eur": line.offer.price_eur,
                            "was_price_eur": line.offer.was_price_eur,
                            "image_url": line.offer.image_url,
                            "base_price_eur": line.offer.base_price_eur,
                            "base_unit": line.offer.base_unit,
                            "quantity": line.offer.quantity,
                            "unit": line.offer.unit,
                            "valid_from": str(line.offer.valid_from) if line.offer.valid_from else None,
                            "valid_to": str(line.offer.valid_to) if line.offer.valid_to else None,
                            "category_id": (line.offer.extra or {}).get("category_id"),
                        }
                    else:
                        lj["offer"] = None
                    lines_json.append(lj)

                diff = None
                if idx > 0 and r.total_eur is not None and rows[0].total_eur is not None:
                    diff = round(r.total_eur - rows[0].total_eur, 2)

                return {
                    "rank": idx + 1,
                    "store_name": r.store.name,
                    "chain": r.store.chain,
                    "address": r.store.address,
                    "lat": r.store.lat,
                    "lon": r.store.lon,
                    "distance_km": round(r.distance_km, 1),
                    "total_eur": round(r.total_eur, 2) if r.total_eur is not None else None,
                    "found": found,
                    "total_items": total_items,
                    "missing_count": r.missing_count,
                    "diff_eur": diff,
                    "lines": lines_json,
                }

            rows_json = [_serialize_row(r, i) for i, r in enumerate(rows)]

            # Serialize spar_mix
            spar_mix_json = None
            if spar_mix and spar_mix.lines:
                sm_lines = []
                for line in spar_mix.lines:
                    sl = {"wanted": line.wanted.q, "price_eur": line.price_eur}
                    if line.store:
                        sl["chain"] = line.store.chain
                        sl["store_name"] = line.store.name
                        sl["address"] = line.store.address
                        sl["lat"] = line.store.lat
                        sl["lon"] = line.store.lon
                    if line.offer:
                        sl["offer_title"] = line.offer.title
                        sl["image_url"] = line.offer.image_url
                    sm_lines.append(sl)
                sm_saving = None
                if spar_mix.total_eur is not None and rows and rows[0].total_eur is not None and spar_mix.total_eur < rows[0].total_eur:
                    sm_saving = round(rows[0].total_eur - spar_mix.total_eur, 2)
                spar_mix_json = {
                    "total_eur": round(spar_mix.total_eur, 2) if spar_mix.total_eur is not None else None,
                    "store_count": spar_mix.store_count,
                    "stores_used": spar_mix.stores_used,
                    "saving_vs_best": sm_saving,
                    "lines": sm_lines,
                }

            # Log prices (fire-and-forget)
            try:
                _log_conn = _sqlite3.connect(str(catalog_db_path))
                for row in rows[:3]:
                    for line in row.lines:
                        if line.offer and line.offer.price_eur is not None:
                            _log_conn.execute(
                                "INSERT INTO price_history (category_id, category_name, chain, price_eur, was_price_eur, store_name, location) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (getattr(line.wanted, 'category_id', None), line.wanted.q, row.store.chain, line.offer.price_eur, getattr(line.offer, 'was_price_eur', None), row.store.name, location),
                            )
                _log_conn.commit()
                _log_conn.close()
            except Exception:
                pass

            return JSONResponse({"rows": rows_json, "spar_mix": spar_mix_json})

    except GeocodeError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except asyncio.TimeoutError:
        return JSONResponse({"error": f"Timeout nach {COMPARE_TIMEOUT_SECONDS:.0f}s. Versuche einen kleineren Radius."}, status_code=504)
    except Exception as exc:
        return JSONResponse({"error": f"Fehler: {exc}"}, status_code=500)


@app.post("/results", response_class=HTMLResponse)
async def results(
    request: Request,
    location: str = Form(...),
    radius_km: float = Form(5.0),
    chains: list[str] = Form(default=[]),
    basket_json: str = Form(default="[]"),
    max_stores: int = Form(default=0),
) -> HTMLResponse:
    try:
        basket_payload = json.loads(basket_json or "[]")
    except json.JSONDecodeError:
        basket_payload = []

    wanted_items: list[WantedItem] = []
    for raw in basket_payload:
        if not isinstance(raw, dict):
            continue
        # Category-based items
        cat_id = raw.get("category_id")
        cat_name = raw.get("category_name", "")
        if cat_id is not None:
            expanded = category_search.expand_category(
                category_id=int(cat_id),
                category_name=str(cat_name),
            ) if category_search.available() else {"ids": [int(cat_id)]}
            wanted_items.append(
                WantedItem(
                    q=str(cat_name),
                    brand=None,
                    any_brand=True,
                    category_id=int(cat_id),
                    category_name=str(cat_name),
                    category_ids=tuple(int(item) for item in expanded.get("ids", [int(cat_id)])),
                )
            )
            continue
        # Legacy text-based items
        query = str(raw.get("q", "")).strip()
        if not query:
            continue
        wanted_items.append(
            WantedItem(
                q=query,
                brand=(str(raw.get("brand")).strip() or None) if raw.get("brand") is not None else None,
                any_brand=bool(raw.get("any_brand", True)),
            )
        )

    if not wanted_items:
        stats = get_index_stats(catalog_db_path)
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "chains": KNOWN_CHAINS,
                "default_radius_km": radius_km,
                "default_location": location,
                "error": "Bitte mindestens einen Artikel zum Einkaufszettel hinzufuegen.",
                "warnings": [],
                "offer_count": stats["offer_count"],
                "chain_count": stats["chain_count"],
                "last_updated": stats["last_updated"],
            },
            status_code=400,
        )

    selected_chains = [chain for chain in chains if chain] or KNOWN_CHAINS
    warnings: list[str] = []

    if not catalog_data.available():
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "error": "Lokale Angebotsdatenbank fehlt oder ist nicht erreichbar.",
                "warnings": [],
                "location": location,
                "radius_km": radius_km,
                "chains": KNOWN_CHAINS,
                "selected_chains": selected_chains,
                "basket_json": json.dumps([item.to_dict() for item in wanted_items], ensure_ascii=False),
                "stores": [],
                "rows": [],
                "prospects_by_chain": {},
            },
            status_code=503,
        )

    try:
        async with asyncio.timeout(COMPARE_TIMEOUT_SECONDS):
            import time as _time
            _t0 = _time.perf_counter()

            geo = await _resolve_location(location)
            _t1 = _time.perf_counter()

            stores = catalog_data.find_stores_in_radius(
                lat=geo.lat,
                lon=geo.lon,
                radius_km=radius_km,
                chains=selected_chains,
            )

            if not stores:
                warnings.append(
                    "Keine lokalen KaufDA-Marktregionen im Radius gefunden. "
                    "Tipp: Radius erhoehen, Standort praezisieren oder andere Ketten waehlen."
                )
                return templates.TemplateResponse(
                    request=request,
                    name="results.html",
                    context={
                        "error": None,
                        "warnings": warnings,
                        "location": location,
                        "radius_km": radius_km,
                        "chains": KNOWN_CHAINS,
                        "selected_chains": selected_chains,
                        "basket_json": json.dumps([item.to_dict() for item in wanted_items], ensure_ascii=False),
                        "stores": [],
                        "rows": [],
                        "prospects_by_chain": {},
                    },
                )

            _t2 = _time.perf_counter()
            total_store_count = len(stores)
            if MAX_EVALUATED_STORES > 0 and total_store_count > MAX_EVALUATED_STORES:
                stores = _smart_store_limit(stores, MAX_EVALUATED_STORES)
                warnings.append(
                    f"Info: Es wurden {total_store_count} Maerkte gefunden. "
                    f"Fuer schnelle Ergebnisse werden {len(stores)} Maerkte verglichen "
                    f"(mind. 1 pro Kette)."
                )
            stores = catalog_data.match_stores_to_regions(stores)
            _t3 = _time.perf_counter()

            chains_in_area = sorted({store.chain for store in stores})
            matched_brochure_ids = sorted(
                {
                    brochure_id
                    for store in stores
                    for brochure_id in (store.brochure_content_ids or ())
                    if brochure_id
                }
            )
            chains_with_brochures = {
                store.chain for store in stores if store.brochure_content_ids
            }
            chains_without_brochures = [
                chain for chain in chains_in_area
                if chain not in chains_with_brochures
            ]
            offers = catalog_data.load_current_offers(
                chains=chains_in_area,
                brochure_content_ids=matched_brochure_ids,
                full_chains=chains_without_brochures,
            )
            _t4 = _time.perf_counter()

            basket_pricer = BasketPricer(offers) if offers else None
            rows = basket_pricer.price_basket_for_stores(
                stores=stores,
                wanted=wanted_items,
                origin=(geo.lat, geo.lon),
            ) if basket_pricer else []
            _t5 = _time.perf_counter()

            effective_max_stores = max_stores if max_stores > 0 else None
            spar_mix = SparMixPricer(basket_pricer).compute(
                stores=stores,
                wanted=wanted_items,
                origin=(geo.lat, geo.lon),
                max_stores=effective_max_stores,
                basket_rows=rows,
            ) if basket_pricer else SparMixResult(total_eur=None, lines=[], store_count=0, stores_used=[])
            _t6 = _time.perf_counter()

            print(f"[PERF] Vergleich: geo={(_t1-_t0)*1000:.0f}ms "
                  f"stores={(_t2-_t1)*1000:.0f}ms "
                  f"regions={(_t3-_t2)*1000:.0f}ms "
                  f"offers={(_t4-_t3)*1000:.0f}ms({len(offers)}rows) "
                  f"basket={(_t5-_t4)*1000:.0f}ms({len(stores)}stores×{len(wanted_items)}items) "
                  f"sparmix={(_t6-_t5)*1000:.0f}ms "
                  f"TOTAL={(_t6-_t0)*1000:.0f}ms")

            missing_store_chains = [chain for chain in selected_chains if chain not in chains_in_area]

            # Fire-and-forget: log prices for trend tracking
            try:
                _log_conn = _sqlite3.connect(str(catalog_db_path))
                for row in rows[:3]:  # Only top 3 stores
                    for line in row.lines:
                        if line.offer and line.offer.price_eur is not None:
                            cat_id = getattr(line.wanted, 'category_id', None)
                            cat_name = getattr(line.wanted, 'q', '')
                            _log_conn.execute(
                                "INSERT INTO price_history (category_id, category_name, chain, price_eur, was_price_eur, store_name, location) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (cat_id, cat_name, row.store.chain, line.offer.price_eur, getattr(line.offer, 'was_price_eur', None), row.store.name, location),
                            )
                _log_conn.commit()
                _log_conn.close()
            except Exception:
                pass

    except GeocodeError as exc:
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "error": str(exc),
                "warnings": [],
                "location": location,
                "radius_km": radius_km,
                "chains": KNOWN_CHAINS,
                "selected_chains": selected_chains,
                "basket_json": json.dumps([item.to_dict() for item in wanted_items], ensure_ascii=False),
                "stores": [],
                "rows": [],
                "prospects_by_chain": {},
            },
            status_code=400,
        )
    except asyncio.TimeoutError:
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "error": (
                    f"Der Vergleich dauert gerade zu lange (Timeout nach {COMPARE_TIMEOUT_SECONDS:.0f}s). "
                    "Tipp: kleineren Radius waehlen oder weniger Artikel vergleichen."
                ),
                "warnings": [],
                "location": location,
                "radius_km": radius_km,
                "chains": KNOWN_CHAINS,
                "selected_chains": selected_chains,
                "basket_json": json.dumps([item.to_dict() for item in wanted_items], ensure_ascii=False),
                "stores": [],
                "rows": [],
                "prospects_by_chain": {},
            },
            status_code=504,
        )
    except Exception as exc:
        return templates.TemplateResponse(
            request=request,
            name="results.html",
            context={
                "error": f"Fehler beim Laden der Daten: {exc}",
                "warnings": [],
                "location": location,
                "radius_km": radius_km,
                "chains": KNOWN_CHAINS,
                "selected_chains": selected_chains,
                "basket_json": json.dumps([item.to_dict() for item in wanted_items], ensure_ascii=False),
                "stores": [],
                "rows": [],
                "prospects_by_chain": {},
            },
            status_code=500,
        )

    stats = get_index_stats(catalog_db_path)
    return templates.TemplateResponse(
        request=request,
        name="results.html",
        context={
            "error": None,
            "warnings": warnings,
            "location": location,
            "radius_km": radius_km,
            "chains": KNOWN_CHAINS,
            "selected_chains": selected_chains,
            "basket_json": json.dumps([item.to_dict() for item in wanted_items], ensure_ascii=False),
            "stores": stores,
            "rows": rows,
            "spar_mix": spar_mix,
            "max_stores": max_stores,
            "prospects_by_chain": {},
            "last_updated": stats.get("last_updated", ""),
        },
    )


@app.get("/api/popular-items")
async def api_popular_items(limit: int = 8) -> JSONResponse:
    """Return most frequently searched categories."""
    import sqlite3
    conn = sqlite3.connect(str(catalog_db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT selected_category_id, selected_category_name, COUNT(*) as cnt
            FROM search_log
            WHERE selected_category_id IS NOT NULL
              AND selected_category_name IS NOT NULL
              AND timestamp > datetime('now', '-30 days')
            GROUP BY selected_category_id
            ORDER BY cnt DESC
            LIMIT ?
        """, (limit,)).fetchall()

        items = [{"id": r["selected_category_id"], "name": r["selected_category_name"], "searches": r["cnt"]} for r in rows]
        return JSONResponse({"items": items})
    finally:
        conn.close()


@app.get("/api/category-tiles")
async def api_category_tiles(
    chains: str = "",
    location: str = "",
    radius_km: float = 10.0,
) -> JSONResponse:
    """Return top-level categories for quick-start tiles, with optional chain/location filtering."""
    if not catalog_data.available():
        return JSONResponse({"tiles": [], "available_chains": []})

    import sqlite3
    conn = sqlite3.connect(str(catalog_db_path))
    conn.row_factory = sqlite3.Row
    try:
        # 0) Get all available chains (always unfiltered)
        all_chains = [r[0] for r in conn.execute(
            "SELECT DISTINCT chain FROM offers WHERE chain IS NOT NULL AND sales_price_eur IS NOT NULL ORDER BY chain"
        ).fetchall()]

        # 1) Parse chain filter
        chain_list = [c.strip() for c in chains.split(",") if c.strip()] if chains.strip() else []
        chain_clause = ""
        chain_params: list = []
        if chain_list:
            chain_ph = ", ".join("?" for _ in chain_list)
            chain_clause = f"AND o.chain IN ({chain_ph})"
            chain_params = chain_list

        # 2) Resolve location scope (optional)
        local_offer_ids: frozenset[int] | None = None
        if location.strip():
            try:
                geo = await _resolve_location(location)
                scope = catalog_data.resolve_location_scope(
                    lat=geo.lat, lon=geo.lon, radius_km=radius_km,
                )
                if scope is not None:
                    local_offer_ids = scope.local_offer_ids
            except (GeocodeError, Exception):
                pass

        # 3) Build SQL with optional chain filter
        if local_offer_ids is not None and len(local_offer_ids) > 0:
            # With location filter: fetch all then filter in Python
            rows = conn.execute(f"""
                SELECT c.id, c.name, o.id as offer_id
                FROM categories_v2 c
                JOIN categories_v2 sub ON (sub.parent_id = c.id
                    OR sub.parent_id IN (SELECT id FROM categories_v2 WHERE parent_id = c.id))
                JOIN product_labels pl ON pl.category_v2_id = sub.id
                JOIN offers o ON o.product_name = pl.product_name
                    AND o.sales_price_eur IS NOT NULL
                    {chain_clause}
                WHERE c.level = 1
            """, chain_params).fetchall()

            # Group by category, count only local offers
            from collections import Counter
            cat_info: dict[int, str] = {}
            cat_counts: Counter = Counter()
            for r in rows:
                cat_info[r["id"]] = r["name"]
                if r["offer_id"] in local_offer_ids:
                    cat_counts[r["id"]] += 1

            tiles_raw = [
                {"id": cid, "name": compact_text(cat_info[cid]), "count": cat_counts[cid]}
                for cid in cat_counts
            ]
            tiles_raw.sort(key=lambda t: t["count"], reverse=True)
            tiles_raw = tiles_raw[:12]
        elif local_offer_ids is not None and len(local_offer_ids) == 0:
            # Location set but no offers in radius
            return JSONResponse({"tiles": [], "available_chains": all_chains})
        else:
            # No location filter
            rows = conn.execute(f"""
                SELECT c.id, c.name,
                       COUNT(DISTINCT o.id) as product_count
                FROM categories_v2 c
                JOIN categories_v2 sub ON (sub.parent_id = c.id
                    OR sub.parent_id IN (SELECT id FROM categories_v2 WHERE parent_id = c.id))
                JOIN product_labels pl ON pl.category_v2_id = sub.id
                JOIN offers o ON o.product_name = pl.product_name
                    AND o.sales_price_eur IS NOT NULL
                    {chain_clause}
                WHERE c.level = 1
                GROUP BY c.id
                ORDER BY product_count DESC
                LIMIT 12
            """, chain_params).fetchall()

            if not rows:
                rows = conn.execute("""
                    SELECT id, name, expanded_offer_count as product_count
                    FROM product_categories
                    WHERE kind = 'family'
                    ORDER BY expanded_offer_count DESC
                    LIMIT 12
                """).fetchall()

            tiles_raw = [{"id": r["id"], "name": compact_text(r["name"]), "count": r["product_count"] or 0} for r in rows]

        # 4) Dedup: if a tile name is a substring of another, remove the longer one
        names = [t["name"].lower() for t in tiles_raw]
        drop_indices: set[int] = set()
        for i, name_i in enumerate(names):
            for j, name_j in enumerate(names):
                if i == j:
                    continue
                if name_i in name_j and len(name_j) > len(name_i):
                    drop_indices.add(j)
        tiles = [t for idx, t in enumerate(tiles_raw) if idx not in drop_indices]

        return JSONResponse({"tiles": tiles, "available_chains": all_chains})
    except Exception:
        return JSONResponse({"tiles": [], "available_chains": []})
    finally:
        conn.close()


@app.get("/api/offers-by-category")
async def api_offers_by_category(
    category_id: int = 0,
    location: str = "",
    radius_km: float = 10.0,
    limit: int = 40,
    offset: int = 0,
    chains: str = "",
) -> JSONResponse:
    """Return offers for a category (with subcategories) for browse UI."""
    if category_id <= 0:
        return JSONResponse({"error": "category_id is required"}, status_code=400)

    if not catalog_data.available():
        return JSONResponse({"error": "database unavailable"}, status_code=503)

    import sqlite3
    conn = sqlite3.connect(str(catalog_db_path))
    conn.row_factory = sqlite3.Row
    try:
        # 1) Fetch the category itself
        cat_row = conn.execute(
            "SELECT id, name, level FROM categories_v2 WHERE id = ?", [category_id]
        ).fetchone()
        if not cat_row:
            return JSONResponse({"error": "category not found"}, status_code=404)
        category = {"id": cat_row["id"], "name": compact_text(cat_row["name"]), "level": cat_row["level"]}

        # 2) Direct subcategories
        sub_rows = conn.execute(
            "SELECT id, name, product_count FROM categories_v2 WHERE parent_id = ? ORDER BY product_count DESC",
            [category_id],
        ).fetchall()
        subcategories = [{"id": r["id"], "name": compact_text(r["name"]), "count": r["product_count"] or 0} for r in sub_rows]

        # 3) Collect all descendant category IDs recursively
        all_ids: list[int] = [category_id]
        queue: list[int] = [category_id]
        while queue:
            current = queue.pop()
            children = conn.execute(
                "SELECT id FROM categories_v2 WHERE parent_id = ?", [current]
            ).fetchall()
            for child in children:
                cid = child["id"]
                all_ids.append(cid)
                queue.append(cid)

        ph = ", ".join("?" for _ in all_ids)

        # 3b) Parse chain filter
        chain_list = [c.strip() for c in chains.split(",") if c.strip()] if chains.strip() else []
        chain_clause = ""
        chain_params: list = []
        if chain_list:
            chain_ph = ", ".join("?" for _ in chain_list)
            chain_clause = f"AND o.chain IN ({chain_ph})"
            chain_params = chain_list

        # 4) Resolve location scope for filtering (optional)
        local_offer_ids: frozenset[int] | None = None
        if location.strip():
            try:
                geo = await _resolve_location(location)
                scope = catalog_data.resolve_location_scope(
                    lat=geo.lat, lon=geo.lon, radius_km=radius_km,
                )
                if scope is not None:
                    local_offer_ids = scope.local_offer_ids
            except GeocodeError:
                pass  # fall through to unfiltered results

        # 5) Query offers
        if local_offer_ids is not None:
            if not local_offer_ids:
                # No offers in scope at all
                return JSONResponse({
                    "category": category,
                    "subcategories": subcategories,
                    "offers": [],
                    "total": 0,
                    "has_more": False,
                })

            # Build a temp table or IN clause for local offer IDs
            # For performance, use a subquery with the location-scoped IDs
            # Since local_offer_ids can be large, we filter in Python after SQL
            count_row = conn.execute(f"""
                SELECT COUNT(DISTINCT o.id) AS cnt
                FROM offers o
                JOIN product_labels pl ON o.product_name = pl.product_name
                WHERE pl.category_v2_id IN ({ph})
                  AND o.sales_price_eur IS NOT NULL
                  {chain_clause}
            """, all_ids + chain_params).fetchone()
            total_unfiltered = count_row["cnt"]

            # Fetch more than needed so we can filter by location in Python
            batch_size = max(limit + offset + 200, 500)
            rows = conn.execute(f"""
                SELECT DISTINCT o.id, o.product_name AS title, o.brand_name AS brand,
                       o.chain, o.sales_price_eur AS price_eur,
                       o.regular_price_eur AS was_price_eur,
                       o.base_price_text, o.offer_image_url AS image_url,
                       pl.category_v2_id
                FROM offers o
                JOIN product_labels pl ON o.product_name = pl.product_name
                WHERE pl.category_v2_id IN ({ph})
                  AND o.sales_price_eur IS NOT NULL
                  {chain_clause}
                ORDER BY o.sales_price_eur ASC
                LIMIT ?
            """, all_ids + chain_params + [batch_size]).fetchall()

            # Filter to local offers
            filtered = [r for r in rows if r["id"] in local_offer_ids]
            total = len(filtered)
            page = filtered[offset:offset + limit]
        else:
            # No location filter — straightforward query
            count_row = conn.execute(f"""
                SELECT COUNT(DISTINCT o.id) AS cnt
                FROM offers o
                JOIN product_labels pl ON o.product_name = pl.product_name
                WHERE pl.category_v2_id IN ({ph})
                  AND o.sales_price_eur IS NOT NULL
                  {chain_clause}
            """, all_ids + chain_params).fetchone()
            total = count_row["cnt"]

            rows = conn.execute(f"""
                SELECT DISTINCT o.id, o.product_name AS title, o.brand_name AS brand,
                       o.chain, o.sales_price_eur AS price_eur,
                       o.regular_price_eur AS was_price_eur,
                       o.base_price_text, o.offer_image_url AS image_url,
                       pl.category_v2_id
                FROM offers o
                JOIN product_labels pl ON o.product_name = pl.product_name
                WHERE pl.category_v2_id IN ({ph})
                  AND o.sales_price_eur IS NOT NULL
                  {chain_clause}
                ORDER BY o.sales_price_eur ASC
                LIMIT ? OFFSET ?
            """, all_ids + chain_params + [limit, offset]).fetchall()
            page = rows

        # 6) Build category name lookup for the IDs we have
        cat_ids_in_result = list({r["category_v2_id"] for r in page if r["category_v2_id"]})
        cat_name_map: dict[int, str] = {}
        if cat_ids_in_result:
            cn_ph = ", ".join("?" for _ in cat_ids_in_result)
            for cn_row in conn.execute(
                f"SELECT id, name FROM categories_v2 WHERE id IN ({cn_ph})", cat_ids_in_result
            ):
                cat_name_map[cn_row["id"]] = compact_text(cn_row["name"])

        # 7) Build response offers
        from app.services.catalog_data import _parse_float
        offers = []
        for r in page:
            offers.append({
                "id": str(r["id"]),
                "title": compact_text(r["title"]),
                "brand": compact_text(r["brand"]) or None,
                "chain": r["chain"],
                "price_eur": _parse_float(r["price_eur"]),
                "was_price_eur": _parse_float(r["was_price_eur"]),
                "base_price_text": r["base_price_text"],
                "image_url": r["image_url"],
                "category_id": r["category_v2_id"],
                "category_name": cat_name_map.get(r["category_v2_id"], ""),
            })

        return JSONResponse({
            "category": category,
            "subcategories": subcategories,
            "offers": offers,
            "total": total,
            "has_more": (offset + limit) < total,
        })
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    finally:
        conn.close()


@app.get("/api/price-trend")
async def api_price_trend(
    category_id: int = 0,
    category_name: str = "",
    days: int = 30,
) -> JSONResponse:
    """Return price trend for a category over the last N days."""
    import sqlite3
    conn = sqlite3.connect(str(catalog_db_path))
    conn.row_factory = sqlite3.Row
    try:
        if category_id > 0:
            where = "category_id = ?"
            params: list = [category_id]
        elif category_name:
            where = "category_name = ?"
            params = [category_name]
        else:
            return JSONResponse({"trend": [], "direction": "stable"})

        rows = conn.execute(f"""
            SELECT
                date(timestamp) as day,
                ROUND(AVG(price_eur), 2) as avg_price,
                ROUND(MIN(price_eur), 2) as min_price,
                ROUND(MAX(price_eur), 2) as max_price,
                COUNT(*) as samples
            FROM price_history
            WHERE {where}
              AND timestamp > datetime('now', '-{days} days')
            GROUP BY date(timestamp)
            ORDER BY day
        """, params).fetchall()

        trend_data = [
            {"day": r["day"], "avg": r["avg_price"], "min": r["min_price"], "max": r["max_price"], "n": r["samples"]}
            for r in rows
        ]

        # Calculate direction
        direction = "stable"
        if len(trend_data) >= 2:
            first_avg = trend_data[0]["avg"]
            last_avg = trend_data[-1]["avg"]
            if last_avg < first_avg * 0.95:
                direction = "down"
            elif last_avg > first_avg * 1.05:
                direction = "up"

        return JSONResponse({
            "trend": trend_data,
            "direction": direction,
            "category_id": category_id,
            "category_name": category_name,
        })
    finally:
        conn.close()


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {"ok": True, "app": APP_NAME}
