from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.services.catalog_data import CatalogDataService
from app.services.catalog_search import CatalogSearchService
from app.services.category_search import CategorySearchService
from app.services.geocode import GeoPoint, GeocodeError
from app.services.matching import Suggestion
from app.services.pricing import BasketPricer, SparMixPricer, SparMixResult, WantedItem
from app.utils.chains import KNOWN_CHAINS
from app.utils.text import compact_text


APP_NAME = "Sparfuchs"

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


async def _resolve_location(location: str) -> GeoPoint:
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
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "chains": KNOWN_CHAINS,
            "default_radius_km": 5,
            "default_location": "Bonn",
            "error": None,
            "warnings": [],
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
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "chains": KNOWN_CHAINS,
                "default_radius_km": radius_km,
                "default_location": location,
                "error": "Bitte mindestens einen Artikel zum Einkaufszettel hinzufuegen.",
                "warnings": [],
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
        },
    )


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {"ok": True, "app": APP_NAME}
