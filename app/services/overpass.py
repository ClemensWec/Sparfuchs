from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx

from app.services.cache import ttl_cache
from app.utils.chains import normalize_chain_from_osm_tags


@dataclass(frozen=True)
class Store:
    osm_type: str
    osm_id: int
    name: str
    chain: str
    lat: float
    lon: float
    address: str | None
    postcode: str | None = None
    city_name: str | None = None
    brochure_content_ids: tuple[str, ...] = ()
    brochure_city_name: str | None = None
    brochure_distance_km: float | None = None
    brochure_match_score: float | None = None


class OverpassClient:
    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._cache = ttl_cache(ttl_seconds=2 * 3600)

    async def find_supermarkets(
        self, lat: float, lon: float, radius_m: int, chains: list[str] | None = None
    ) -> list[Store]:
        key = (round(lat, 5), round(lon, 5), int(radius_m))
        if key in self._cache:
            return self._cache[key]

        query = _build_query(lat=lat, lon=lon, radius_m=radius_m, chains=chains)
        headers = {"User-Agent": self._user_agent}
        urls = _overpass_urls()

        async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
            payload = None
            errors: list[str] = []
            for url in urls:
                try:
                    resp = await client.post(url, content=query.encode("utf-8"))
                    if resp.status_code == 200:
                        payload = resp.json()
                        break
                    errors.append(f"{url} (HTTP {resp.status_code})")
                except httpx.HTTPError as exc:
                    errors.append(f"{url} ({type(exc).__name__})")

            if payload is None:
                raise RuntimeError("Overpass fehlgeschlagen: " + "; ".join(errors[:3]))

        stores = _parse_overpass(payload)
        self._cache[key] = stores
        return stores


def _overpass_urls() -> list[str]:
    raw = (os.getenv("SPARFUCHS_OVERPASS_URLS") or "").strip()
    if raw:
        urls = [u.strip() for u in raw.split(",") if u.strip()]
        if urls:
            return urls
    return [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.nchc.org.tw/api/interpreter",
    ]


def _build_query(lat: float, lon: float, radius_m: int, chains: list[str] | None = None) -> str:
    # Erweiterte Shop-Typen für bessere Abdeckung:
    # - supermarket: Standard-Supermärkte (Rewe, Edeka, etc.)
    # - discount: Discounter (Aldi, Lidl, Penny, Netto, Norma)
    # - department_store: Große Märkte (Kaufland, Globus, Marktkauf)
    # - convenience: Kleinere Märkte (manchmal Nahkauf, etc.)
    # - grocery: Lebensmittelgeschäfte (selten, aber möglich)
    shop_re = r"^(supermarket|discount|department_store|convenience|grocery)$"
    return f"""
[out:json][timeout:25];
(
  nwr(around:{radius_m},{lat},{lon})[shop~"{shop_re}"];
);
out center tags;
""".strip()


def _parse_overpass(payload: dict[str, Any]) -> list[Store]:
    out: list[Store] = []
    for el in payload.get("elements", []):
        tags = el.get("tags", {}) or {}
        name = str(tags.get("name") or tags.get("brand") or tags.get("operator") or "Unbekannt")
        chain = normalize_chain_from_osm_tags(tags)

        lat = el.get("lat")
        lon = el.get("lon")
        if lat is None or lon is None:
            center = el.get("center") or {}
            lat, lon = center.get("lat"), center.get("lon")
        if lat is None or lon is None:
            continue

        address = _format_address(tags)
        out.append(
            Store(
                osm_type=str(el.get("type", "node")),
                osm_id=int(el.get("id")),
                name=name,
                chain=chain,
                lat=float(lat),
                lon=float(lon),
                address=address,
                postcode=compact_str(tags.get("addr:postcode")),
                city_name=compact_str(tags.get("addr:city")),
            )
        )

    # entferne Dubletten (z. B. node+way)
    uniq: dict[tuple[str, int], Store] = {(s.osm_type, s.osm_id): s for s in out}
    return list(uniq.values())


def _format_address(tags: dict[str, Any]) -> str | None:
    street = tags.get("addr:street")
    housenumber = tags.get("addr:housenumber")
    postcode = tags.get("addr:postcode")
    city = tags.get("addr:city")
    parts = []
    if street:
        parts.append(f"{street} {housenumber or ''}".strip())
    if postcode or city:
        parts.append(f"{postcode or ''} {city or ''}".strip())
    out = ", ".join(p for p in parts if p)
    return out or None


def compact_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
