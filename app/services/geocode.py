from __future__ import annotations

from dataclasses import dataclass

import httpx

from app.services.cache import ttl_cache
from app.utils.text import compact_text


class GeocodeError(RuntimeError):
    pass


@dataclass(frozen=True)
class GeoPoint:
    lat: float
    lon: float
    display_name: str


class Geocoder:
    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._cache = ttl_cache(ttl_seconds=24 * 3600)

    async def geocode_de(self, query: str) -> GeoPoint:
        q = compact_text(query)
        if not q:
            raise GeocodeError("Standort fehlt.")

        if q in self._cache:
            return self._cache[q]

        candidates = [q]
        simplified = _simplify_query(q)
        if simplified and simplified not in candidates:
            candidates.append(simplified)

        url = "https://nominatim.openstreetmap.org/search"
        headers = {"User-Agent": self._user_agent}

        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            last_error: GeocodeError | None = None
            for candidate in candidates:
                params = {
                    "q": candidate,
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,
                    "countrycodes": "de",
                }
                resp = await client.get(url, params=params)
                if resp.status_code == 403:
                    raise GeocodeError(
                        "Geocoding geblockt (HTTP 403). Setze `SPARFUCHS_USER_AGENT` auf einen sauberen User-Agent "
                        "mit echter Kontaktadresse oder nutze Koordinaten (z. B. `50.7374, 7.0982`)."
                    )
                if resp.status_code == 429:
                    raise GeocodeError(
                        "Geocoding rate-limited (HTTP 429). Bitte kurz warten, Koordinaten nutzen oder Cache aktiv lassen."
                    )
                if resp.status_code != 200:
                    last_error = GeocodeError(f"Geocoding fehlgeschlagen (HTTP {resp.status_code}).")
                    continue
                data = resp.json()
                if not data:
                    continue

                item = data[0]
                point = GeoPoint(lat=float(item["lat"]), lon=float(item["lon"]), display_name=str(item["display_name"]))
                self._cache[q] = point
                return point

        if last_error is not None:
            raise last_error
        raise GeocodeError("Standort nicht gefunden. Versuch z. B. 'Bonn' oder '10115 Berlin'.")


def _simplify_query(query: str) -> str | None:
    parts = query.split()
    if len(parts) >= 2 and parts[0].isdigit() and len(parts[0]) == 5:
        return " ".join(parts[1:]).strip()
    return None
