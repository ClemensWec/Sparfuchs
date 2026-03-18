from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import httpx

from app.connectors.base import Offer
from app.utils.chains import normalize_chain_with_extra
from app.utils.keywords import generate_keyword_variants
from app.utils.unit_parser import parse_base_price, extract_qty_unit_from_description, ParsedBasePrice, _UNIT_MAP


def _resp_html_utf8(resp: httpx.Response) -> str:
    # kaufDA pages are UTF-8, but auto-detection can occasionally produce mojibake (e.g. "BÃ¤renmarke").
    return resp.content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class KaufdaLocation:
    lat: float
    lng: float
    city: str
    zip: str
    countryCode: str = "DE"

    def to_cookie_value(self) -> str:
        return json.dumps(
            {
                "lat": float(self.lat),
                "lng": float(self.lng),
                "city": str(self.city),
                "zip": str(self.zip),
                "countryCode": str(self.countryCode),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )


def _parse_next_data(html: str) -> dict[str, Any] | None:
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _parse_kaufda_dt_to_date(raw: str | None) -> date | None:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Example: 2026-03-08T23:00:00.000+0000
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f%z")
        return dt.date()
    except Exception:
        pass
    try:
        # fallback: ISO-ish without millis
        dt = datetime.fromisoformat(s.replace("+0000", "+00:00"))
        return dt.date()
    except Exception:
        return None


_RE_BONUS = re.compile(r"(?P<eur>\d+[.,]\d{1,2})\s*€\s*Bonus", re.IGNORECASE)
_SEARCH_BUCKETS = ("main", "topRanked", "otherPublishers")
_ASSORTMENT_PUBLISHER_SLUGS = (
    "Aldi-Sued",
    "Aldi-Nord",
    "Lidl",
    "REWE",
    "Edeka",
    "Kaufland",
    "Penny",
    "Netto-Marken-Discount",
    "Norma",
)


def _parse_bonus_cents(text: str | None) -> int | None:
    if not text:
        return None
    m = _RE_BONUS.search(str(text))
    if not m:
        return None
    eur = m.group("eur").replace(".", "").replace(",", ".")
    try:
        return int(round(float(eur) * 100))
    except Exception:
        return None


def _parse_base_price(price_by_base_unit: str | None) -> tuple[float | None, str | None]:
    parsed = parse_base_price(price_by_base_unit)
    return (parsed.price_eur, parsed.unit)




class KaufdaOffersSeoConnector:
    """
    Offer-only connector for kaufDA (Bonial) SEO pages.

    We currently parse offers embedded into the server-rendered Next.js HTML (`__NEXT_DATA__`).
    This is deliberately conservative (no headless browser / no bot-bypass logic).

    Coverage caveat:
    - Product pages often embed only an initial slice of hits, while `totalItems` is larger.
      We augment truncated search pages with retailer assortment pages for the same keyword,
      which substantially improves coverage without needing a browser.
    """

    def __init__(self, *, user_agent: str, location: KaufdaLocation) -> None:
        self._user_agent = user_agent
        self._location = location

    async def fetch_assortment_keyword_urls(self, *, anchor_publisher_slug: str = "Edeka") -> list[str]:
        url = f"https://www.kaufda.de/{anchor_publisher_slug}/Sortiment"
        headers = {"User-Agent": self._user_agent}
        cookies = {"location": self._location.to_cookie_value()}

        async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
            html = await self._fetch_html(client=client, url=url, cookies=cookies)
            if html is None:
                return []

        # The assortment overview HTML contains thousands of keyword links. Example:
        # /Edeka/Sortiment/Tomaten
        pattern = rf"/{re.escape(anchor_publisher_slug)}/Sortiment/[^\"\\s<]+"
        paths = set(re.findall(pattern, html))
        return [f"https://www.kaufda.de{p}" for p in sorted(paths)]

    async def fetch_offers_for_keyword_url(self, *, url: str) -> list[Offer]:
        headers = {"User-Agent": self._user_agent}
        cookies = {"location": self._location.to_cookie_value()}
        async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
            next_data = await self._fetch_next_data(client=client, url=url, cookies=cookies)

        if not next_data:
            return []

        return self._parse_offers_from_next_data(next_data, url)

    async def fetch_search_offers(self, *, keyword: str) -> list[Offer]:
        """
        Live search endpoint for offers.

        Example: https://www.kaufda.de/angebote/Milch
        This tends to return a much better, query-driven offer list than retailer/assortment pages.

        KaufDA is picky:
        - Case-sensitive: "tomaten" -> 404, "Tomaten" -> OK
        - Singular/plural: "Banane" -> 404, "Bananen" -> OK

        We therefore try multiple keyword variants and, when KaufDA's product page is
        visibly truncated, supplement it with retailer assortment pages for the same keyword.
        """
        q = (keyword or "").strip()
        if not q:
            return []

        variants = generate_keyword_variants(q)
        if not variants:
            variants = [q]

        headers = {"User-Agent": self._user_agent}
        cookies = {"location": self._location.to_cookie_value()}
        deduped: dict[tuple[str | None, str | None], Offer] = {}
        fallback_variant: str | None = None
        fallback_gap = 0

        async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
            for variant in variants:
                url = "https://www.kaufda.de/angebote/" + quote(variant, safe="")
                try:
                    next_data = await self._fetch_next_data(client=client, url=url, cookies=cookies)
                except httpx.HTTPStatusError:
                    continue
                if not next_data:
                    continue

                results = self._parse_offers_from_next_data(next_data, url)
                if not results:
                    continue

                self._merge_offers_into(deduped, results)

                embedded_count, total_count = self._count_embedded_offer_items(next_data)
                gap = max(0, total_count - embedded_count)
                if gap > fallback_gap:
                    fallback_gap = gap
                    fallback_variant = variant

            if fallback_variant and fallback_gap > 0:
                extra_offers = await self._fetch_assortment_fallback_offers(
                    client=client,
                    cookies=cookies,
                    keyword=fallback_variant,
                )
                self._merge_offers_into(deduped, extra_offers)

        return list(deduped.values())

    async def _fetch_html(
        self,
        *,
        client: httpx.AsyncClient,
        url: str,
        cookies: dict[str, str],
    ) -> str | None:
        resp = await client.get(url, cookies=cookies)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return _resp_html_utf8(resp)

    async def _fetch_next_data(
        self,
        *,
        client: httpx.AsyncClient,
        url: str,
        cookies: dict[str, str],
    ) -> dict[str, Any] | None:
        html = await self._fetch_html(client=client, url=url, cookies=cookies)
        if html is None:
            return None
        return _parse_next_data(html)

    async def _fetch_assortment_fallback_offers(
        self,
        *,
        client: httpx.AsyncClient,
        cookies: dict[str, str],
        keyword: str,
    ) -> list[Offer]:
        tasks = [
            self._fetch_assortment_page_offers(
                client=client,
                cookies=cookies,
                url=f"https://www.kaufda.de/{slug}/Sortiment/{quote(keyword, safe='')}",
            )
            for slug in _ASSORTMENT_PUBLISHER_SLUGS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        offers: list[Offer] = []
        for result in results:
            if isinstance(result, Exception):
                continue
            offers.extend(result)
        return offers

    async def _fetch_assortment_page_offers(
        self,
        *,
        client: httpx.AsyncClient,
        cookies: dict[str, str],
        url: str,
    ) -> list[Offer]:
        try:
            next_data = await self._fetch_next_data(client=client, url=url, cookies=cookies)
        except httpx.HTTPStatusError:
            return []
        if not next_data:
            return []
        return self._parse_offers_from_next_data(next_data, url)

    def _count_embedded_offer_items(self, next_data: dict[str, Any]) -> tuple[int, int]:
        try:
            page_info = next_data["props"]["pageProps"]["pageInformation"]
        except Exception:
            return (0, 0)

        offers_node = page_info.get("offers")
        if not isinstance(offers_node, dict):
            return (0, 0)

        embedded = 0
        total = 0
        for bucket_name in _SEARCH_BUCKETS:
            bucket = offers_node.get(bucket_name)
            if not isinstance(bucket, dict):
                continue
            items = bucket.get("items")
            item_count = len(items) if isinstance(items, list) else 0
            embedded += item_count
            total_items = bucket.get("totalItems")
            try:
                total += max(item_count, int(total_items))
            except Exception:
                total += item_count
        return (embedded, total)

    def _merge_offers_into(self, target: dict[tuple[str | None, str | None], Offer], offers: list[Offer]) -> None:
        for offer in offers:
            target[(getattr(offer, "chain", None), getattr(offer, "id", None))] = offer

    def _parse_offers_from_next_data(self, next_data: dict[str, Any], url: str) -> list[Offer]:
        """Extrahiert Angebote aus __NEXT_DATA__."""
        try:
            page_info = next_data["props"]["pageProps"]["pageInformation"]
        except Exception:
            return []

        offers_node = page_info.get("offers")
        if not isinstance(offers_node, dict):
            return []

        results: list[Offer] = []
        for bucket_name in _SEARCH_BUCKETS:
            bucket = offers_node.get(bucket_name)
            if not isinstance(bucket, dict):
                continue
            items = bucket.get("items")
            if not isinstance(items, list):
                continue
            for it in items:
                offer = self._offer_from_item(it, page_info=page_info, url=url)
                if offer is not None:
                    results.append(offer)
        return results

    def _offer_from_item(self, it: Any, *, page_info: dict[str, Any], url: str) -> Offer | None:
        if not isinstance(it, dict):
            return None

        chain, chain_extra = normalize_chain_with_extra(it.get("publisherName"))
        if chain is None:
            return None

        offer_uuid = str(it.get("id") or "").strip()
        if not offer_uuid:
            return None

        title = str(it.get("title") or "").strip()
        if not title:
            return None

        prices = it.get("prices") if isinstance(it.get("prices"), dict) else {}
        main_price = prices.get("mainPrice")
        price_eur: float | None = None
        if main_price is not None:
            try:
                price_eur = float(main_price)
            except Exception:
                price_eur = None
        if price_eur is not None and price_eur <= 0:
            price_eur = None

        # Some offers show a secondary/UVP price; treat it as "was" if it looks reasonable.
        was_price_eur: float | None = None
        secondary_price = prices.get("secondaryPrice")
        if secondary_price not in (None, 0, 0.0, "0"):
            try:
                was_price_eur = float(secondary_price)
            except Exception:
                was_price_eur = None
        if was_price_eur is not None and was_price_eur <= 0:
            was_price_eur = None

        parsed_bp = parse_base_price(prices.get("priceByBaseUnit"))

        # Fallback: extract qty/unit from description when priceByBaseUnit is absent
        if not parsed_bp.is_comparable and price_eur is not None:
            description = str(it.get("description") or "").strip()
            desc_result = extract_qty_unit_from_description(description)
            if desc_result is not None:
                qty, unit_key, group, norm_sym = desc_result
                _, multiplier, _ = _UNIT_MAP[unit_key]
                parsed_bp = ParsedBasePrice(
                    raw="",
                    quantity=qty,
                    unit=unit_key,
                    unit_group=group,
                    price_eur=price_eur,
                    normalized_unit=norm_sym,
                    price_per_normalized=price_eur / (qty * multiplier),
                )

        base_price_eur = parsed_bp.price_eur
        base_unit = parsed_bp.unit
        bonus_cents = _parse_bonus_cents(prices.get("description")) or _parse_bonus_cents(it.get("description"))

        img = None
        offer_images = it.get("offerImages")
        if isinstance(offer_images, dict):
            url_node = offer_images.get("url")
            if isinstance(url_node, dict):
                img = str(url_node.get("large") or url_node.get("normal") or url_node.get("thumbnail") or "").strip() or None

        valid_from = _parse_kaufda_dt_to_date(it.get("validFrom"))
        valid_to = _parse_kaufda_dt_to_date(it.get("validUntil"))

        # Location context (important for caching and later filtering)
        loc = page_info.get("location") if isinstance(page_info.get("location"), dict) else {}
        zip_code = str(loc.get("zip") or self._location.zip).strip() or self._location.zip
        city = str(loc.get("city") or self._location.city).strip() or self._location.city

        extra: dict[str, Any] = {
            "kaufda": {
                "offer_id": offer_uuid,
                "publisher_id": it.get("publisherId"),
                "publisher_name": it.get("publisherName"),
                "parent_content": it.get("parentContent"),
                "prices": prices,
                "url": url,
                "location": {"zip": zip_code, "city": city, "lat": loc.get("lat"), "lng": loc.get("lng")},
            },
            "unit_group": parsed_bp.unit_group,
            "normalized_unit": parsed_bp.normalized_unit,
            "price_per_normalized": parsed_bp.price_per_normalized,
        }
        extra.update(chain_extra)

        item_id = f"kaufda:{offer_uuid}:{zip_code}"

        return Offer(
            id=item_id,
            title=title,
            brand=(str(it.get("brand")).strip() or None) if it.get("brand") is not None else None,
            chain=chain,
            price_eur=price_eur,
            is_offer=True,
            member_price_eur=None,
            was_price_eur=was_price_eur,
            valid_from=valid_from,
            valid_to=valid_to,
            unit=parsed_bp.unit,
            quantity=parsed_bp.quantity,
            base_unit=base_unit,
            base_price_eur=base_price_eur,
            source="kaufda",
            loyalty_bonus_cents=bonus_cents,
            bulk_discount_tiers=None,
            image_url=img,
            product_url=None,
            service_types=None,
            tags=None,
            extra=extra,
        )
