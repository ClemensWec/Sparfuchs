from __future__ import annotations

import re
from dataclasses import dataclass
from itertools import combinations

from app.connectors.base import Offer
from app.services.overpass import Store
from app.utils.geo import haversine_km
from app.utils.matching import calculate_match_score, normalize_text, MIN_SCORE_WITH_PRICE, MIN_SCORE_WITHOUT_PRICE
from app.utils.german_stems import get_token_variants


# Mirror of _CONSUMER_SYNONYMS from matching.py for pre-filter expansion
_CONSUMER_SYNONYM_PAIRS: list[tuple[str, str]] = [
    ("marmelade", "konfituere"),
    ("zahnpasta", "zahncreme"),
    ("weintrauben", "trauben"),
    ("weintraube", "traube"),
    ("wodka", "vodka"),
]


@dataclass(frozen=True)
class WantedItem:
    q: str
    brand: str | None
    any_brand: bool = True
    category_id: int | None = None
    category_name: str | None = None
    category_ids: tuple[int, ...] = ()

    def to_dict(self) -> dict[str, object]:
        d: dict[str, object] = {"q": self.q, "brand": self.brand, "any_brand": self.any_brand}
        if self.category_id is not None:
            d["category_id"] = self.category_id
            d["category_name"] = self.category_name
            if self.category_ids:
                d["category_ids"] = list(self.category_ids)
        return d


@dataclass(frozen=True)
class LineMatch:
    wanted: WantedItem
    offer: Offer | None
    score: float | None


@dataclass(frozen=True)
class StoreBasketRow:
    store: Store
    distance_km: float
    total_eur: float | None
    missing_count: int           # Kein Angebot gefunden
    no_price_count: int          # Angebot gefunden, aber ohne Preis (z.B. "2+1 Gratis")
    offer_count: int             # Angebote mit Preis
    lines: list[LineMatch]


class BasketPricer:
    def __init__(self, offers: list[Offer]) -> None:
        self._offers = offers
        self._offers_by_chain: dict[str, list[Offer]] = {}
        for offer in offers:
            self._offers_by_chain.setdefault(offer.chain, []).append(offer)
        self._scope_offers_cache: dict[tuple[str, tuple[str, ...]], list[Offer]] = {}
        self._best_match_cache: dict[tuple[str, tuple[str, ...], str, str | None, bool], LineMatch] = {}
        # Pre-compute normalized offer text for pre-filter (avoids repeated normalize_text calls)
        self._offer_text_norm: dict[int, str] = {}
        for offer in offers:
            self._offer_text_norm[id(offer)] = normalize_text(f"{offer.brand or ''} {offer.title}")

    def price_basket_for_stores(
        self, stores: list[Store], wanted: list[WantedItem], origin: tuple[float, float]
    ) -> list[StoreBasketRow]:
        rows: list[StoreBasketRow] = []
        for store in stores:
            lines: list[LineMatch] = []
            total = 0.0
            missing = 0
            no_price = 0
            offer_count = 0

            brochure_ids = tuple(sorted(set(getattr(store, "brochure_content_ids", ()) or ())))
            chain_offers = self._scope_offers(store.chain, brochure_ids)

            for w in wanted:
                cache_key = (
                    store.chain,
                    brochure_ids,
                    w.q.strip(),
                    (w.brand or "").strip().lower() or None,
                    bool(w.any_brand),
                    tuple(sorted(w.category_ids)) if w.category_ids else (w.category_id,) if w.category_id is not None else (),
                )
                best = self._best_match_cache.get(cache_key)
                if best is None:
                    best = self._best_match(w, chain_offers)
                    self._best_match_cache[cache_key] = best
                if best.offer is None:
                    missing += 1
                else:
                    if best.offer.price_eur is None:
                        # Angebot gefunden, aber ohne Preis (z.B. "2+1 Gratis", "Aktion")
                        no_price += 1
                    else:
                        total += float(best.offer.price_eur)
                        if best.offer.is_offer:
                            offer_count += 1
                lines.append(best)

            # Für Ranking: Items ohne Preis nicht als "missing" zählen
            items_without_total = missing + no_price
            distance_km = haversine_km(origin[0], origin[1], store.lat, store.lon)
            rows.append(
                StoreBasketRow(
                    store=store,
                    distance_km=distance_km,
                    total_eur=None if items_without_total == len(wanted) else round(total, 2),
                    missing_count=missing,
                    no_price_count=no_price,
                    offer_count=offer_count,
                    lines=lines,
                )
            )

        # Ranking: Meiste Treffer zuerst, dann günstigster Preis, dann kürzeste Distanz
        # found_count = Artikel mit Angebot (auch ohne Preis zählt als "gefunden")
        def _sort_key(r: StoreBasketRow) -> tuple[int, int, float, float]:
            found_count = len(wanted) - r.missing_count  # Je mehr gefunden, desto besser
            has_price = 0 if r.total_eur is not None else 1  # Preis vorhanden = besser
            price = r.total_eur if r.total_eur is not None else float("inf")
            return (-found_count, has_price, price, r.distance_km)

        rows.sort(key=_sort_key)
        # Stores ohne irgendeinen bepreisten Treffer ausblenden.
        rows = [r for r in rows if r.offer_count > 0]
        return rows

    def _scope_offers(self, chain: str, brochure_ids: tuple[str, ...]) -> list[Offer]:
        # With the normalized schema, offers are already pre-filtered at the SQL
        # level via offer_brochures JOIN, so per-offer brochure scoping is not needed.
        # Just return all chain offers (which are already the relevant regional set).
        return self._offers_by_chain.get(chain, [])

    def _best_match(self, wanted: WantedItem, offers: list[Offer]) -> LineMatch:
        # Category-based matching: filter by category_id, then pick cheapest
        if wanted.category_id is not None:
            result = self._best_match_by_category(wanted, offers)
            if result.offer is not None:
                return result
            # Fallback to text-based matching using category_name

        # Text-based matching with pre-filter
        query = wanted.q.strip()
        brand = (wanted.brand or "").strip().lower() if (wanted.brand and not wanted.any_brand) else None

        # Pre-filter: build expanded token set for cheap substring check.
        # Uses normalized text (ä→ae) + stem variants + synonyms to avoid
        # losing matches like huhn→hähnchen, brot→brötchen, nudeln→nudel.
        q_norm = normalize_text(query)
        q_tokens_raw = [t for t in q_norm.split() if len(t) >= 3]
        pf_tokens: list[str] = []
        for t in q_tokens_raw:
            variants = get_token_variants(t)
            pf_tokens.extend(variants)
        # Also add synonym variants
        for term_a, term_b in _CONSUMER_SYNONYM_PAIRS:
            for t in q_tokens_raw:
                if t == term_a:
                    pf_tokens.append(term_b)
                elif t == term_b:
                    pf_tokens.append(term_a)
        # Deduplicate
        pf_tokens = list(set(pf_tokens))

        best_offer: Offer | None = None
        best_score: float = 0.0

        for offer in offers:
            # Brand-Filter mit Wort-Grenzen
            if brand:
                if not offer.brand:
                    continue
                offer_brand_lower = offer.brand.lower()
                # Prüfe ob Brand als ganzes Wort vorkommt (nicht nur Substring)
                if not _brand_matches(brand, offer_brand_lower):
                    continue

            # Fast pre-filter: skip offers where no token/variant appears in normalized text.
            if pf_tokens:
                offer_text_norm = self._offer_text_norm.get(id(offer), "")
                if not any(t in offer_text_norm for t in pf_tokens):
                    continue

            # Verbessertes Matching mit Token-basiertem Score
            offer_text = f"{offer.brand or ''} {offer.title}".strip()
            score = calculate_match_score(query, offer_text)

            # Schwellenwert abhängig von Preis-Verfügbarkeit
            min_score = MIN_SCORE_WITH_PRICE if offer.price_eur is not None else MIN_SCORE_WITHOUT_PRICE
            if score < min_score:
                continue

            if score > best_score:
                best_offer, best_score = offer, score
            elif score == best_score and best_offer is not None:
                # Bei gleichem Score: Angebot mit Preis bevorzugen, dann günstigeren Preis
                if best_offer.price_eur is None and offer.price_eur is not None:
                    best_offer = offer
                elif offer.price_eur is not None and best_offer.price_eur is not None:
                    if float(offer.price_eur) < float(best_offer.price_eur):
                        best_offer = offer

        return LineMatch(wanted=wanted, offer=best_offer, score=(best_score if best_offer else None))

    def _best_match_by_category(self, wanted: WantedItem, offers: list[Offer]) -> LineMatch:
        """Find best offer matching a product category (by category_id in offer.extra)."""
        cat_ids = set(wanted.category_ids or (() if wanted.category_id is None else (wanted.category_id,)))
        best_offer: Offer | None = None
        best_price: float | None = None

        for offer in offers:
            offer_cat_id = (offer.extra or {}).get("category_id")
            if offer_cat_id not in cat_ids:
                continue
            # Found a match — pick cheapest with price
            if offer.price_eur is not None:
                price = float(offer.price_eur)
                if best_price is None or price < best_price:
                    best_offer = offer
                    best_price = price
            elif best_offer is None:
                best_offer = offer

        score = 100.0 if best_offer else None
        return LineMatch(wanted=wanted, offer=best_offer, score=score)


@dataclass(frozen=True)
class SparMixLine:
    wanted: WantedItem
    offer: Offer | None
    store: Store | None
    price_eur: float | None
    score: float | None


@dataclass(frozen=True)
class SparMixResult:
    total_eur: float | None
    lines: list[SparMixLine]
    store_count: int
    stores_used: list[str]


class SparMixPricer:
    """Find the cheapest offer for each item, optionally limited to max_stores stores."""

    def __init__(self, pricer: BasketPricer) -> None:
        self._pricer = pricer

    def compute(
        self,
        stores: list[Store],
        wanted: list[WantedItem],
        origin: tuple[float, float],
        max_stores: int | None = None,
        basket_rows: list[StoreBasketRow] | None = None,
    ) -> SparMixResult:
        pricer = self._pricer

        # Phase 1: Build price matrix from pre-computed basket rows if available,
        # otherwise compute from scratch.
        store_matches: dict[int, dict[int, tuple[LineMatch, float | None]]] = {}
        if basket_rows is not None:
            # Reuse already-computed matches from BasketPricer
            store_index = {id(store): si for si, store in enumerate(stores)}
            for row in basket_rows:
                si = store_index.get(id(row.store))
                if si is None:
                    continue
                for wi, line in enumerate(row.lines):
                    if line.offer is not None:
                        price = float(line.offer.price_eur) if line.offer.price_eur is not None else None
                        store_matches.setdefault(si, {})[wi] = (line, price)
        else:
            for si, store in enumerate(stores):
                brochure_ids = tuple(sorted(set(getattr(store, "brochure_content_ids", ()) or ())))
                chain_offers = pricer._scope_offers(store.chain, brochure_ids)
                for wi, w in enumerate(wanted):
                    match = pricer._best_match(w, chain_offers)
                    if match.offer is not None:
                        price = float(match.offer.price_eur) if match.offer.price_eur is not None else None
                        store_matches.setdefault(si, {})[wi] = (match, price)

        # Filter to stores that have at least 1 priced item
        priced_stores = [si for si in store_matches if any(
            p is not None for _, p in store_matches[si].values()
        )]

        if not priced_stores:
            empty_lines = [
                SparMixLine(wanted=w, offer=None, store=None, price_eur=None, score=None)
                for w in wanted
            ]
            return SparMixResult(total_eur=None, lines=empty_lines, store_count=0, stores_used=[])

        # Phase 2: Find optimal store subset
        if max_stores is not None and max_stores > 0 and len(priced_stores) > max_stores:
            selected = self._find_best_combination(
                priced_stores, store_matches, len(wanted), max_stores,
            )
        else:
            selected = set(priced_stores)

        # Phase 3: Build result from selected stores
        lines: list[SparMixLine] = []
        total = 0.0
        stores_used: set[str] = set()

        for wi, w in enumerate(wanted):
            best_match: LineMatch | None = None
            best_price: float | None = None
            best_store: Store | None = None

            for si in selected:
                entry = store_matches.get(si, {}).get(wi)
                if entry is None:
                    continue
                match, price = entry
                if price is not None:
                    if best_price is None or price < best_price:
                        best_match = match
                        best_price = price
                        best_store = stores[si]
                elif best_match is None:
                    best_match = match
                    best_store = stores[si]

            if best_price is not None:
                total += best_price
                stores_used.add(best_store.chain if best_store else "")

            lines.append(SparMixLine(
                wanted=w,
                offer=best_match.offer if best_match else None,
                store=best_store,
                price_eur=best_price,
                score=best_match.score if best_match else None,
            ))

        return SparMixResult(
            total_eur=round(total, 2) if any(l.price_eur is not None for l in lines) else None,
            lines=lines,
            store_count=len(stores_used),
            stores_used=sorted(stores_used),
        )

    def _find_best_combination(
        self,
        candidates: list[int],
        store_matches: dict[int, dict[int, tuple[LineMatch, float | None]]],
        num_items: int,
        max_stores: int,
    ) -> set[int]:
        # Sort candidates by coverage (items with price), keep top N for search
        candidates_sorted = sorted(
            candidates,
            key=lambda si: -sum(1 for _, p in store_matches.get(si, {}).values() if p is not None),
        )
        search_limit = min(len(candidates_sorted), 25 if max_stores <= 2 else 15)
        search_pool = candidates_sorted[:search_limit]

        best_combo: tuple[int, ...] | None = None
        best_score = (0, float("inf"))  # (-coverage, total_price)

        for combo in combinations(search_pool, max_stores):
            coverage = 0
            total = 0.0
            for wi in range(num_items):
                item_best: float | None = None
                for si in combo:
                    entry = store_matches.get(si, {}).get(wi)
                    if entry is not None:
                        _, price = entry
                        if price is not None and (item_best is None or price < item_best):
                            item_best = price
                if item_best is not None:
                    coverage += 1
                    total += item_best
            score = (-coverage, total)
            if best_combo is None or score < best_score:
                best_score = score
                best_combo = combo

        return set(best_combo) if best_combo else set()


def _brand_matches(wanted_brand: str, offer_brand: str) -> bool:
    """
    Prüft ob eine gewünschte Marke im Angebot vorkommt.

    Verwendet Wort-Grenzen um falsche Matches zu vermeiden.
    Z.B. "milka" sollte "Milka" matchen, aber nicht "amilka" oder "milkas".
    """
    # Escape für Regex und Wort-Grenzen hinzufügen
    pattern = r"\b" + re.escape(wanted_brand) + r"\b"
    return bool(re.search(pattern, offer_brand, re.IGNORECASE))
