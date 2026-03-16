from __future__ import annotations

from dataclasses import dataclass

from app.connectors.base import Offer
from app.utils.matching import calculate_match_score, MIN_SCORE_WITH_PRICE, MIN_SCORE_WITHOUT_PRICE


@dataclass(frozen=True)
class Suggestion:
    offer_id: str
    title: str
    brand: str | None
    chain: str
    price_eur: float | None
    was_price_eur: float | None
    is_offer: bool
    discount_percent: int | None
    base_price_eur: float | None
    base_unit: str | None
    score: float
    image_url: str | None = None
    valid_from: str | None = None
    valid_until: str | None = None


class SuggestionEngine:
    def __init__(self, offers: list[Offer]) -> None:
        self._offers = offers

    def suggest(self, q: str, chains: list[str] | None = None) -> list[Suggestion]:
        query = (q or "").strip()
        if not query:
            return []

        candidates = self._offers
        if chains:
            chain_set = {c.lower() for c in chains}
            candidates = [o for o in candidates if o.chain.lower() in chain_set]

        scored: list[Suggestion] = []
        for offer in candidates:
            text = offer.title
            if offer.brand:
                text = f"{offer.brand} {text}"

            # Verbessertes Matching
            score = calculate_match_score(query, text)

            # Höhere Schwelle für Angebote ohne Preis
            min_score = MIN_SCORE_WITHOUT_PRICE if offer.price_eur is None else MIN_SCORE_WITH_PRICE
            if score < min_score:
                continue
            scored.append(
                Suggestion(
                    offer_id=offer.id,
                    title=offer.title,
                    brand=offer.brand,
                    chain=offer.chain,
                    price_eur=offer.price_eur,
                    was_price_eur=offer.was_price_eur,
                    is_offer=offer.is_offer,
                    discount_percent=_discount_percent(offer.price_eur, offer.was_price_eur),
                    base_price_eur=offer.base_price_eur,
                    base_unit=offer.base_unit,
                    score=score,
                )
            )

        def _sort_key(s: Suggestion) -> tuple[float, float]:
            # Prefer known prices for UX; then cheaper.
            known = 1.0 if s.price_eur is not None else 0.0
            price = float(s.price_eur) if s.price_eur is not None else 1e9
            return (s.score + known, -price)

        scored.sort(key=_sort_key, reverse=True)
        return scored


def _discount_percent(price_eur: float | None, was_price_eur: float | None) -> int | None:
    if price_eur is None:
        return None
    if was_price_eur is None:
        return None
    try:
        was = float(was_price_eur)
        price = float(price_eur)
    except (TypeError, ValueError):
        return None
    if was <= 0 or price < 0 or was <= price:
        return None
    return int(round((1.0 - (price / was)) * 100))
