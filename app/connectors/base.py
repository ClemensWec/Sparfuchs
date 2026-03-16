from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


@dataclass(frozen=True)
class Offer:
    id: str
    title: str
    brand: str | None
    chain: str
    price_eur: float | None
    is_offer: bool
    member_price_eur: float | None = None
    was_price_eur: float | None = None
    valid_from: date | None = None
    valid_to: date | None = None
    unit: str | None = None
    quantity: float | None = None
    base_unit: str | None = None
    base_price_eur: float | None = None
    source: str | None = None
    loyalty_bonus_cents: int | None = None
    bulk_discount_tiers: list[dict] | None = None
    image_url: str | None = None
    product_url: str | None = None
    service_types: list[str] | None = None
    tags: list[str] | None = None
    extra: dict[str, Any] | None = None


class OffersConnector(Protocol):
    def load_offers(self) -> list[Offer]: ...
