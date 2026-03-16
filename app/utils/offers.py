"""
Hilfsfunktionen für Angebote.
"""
from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.connectors.base import Offer


def is_offer_valid(offer: Offer, reference_date: date | None = None) -> bool:
    """
    Prüft, ob ein Angebot an einem bestimmten Datum gültig ist.

    Args:
        offer: Das zu prüfende Angebot
        reference_date: Referenzdatum (default: heute)

    Returns:
        True wenn das Angebot gültig ist
    """
    if reference_date is None:
        reference_date = date.today()

    # Wenn kein Gültigkeitszeitraum angegeben, nehmen wir an es ist gültig
    if offer.valid_from is None and offer.valid_to is None:
        return True

    # Nur valid_from: Angebot muss begonnen haben
    if offer.valid_from is not None and offer.valid_to is None:
        return reference_date >= offer.valid_from

    # Nur valid_to: Angebot darf nicht abgelaufen sein
    if offer.valid_from is None and offer.valid_to is not None:
        return reference_date <= offer.valid_to

    # Beide gesetzt: muss im Zeitraum liegen
    return offer.valid_from <= reference_date <= offer.valid_to


def filter_valid_offers(offers: list[Offer], reference_date: date | None = None) -> list[Offer]:
    """
    Filtert eine Liste von Angeboten auf nur gültige.

    Args:
        offers: Liste von Angeboten
        reference_date: Referenzdatum (default: heute)

    Returns:
        Liste mit nur gültigen Angeboten
    """
    if reference_date is None:
        reference_date = date.today()

    return [o for o in offers if is_offer_valid(o, reference_date)]
