"""
Zentrale Chain-Normalisierung für alle Datenquellen.

Stellt sicher, dass Ketten-Namen konsistent sind zwischen:
- KaufDA (Publisher-Namen)
- OpenStreetMap/Overpass (brand/operator/name Tags)
"""
from __future__ import annotations

from typing import Any

# Kanonische Ketten-Namen (so wie sie in der App angezeigt werden)
KNOWN_CHAINS = [
    "Aldi",
    "Lidl",
    "Rewe",
    "Edeka",
    "Kaufland",
    "Penny",
    "Netto",
    "Norma",
    "Globus",
    "Marktkauf",
]

# Aliase: Verschiedene Schreibweisen/Marken → kanonischer Name
# Lowercase für Matching
_CHAIN_ALIASES: dict[str, str] = {
    # Aldi
    "aldi": "Aldi",
    "aldi süd": "Aldi",
    "aldi sued": "Aldi",
    "aldi nord": "Aldi",
    "aldi-nord": "Aldi",
    # Lidl
    "lidl": "Lidl",
    # Rewe-Gruppe
    "rewe": "Rewe",
    "rewe city": "Rewe",
    "rewe center": "Rewe",
    "nahkauf": "Rewe",  # Gehört zu Rewe
    # Edeka-Gruppe
    "edeka": "Edeka",
    "e center": "Edeka",
    "e-center": "Edeka",
    "edeka center": "Edeka",
    "netto marken-discount": "Netto",  # Edeka-Tochter, aber eigene Kette
    # Kaufland
    "kaufland": "Kaufland",
    # Penny (Rewe-Gruppe, aber eigene Kette)
    "penny": "Penny",
    "penny markt": "Penny",
    "penny-markt": "Penny",
    # Netto
    "netto": "Netto",
    # Norma
    "norma": "Norma",
    # Globus
    "globus": "Globus",
    # Marktkauf (Edeka-Gruppe)
    "marktkauf": "Marktkauf",
}

# Patterns für Substring-Matching (wenn exaktes Matching fehlschlägt)
_CHAIN_SUBSTRINGS: list[tuple[str, str]] = [
    ("aldi", "Aldi"),
    ("lidl", "Lidl"),
    ("rewe", "Rewe"),
    ("nahkauf", "Rewe"),
    ("edeka", "Edeka"),
    ("kaufland", "Kaufland"),
    ("penny", "Penny"),
    ("netto", "Netto"),
    ("norma", "Norma"),
    ("globus", "Globus"),
    ("marktkauf", "Marktkauf"),
]


def normalize_chain(raw: str | None) -> str | None:
    """
    Normalisiert einen Ketten-Namen zu einem kanonischen Namen.

    Args:
        raw: Roher Ketten-Name (z.B. "ALDI SÜD", "Nahkauf", "REWE City")

    Returns:
        Kanonischer Name (z.B. "Aldi", "Rewe") oder None wenn unbekannt
    """
    if not raw:
        return None

    cleaned = str(raw).strip()
    if not cleaned:
        return None

    lower = cleaned.lower()

    # 1. Exaktes Matching (nach Lowercase)
    if lower in _CHAIN_ALIASES:
        return _CHAIN_ALIASES[lower]

    # 2. Substring-Matching
    for substring, canonical in _CHAIN_SUBSTRINGS:
        if substring in lower:
            return canonical

    return None


def normalize_chain_with_extra(publisher_name: str | None) -> tuple[str | None, dict[str, Any]]:
    """
    Wie normalize_chain, aber mit zusätzlichen Metadaten.

    Für KaufDA-Kompatibilität: Gibt auch extra-Dict zurück.
    """
    if not publisher_name:
        return (None, {})

    raw = str(publisher_name).strip()
    if not raw:
        return (None, {})

    extra: dict[str, Any] = {"publisher_name": raw}
    up = raw.upper()

    # Aldi-Spezifika: Nord/Süd merken
    if "ALDI SÜD" in up or "ALDI SUED" in up:
        extra["aldi_territory"] = "sued"
    elif "ALDI NORD" in up or "ALDI-NORD" in up:
        extra["aldi_territory"] = "nord"

    chain = normalize_chain(raw)
    return (chain, extra)


def normalize_chain_from_osm_tags(tags: dict[str, Any]) -> str:
    """
    Normalisiert eine Kette aus OSM-Tags.

    Prüft brand, operator, name in dieser Reihenfolge.

    Returns:
        Kanonischer Name oder "Sonstige" wenn unbekannt
    """
    # Alle relevanten Tags zusammenfügen
    raw_parts = []
    for key in ("brand", "operator", "name"):
        val = tags.get(key)
        if val:
            raw_parts.append(str(val))

    combined = " ".join(raw_parts).lower()

    # Substring-Matching auf kombiniertem String
    for substring, canonical in _CHAIN_SUBSTRINGS:
        if substring in combined:
            return canonical

    return "Sonstige"
