"""
Keyword-Normalisierung für Suchanfragen.

KaufDA ist sehr wählerisch:
- Case-sensitive: "tomaten" → 404, "Tomaten" → OK
- Singular/Plural: "Banane" → 404, "Bananen" → OK
"""
from __future__ import annotations


_UMLAUT_TRANSLATION = str.maketrans(
    {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
        "ß": "ss",
    }
)


def _to_ascii_variant(text: str) -> str:
    return text.translate(_UMLAUT_TRANSLATION)


def generate_keyword_variants(keyword: str) -> list[str]:
    """
    Generiert Varianten eines Keywords für die Suche.

    KaufDA erwartet oft:
    - Großgeschriebene Keywords (erster Buchstabe)
    - Plural-Formen

    Returns:
        Liste von Varianten, sortiert nach Wahrscheinlichkeit
    """
    if not keyword:
        return []

    base = keyword.strip()
    if not base:
        return []

    variants: list[str] = []
    seen: set[str] = set()

    def add(v: str) -> None:
        if v and v not in seen:
            seen.add(v)
            variants.append(v)

    def capitalize(s: str) -> str:
        return s[0].upper() + s[1:] if len(s) > 1 else s.upper()

    def add_with_plural_rules(seed: str) -> None:
        lower = seed.lower()

        add(capitalize(seed))
        add(seed)

        if lower.endswith("e"):
            add(capitalize(seed + "n"))
        elif lower.endswith("el") or lower.endswith("er") or lower.endswith("en"):
            pass
        elif lower.endswith(("a", "i", "o", "u")):
            add(capitalize(seed + "s"))
        else:
            add(capitalize(seed + "e"))
            add(capitalize(seed + "en"))

        if lower.endswith("en") and len(lower) > 3:
            singular = seed[:-1]
            add(capitalize(singular))
        elif lower.endswith("n") and len(lower) > 2 and not lower.endswith("en"):
            pass
        elif lower.endswith("e") and len(lower) > 2:
            pass
        elif lower.endswith("s") and len(lower) > 2:
            singular = seed[:-1]
            add(capitalize(singular))

    # KaufDA verträgt Umlaute in Pfaden unzuverlässig; die ASCII-Variante erhöht Recall deutlich.
    add_with_plural_rules(base)
    ascii_base = _to_ascii_variant(base)
    if ascii_base != base:
        add_with_plural_rules(ascii_base)

    return variants


def normalize_keyword_for_search(keyword: str) -> str:
    """
    Normalisiert ein Keyword für die Suche.

    - Erster Buchstabe groß
    - Trimmed
    """
    if not keyword:
        return ""

    base = keyword.strip()
    if not base:
        return ""

    return base[0].upper() + base[1:] if len(base) > 1 else base.upper()
