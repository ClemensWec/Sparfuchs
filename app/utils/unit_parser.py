"""
Parses and normalizes product size/unit information from KaufDA base_price_text strings.

Handles the full range of formats found in German supermarket brochures:
  - Weight:        kg, g
  - Volume:        l, L, Liter, ml
  - Laundry load:  WL, WA, Waschladung, Wäsche, WI  (alle = "pro Waschladung")
  - Application:   Tab, Anwendung, ME, por           (alle = "pro Anwendung")
  - Sheet:         Blatt
  - Wipe:          Tücher
  - Piece:         Stück, Dose
  - Meter:         m

Formats handled:
  "1 kg = 9.95"          →  qty=1,    unit=kg,  price=9.95
  "100 g = 0,79 €"       →  qty=100,  unit=g,   price=0.79
  "(1 L = 2,39)"         →  qty=1,    unit=l,   price=2.39
  "1.000 Blatt = 2.88"   →  qty=1000, unit=blatt, price=2.88  (German thousands sep)
  "9.27 €/kg"            →  qty=1,    unit=kg,  price=9.27
  "10.- / kg"            →  qty=1,    unit=kg,  price=10.0
  "kg-Preis 9.90"        →  qty=1,    unit=kg,  price=9.90
  "kg = 3.76"            →  qty=1,    unit=kg,  price=3.76
  "1 WL = 0,21–0,18"     →  qty=1,    unit=wl,  price=0.21, price_max=0.18
  "1 WA = ab €0.18"      →  qty=1,    unit=wl,  price=0.18
  "8.99 pro kg"          →  qty=1,    unit=kg,  price=8.99
  "1.89 per 100 g"       →  qty=100,  unit=g,   price=1.89
  "per Dose 0.67"        →  qty=1,    unit=stk, price=0.67
  "1 Tab = 0,09"         →  qty=1,    unit=tab, price=0.09
  "10 Tücher = 0.16"     →  qty=10,   unit=tuch, price=0.16
"""
from __future__ import annotations

import re
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Unit map: lowercase token → (group, multiplier_to_normalized, normalized_symbol)
#
# multiplier means: 1 <token> = multiplier <normalized_symbol>
# e.g. "kg" → 1 kg = 1000 g  →  multiplier=1000, normalized="g"
# ---------------------------------------------------------------------------
_UNIT_MAP: dict[str, tuple[str, float, str]] = {
    # --- Weight (normalize to g) ---
    "kg":          ("weight",       1000.0, "g"),
    "g":           ("weight",       1.0,    "g"),
    # --- Volume (normalize to ml) ---
    "l":           ("volume",       1000.0, "ml"),
    "liter":       ("volume",       1000.0, "ml"),
    "ml":          ("volume",       1.0,    "ml"),
    # --- Laundry load (normalize to wl, 1:1) ---
    "wl":          ("laundry_load", 1.0,    "wl"),
    "wa":          ("laundry_load", 1.0,    "wl"),
    "waschladung": ("laundry_load", 1.0,    "wl"),
    "waesche":     ("laundry_load", 1.0,    "wl"),   # Wäsche → waesche via alias
    "wi":          ("laundry_load", 1.0,    "wl"),   # Waschanwendung
    # --- Application / use unit (normalize to tab, 1:1) ---
    "tab":         ("application",  1.0,    "tab"),
    "anwendung":   ("application",  1.0,    "tab"),
    "me":          ("application",  1.0,    "tab"),  # Mengeneinheit/dose
    "por":         ("application",  1.0,    "tab"),  # porción (Spanish brochures)
    # --- Sheet (normalize to blatt, 1:1) ---
    "blatt":       ("sheet",        1.0,    "blatt"),
    # --- Wipe / cloth (normalize to tuch, 1:1) ---
    "tuecher":     ("wipe",         1.0,    "tuch"),  # Tücher → tuecher via alias
    # --- Piece / unit (normalize to stk, 1:1) ---
    "stueck":      ("piece",        1.0,    "stk"),   # Stück → stueck via alias
    "st":          ("piece",        1.0,    "stk"),   # St / St. abbreviation
    "stk":         ("piece",        1.0,    "stk"),   # Stk abbreviation
    "dose":        ("piece",        1.0,    "stk"),
    # --- Meter (normalize to m, 1:1) ---
    "m":           ("meter",        1.0,    "m"),
}

# Umlaut / spelling aliases (lowercase raw form → key in _UNIT_MAP)
_UNIT_ALIASES: dict[str, str] = {
    "wäsche":  "waesche",
    "tücher":  "tuecher",
    "stück":   "stueck",
    "stk":     "stk",    # already in map, keep for explicitness
}

# Human-readable label for each normalized unit (used in UI)
UNIT_DISPLAY: dict[str, str] = {
    "g":     "100 g",
    "ml":    "100 ml",
    "wl":    "WL",
    "tab":   "Tab",
    "blatt": "Blatt",
    "tuch":  "Tuch",
    "stk":   "Stück",
    "m":     "m",
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ParsedBasePrice:
    raw: str
    quantity: float | None = None          # reference quantity (e.g. 100 for "100 g")
    unit: str | None = None                # canonical unit key (e.g. "g", "wl")
    unit_group: str | None = None          # "weight"|"volume"|"laundry_load"|...
    price_eur: float | None = None         # primary price
    price_eur_max: float | None = None     # upper bound for range prices
    normalized_unit: str | None = None     # normalized symbol (e.g. "g", "ml")
    price_per_normalized: float | None = None  # price per 1 normalized unit

    @property
    def is_comparable(self) -> bool:
        return self.unit_group is not None and self.price_per_normalized is not None


# ---------------------------------------------------------------------------
# Number parsing
# ---------------------------------------------------------------------------

def _parse_german_number(s: str | None) -> float | None:
    """Parse a German-style number string.

    Rules:
      - "1,29"  → 1.29    (comma = decimal separator)
      - "1.000" → 1000.0  (dot + exactly 3 digits = thousands separator)
      - "9.27"  → 9.27    (dot + 1-2 digits = decimal separator)
      - "10.-"  → 10.0    (dash = no decimal places, common German price notation)
    """
    if not s:
        return None
    s = s.strip()
    # "10.-" form: trailing dash means zero cents
    s = re.sub(r"(\d+)\.$", r"\1", s)   # "10." → "10"
    s = re.sub(r"(\d+)\.-$", r"\1", s)  # "10.-" → "10"

    if "," in s and "." in s:
        # Both separators present → dot is thousands, comma is decimal
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Only comma → comma is decimal
        s = s.replace(",", ".")
    elif "." in s:
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            # Exactly 3 decimal digits → German thousands separator
            s = s.replace(".", "")
        # else: normal decimal dot, leave as-is

    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Unit token normalization
# ---------------------------------------------------------------------------

def _normalize_unit_token(raw: str) -> str | None:
    """Return the canonical _UNIT_MAP key for a raw unit token, or None if unknown."""
    key = raw.strip().rstrip(".").lower()   # strip trailing dot: "St." → "st"
    key = _UNIT_ALIASES.get(key, key)
    return key if key in _UNIT_MAP else None


# ---------------------------------------------------------------------------
# Regex patterns (tried in order, most-specific first)
# ---------------------------------------------------------------------------

# A  "qty unit = price"  and  "qty unit = price1 – price2"
# Handles:  "1 kg = 9.95", "100 g = 0,79 €", "(1 L = 2,39)", "1.000 Blatt = 2.88",
#           "1 WL = 0,21–0,18", "1 WA = ab €0.18", "1 ME = 0,03/0,02",
#           "1 kg: 14.80–19.30" (colon), "1 kg ab 3.98" (ab without =)
_PAT_A = re.compile(
    r"\(?"                                               # optional opening paren
    r"(?P<qty>\d[\d.]*(?:,\d+)?)"                       # qty (allows "1.000", "0,5", "100")
    r"\s*"
    r"(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})"                 # unit token
    r"\s*(?:[=:]|ab)\s*"                                # separator: "=", ":", or "ab"
    r"(?:ab\s*)?[€$]?\s*"                               # optional "ab" + currency
    r"(?P<price>\d[\d.,]*(?:\.-)?)"                     # price (allows "10.-")
    r"(?:\s*[€])?"
    r"(?:\s*[-–/]\s*[€$]?\s*(?P<price_max>\d[\d.,]*))?"  # optional range / second price
    r"\s*\)?",
    re.IGNORECASE,)

# A2  "qty unit ab price"  (no "=" at all, just whitespace + "ab")
# Handles:  "1 kg ab 3.98", "1 WL ab 0.18", "1 Liter ab 1.66"
_PAT_A2 = re.compile(
    r"(?P<qty>\d[\d.]*(?:,\d+)?)"
    r"\s+"
    r"(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})"
    r"\s+ab\s+"
    r"[€$]?\s*(?P<price>\d[\d.,]*)",
    re.IGNORECASE,
)

# G  "price €/NNunit"  — price per combined qty+unit string like "100g", "100ml"
# Handles:  "0.39 €/100g", "0.49 €/100g", "1.89 per 100 g" already in D
_PAT_G = re.compile(
    r"(?P<price>\d[\d.,]*)\s*(?:€)?\s*/\s*(?P<qty>\d+)\s*(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})",
    re.IGNORECASE,
)

# B  "price / unit"  or  "price €/unit"
# Handles:  "9.27 €/kg", "1.10 / l", "10.- / kg", "0,14–0,56 €/Stück"
_PAT_B = re.compile(
    r"(?P<price>\d[\d.,]*(?:\.-)?)\s*(?:€)?\s*/\s*(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})",
    re.IGNORECASE,
)

# C  "unit-Preis price"
# Handles:  "kg-Preis 9.90"
_PAT_C = re.compile(
    r"(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})-[Pp]reis\s+(?P<price>\d[\d.,]*)",
    re.IGNORECASE,
)

# D  "price pro/per/je [qty] unit"
# Handles:  "8.99 pro kg", "1.89 per 100 g"
_PAT_D = re.compile(
    r"(?P<price>\d[\d.,]*)\s*(?:pro|per|je)\s+(?P<qty>\d[\d.,]*)?\s*"
    r"(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})",
    re.IGNORECASE,
)

# E  "per/pro/je unit price"  (unit before price)
# Handles:  "per Dose 0.67", "pro Anwendung = 0.14"
_PAT_E = re.compile(
    r"(?:per|pro|je)\s+(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})\s*(?:=\s*)?(?P<price>\d[\d.,]*)",
    re.IGNORECASE,
)

# F  "unit = price"  (no explicit quantity → qty = 1)
# Handles:  "kg = 3.76", "kg = 2.47 ATG"
_PAT_F = re.compile(
    r"(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})\s*=\s*(?P<price>\d[\d.,]*)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Internal parse helpers
# ---------------------------------------------------------------------------

# (qty, unit_raw, price, price_max) — all raw strings still
_Parsed = tuple[float, str, float, float | None]


def _try_A2(s: str) -> _Parsed | None:
    m = _PAT_A2.search(s)
    if not m:
        return None
    qty = _parse_german_number(m.group("qty"))
    unit = m.group("unit")
    price = _parse_german_number(m.group("price"))
    if qty is None or qty <= 0 or price is None:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (qty, unit, price, None)


def _try_G(s: str) -> _Parsed | None:
    m = _PAT_G.search(s)
    if not m:
        return None
    price = _parse_german_number(m.group("price"))
    qty = _parse_german_number(m.group("qty"))
    unit = m.group("unit")
    if price is None or not qty or qty <= 0:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (qty, unit, price, None)


def _try_A(s: str) -> _Parsed | None:
    m = _PAT_A.search(s)
    if not m:
        return None
    qty = _parse_german_number(m.group("qty"))
    unit = m.group("unit")
    price = _parse_german_number(m.group("price"))
    price_max = _parse_german_number(m.group("price_max")) if m.group("price_max") else None
    if qty is None or qty <= 0 or price is None:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (qty, unit, price, price_max)


def _try_B(s: str) -> _Parsed | None:
    m = _PAT_B.search(s)
    if not m:
        return None
    price = _parse_german_number(m.group("price"))
    unit = m.group("unit")
    if price is None:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (1.0, unit, price, None)


def _try_C(s: str) -> _Parsed | None:
    m = _PAT_C.search(s)
    if not m:
        return None
    unit = m.group("unit")
    price = _parse_german_number(m.group("price"))
    if price is None:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (1.0, unit, price, None)


def _try_D(s: str) -> _Parsed | None:
    m = _PAT_D.search(s)
    if not m:
        return None
    price = _parse_german_number(m.group("price"))
    qty_raw = m.group("qty")
    qty = _parse_german_number(qty_raw) if qty_raw else 1.0
    unit = m.group("unit")
    if price is None or not qty:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (qty, unit, price, None)


def _try_E(s: str) -> _Parsed | None:
    m = _PAT_E.search(s)
    if not m:
        return None
    unit = m.group("unit")
    price = _parse_german_number(m.group("price"))
    if price is None:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (1.0, unit, price, None)


def _try_F(s: str) -> _Parsed | None:
    m = _PAT_F.search(s)
    if not m:
        return None
    unit = m.group("unit")
    price = _parse_german_number(m.group("price"))
    if price is None:
        return None
    if _normalize_unit_token(unit) is None:
        return None
    return (1.0, unit, price, None)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_base_price(text: str | None) -> ParsedBasePrice:
    """Parse a KaufDA base_price_text string into a structured ParsedBasePrice.

    Returns a ParsedBasePrice with all optional fields as None if parsing fails.
    """
    raw = (text or "").strip()
    empty = ParsedBasePrice(raw=raw)
    if not raw:
        return empty

    parsed = (
        _try_A(raw)
        or _try_A2(raw)
        or _try_B(raw)
        or _try_G(raw)
        or _try_C(raw)
        or _try_D(raw)
        or _try_E(raw)
        or _try_F(raw)
    )
    if parsed is None:
        return empty

    qty, unit_raw, price, price_max = parsed
    unit_key = _normalize_unit_token(unit_raw)
    if unit_key is None:
        return empty

    group, multiplier, norm_sym = _UNIT_MAP[unit_key]

    # price_per_normalized = cost of 1 normalized unit
    # e.g. qty=100, unit=g (mult=1), price=0.79 → price_per_g = 0.79 / 100
    price_per_norm = price / (qty * multiplier) if qty > 0 else None

    return ParsedBasePrice(
        raw=raw,
        quantity=qty,
        unit=unit_key,
        unit_group=group,
        price_eur=price,
        price_eur_max=price_max,
        normalized_unit=norm_sym,
        price_per_normalized=price_per_norm,
    )


# ---------------------------------------------------------------------------
# Description-text extraction patterns (fallback when base_price_text is empty)
# ---------------------------------------------------------------------------

# H1  "qty unit"  or  "je qty unit"
# Handles: "100 g", "je 100 g", "je 1 kg", "1 kg", "250 ml", "HKL A 100 g"
_PAT_DESC_QTY_UNIT = re.compile(
    r"(?:je\s+)?(?P<qty>\d[\d.,]*)\s*(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})(?:\s|$|,|\.|;|/)",
    re.IGNORECASE,
)

# H2  "qty-unit[-Suffix]"  — hyphenated formats
# Handles: "1-kg-Packung", "0,75-l-Flasche", "500-g-Packung", "1-kg"
_PAT_DESC_HYPHEN = re.compile(
    r"(?P<qty>\d[\d.,]*)-(?P<unit>[A-Za-zÄÖÜäöüß]{1,6})"
    r"(?:-(?:Packung|Flasche|Dose|Pack|Packet|Beutel|Becher|Tube))?",
    re.IGNORECASE,
)

# H3  "je unit"  — no explicit quantity (implies qty = 1)
# Handles: "je kg", "je Stück", "je St.", "besonders zart je kg"
_PAT_DESC_JE_UNIT = re.compile(
    r"\bje\s+(?P<unit>[A-Za-zÄÖÜäöüß]{1,14})\.?(?:\s|$|,|;)",
    re.IGNORECASE,
)


def extract_qty_unit_from_description(text: str | None) -> tuple[float, str, str, str] | None:
    """Extract (qty, unit_key, unit_group, normalized_unit) from a free-text description.

    Tried in order of specificity:
      H2 (hyphenated) → H1 (qty unit) → H3 (je unit, qty=1)

    Used as a fallback when base_price_text is empty.
    Examples:
      "auf Wunsch auch gewürzt, HKL A 100 g"  → (100.0, "g",  "weight", "g")
      "je 1 kg"                                → (1.0,   "kg", "weight", "g")
      "1-kg-Packung"                           → (1.0,   "kg", "weight", "g")
      "je St."                                 → (1.0,   "st", "piece",  "stk")
      "besonders zart je kg"                   → (1.0,   "kg", "weight", "g")

    Returns None if no parseable unit is found.
    """
    if not text:
        return None

    def _resolve(qty: float, unit_raw: str) -> tuple[float, str, str, str] | None:
        unit_key = _normalize_unit_token(unit_raw)
        if unit_key is None or qty <= 0:
            return None
        group, _mult, norm_sym = _UNIT_MAP[unit_key]
        return (qty, unit_key, group, norm_sym)

    # H2: hyphenated "1-kg-Packung"
    for m in _PAT_DESC_HYPHEN.finditer(text):
        qty = _parse_german_number(m.group("qty"))
        if qty and (r := _resolve(qty, m.group("unit"))):
            return r

    # H1: "100 g", "je 100 g"
    for m in _PAT_DESC_QTY_UNIT.finditer(text):
        qty = _parse_german_number(m.group("qty"))
        if qty and (r := _resolve(qty, m.group("unit"))):
            return r

    # H3: "je kg", "je St." — implied qty = 1
    for m in _PAT_DESC_JE_UNIT.finditer(text):
        if r := _resolve(1.0, m.group("unit")):
            return r

    return None


def can_compare(a: ParsedBasePrice, b: ParsedBasePrice) -> bool:
    """Return True if two ParsedBasePrices can be meaningfully compared."""
    return (
        a.unit_group is not None
        and b.unit_group is not None
        and a.unit_group == b.unit_group
        and a.price_per_normalized is not None
        and b.price_per_normalized is not None
    )
