"""
Tests für den Grundpreis-Parser.

Testet: app/utils/unit_parser.py
"""
import pytest
from app.utils.unit_parser import (
    parse_base_price,
    ParsedBasePrice,
    can_compare,
    extract_qty_unit_from_description,
    _parse_german_number,
)


# =============================================================================
# Hilfsfunktion
# =============================================================================

def _approx(val: float | None, expected: float, rel: float = 1e-4) -> bool:
    """Ungefährer Vergleich für Fließkommazahlen."""
    if val is None:
        return False
    return abs(val - expected) < abs(expected * rel) + 1e-9


# =============================================================================
# Tests: Deutsche Zahlenformate
# =============================================================================

class TestParseGermanNumber:
    """Tests für _parse_german_number() — deutsche Zahlenformate."""

    def test_comma_decimal(self):
        """Komma als Dezimaltrenner: 1,99 → 1.99"""
        assert _parse_german_number("1,99") == pytest.approx(1.99)

    def test_dot_decimal(self):
        """Punkt als Dezimaltrenner bei 1-2 Nachkommastellen: 9.27 → 9.27"""
        assert _parse_german_number("9.27") == pytest.approx(9.27)

    def test_german_thousands_separator(self):
        """Punkt als Tausendertrenner bei genau 3 Nachkommastellen: 1.000 → 1000"""
        assert _parse_german_number("1.000") == pytest.approx(1000.0)

    def test_both_separators(self):
        """Punkt + Komma: 1.000,50 → 1000.50"""
        assert _parse_german_number("1.000,50") == pytest.approx(1000.50)

    def test_dash_price(self):
        """Strich-Notation: 10.- → 10.0"""
        assert _parse_german_number("10.-") == pytest.approx(10.0)

    def test_none_returns_none(self):
        """None-Eingabe → None"""
        assert _parse_german_number(None) is None

    def test_empty_string_returns_none(self):
        """Leerer String → None"""
        assert _parse_german_number("") is None

    def test_invalid_returns_none(self):
        """Ungültiger String → None"""
        assert _parse_german_number("abc") is None


# =============================================================================
# Tests: Grundpreis-Formate (Gewicht)
# =============================================================================

class TestParseBasePriceWeight:
    """Tests für Gewichts-Grundpreise (kg, g)."""

    def test_euro_per_kg(self):
        """Format: '1,99 €/kg' → Preis pro kg"""
        result = parse_base_price("1,99 €/kg")
        assert result.price_eur == pytest.approx(1.99)
        assert result.unit == "kg"
        assert result.unit_group == "weight"
        assert result.normalized_unit == "g"
        # 1,99 €/kg = 1,99 / 1000 €/g
        assert result.price_per_normalized == pytest.approx(1.99 / 1000)

    def test_euro_per_100g(self):
        """Format: '2,49€/100g' → normalisiert auf €/g"""
        result = parse_base_price("2,49€/100g")
        assert result.price_eur == pytest.approx(2.49)
        assert result.unit_group == "weight"
        assert result.normalized_unit == "g"
        # 2,49 €/100g = 2,49 / 100 €/g
        assert result.price_per_normalized == pytest.approx(2.49 / 100)

    def test_100g_equals_format(self):
        """Format: '100 g = 0,79 €' → normalisiert auf €/g"""
        result = parse_base_price("100 g = 0,79 €")
        assert result.quantity == pytest.approx(100)
        assert result.unit == "g"
        assert result.price_eur == pytest.approx(0.79)
        assert result.price_per_normalized == pytest.approx(0.79 / 100)

    def test_1kg_equals_format(self):
        """Format: '1 kg = 9.95' → normalisiert auf €/g"""
        result = parse_base_price("1 kg = 9.95")
        assert result.quantity == pytest.approx(1)
        assert result.unit == "kg"
        assert result.price_eur == pytest.approx(9.95)
        # 1 kg = 1000 g → 9.95 / 1000 €/g
        assert result.price_per_normalized == pytest.approx(9.95 / 1000)

    def test_kg_preis_format(self):
        """Format: 'kg-Preis 9.90'"""
        result = parse_base_price("kg-Preis 9.90")
        assert result.unit == "kg"
        assert result.price_eur == pytest.approx(9.90)
        assert result.unit_group == "weight"

    def test_kg_normalization_consistency(self):
        """1 kg und 1000 g sollten den gleichen normalisierten Preis ergeben."""
        r_kg = parse_base_price("1 kg = 9.95")
        r_g = parse_base_price("1000 g = 9.95")
        assert r_kg.price_per_normalized == pytest.approx(r_g.price_per_normalized)


# =============================================================================
# Tests: Grundpreis-Formate (Volumen)
# =============================================================================

class TestParseBasePriceVolume:
    """Tests für Volumen-Grundpreise (l, ml)."""

    def test_euro_per_liter(self):
        """Format: '0,89 €/l' → Preis pro Liter"""
        result = parse_base_price("0,89 €/l")
        assert result.price_eur == pytest.approx(0.89)
        assert result.unit == "l"
        assert result.unit_group == "volume"
        assert result.normalized_unit == "ml"
        # 0,89 €/l = 0,89 / 1000 €/ml
        assert result.price_per_normalized == pytest.approx(0.89 / 1000)

    def test_euro_per_100ml(self):
        """Format: '3,99€/100ml' → normalisiert auf €/ml"""
        result = parse_base_price("3,99€/100ml")
        assert result.price_eur == pytest.approx(3.99)
        assert result.unit_group == "volume"
        assert result.normalized_unit == "ml"
        # 3,99 €/100ml = 3,99 / 100 €/ml
        assert result.price_per_normalized == pytest.approx(3.99 / 100)

    def test_1l_equals_format(self):
        """Format: '(1 L = 2,39)' mit Klammern"""
        result = parse_base_price("(1 L = 2,39)")
        assert result.quantity == pytest.approx(1)
        assert result.unit == "l"
        assert result.price_eur == pytest.approx(2.39)
        assert result.unit_group == "volume"

    def test_liter_keyword(self):
        """Einheit 'Liter' wird erkannt."""
        result = parse_base_price("1 Liter = 1.66")
        assert result.unit == "liter"
        assert result.unit_group == "volume"
        assert result.normalized_unit == "ml"

    def test_liter_normalization_consistency(self):
        """1 l und 1000 ml sollten den gleichen normalisierten Preis ergeben."""
        r_l = parse_base_price("1 l = 2.39")
        r_ml = parse_base_price("1000 ml = 2.39")
        assert r_l.price_per_normalized == pytest.approx(r_ml.price_per_normalized)


# =============================================================================
# Tests: Stück / Packung / Sondereinheiten
# =============================================================================

class TestParseBasePricePiece:
    """Tests für Stück- und Verpackungspreise."""

    def test_per_stueck(self):
        """Format: '0,14–0,56 €/Stück'"""
        result = parse_base_price("0,14 €/Stück")
        assert result.unit == "stueck"
        assert result.unit_group == "piece"
        assert result.normalized_unit == "stk"

    def test_per_dose(self):
        """Format: 'per Dose 0.67'"""
        result = parse_base_price("per Dose 0.67")
        assert result.unit == "dose"
        assert result.unit_group == "piece"
        assert result.price_eur == pytest.approx(0.67)

    def test_waschladung(self):
        """Format: '1 WL = 0,21'"""
        result = parse_base_price("1 WL = 0,21")
        assert result.unit == "wl"
        assert result.unit_group == "laundry_load"
        assert result.normalized_unit == "wl"
        assert result.price_eur == pytest.approx(0.21)

    def test_tab(self):
        """Format: '1 Tab = 0,09'"""
        result = parse_base_price("1 Tab = 0,09")
        assert result.unit == "tab"
        assert result.unit_group == "application"
        assert result.price_eur == pytest.approx(0.09)

    def test_blatt(self):
        """Format: '1.000 Blatt = 2.88' — Tausendertrenner"""
        result = parse_base_price("1.000 Blatt = 2.88")
        assert result.quantity == pytest.approx(1000)
        assert result.unit == "blatt"
        assert result.unit_group == "sheet"
        assert result.price_eur == pytest.approx(2.88)

    def test_tuecher(self):
        """Format: '10 Tücher = 0.16'"""
        result = parse_base_price("10 Tücher = 0.16")
        assert result.unit == "tuecher"
        assert result.unit_group == "wipe"
        assert result.normalized_unit == "tuch"
        assert result.price_eur == pytest.approx(0.16)


# =============================================================================
# Tests: Sonderformate
# =============================================================================

class TestParseBasePriceSpecialFormats:
    """Tests für besondere Grundpreis-Formate."""

    def test_pro_kg(self):
        """Format: '8.99 pro kg'"""
        result = parse_base_price("8.99 pro kg")
        assert result.price_eur == pytest.approx(8.99)
        assert result.unit == "kg"

    def test_per_100g(self):
        """Format: '1.89 per 100 g'"""
        result = parse_base_price("1.89 per 100 g")
        assert result.price_eur == pytest.approx(1.89)
        assert result.quantity == pytest.approx(100)
        assert result.unit == "g"

    def test_kg_equals_without_qty(self):
        """Format: 'kg = 3.76' — keine Menge angegeben, implizit 1"""
        result = parse_base_price("kg = 3.76")
        assert result.quantity == pytest.approx(1)
        assert result.unit == "kg"
        assert result.price_eur == pytest.approx(3.76)

    def test_dash_price_format(self):
        """Format: '10.- / kg'"""
        result = parse_base_price("10.- / kg")
        assert result.price_eur == pytest.approx(10.0)
        assert result.unit == "kg"

    def test_range_price(self):
        """Format: '1 WL = 0,21–0,18' — Preisspanne"""
        result = parse_base_price("1 WL = 0,21–0,18")
        assert result.price_eur == pytest.approx(0.21)
        assert result.price_eur_max == pytest.approx(0.18)

    def test_ab_prefix(self):
        """Format: '1 WA = ab €0.18' — ab-Preis"""
        result = parse_base_price("1 WA = ab €0.18")
        assert result.price_eur == pytest.approx(0.18)
        assert result.unit_group == "laundry_load"

    def test_ab_without_equals(self):
        """Format: '1 kg ab 3.98'"""
        result = parse_base_price("1 kg ab 3.98")
        assert result.price_eur == pytest.approx(3.98)
        assert result.unit == "kg"


# =============================================================================
# Tests: Edge Cases
# =============================================================================

class TestParseBasePriceEdgeCases:
    """Tests für Grenzfälle und ungültige Eingaben."""

    def test_none_input(self):
        """None → leeres ParsedBasePrice"""
        result = parse_base_price(None)
        assert result.raw == ""
        assert result.price_eur is None
        assert result.unit is None
        assert result.price_per_normalized is None
        assert result.is_comparable is False

    def test_empty_string(self):
        """Leerer String → leeres ParsedBasePrice"""
        result = parse_base_price("")
        assert result.raw == ""
        assert result.price_eur is None
        assert result.is_comparable is False

    def test_whitespace_only(self):
        """Nur Leerzeichen → leeres ParsedBasePrice"""
        result = parse_base_price("   ")
        assert result.price_eur is None
        assert result.is_comparable is False

    def test_invalid_format(self):
        """Ungültiges Format → keine Felder gesetzt"""
        result = parse_base_price("Sonderangebot!")
        assert result.price_eur is None
        assert result.unit is None

    def test_unknown_unit(self):
        """Unbekannte Einheit wird nicht geparst."""
        result = parse_base_price("1 Flakon = 3.99")
        assert result.unit is None
        assert result.price_per_normalized is None

    def test_price_text_without_unit(self):
        """Nur Preis ohne Einheit → kein Parsing"""
        result = parse_base_price("3,99 €")
        assert result.unit is None


# =============================================================================
# Tests: is_comparable und can_compare
# =============================================================================

class TestComparability:
    """Tests für Vergleichbarkeit von Grundpreisen."""

    def test_is_comparable_true(self):
        """Gültiger Grundpreis sollte vergleichbar sein."""
        result = parse_base_price("1,99 €/kg")
        assert result.is_comparable is True

    def test_is_comparable_false_on_failure(self):
        """Ungültiger Input sollte nicht vergleichbar sein."""
        result = parse_base_price(None)
        assert result.is_comparable is False

    def test_can_compare_same_group(self):
        """Gleiche Einheitengruppe → vergleichbar"""
        a = parse_base_price("1,99 €/kg")
        b = parse_base_price("0,79 €/100g")
        assert can_compare(a, b) is True

    def test_can_compare_different_group(self):
        """Verschiedene Einheitengruppen → nicht vergleichbar"""
        a = parse_base_price("1,99 €/kg")
        b = parse_base_price("0,89 €/l")
        assert can_compare(a, b) is False

    def test_can_compare_with_invalid(self):
        """Ungültiger Preis → nicht vergleichbar"""
        a = parse_base_price("1,99 €/kg")
        b = parse_base_price(None)
        assert can_compare(a, b) is False


# =============================================================================
# Tests: Beschreibungs-Extraktion (Fallback)
# =============================================================================

class TestExtractQtyUnitFromDescription:
    """Tests für extract_qty_unit_from_description() — Fallback-Parser."""

    def test_100g_in_text(self):
        """'HKL A 100 g' → (100.0, 'g', 'weight', 'g')"""
        result = extract_qty_unit_from_description("auf Wunsch auch gewürzt, HKL A 100 g")
        assert result is not None
        qty, unit_key, group, norm = result
        assert qty == pytest.approx(100.0)
        assert unit_key == "g"
        assert group == "weight"

    def test_je_1_kg(self):
        """'je 1 kg' → (1.0, 'kg', 'weight', 'g')"""
        result = extract_qty_unit_from_description("je 1 kg")
        assert result is not None
        assert result[0] == pytest.approx(1.0)
        assert result[1] == "kg"

    def test_hyphenated_format(self):
        """'1-kg-Packung' → (1.0, 'kg', 'weight', 'g')"""
        result = extract_qty_unit_from_description("1-kg-Packung")
        assert result is not None
        assert result[0] == pytest.approx(1.0)
        assert result[1] == "kg"

    def test_je_stueck(self):
        """'je St.' → (1.0, 'st', 'piece', 'stk')"""
        result = extract_qty_unit_from_description("je St.")
        assert result is not None
        assert result[1] == "st"
        assert result[2] == "piece"

    def test_none_input(self):
        """None → None"""
        assert extract_qty_unit_from_description(None) is None

    def test_no_unit_found(self):
        """Kein erkennbare Einheit → None"""
        assert extract_qty_unit_from_description("Sonderangebot diese Woche") is None


# =============================================================================
# Parametrisierte Massentests
# =============================================================================

@pytest.mark.parametrize("text,expected_unit_group,expected_price", [
    ("1,99 €/kg",           "weight",       1.99),
    ("2,49€/100g",          "weight",       2.49),
    ("0,89 €/l",            "volume",       0.89),
    ("3,99€/100ml",         "volume",       3.99),
    ("100 g = 0,79 €",      "weight",       0.79),
    ("(1 L = 2,39)",         "volume",       2.39),
    ("1.000 Blatt = 2.88",  "sheet",        2.88),
    ("9.27 €/kg",           "weight",       9.27),
    ("10.- / kg",            "weight",       10.0),
    ("kg-Preis 9.90",       "weight",       9.90),
    ("kg = 3.76",           "weight",       3.76),
    ("1 WL = 0,21",         "laundry_load", 0.21),
    ("8.99 pro kg",         "weight",       8.99),
    ("per Dose 0.67",       "piece",        0.67),
    ("1 Tab = 0,09",        "application",  0.09),
])
def test_bulk_parsing(text, expected_unit_group, expected_price):
    """Massentest: verschiedene Formate korrekt geparst."""
    result = parse_base_price(text)
    assert result.unit_group == expected_unit_group, f"Falsches unit_group für '{text}': {result.unit_group}"
    assert result.price_eur == pytest.approx(expected_price), f"Falscher Preis für '{text}': {result.price_eur}"
    assert result.price_per_normalized is not None, f"Kein price_per_normalized für '{text}'"
