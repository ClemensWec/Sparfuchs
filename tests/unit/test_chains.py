"""
Tests für Chain-Normalisierung.

Testet: app/utils/chains.py
"""
import pytest
from app.utils.chains import (
    normalize_chain,
    normalize_chain_with_extra,
    normalize_chain_from_osm_tags,
    KNOWN_CHAINS,
)


class TestNormalizeChain:
    """Tests für normalize_chain()"""

    @pytest.mark.critical
    @pytest.mark.parametrize("input_name,expected", [
        # Aldi Varianten
        ("ALDI SÜD", "Aldi"),
        ("ALDI NORD", "Aldi"),
        ("aldi süd", "Aldi"),
        ("aldi", "Aldi"),
        ("Aldi", "Aldi"),
        # Lidl
        ("lidl", "Lidl"),
        ("LIDL", "Lidl"),
        ("Lidl", "Lidl"),
        # Rewe Varianten
        ("REWE", "Rewe"),
        ("Rewe", "Rewe"),
        ("REWE City", "Rewe"),
        ("Nahkauf", "Rewe"),
        # Edeka Varianten
        ("EDEKA", "Edeka"),
        ("Edeka", "Edeka"),
        ("E center", "Edeka"),
        ("EDEKA Center", "Edeka"),
        # Penny
        ("Penny", "Penny"),
        ("Penny-Markt", "Penny"),
        ("PENNY MARKT", "Penny"),
        # Netto
        ("Netto", "Netto"),
        ("Netto Marken-Discount", "Netto"),
        # Kaufland
        ("Kaufland", "Kaufland"),
        ("KAUFLAND", "Kaufland"),
    ])
    def test_known_chain_variants(self, input_name, expected):
        """Bekannte Chain-Varianten sollten korrekt normalisiert werden."""
        result = normalize_chain(input_name)
        assert result == expected, f"'{input_name}' sollte zu '{expected}' werden, nicht '{result}'"

    def test_unknown_chain_returns_none(self):
        """Unbekannte Chains sollten None zurückgeben."""
        assert normalize_chain("Unbekannter Laden") is None
        assert normalize_chain("XYZ Markt") is None

    def test_empty_string_returns_none(self):
        """Leerer String sollte None zurückgeben."""
        assert normalize_chain("") is None
        assert normalize_chain("   ") is None

    def test_none_returns_none(self):
        """None sollte None zurückgeben."""
        assert normalize_chain(None) is None


class TestNormalizeChainWithExtra:
    """Tests für normalize_chain_with_extra()"""

    def test_returns_tuple(self):
        """Sollte Tuple (chain, extra) zurückgeben."""
        chain, extra = normalize_chain_with_extra("ALDI SÜD")
        assert isinstance(chain, str)
        assert isinstance(extra, dict)

    def test_aldi_sued_territory(self):
        """ALDI SÜD sollte aldi_territory=sued haben."""
        chain, extra = normalize_chain_with_extra("ALDI SÜD")
        assert chain == "Aldi"
        assert extra.get("aldi_territory") == "sued"

    def test_aldi_nord_territory(self):
        """ALDI NORD sollte aldi_territory=nord haben."""
        chain, extra = normalize_chain_with_extra("ALDI NORD")
        assert chain == "Aldi"
        assert extra.get("aldi_territory") == "nord"

    def test_preserves_publisher_name(self):
        """Original Publisher-Name sollte in extra sein."""
        chain, extra = normalize_chain_with_extra("REWE City")
        assert extra.get("publisher_name") == "REWE City"

    def test_none_returns_tuple(self):
        """None sollte (None, {}) zurückgeben."""
        chain, extra = normalize_chain_with_extra(None)
        assert chain is None
        assert extra == {}


class TestNormalizeChainFromOsmTags:
    """Tests für normalize_chain_from_osm_tags()"""

    @pytest.mark.critical
    def test_brand_tag(self):
        """Brand-Tag sollte erkannt werden."""
        tags = {"brand": "REWE"}
        assert normalize_chain_from_osm_tags(tags) == "Rewe"

    def test_operator_tag(self):
        """Operator-Tag sollte erkannt werden."""
        tags = {"operator": "Edeka"}
        assert normalize_chain_from_osm_tags(tags) == "Edeka"

    def test_name_tag(self):
        """Name-Tag sollte erkannt werden."""
        tags = {"name": "Lidl"}
        assert normalize_chain_from_osm_tags(tags) == "Lidl"

    def test_brand_takes_precedence(self):
        """Brand sollte vor operator/name kommen."""
        tags = {"brand": "Aldi", "name": "Supermarkt XYZ"}
        assert normalize_chain_from_osm_tags(tags) == "Aldi"

    def test_nahkauf_becomes_rewe(self):
        """Nahkauf sollte zu Rewe werden."""
        tags = {"brand": "Nahkauf"}
        assert normalize_chain_from_osm_tags(tags) == "Rewe"

    def test_unknown_returns_sonstige(self):
        """Unbekannte Chains sollten 'Sonstige' zurückgeben."""
        tags = {"name": "Tante Emma Laden"}
        assert normalize_chain_from_osm_tags(tags) == "Sonstige"

    def test_empty_tags_returns_sonstige(self):
        """Leere Tags sollten 'Sonstige' zurückgeben."""
        assert normalize_chain_from_osm_tags({}) == "Sonstige"


class TestKnownChains:
    """Tests für die KNOWN_CHAINS Liste."""

    def test_all_major_chains_present(self):
        """Alle großen deutschen Ketten sollten vorhanden sein."""
        expected = ["Aldi", "Lidl", "Rewe", "Edeka", "Kaufland", "Penny", "Netto"]
        for chain in expected:
            assert chain in KNOWN_CHAINS, f"'{chain}' fehlt in KNOWN_CHAINS"

    def test_chains_are_capitalized(self):
        """Alle Chain-Namen sollten kapitalisiert sein."""
        for chain in KNOWN_CHAINS:
            assert chain[0].isupper(), f"'{chain}' sollte mit Großbuchstabe beginnen"
