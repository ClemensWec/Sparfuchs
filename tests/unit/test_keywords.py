"""
Tests für Keyword-Normalisierung.

Testet: app/utils/keywords.py
"""
import pytest
from app.utils.keywords import generate_keyword_variants, normalize_keyword_for_search


class TestGenerateKeywordVariants:
    """Tests für generate_keyword_variants()"""

    @pytest.mark.critical
    def test_tomate_generates_plural(self):
        """Tomate sollte Tomaten als Variante haben."""
        variants = generate_keyword_variants("Tomate")
        assert "Tomate" in variants
        assert "Tomaten" in variants

    @pytest.mark.critical
    def test_lowercase_gets_capitalized(self):
        """Kleingeschriebene Keywords sollten kapitalisiert werden."""
        variants = generate_keyword_variants("tomate")
        assert "Tomate" in variants
        # Original auch behalten
        assert "tomate" in variants

    @pytest.mark.critical
    def test_banane_generates_plural(self):
        """Banane sollte Bananen als Variante haben."""
        variants = generate_keyword_variants("Banane")
        assert "Banane" in variants
        assert "Bananen" in variants

    def test_milch_variants(self):
        """Milch sollte verschiedene Varianten generieren."""
        variants = generate_keyword_variants("Milch")
        assert "Milch" in variants
        # Konsonant-Endung sollte -e und -en bekommen
        assert any("Milch" in v for v in variants)

    def test_pizza_gets_s_plural(self):
        """Pizza sollte Pizzas als Variante haben (Fremdwort)."""
        variants = generate_keyword_variants("Pizza")
        assert "Pizza" in variants
        assert "Pizzas" in variants

    def test_empty_string(self):
        """Leerer String sollte leere Liste zurückgeben."""
        assert generate_keyword_variants("") == []

    def test_whitespace_only(self):
        """Nur Whitespace sollte leere Liste zurückgeben."""
        assert generate_keyword_variants("   ") == []

    def test_capitalized_first(self):
        """Kapitalisierte Version sollte zuerst kommen."""
        variants = generate_keyword_variants("milch")
        assert variants[0] == "Milch"

    def test_tomaten_gets_singular(self):
        """Plural-Form sollte auch Singular generieren."""
        variants = generate_keyword_variants("Tomaten")
        assert "Tomaten" in variants
        assert "Tomate" in variants

    def test_no_invalid_plurals(self):
        """Sollte keine unsinnigen Plural-Formen generieren."""
        variants = generate_keyword_variants("Tomate")
        # "Tomateen" wäre falsch
        assert "Tomateen" not in variants


class TestNormalizeKeywordForSearch:
    """Tests für normalize_keyword_for_search()"""

    def test_capitalizes_first_letter(self):
        """Erster Buchstabe sollte groß sein."""
        assert normalize_keyword_for_search("milch") == "Milch"
        assert normalize_keyword_for_search("tomate") == "Tomate"

    def test_preserves_rest(self):
        """Rest des Wortes sollte erhalten bleiben."""
        assert normalize_keyword_for_search("milch") == "Milch"
        assert normalize_keyword_for_search("MILCH") == "MILCH"

    def test_trims_whitespace(self):
        """Whitespace sollte entfernt werden."""
        assert normalize_keyword_for_search("  milch  ") == "Milch"

    def test_empty_string(self):
        """Leerer String sollte leer bleiben."""
        assert normalize_keyword_for_search("") == ""

    def test_single_char(self):
        """Einzelnes Zeichen sollte groß werden."""
        assert normalize_keyword_for_search("m") == "M"


# =============================================================================
# Parametrisierte Tests für alle Standard-Produkte
# =============================================================================

PRODUCT_TESTS = [
    # (input, expected_in_variants)
    ("Milch", ["Milch"]),
    ("Butter", ["Butter"]),
    ("Käse", ["Käse"]),
    ("Tomaten", ["Tomaten", "Tomate"]),
    ("Bananen", ["Bananen"]),
    ("Äpfel", ["Äpfel"]),
    ("Brot", ["Brot", "Brote"]),
]


@pytest.mark.parametrize("input_word,expected", PRODUCT_TESTS)
def test_product_variants(input_word, expected):
    """Teste dass erwartete Varianten generiert werden."""
    variants = generate_keyword_variants(input_word)
    for exp in expected:
        assert exp in variants, f"'{exp}' sollte in Varianten von '{input_word}' sein"
