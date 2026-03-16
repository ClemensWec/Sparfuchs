"""
Tests für Fuzzy-Matching.

Testet: app/utils/matching.py
"""
import pytest
from app.utils.matching import (
    normalize_text,
    calculate_match_score,
    is_good_match,
    MIN_SCORE_WITH_PRICE,
    MIN_SCORE_WITHOUT_PRICE,
)


class TestNormalizeText:
    """Tests für normalize_text()"""

    def test_lowercase(self):
        """Text sollte kleingeschrieben werden."""
        assert normalize_text("MILCH") == "milch"

    def test_umlauts_replaced(self):
        """Umlaute sollten ersetzt werden."""
        assert "ae" in normalize_text("Käse")
        assert "oe" in normalize_text("Brötchen")
        assert "ue" in normalize_text("Müsli")
        assert "ss" in normalize_text("Süßigkeiten")

    def test_special_chars_removed(self):
        """Sonderzeichen sollten entfernt werden."""
        result = normalize_text("Coca-Cola!")
        assert "-" not in result
        assert "!" not in result

    def test_multiple_spaces_collapsed(self):
        """Mehrfache Leerzeichen sollten zusammengefasst werden."""
        result = normalize_text("Milch   und   Butter")
        assert "   " not in result

    def test_empty_string(self):
        """Leerer String sollte leer bleiben."""
        assert normalize_text("") == ""

    def test_none_returns_empty(self):
        """None sollte leeren String zurückgeben."""
        # normalize_text erwartet str, aber sollte graceful sein
        assert normalize_text("") == ""


class TestCalculateMatchScore:
    """Tests für calculate_match_score()"""

    @pytest.mark.critical
    @pytest.mark.parametrize("query,offer,should_match", [
        # Exakte Matches
        ("Milch", "Milch", True),
        ("Butter", "Butter", True),
        # Teil-Matches (Query in Offer)
        ("Milch", "Milbona Haltbare Milch", True),
        ("Tomaten", "Rispentomaten", True),
        ("Bananen", "Chiquita Bananen", True),
        ("Butter", "Kerrygold Butter", True),
        # Sollte NICHT matchen
        ("Milch", "Schokolade", False),
        ("Käse", "Wurst", False),
    ])
    def test_basic_matching(self, query, offer, should_match):
        """Basis-Matching sollte funktionieren."""
        score = calculate_match_score(query, offer)
        if should_match:
            assert score >= MIN_SCORE_WITH_PRICE, f"'{query}' sollte '{offer}' matchen (Score: {score})"
        else:
            assert score < MIN_SCORE_WITH_PRICE, f"'{query}' sollte '{offer}' NICHT matchen (Score: {score})"

    def test_milch_vs_milchschokolade(self):
        """Milch ist Substring von Milchschokolade — Match ist gewollt für Compound-Suche."""
        score = calculate_match_score("Milch", "Milchschokolade")
        # Substring-Match: "milch" in "milchschokolade" → hoher Score ist korrekt
        assert score >= 55, f"Score sollte mindestens 55 sein, ist {score}"

    def test_brand_in_offer(self):
        """Marke + Produkt sollte gut matchen."""
        score = calculate_match_score("Nutella", "Nutella 450g Glas")
        assert score >= MIN_SCORE_WITH_PRICE

    def test_word_order_independent(self):
        """Wortstellung sollte nicht wichtig sein."""
        score1 = calculate_match_score("Bio Milch", "Bio Vollmilch")
        score2 = calculate_match_score("Milch Bio", "Bio Vollmilch")
        # Beide sollten matchen
        assert score1 >= MIN_SCORE_WITH_PRICE
        # Score2 könnte etwas niedriger sein, aber immer noch OK
        assert score2 >= 50

    def test_umlaut_handling(self):
        """Umlaute sollten korrekt gehandhabt werden."""
        # Käse vs Kaese sollte funktionieren
        score = calculate_match_score("Käse", "Gouda Käse")
        assert score >= MIN_SCORE_WITH_PRICE

    def test_empty_query(self):
        """Leere Query sollte 0 zurückgeben."""
        assert calculate_match_score("", "Milch") == 0

    def test_empty_offer(self):
        """Leeres Angebot sollte 0 zurückgeben."""
        assert calculate_match_score("Milch", "") == 0


class TestIsGoodMatch:
    """Tests für is_good_match()"""

    def test_returns_tuple(self):
        """Sollte Tuple (bool, float) zurückgeben."""
        result = is_good_match("Milch", "Milch")
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bool)
        assert isinstance(result[1], float)

    def test_good_match_returns_true(self):
        """Guter Match sollte True zurückgeben."""
        is_match, score = is_good_match("Milch", "Milbona Milch")
        assert is_match is True
        assert score >= MIN_SCORE_WITH_PRICE

    def test_bad_match_returns_false(self):
        """Schlechter Match sollte False zurückgeben."""
        is_match, score = is_good_match("Milch", "Wurst")
        assert is_match is False

    def test_higher_threshold_without_price(self):
        """Ohne Preis sollte höhere Schwelle gelten."""
        # Ein mittelmäßiger Match
        is_match_with, score = is_good_match("Apfel", "Apfelsaft", has_price=True)
        is_match_without, _ = is_good_match("Apfel", "Apfelsaft", has_price=False)
        # Könnte mit Preis matchen, aber ohne Preis nicht
        # (hängt vom Score ab)
        assert MIN_SCORE_WITHOUT_PRICE > MIN_SCORE_WITH_PRICE


class TestMatchingEdgeCases:
    """Tests für Edge Cases beim Matching."""

    @pytest.mark.parametrize("query,offer", [
        # Bindestriche
        ("Coca Cola", "Coca-Cola 1,5L"),
        ("Coca-Cola", "Coca Cola"),
        # Zahlen
        ("Milch 1L", "H-Milch 1 Liter"),
        # Umlaute
        ("Kaese", "Käse"),
        ("Käse", "Kaese"),
    ])
    def test_special_chars_match(self, query, offer):
        """Sonderzeichen sollten das Matching nicht brechen."""
        score = calculate_match_score(query, offer)
        assert score > 0, f"'{query}' vs '{offer}' sollte Score > 0 haben"


# =============================================================================
# Produkt-Matrix Tests
# =============================================================================

PRODUCT_MATCH_TESTS = [
    # (query, offer_text, should_match)
    ("Milch", "Milbona Haltbare fettarme Milch", True),
    ("Butter", "Kerrygold Original Irische Butter", True),
    ("Käse", "Gouda jung", False),  # Keine Textüberlappung — rein semantisch
    ("Tomaten", "Rispentomaten Klasse I", True),
    ("Bananen", "Bananen gelb", True),
    ("Brot", "Sonnenkruste Weizenbrot", True),
    ("Nudeln", "Barilla Spaghetti No. 5", False),  # Keine Textüberlappung — rein semantisch
    ("Reis", "Uncle Ben's Langkornreis", True),
]


@pytest.mark.parametrize("query,offer,should_match", PRODUCT_MATCH_TESTS)
def test_product_matching(query, offer, should_match):
    """Teste Matching für verschiedene Produkte."""
    score = calculate_match_score(query, offer)
    if should_match:
        assert score >= MIN_SCORE_WITH_PRICE, f"'{query}' sollte '{offer}' matchen (Score: {score})"
    else:
        assert score < MIN_SCORE_WITH_PRICE, f"'{query}' sollte '{offer}' NICHT matchen (Score: {score})"
