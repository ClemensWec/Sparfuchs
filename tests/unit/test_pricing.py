"""
Tests für das Preisranking und die Basket-Logik.

Testet: app/services/pricing.py
"""
from __future__ import annotations

import pytest
from dataclasses import replace
from app.connectors.base import Offer
from app.services.overpass import Store
from app.utils.matching import MIN_SCORE_WITH_PRICE
from app.services.pricing import (
    BasketPricer,
    SparMixPricer,
    WantedItem,
    LineMatch,
    StoreBasketRow,
    _cheaper,
    _get_normalized_price,
)


# =============================================================================
# Fixture-Helfer: Offer- und Store-Erzeugung
# =============================================================================

def _make_offer(
    title: str,
    chain: str = "Aldi",
    price_eur: float | None = 1.99,
    brand: str | None = None,
    is_offer: bool = True,
    category_id: int | None = None,
    unit_group: str | None = None,
    price_per_normalized: float | None = None,
    offer_id: str | None = None,
) -> Offer:
    """Erstellt ein Test-Offer mit optionalen extra-Feldern."""
    extra: dict | None = None
    if category_id is not None or unit_group is not None or price_per_normalized is not None:
        extra = {}
        if category_id is not None:
            extra["category_id"] = category_id
        if unit_group is not None:
            extra["unit_group"] = unit_group
        if price_per_normalized is not None:
            extra["price_per_normalized"] = price_per_normalized
    return Offer(
        id=offer_id or f"test-{title.lower().replace(' ', '-')}",
        title=title,
        brand=brand,
        chain=chain,
        price_eur=price_eur,
        is_offer=is_offer,
        extra=extra,
    )


def _make_store(
    chain: str = "Aldi",
    lat: float = 50.7374,
    lon: float = 7.0982,
    name: str | None = None,
) -> Store:
    """Erstellt einen Test-Store."""
    return Store(
        osm_type="node",
        osm_id=hash(f"{chain}-{lat}-{lon}") % 10**9,
        name=name or f"{chain} Testfiliale",
        chain=chain,
        lat=lat,
        lon=lon,
        address="Teststraße 1",
        postcode="53111",
        city_name="Bonn",
    )


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def milch_offers() -> list[Offer]:
    """Milch-Angebote verschiedener Ketten mit category_id."""
    return [
        _make_offer("Milbona Frische Milch 1L", chain="Lidl", price_eur=1.09,
                     category_id=10, unit_group="volume", price_per_normalized=0.00109),
        _make_offer("ja! Frische Vollmilch 1L", chain="Rewe", price_eur=1.19,
                     category_id=10, unit_group="volume", price_per_normalized=0.00119),
        _make_offer("GUT&GÜNSTIG Milch 1,5L", chain="Edeka", price_eur=1.49,
                     category_id=10, unit_group="volume", price_per_normalized=0.000993),
    ]


@pytest.fixture
def butter_offers() -> list[Offer]:
    """Butter-Angebote mit text-basiertem Matching."""
    return [
        _make_offer("Kerrygold Butter 250g", chain="Lidl", price_eur=2.29,
                     brand="Kerrygold", category_id=20),
        _make_offer("Deutsche Markenbutter 250g", chain="Rewe", price_eur=1.89,
                     category_id=20),
        _make_offer("Butter mild gesäuert", chain="Aldi", price_eur=1.69,
                     category_id=20),
    ]


@pytest.fixture
def mixed_offers(milch_offers, butter_offers) -> list[Offer]:
    """Gemischte Angebote: Milch + Butter."""
    return milch_offers + butter_offers


@pytest.fixture
def stores_bonn() -> list[Store]:
    """Test-Stores in Bonn-Nähe."""
    return [
        _make_store("Lidl", 50.7380, 7.0990),
        _make_store("Rewe", 50.7370, 7.0970),
        _make_store("Edeka", 50.7360, 7.1000),
        _make_store("Aldi", 50.7390, 7.0960),
    ]


BONN_ORIGIN = (50.7374, 7.0982)


# =============================================================================
# Tests: _get_normalized_price
# =============================================================================

class TestGetNormalizedPrice:
    """Tests für die Extraktion des normalisierten Preises."""

    def test_from_extra(self):
        """price_per_normalized aus offer.extra lesen."""
        offer = _make_offer("Test", price_per_normalized=0.00199)
        assert _get_normalized_price(offer) == pytest.approx(0.00199)

    def test_no_extra(self):
        """Kein extra-Dict → None"""
        offer = _make_offer("Test")
        assert _get_normalized_price(offer) is None

    def test_extra_without_ppn(self):
        """extra vorhanden, aber ohne price_per_normalized → None"""
        offer = _make_offer("Test", category_id=5)
        assert _get_normalized_price(offer) is None


# =============================================================================
# Tests: _cheaper — Preisvergleich
# =============================================================================

class TestCheaper:
    """Tests für _cheaper() — normalisierter vs. absoluter Preisvergleich."""

    def test_absolute_price_comparison(self):
        """Ohne Einheitengruppe: absoluter Preisvergleich."""
        cheap = _make_offer("Billig", price_eur=1.00)
        expensive = _make_offer("Teuer", price_eur=2.00)
        assert _cheaper(cheap, expensive) is True
        assert _cheaper(expensive, cheap) is False

    def test_normalized_price_comparison(self):
        """Gleiche Einheitengruppe: normalisierter Preisvergleich.

        500g @ 5,19€ (= 0,01038 €/g) ist günstiger als 100g @ 1,79€ (= 0,0179 €/g),
        auch wenn der absolute Preis höher ist.
        """
        big_pack = _make_offer("500g Packung", price_eur=5.19,
                               unit_group="weight", price_per_normalized=0.01038)
        small_pack = _make_offer("100g Packung", price_eur=1.79,
                                 unit_group="weight", price_per_normalized=0.0179)
        # Normalisiert ist big_pack günstiger
        assert _cheaper(big_pack, small_pack) is True

    def test_different_unit_groups_fallback(self):
        """Verschiedene Einheitengruppen → Fallback auf absoluten Preis."""
        weight_offer = _make_offer("Mehl 1kg", price_eur=1.50,
                                   unit_group="weight", price_per_normalized=0.0015)
        volume_offer = _make_offer("Milch 1L", price_eur=1.20,
                                   unit_group="volume", price_per_normalized=0.0012)
        # Verschiedene Gruppen → absoluter Vergleich: 1.20 < 1.50
        assert _cheaper(volume_offer, weight_offer) is True


# =============================================================================
# Tests: BasketPricer._best_match_by_category
# =============================================================================

class TestBestMatchByCategory:
    """Tests für Kategorie-basiertes Matching im BasketPricer."""

    def test_finds_cheapest_by_category(self, milch_offers):
        """Kategorie-Match: günstigstes Angebot innerhalb der Kategorie."""
        pricer = BasketPricer(milch_offers)
        wanted = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")

        result = pricer._best_match_by_category(wanted, milch_offers)
        assert result.offer is not None
        assert result.score == 100.0
        # Günstigstes Milch-Angebot (normalisiert) ist Edeka mit 0.000993 €/ml
        assert result.offer.chain == "Edeka"

    def test_category_not_found(self):
        """Kategorie ohne Treffer → offer=None."""
        offers = [_make_offer("Brot", category_id=99)]
        pricer = BasketPricer(offers)
        wanted = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")

        result = pricer._best_match_by_category(wanted, offers)
        assert result.offer is None
        assert result.score is None

    def test_category_prefers_priced_over_unpriced(self):
        """Kategorie-Match bevorzugt Angebot mit Preis gegenüber ohne Preis."""
        offers = [
            _make_offer("Milch Aktion", price_eur=None, category_id=10),
            _make_offer("Milch 1L", price_eur=1.09, category_id=10),
        ]
        pricer = BasketPricer(offers)
        wanted = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")

        result = pricer._best_match_by_category(wanted, offers)
        assert result.offer is not None
        assert result.offer.price_eur == pytest.approx(1.09)

    def test_expanded_category_ids(self):
        """Family-Expansion: category_ids enthält mehrere IDs."""
        offers = [
            _make_offer("Hähnchenbrust", price_eur=4.99, category_id=30),
            _make_offer("Suppenhuhn", price_eur=3.49, category_id=31),
        ]
        pricer = BasketPricer(offers)
        # Family-Node "Hähnchen" expandiert zu IDs 30 und 31
        wanted = WantedItem(
            q="Hähnchen", brand=None,
            category_id=30, category_name="Hähnchen",
            category_ids=(30, 31),
        )

        result = pricer._best_match_by_category(wanted, offers)
        assert result.offer is not None
        # Günstigstes: Suppenhuhn @ 3,49€
        assert result.offer.price_eur == pytest.approx(3.49)


# =============================================================================
# Tests: BasketPricer._best_match (Text + Kategorie-Fallback)
# =============================================================================

class TestBestMatch:
    """Tests für das kombinierte Matching (Kategorie + Text-Fallback)."""

    def test_category_match_preferred(self, milch_offers):
        """Wenn category_id gesetzt, wird Kategorie-Match bevorzugt."""
        pricer = BasketPricer(milch_offers)
        wanted = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")

        result = pricer._best_match(wanted, milch_offers)
        # Kategorie-Match liefert Score 100
        assert result.score == 100.0
        assert result.offer is not None

    def test_text_fallback_when_no_category(self, butter_offers):
        """Ohne category_id: Text-basiertes Matching."""
        pricer = BasketPricer(butter_offers)
        wanted = WantedItem(q="Butter", brand=None)

        result = pricer._best_match(wanted, butter_offers)
        assert result.offer is not None
        assert result.score is not None
        assert result.score >= MIN_SCORE_WITH_PRICE  # Text-Match gefunden

    def test_category_fallback_to_text(self):
        """Kategorie-Match findet nichts → Fallback auf Text-Match."""
        offers = [
            # Kein Offer mit category_id=99 vorhanden
            _make_offer("Frische Vollmilch 1L", price_eur=1.19, category_id=10),
        ]
        pricer = BasketPricer(offers)
        # category_id=99 existiert nicht, aber Text "Milch" matcht "Vollmilch"
        wanted = WantedItem(q="Milch", brand=None, category_id=99, category_name="Milch")

        result = pricer._best_match(wanted, offers)
        # Sollte über Text-Fallback matchen
        assert result.offer is not None
        assert result.score < 100.0  # Text-Match, nicht Category

    def test_brand_filter(self):
        """Brand-Filter: nur passende Marke wird berücksichtigt."""
        offers = [
            _make_offer("Kerrygold Butter", price_eur=2.29, brand="Kerrygold"),
            _make_offer("Butter mild", price_eur=1.69, brand="Eigenmarke"),
        ]
        pricer = BasketPricer(offers)
        wanted = WantedItem(q="Butter", brand="Kerrygold", any_brand=False)

        result = pricer._best_match(wanted, offers)
        assert result.offer is not None
        assert result.offer.brand == "Kerrygold"


# =============================================================================
# Tests: BasketPricer.price_basket_for_stores
# =============================================================================

class TestPriceBasketForStores:
    """Tests für die Store-übergreifende Basket-Bepreisung."""

    def test_basic_basket(self):
        """Einfacher Basket: 1 Artikel, 2 Stores."""
        offers = [
            _make_offer("Milch 1L", chain="Lidl", price_eur=1.09, category_id=10),
            _make_offer("Milch 1L", chain="Rewe", price_eur=1.19, category_id=10),
        ]
        stores = [
            _make_store("Lidl", 50.738, 7.099),
            _make_store("Rewe", 50.737, 7.097),
        ]
        pricer = BasketPricer(offers)
        wanted = [WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")]

        rows = pricer.price_basket_for_stores(stores, wanted, BONN_ORIGIN)

        assert len(rows) >= 1
        # Günstigster Store zuerst
        assert rows[0].total_eur is not None
        assert rows[0].missing_count == 0

    def test_missing_item_counted(self):
        """Fehlender Artikel wird als missing_count gezählt."""
        offers = [
            _make_offer("Milch 1L", chain="Lidl", price_eur=1.09, category_id=10),
        ]
        stores = [_make_store("Lidl")]
        pricer = BasketPricer(offers)
        wanted = [
            WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch"),
            WantedItem(q="Kaviar", brand=None, category_id=999, category_name="Kaviar"),
        ]

        rows = pricer.price_basket_for_stores(stores, wanted, BONN_ORIGIN)

        assert len(rows) == 1
        assert rows[0].missing_count == 1  # Kaviar nicht gefunden
        assert rows[0].offer_count == 1    # Milch gefunden

    def test_stores_without_offers_excluded(self):
        """Stores ohne bepreiste Treffer werden ausgeblendet."""
        offers = [
            _make_offer("Milch 1L", chain="Lidl", price_eur=1.09, category_id=10),
        ]
        stores = [
            _make_store("Lidl"),
            _make_store("Norma"),  # Keine Norma-Offers vorhanden
        ]
        pricer = BasketPricer(offers)
        wanted = [WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")]

        rows = pricer.price_basket_for_stores(stores, wanted, BONN_ORIGIN)

        # Norma sollte rausgefiltert sein (offer_count=0)
        chains_in_result = [r.store.chain for r in rows]
        assert "Norma" not in chains_in_result

    def test_ranking_by_coverage_then_price(self):
        """Ranking: mehr Treffer > günstigerer Preis > kürzere Distanz."""
        offers = [
            # Lidl: 2 Artikel
            _make_offer("Milch", chain="Lidl", price_eur=1.09, category_id=10),
            _make_offer("Butter", chain="Lidl", price_eur=1.89, category_id=20),
            # Rewe: nur 1 Artikel, aber günstiger
            _make_offer("Milch", chain="Rewe", price_eur=0.99, category_id=10),
        ]
        stores = [
            _make_store("Lidl"),
            _make_store("Rewe"),
        ]
        pricer = BasketPricer(offers)
        wanted = [
            WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch"),
            WantedItem(q="Butter", brand=None, category_id=20, category_name="Butter"),
        ]

        rows = pricer.price_basket_for_stores(stores, wanted, BONN_ORIGIN)

        # Lidl hat mehr Treffer (2 vs 1), sollte zuerst kommen
        assert rows[0].store.chain == "Lidl"


# =============================================================================
# Tests: BasketPricer — Scoring-Vergleich: Category vs Text
# =============================================================================

class TestScoringCategoryVsText:
    """Kategorie-Match gibt Score 100, Text-Match niedrigeren Score."""

    def test_category_score_is_100(self, milch_offers):
        """Kategorie-Match liefert immer Score 100."""
        pricer = BasketPricer(milch_offers)
        wanted = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")
        result = pricer._best_match_by_category(wanted, milch_offers)
        assert result.score == 100.0

    def test_text_score_lower_for_partial_match(self):
        """Text-Match für Teil-Treffer liefert Score < 100."""
        offers = [
            _make_offer("Frische Alpenmilch fettarm 1L", price_eur=1.19),
        ]
        pricer = BasketPricer(offers)
        wanted = WantedItem(q="Milch", brand=None)
        result = pricer._best_match(wanted, offers)
        assert result.offer is not None
        assert result.score < 100.0  # "Milch" in "Alpenmilch" = Teil-Match

    def test_category_score_none_when_no_match(self):
        """Kategorie ohne Treffer → Score None."""
        offers = [_make_offer("Brot", category_id=99)]
        pricer = BasketPricer(offers)
        wanted = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")
        result = pricer._best_match_by_category(wanted, offers)
        assert result.score is None


# =============================================================================
# Tests: SparMixPricer
# =============================================================================

class TestSparMixPricer:
    """Tests für den SparMix — günstigstes Angebot pro Artikel über alle Stores."""

    def test_basic_sparmix(self):
        """SparMix findet günstigstes Angebot pro Artikel."""
        offers = [
            _make_offer("Milch", chain="Lidl", price_eur=1.09, category_id=10),
            _make_offer("Milch", chain="Rewe", price_eur=1.19, category_id=10),
            _make_offer("Butter", chain="Lidl", price_eur=2.29, category_id=20),
            _make_offer("Butter", chain="Rewe", price_eur=1.89, category_id=20),
        ]
        stores = [
            _make_store("Lidl"),
            _make_store("Rewe"),
        ]
        pricer = BasketPricer(offers)
        sparmix = SparMixPricer(pricer)
        wanted = [
            WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch"),
            WantedItem(q="Butter", brand=None, category_id=20, category_name="Butter"),
        ]

        result = sparmix.compute(stores, wanted, BONN_ORIGIN)

        assert result.total_eur is not None
        # Milch günstigster: Lidl 1,09; Butter günstigster: Rewe 1,89
        assert result.total_eur == pytest.approx(1.09 + 1.89)
        assert result.store_count == 2
        assert "Lidl" in result.stores_used
        assert "Rewe" in result.stores_used

    def test_sparmix_empty_offers(self):
        """SparMix ohne Angebote → leeres Ergebnis."""
        pricer = BasketPricer([])
        sparmix = SparMixPricer(pricer)
        stores = [_make_store("Lidl")]
        wanted = [WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")]

        result = sparmix.compute(stores, wanted, BONN_ORIGIN)

        assert result.total_eur is None
        assert result.store_count == 0
        assert len(result.lines) == 1
        assert result.lines[0].offer is None

    def test_sparmix_max_stores(self):
        """SparMix mit max_stores-Limit beschränkt Store-Anzahl."""
        offers = [
            _make_offer("Milch", chain="Lidl", price_eur=1.09, category_id=10),
            _make_offer("Butter", chain="Rewe", price_eur=1.89, category_id=20),
            _make_offer("Brot", chain="Edeka", price_eur=1.49, category_id=30),
        ]
        stores = [
            _make_store("Lidl"),
            _make_store("Rewe"),
            _make_store("Edeka"),
        ]
        pricer = BasketPricer(offers)
        sparmix = SparMixPricer(pricer)
        wanted = [
            WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch"),
            WantedItem(q="Butter", brand=None, category_id=20, category_name="Butter"),
            WantedItem(q="Brot", brand=None, category_id=30, category_name="Brot"),
        ]

        result = sparmix.compute(stores, wanted, BONN_ORIGIN, max_stores=2)

        # Maximal 2 verschiedene Stores
        assert result.store_count <= 2

    def test_sparmix_with_basket_rows(self):
        """SparMix nutzt vorberechnete basket_rows wenn vorhanden."""
        offers = [
            _make_offer("Milch", chain="Lidl", price_eur=1.09, category_id=10),
            _make_offer("Milch", chain="Rewe", price_eur=1.19, category_id=10),
        ]
        stores = [
            _make_store("Lidl"),
            _make_store("Rewe"),
        ]
        pricer = BasketPricer(offers)
        wanted = [WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch")]

        # Erst Basket berechnen, dann SparMix mit Ergebnis
        basket_rows = pricer.price_basket_for_stores(stores, wanted, BONN_ORIGIN)
        sparmix = SparMixPricer(pricer)
        result = sparmix.compute(stores, wanted, BONN_ORIGIN, basket_rows=basket_rows)

        assert result.total_eur is not None
        assert result.total_eur == pytest.approx(1.09)


# =============================================================================
# Tests: WantedItem
# =============================================================================

class TestWantedItem:
    """Tests für WantedItem Serialisierung."""

    def test_to_dict_text_only(self):
        """Text-basierter WantedItem → dict ohne category_id."""
        w = WantedItem(q="Milch", brand=None)
        d = w.to_dict()
        assert d["q"] == "Milch"
        assert "category_id" not in d

    def test_to_dict_with_category(self):
        """Kategorie-basierter WantedItem → dict mit category_id."""
        w = WantedItem(q="Milch", brand=None, category_id=10, category_name="Milch",
                       category_ids=(10, 11, 12))
        d = w.to_dict()
        assert d["category_id"] == 10
        assert d["category_name"] == "Milch"
        assert d["category_ids"] == [10, 11, 12]

    def test_to_dict_with_brand(self):
        """WantedItem mit Marke."""
        w = WantedItem(q="Butter", brand="Kerrygold", any_brand=False)
        d = w.to_dict()
        assert d["brand"] == "Kerrygold"
        assert d["any_brand"] is False
