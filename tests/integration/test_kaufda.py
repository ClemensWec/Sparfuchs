"""
Integrationstests für KaufDA Connector.

Diese Tests machen echte API-Aufrufe und benötigen Netzwerk.
"""
import httpx
import pytest
from app.connectors.kaufda import KaufdaOffersSeoConnector, KaufdaLocation
from app.utils.chains import KNOWN_CHAINS


# Skip wenn kein Netzwerk
pytestmark = pytest.mark.network


@pytest.fixture
def connector_bonn():
    """KaufDA Connector für Bonn."""
    return KaufdaOffersSeoConnector(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sparfuchs-Test/1.0",
        location=KaufdaLocation(lat=50.7374, lng=7.0982, city="Bonn", zip="53111"),
    )


@pytest.fixture
def connector_berlin():
    """KaufDA Connector für Berlin."""
    return KaufdaOffersSeoConnector(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sparfuchs-Test/1.0",
        location=KaufdaLocation(lat=52.52, lng=13.405, city="Berlin", zip="10117"),
    )


@pytest.fixture
def connector_bonn_53113():
    """KaufDA Connector für Bonn 53113."""
    return KaufdaOffersSeoConnector(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sparfuchs-Test/1.0",
        location=KaufdaLocation(lat=50.7206, lng=7.1187, city="Bonn", zip="53113"),
    )


class TestKaufdaSearchOffers:
    """Tests für fetch_search_offers()"""

    @pytest.mark.critical
    @pytest.mark.asyncio
    async def test_milch_returns_offers(self, connector_bonn):
        """Suche nach 'Milch' sollte Angebote finden."""
        offers = await connector_bonn.fetch_search_offers(keyword="Milch")
        assert len(offers) > 0, "Sollte Angebote für 'Milch' finden"

    @pytest.mark.critical
    @pytest.mark.asyncio
    async def test_tomate_finds_tomaten(self, connector_bonn):
        """Suche nach 'Tomate' (Singular) sollte dank Varianten funktionieren."""
        offers = await connector_bonn.fetch_search_offers(keyword="Tomate")
        assert len(offers) > 0, "Sollte Angebote für 'Tomate' finden (via 'Tomaten')"

    @pytest.mark.asyncio
    async def test_banane_finds_bananen(self, connector_bonn):
        """Suche nach 'Banane' sollte dank Varianten funktionieren."""
        offers = await connector_bonn.fetch_search_offers(keyword="Banane")
        assert len(offers) > 0, "Sollte Angebote für 'Banane' finden"

    @pytest.mark.asyncio
    async def test_haehnchen_umlaut_works(self, connector_bonn):
        """Suche nach 'Hähnchen' sollte dank Umlaut-Varianten funktionieren."""
        offers = await connector_bonn.fetch_search_offers(keyword="Hähnchen")
        assert len(offers) > 0, "Sollte Angebote für 'Hähnchen' finden"

    @pytest.mark.asyncio
    async def test_lowercase_works(self, connector_bonn):
        """Kleingeschriebene Keywords sollten funktionieren."""
        offers = await connector_bonn.fetch_search_offers(keyword="milch")
        assert len(offers) > 0, "Sollte auch mit 'milch' (lowercase) funktionieren"

    @pytest.mark.asyncio
    async def test_empty_keyword_returns_empty(self, connector_bonn):
        """Leeres Keyword sollte leere Liste zurückgeben."""
        offers = await connector_bonn.fetch_search_offers(keyword="")
        assert offers == []

    @pytest.mark.asyncio
    async def test_nonsense_keyword_returns_empty(self, connector_bonn):
        """Unsinn-Keyword sollte leere Liste zurückgeben."""
        offers = await connector_bonn.fetch_search_offers(keyword="xyz123abc")
        assert offers == []

    @pytest.mark.asyncio
    async def test_offers_have_required_fields(self, connector_bonn):
        """Angebote sollten alle erforderlichen Felder haben."""
        offers = await connector_bonn.fetch_search_offers(keyword="Butter")
        assert len(offers) > 0

        for offer in offers[:5]:  # Prüfe erste 5
            assert offer.id, "Offer sollte ID haben"
            assert offer.title, "Offer sollte Title haben"
            assert offer.chain, "Offer sollte Chain haben"
            assert offer.source == "kaufda", "Source sollte 'kaufda' sein"

    @pytest.mark.asyncio
    async def test_chains_are_normalized(self, connector_bonn):
        """Chains sollten normalisiert sein."""
        offers = await connector_bonn.fetch_search_offers(keyword="Milch")

        for offer in offers:
            assert offer.chain in KNOWN_CHAINS, f"Chain '{offer.chain}' sollte in KNOWN_CHAINS sein"


class TestKaufdaMultipleLocations:
    """Tests für verschiedene Standorte."""

    @pytest.mark.asyncio
    async def test_berlin_has_offers(self, connector_berlin):
        """Berlin sollte auch Angebote haben."""
        offers = await connector_berlin.fetch_search_offers(keyword="Milch")
        assert len(offers) > 0

    @pytest.mark.asyncio
    async def test_different_locations_different_offers(self, connector_bonn, connector_berlin):
        """Verschiedene Standorte könnten unterschiedliche Angebote haben."""
        offers_bonn = await connector_bonn.fetch_search_offers(keyword="Milch")
        offers_berlin = await connector_berlin.fetch_search_offers(keyword="Milch")

        # Beide sollten Angebote haben
        assert len(offers_bonn) > 0
        assert len(offers_berlin) > 0

        # IDs könnten unterschiedlich sein (abhängig von Region)
        ids_bonn = {o.id for o in offers_bonn}
        ids_berlin = {o.id for o in offers_berlin}
        # Nicht alle gleich (normalerweise)
        # Aber das ist nicht garantiert, also nur prüfen dass beide existieren


class TestKaufdaCoverageVsSeed:
    """Tests für Retrieval-Coverage gegenüber dem initialen KaufDA-HTML-Slice."""

    @pytest.mark.asyncio
    async def test_53113_milch_recovers_more_than_initial_seed(self, connector_bonn_53113):
        url = "https://www.kaufda.de/angebote/Milch"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sparfuchs-Test/1.0"}
        cookies = {"location": connector_bonn_53113._location.to_cookie_value()}

        async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
            next_data = await connector_bonn_53113._fetch_next_data(client=client, url=url, cookies=cookies)

        assert next_data is not None, "KaufDA Produktseite sollte __NEXT_DATA__ liefern"

        seed_embedded, seed_total = connector_bonn_53113._count_embedded_offer_items(next_data)
        seed_offers = connector_bonn_53113._parse_offers_from_next_data(next_data, url)
        recovered_offers = await connector_bonn_53113.fetch_search_offers(keyword="Milch")

        assert seed_total > seed_embedded, "KaufDA sollte für 53113/Milch mehr Treffer melden als im HTML-Seed eingebettet sind"
        assert len(recovered_offers) > len(seed_offers), "Connector sollte über den initialen KaufDA-Seed hinaus zusätzliche Treffer holen"
        assert len(recovered_offers) >= 30, "Connector sollte für 53113/Milch deutlich breitere Coverage liefern"


class TestKaufdaProductMatrix:
    """Tests für verschiedene Produktkategorien."""

    PRODUCTS_TO_TEST = [
        ("Milch", 5),
        ("Butter", 3),
        ("Brot", 3),
        ("Tomaten", 3),
        ("Bananen", 1),
        ("Käse", 3),
        ("Joghurt", 3),
        ("Wasser", 3),
        ("Cola", 3),
        ("Nudeln", 3),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("keyword,min_offers", PRODUCTS_TO_TEST)
    async def test_product_has_offers(self, connector_bonn, keyword, min_offers):
        """Verschiedene Produkte sollten Angebote haben."""
        offers = await connector_bonn.fetch_search_offers(keyword=keyword)
        assert len(offers) >= min_offers, f"'{keyword}' sollte mindestens {min_offers} Angebote haben"


class TestKaufdaOfferValidity:
    """Tests für Angebots-Validität."""

    @pytest.mark.asyncio
    async def test_offers_have_valid_dates(self, connector_bonn):
        """Angebote sollten gültige Datums-Felder haben."""
        from datetime import date

        offers = await connector_bonn.fetch_search_offers(keyword="Milch")

        for offer in offers[:10]:
            # valid_from/valid_to können None sein
            if offer.valid_from is not None:
                assert isinstance(offer.valid_from, date)
            if offer.valid_to is not None:
                assert isinstance(offer.valid_to, date)

            # Wenn beide gesetzt, sollte valid_from <= valid_to
            if offer.valid_from and offer.valid_to:
                assert offer.valid_from <= offer.valid_to

    @pytest.mark.asyncio
    async def test_prices_are_positive(self, connector_bonn):
        """Preise sollten positiv sein (wenn vorhanden)."""
        offers = await connector_bonn.fetch_search_offers(keyword="Butter")

        for offer in offers:
            if offer.price_eur is not None:
                assert offer.price_eur > 0, f"Preis sollte positiv sein: {offer.price_eur}"
