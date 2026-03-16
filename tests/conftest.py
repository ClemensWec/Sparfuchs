"""
Pytest-Konfiguration und gemeinsame Fixtures für alle Tests.
"""
from __future__ import annotations

import pytest
from datetime import date, timedelta


# =============================================================================
# Standort-Fixtures
# =============================================================================

LOCATIONS = {
    "bonn": {"lat": 50.7374, "lon": 7.0982, "plz": "53111", "city": "Bonn"},
    "berlin": {"lat": 52.52, "lon": 13.405, "plz": "10117", "city": "Berlin"},
    "muenchen": {"lat": 48.137, "lon": 11.576, "plz": "80331", "city": "München"},
    "hamburg": {"lat": 53.551, "lon": 9.993, "plz": "20095", "city": "Hamburg"},
    "koeln": {"lat": 50.938, "lon": 6.960, "plz": "50667", "city": "Köln"},
    "frankfurt": {"lat": 50.110, "lon": 8.682, "plz": "60311", "city": "Frankfurt"},
}


@pytest.fixture
def location_bonn():
    return LOCATIONS["bonn"]


@pytest.fixture
def location_berlin():
    return LOCATIONS["berlin"]


@pytest.fixture
def all_locations():
    return LOCATIONS


# =============================================================================
# Produkt-Fixtures
# =============================================================================

STANDARD_PRODUCTS = {
    "milchprodukte": ["Milch", "Butter", "Käse", "Joghurt", "Quark", "Sahne"],
    "obst": ["Bananen", "Äpfel", "Orangen", "Erdbeeren", "Weintrauben"],
    "gemuese": ["Tomaten", "Gurken", "Paprika", "Karotten", "Zwiebeln"],
    "fleisch": ["Hähnchen", "Hackfleisch", "Schnitzel", "Wurst"],
    "backwaren": ["Brot", "Brötchen", "Toast", "Croissant"],
    "getraenke": ["Wasser", "Cola", "Bier", "Saft", "Kaffee"],
    "suesswaren": ["Schokolade", "Chips", "Kekse", "Eis"],
    "basics": ["Nudeln", "Reis", "Mehl", "Zucker", "Öl", "Eier"],
}

BRAND_PRODUCTS = {
    "Coca-Cola": ["Cola", "Fanta", "Sprite"],
    "Nutella": ["Nutella"],
    "Milka": ["Schokolade", "Milka Schokolade"],
    "Barilla": ["Nudeln", "Barilla Spaghetti"],
    "Kerrygold": ["Butter", "Kerrygold Butter"],
    "Dr. Oetker": ["Pizza", "Pudding"],
}

EDGE_CASE_PRODUCTS = [
    # Umlaute
    "Käse", "Müsli", "Brötchen", "Würstchen",
    # Bindestriche
    "Coca-Cola", "H-Milch",
    # Zahlen
    "1,5L Wasser", "500g Hackfleisch",
    # Zusammengesetzte
    "Vollmilch", "Hackfleisch", "Orangensaft",
]


@pytest.fixture
def standard_basket():
    """Ein typischer Einkaufskorb."""
    return ["Milch", "Butter", "Brot", "Tomaten", "Bananen"]


@pytest.fixture
def all_products():
    """Alle Testprodukte als flache Liste."""
    products = []
    for category in STANDARD_PRODUCTS.values():
        products.extend(category)
    return products


@pytest.fixture
def edge_case_products():
    return EDGE_CASE_PRODUCTS


# =============================================================================
# Chain-Fixtures
# =============================================================================

KNOWN_CHAINS = [
    "Aldi", "Lidl", "Rewe", "Edeka", "Kaufland",
    "Penny", "Netto", "Norma", "Globus", "Marktkauf"
]

CHAIN_ALIASES = {
    # KaufDA-Varianten
    "ALDI SÜD": "Aldi",
    "ALDI NORD": "Aldi",
    "aldi süd": "Aldi",
    "REWE City": "Rewe",
    "REWE Center": "Rewe",
    "EDEKA Center": "Edeka",
    "E center": "Edeka",
    "Penny-Markt": "Penny",
    "Penny Markt": "Penny",
    "Netto Marken-Discount": "Netto",
    # OSM-Varianten
    "Nahkauf": "Rewe",
}


@pytest.fixture
def known_chains():
    return KNOWN_CHAINS


@pytest.fixture
def chain_aliases():
    return CHAIN_ALIASES


# =============================================================================
# Datums-Fixtures
# =============================================================================

@pytest.fixture
def today():
    return date.today()


@pytest.fixture
def yesterday():
    return date.today() - timedelta(days=1)


@pytest.fixture
def tomorrow():
    return date.today() + timedelta(days=1)


@pytest.fixture
def last_week():
    return date.today() - timedelta(days=7)


@pytest.fixture
def next_week():
    return date.today() + timedelta(days=7)


# =============================================================================
# Marker
# =============================================================================

def pytest_configure(config):
    config.addinivalue_line("markers", "critical: Tests die immer laufen müssen")
    config.addinivalue_line("markers", "network: Tests die Netzwerk brauchen")
    config.addinivalue_line("markers", "slow: Langsame Tests")
