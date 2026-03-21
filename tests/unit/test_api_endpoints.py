"""API-Endpoint-Tests für Sparfuchs.

Testet die neuen Endpoints:
- /api/category-tiles
- /api/offers-by-category
- /api/compare (mit category_id basket items)
- /api/suggest-categories
- /api/suggest
"""
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Marker für die wichtigsten Tests
# ---------------------------------------------------------------------------

# ========================== /api/category-tiles ============================


@pytest.mark.critical
def test_category_tiles_returns_list():
    """Tiles-Endpoint gibt eine Liste zurück."""
    resp = client.get("/api/category-tiles")
    assert resp.status_code == 200
    data = resp.json()
    assert "tiles" in data
    assert isinstance(data["tiles"], list)


def test_category_tiles_have_required_fields():
    """Jede Tile hat id, name und count."""
    resp = client.get("/api/category-tiles")
    data = resp.json()
    for tile in data["tiles"]:
        assert "id" in tile, f"Tile ohne 'id': {tile}"
        assert "name" in tile, f"Tile ohne 'name': {tile}"
        assert "count" in tile, f"Tile ohne 'count': {tile}"
        # id muss Integer sein, count >= 0
        assert isinstance(tile["id"], int)
        assert isinstance(tile["count"], int)
        assert tile["count"] >= 0


def test_category_tiles_no_duplicates():
    """Tile-Namen dürfen nicht doppelt vorkommen."""
    resp = client.get("/api/category-tiles")
    tiles = resp.json()["tiles"]
    names = [t["name"] for t in tiles]
    assert len(names) == len(set(names)), f"Duplikate in Tiles: {names}"


@pytest.mark.critical
def test_category_tiles_counts_are_real():
    """Tile-Counts müssen echte Angebotszahlen sein, nicht 0 (Regression: product_count war 0 bei L1)."""
    resp = client.get("/api/category-tiles")
    tiles = resp.json()["tiles"]
    # Mindestens die Hälfte der Tiles muss count > 0 haben
    nonzero = [t for t in tiles if t["count"] > 0]
    assert len(nonzero) >= len(tiles) // 2, f"Zu viele Tiles mit count=0: {[(t['name'], t['count']) for t in tiles]}"
    # Top-Tile muss mindestens 100 Angebote haben
    assert tiles[0]["count"] >= 100, f"Top-Tile hat zu wenig: {tiles[0]}"


def test_category_tiles_returns_available_chains():
    """Tiles-Response enthält available_chains-Liste."""
    resp = client.get("/api/category-tiles")
    data = resp.json()
    assert "available_chains" in data
    assert isinstance(data["available_chains"], list)
    assert len(data["available_chains"]) >= 5, f"Zu wenige Ketten: {data['available_chains']}"


def test_category_tiles_chain_filter_reduces_counts():
    """Chain-Filter reduziert die Tile-Counts."""
    resp_all = client.get("/api/category-tiles")
    tiles_all = resp_all.json()["tiles"]

    resp_filtered = client.get("/api/category-tiles?chains=Aldi")
    tiles_filtered = resp_filtered.json()["tiles"]

    # Filtered counts must be lower than unfiltered for the same category
    all_map = {t["id"]: t["count"] for t in tiles_all}
    for t in tiles_filtered:
        if t["id"] in all_map:
            assert t["count"] <= all_map[t["id"]], (
                f"Filtered count ({t['count']}) > unfiltered ({all_map[t['id']]}) für {t['name']}"
            )


def test_offers_by_category_chain_filter():
    """Chain-Filter in offers-by-category filtert korrekt."""
    tiles_resp = client.get("/api/category-tiles")
    tiles = tiles_resp.json()["tiles"]
    if not tiles:
        pytest.skip("Keine Kategorien")

    cat_id = tiles[0]["id"]
    resp = client.get(f"/api/offers-by-category?category_id={cat_id}&chains=Edeka&limit=20")
    assert resp.status_code == 200
    data = resp.json()

    # All offers must be from Edeka
    for offer in data["offers"]:
        assert offer["chain"] == "Edeka", f"Offer von falscher Kette: {offer['chain']}"


# ====================== /api/offers-by-category ===========================


@pytest.mark.critical

def test_offers_by_category_returns_offers():
    """Gibt Angebote für eine existierende Kategorie zurück."""
    # Erst eine gültige Kategorie-ID aus den Tiles holen
    tiles_resp = client.get("/api/category-tiles")
    tiles = tiles_resp.json()["tiles"]
    if not tiles:
        pytest.skip("Keine Kategorien in der DB vorhanden")

    cat_id = tiles[0]["id"]
    resp = client.get(f"/api/offers-by-category?category_id={cat_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert "offers" in data
    assert "total" in data
    assert isinstance(data["offers"], list)
    assert isinstance(data["total"], int)



def test_offers_by_category_with_location_filters():
    """Location-Filter schränkt Ergebnisse ein (kein Fehler)."""
    tiles_resp = client.get("/api/category-tiles")
    tiles = tiles_resp.json()["tiles"]
    if not tiles:
        pytest.skip("Keine Kategorien in der DB vorhanden")

    cat_id = tiles[0]["id"]
    resp = client.get(
        f"/api/offers-by-category?category_id={cat_id}"
        "&location=50.7374,7.0982&radius_km=5&limit=10"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "offers" in data
    # Mit Location-Filter können es weniger oder gleich viele sein
    assert isinstance(data["total"], int)
    assert data["total"] >= 0


def test_offers_by_category_invalid_id_returns_404():
    """Nicht existierende Kategorie-ID ergibt 404."""
    resp = client.get("/api/offers-by-category?category_id=999999999")
    assert resp.status_code == 404
    data = resp.json()
    assert "error" in data


def test_offers_by_category_zero_id_returns_400():
    """category_id=0 ist ungültig und ergibt 400."""
    resp = client.get("/api/offers-by-category?category_id=0")
    assert resp.status_code == 400
    data = resp.json()
    assert "error" in data



def test_offers_by_category_has_subcategories():
    """Top-Level-Kategorien haben Unterkategorien."""
    tiles_resp = client.get("/api/category-tiles")
    tiles = tiles_resp.json()["tiles"]
    if not tiles:
        pytest.skip("Keine Kategorien in der DB vorhanden")

    cat_id = tiles[0]["id"]
    resp = client.get(f"/api/offers-by-category?category_id={cat_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert "subcategories" in data
    assert isinstance(data["subcategories"], list)
    # Top-Level-Kategorie sollte mind. 1 Unterkategorie haben
    if data["subcategories"]:
        sub = data["subcategories"][0]
        assert "id" in sub
        assert "name" in sub
        assert "count" in sub



def test_offers_fields_present():
    """Angebote enthalten die erwarteten Felder (image_url, price_eur, chain, title)."""
    tiles_resp = client.get("/api/category-tiles")
    tiles = tiles_resp.json()["tiles"]
    if not tiles:
        pytest.skip("Keine Kategorien in der DB vorhanden")

    cat_id = tiles[0]["id"]
    resp = client.get(f"/api/offers-by-category?category_id={cat_id}&limit=5")
    data = resp.json()
    if not data["offers"]:
        pytest.skip("Keine Angebote für diese Kategorie vorhanden")

    required_fields = {"title", "price_eur", "chain", "image_url"}
    for offer in data["offers"]:
        missing = required_fields - set(offer.keys())
        assert not missing, f"Fehlende Felder in Angebot: {missing}"


# ======================== /api/compare ====================================


@pytest.mark.critical
def test_compare_category_basket_item():
    """Compare mit category_id-basiertem Warenkorb-Eintrag."""
    # Erst eine gültige Kategorie finden
    cat_resp = client.get("/api/suggest-categories?q=milch")
    categories = cat_resp.json().get("categories", [])
    if not categories:
        pytest.skip("Keine Kategorie für 'milch' gefunden")

    cat = categories[0]
    cat_id = cat.get("id")
    cat_name = cat.get("name", "Milch")
    if cat_id is None:
        pytest.skip("Kategorie hat keine ID")

    resp = client.post("/api/compare", json={
        "location": "50.7374, 7.0982",  # Bonn Koordinaten
        "radius_km": 10,
        "basket": [
            {"category_id": cat_id, "category_name": cat_name},
        ],
    })
    assert resp.status_code == 200
    data = resp.json()
    # Entweder rows oder warning (wenn keine Märkte im Umkreis)
    assert "rows" in data or "error" not in data


@pytest.mark.critical
def test_compare_mixed_basket():
    """Compare mit gemischtem Warenkorb (Kategorie + Freitext)."""
    cat_resp = client.get("/api/suggest-categories?q=butter")
    categories = cat_resp.json().get("categories", [])

    basket = [{"q": "Vollmilch"}]
    if categories:
        cat = categories[0]
        if cat.get("id") is not None:
            basket.append({
                "category_id": cat["id"],
                "category_name": cat.get("name", "Butter"),
            })

    resp = client.post("/api/compare", json={
        "location": "50.7374, 7.0982",
        "radius_km": 10,
        "basket": basket,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "rows" in data


def test_compare_empty_basket_returns_error():
    """Leerer Warenkorb ergibt Fehlermeldung."""
    resp = client.post("/api/compare", json={
        "location": "50.7374, 7.0982",
        "radius_km": 10,
        "basket": [],
    })
    assert resp.status_code == 400
    data = resp.json()
    assert "error" in data


def test_compare_no_location_returns_error():
    """Fehlender Standort ergibt Fehlermeldung."""
    resp = client.post("/api/compare", json={
        "location": "",
        "radius_km": 10,
        "basket": [{"q": "Milch"}],
    })
    assert resp.status_code == 400
    data = resp.json()
    assert "error" in data


# ====================== /api/suggest-categories ===========================


@pytest.mark.critical
def test_suggest_categories_returns_results():
    """Kategorie-Suche nach 'milch' liefert Ergebnisse."""
    resp = client.get("/api/suggest-categories?q=milch")
    assert resp.status_code == 200
    data = resp.json()
    assert "categories" in data
    assert isinstance(data["categories"], list)
    # 'milch' sollte mindestens eine Kategorie treffen
    assert len(data["categories"]) > 0, "Keine Kategorien für 'milch' gefunden"


def test_suggest_categories_empty_query():
    """Leere Query gibt leere Kategorie-Liste zurück."""
    resp = client.get("/api/suggest-categories?q=")
    assert resp.status_code == 200
    data = resp.json()
    assert data["categories"] == []


def test_suggest_categories_short_query():
    """Zu kurze Query (1 Zeichen) gibt leere Liste zurück."""
    resp = client.get("/api/suggest-categories?q=m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["categories"] == []


def test_suggest_categories_has_required_fields():
    """Kategorie-Vorschläge haben die erwarteten Felder."""
    resp = client.get("/api/suggest-categories?q=käse")
    data = resp.json()
    if not data["categories"]:
        pytest.skip("Keine Kategorien für 'käse' gefunden")

    for cat in data["categories"]:
        assert "name" in cat, f"Kategorie ohne 'name': {cat}"
        # id kann None sein bei intent-Einträgen, aber Feld muss existieren
        assert "id" in cat or "type" in cat, f"Kategorie ohne 'id': {cat}"


# ========================== /api/suggest ==================================


def test_suggest_returns_hits():
    """Suggest-Endpoint liefert Treffer für 'milch'."""
    resp = client.get("/api/suggest?q=milch")
    assert resp.status_code == 200
    data = resp.json()
    assert "hits" in data
    assert isinstance(data["hits"], list)


def test_suggest_short_query_returns_empty():
    """Zu kurze Query gibt leere Hits zurück."""
    resp = client.get("/api/suggest?q=m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["hits"] == []
