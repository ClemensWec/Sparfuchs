"""Tests for category_search.py using the new categories_v2 schema.

Schema: categories_v2 (level 1=Ober, 2=Unter), product_labels, search_labels + FTS5.
"""
import sqlite3
from pathlib import Path

from app.services.category_search import CategorySearchService


def _build_test_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # categories_v2: level 1 = Oberkategorie, level 2 = Unterkategorie
    cur.execute("""
        CREATE TABLE categories_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            name_normalized TEXT NOT NULL,
            level INTEGER NOT NULL,
            parent_id INTEGER REFERENCES categories_v2(id),
            product_count INTEGER DEFAULT 0,
            UNIQUE(name_normalized, level, parent_id)
        )
    """)

    cur.execute("""
        CREATE TABLE product_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            product_name_normalized TEXT NOT NULL,
            category_v2_id INTEGER REFERENCES categories_v2(id),
            marke TEXT,
            gattungsbegriff TEXT,
            ist_lebensmittel BOOLEAN NOT NULL DEFAULT 1,
            UNIQUE(product_name)
        )
    """)

    cur.execute("""
        CREATE TABLE search_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            label TEXT NOT NULL,
            label_type TEXT NOT NULL DEFAULT 'suchbegriff'
        )
    """)

    # Oberkategorien
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (1, 'Fleisch & Wurst', 'fleisch & wurst', 1, NULL, 6)")
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (2, 'Milchprodukte & Käse', 'milchprodukte & käse', 1, NULL, 3)")
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (3, 'Süßwaren & Snacks', 'süßwaren & snacks', 1, NULL, 4)")

    # Unterkategorien
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (10, 'Hähnchen', 'hähnchen', 2, 1, 3)")
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (11, 'Hähnchenbrustfilet', 'hähnchenbrustfilet', 2, 1, 2)")
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (12, 'Joghurt', 'joghurt', 2, 2, 3)")
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (13, 'Schokolade', 'schokolade', 2, 3, 4)")
    cur.execute("INSERT INTO categories_v2 (id, name, name_normalized, level, parent_id, product_count) VALUES (14, 'Bratwurst', 'bratwurst', 2, 1, 1)")

    # Products
    products = [
        ("Wiesenhof Hähnchenschenkel", 10, "Wiesenhof", "hähnchenschenkel"),
        ("Hähnchen Griller", 10, None, "hähnchen"),
        ("Bio Hähnchenbrust", 11, None, "hähnchenbrustfilet"),
        ("Joghurt Natur", 12, "Danone", "joghurt"),
        ("Schokolade Vollmilch", 13, "Milka", "schokolade"),
        ("Milka Haselnuss", 13, "Milka", "schokolade"),
        ("Lindt Excellence", 13, "Lindt", "schokolade"),
        ("Bratwurst grob", 14, None, "bratwurst"),
    ]
    for pname, cat_id, marke, gattung in products:
        cur.execute(
            "INSERT INTO product_labels (product_name, product_name_normalized, category_v2_id, marke, gattungsbegriff) VALUES (?, ?, ?, ?, ?)",
            (pname, pname.lower(), cat_id, marke, gattung),
        )

    # Search labels
    search_labels = [
        ("Wiesenhof Hähnchenschenkel", "hähnchen", "suchbegriff"),
        ("Wiesenhof Hähnchenschenkel", "hähnchenschenkel", "suchbegriff"),
        ("Wiesenhof Hähnchenschenkel", "geflügel", "suchbegriff"),
        ("Wiesenhof Hähnchenschenkel", "wiesenhof", "marke"),
        ("Wiesenhof Hähnchenschenkel", "hähnchenschenkel", "gattung"),
        ("Hähnchen Griller", "hähnchen", "suchbegriff"),
        ("Hähnchen Griller", "grillhähnchen", "suchbegriff"),
        ("Hähnchen Griller", "hähnchen", "gattung"),
        ("Bio Hähnchenbrust", "hähnchenbrust", "suchbegriff"),
        ("Bio Hähnchenbrust", "hähnchenbrustfilet", "suchbegriff"),
        ("Bio Hähnchenbrust", "hähnchen", "suchbegriff"),
        ("Joghurt Natur", "joghurt", "suchbegriff"),
        ("Joghurt Natur", "naturjoghurt", "suchbegriff"),
        ("Joghurt Natur", "danone", "marke"),
        ("Joghurt Natur", "joghurt", "gattung"),
        ("Schokolade Vollmilch", "schokolade", "suchbegriff"),
        ("Schokolade Vollmilch", "vollmilchschokolade", "suchbegriff"),
        ("Schokolade Vollmilch", "milka", "marke"),
        ("Milka Haselnuss", "schokolade", "suchbegriff"),
        ("Milka Haselnuss", "milka", "marke"),
        ("Milka Haselnuss", "schokolade", "gattung"),
        ("Lindt Excellence", "schokolade", "suchbegriff"),
        ("Lindt Excellence", "lindt", "marke"),
        ("Lindt Excellence", "schokolade", "gattung"),
        ("Bratwurst grob", "bratwurst", "suchbegriff"),
        ("Bratwurst grob", "wurst", "suchbegriff"),
        ("Bratwurst grob", "bratwurst", "gattung"),
    ]
    for pname, label, ltype in search_labels:
        cur.execute(
            "INSERT INTO search_labels (product_name, label, label_type) VALUES (?, ?, ?)",
            (pname, label, ltype),
        )

    # FTS5 index
    cur.execute("""
        CREATE VIRTUAL TABLE search_labels_fts USING fts5(
            product_name, label, label_type,
            content='search_labels', content_rowid='id',
            tokenize='unicode61'
        )
    """)
    cur.execute("INSERT INTO search_labels_fts(search_labels_fts) VALUES('rebuild')")

    # Indexes
    cur.execute("CREATE INDEX idx_sl_product ON search_labels(product_name)")
    cur.execute("CREATE INDEX idx_sl_label ON search_labels(label)")
    cur.execute("CREATE INDEX idx_pl_cat ON product_labels(category_v2_id)")
    cur.execute("CREATE INDEX idx_pl_name ON product_labels(product_name)")
    cur.execute("CREATE INDEX idx_cv2_parent ON categories_v2(parent_id)")

    conn.commit()
    conn.close()


def test_exact_category_name_match_ranks_highest(tmp_path: Path) -> None:
    """Searching 'hähnchen' should rank the Hähnchen category first."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    results = svc.search("hähnchen")
    assert len(results) >= 1
    assert results[0]["name"] == "Hähnchen"


def test_compound_prefix_finds_related(tmp_path: Path) -> None:
    """'hähnchen' should also find 'Hähnchenbrustfilet'."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    results = svc.search("hähnchen")
    names = [r["name"] for r in results]
    assert "Hähnchenbrustfilet" in names


def test_brand_search_finds_category(tmp_path: Path) -> None:
    """Searching a brand name should find the category of that brand's products."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    results = svc.search("milka")
    assert len(results) >= 1
    names = [r["name"] for r in results]
    assert "Schokolade" in names


def test_empty_query_returns_empty(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    assert svc.search("") == []
    assert svc.search("   ") == []


def test_expand_level2_category(tmp_path: Path) -> None:
    """Expanding a level-2 (Unterkategorie) returns just its own ID."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    result = svc.expand_category(category_id=10, category_name="Hähnchen")
    assert result["ids"] == [10]
    assert result["offer_count"] == 3


def test_expand_level1_category(tmp_path: Path) -> None:
    """Expanding a level-1 (Oberkategorie) returns all children IDs."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    result = svc.expand_category(category_id=1, category_name="Fleisch & Wurst")
    assert sorted(result["ids"]) == [10, 11, 14]
    assert result["offer_count"] == 6  # 3 + 2 + 1


def test_compound_suffix_match(tmp_path: Path) -> None:
    """'wurst' should find 'Bratwurst' (compound suffix Brat+Wurst)."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    results = svc.search("wurst")
    names = [r["name"] for r in results]
    assert "Bratwurst" in names


def test_result_payload_fields(tmp_path: Path) -> None:
    """Each result should have the expected fields."""
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)

    results = svc.search("joghurt")
    assert len(results) >= 1
    r = results[0]
    assert "id" in r
    assert "name" in r
    assert "offer_count" in r
    assert "kind" in r
    assert r["kind"] == "category"
    assert "oberkategorie" in r
    assert r["oberkategorie"] == "Milchprodukte & Käse"


def test_available_returns_true(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite3"
    _build_test_db(db)
    svc = CategorySearchService(db)
    assert svc.available() is True


def test_available_returns_false_for_missing_db(tmp_path: Path) -> None:
    svc = CategorySearchService(tmp_path / "nonexistent.sqlite3")
    assert svc.available() is False
