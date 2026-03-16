"""Build structured product categories from offer names."""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from app.services.category_classifier import classify_category_name
from app.utils.text import compact_text, normalize_search_text

DB_PATH = Path("data/kaufda_dataset/offers.sqlite3")

_STRIP_PREFIXES = [
    r"frische[rs]?\s+",
    r"deutsche[rs]?\s+",
    r"original\s+",
    r"delikatess\s+",
    r"premium\s+",
    r"xxl\s+",
    r"mini[- ]",
    r"bio[- ]",
    r"demeter\s+",
    r"bioland\s+",
    r"regionale[rs]?\s+",
    r"beste[rs]?\s+(?:vom?|von)\s+(?:huhn|rind|schwein)\s+",
    r"freiland[- ]",
    r"fairmast\s+",
    r"landbauern\s+",
    r"metzgerfrisch\s+",
    r"gqb\s+",
    r"kikok[- ]mais[- ]",
]

_STRIP_SUFFIXES = [
    r"\s+natur$",
    r"\s+gewuerzt$",
    r"\s+mariniert\s+\w+$",
    r"\s+mariniert$",
    r"\s+gefuellt\s+.*$",
    r"\s+nach art\s+.*$",
    r"\s+style$",
    r"\s+art$",
    r"\s+klassisch$",
    r"\s+classic$",
    r"\s+xxl$",
]

# Synonym normalisation applied during grouping so that e.g.
# "H-Milch 1,5% Fett" and "Haltbare Milch 1,5% Fett" collapse
# into a single category.  Order matters: first match wins.
_GROUPING_SYNONYMS: list[tuple[str, str]] = [
    # H-Milch is short for Haltbare Milch
    (r"\bh\s*milch\b", "haltbare milch"),
    (r"\bhmilch\b", "haltbare milch"),
    (r"\bh\s*vollmilch\b", "haltbare vollmilch"),
]

# Suffixes that are redundant after a percentage value (e.g. "1 5 fett" → "1 5")
_TRAILING_FETT_RE = re.compile(r"(\d)\s+fett$")

# Strip "unsere/unserer/unser" prefix – branding noise
_UNSERE_RE = re.compile(r"^unsere[rs]?\s+", re.IGNORECASE)

_EXCLUDE_PREFIXES = (
    "rabatt",
    "bis zu ",
    "gutschein",
    "guthabenkarte",
    "coupon",
)


def _normalize_for_grouping(name: str) -> str:
    text = normalize_search_text(name)
    if not text:
        return ""

    for pattern in _STRIP_PREFIXES:
        text = re.sub(f"^{pattern}", "", text, flags=re.IGNORECASE).strip()
    for pattern in _STRIP_SUFFIXES:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()

    text = re.sub(r"(\w)-(\w)", r"\1\2", text)
    text = re.sub(r"\s+", " ", text).strip()

    # Apply synonym normalisation (H-Milch → Haltbare Milch, etc.)
    for pattern, replacement in _GROUPING_SYNONYMS:
        text = re.sub(pattern, replacement, text)

    # Strip redundant trailing "fett" after percentage ("1 5 fett" → "1 5")
    text = _TRAILING_FETT_RE.sub(r"\1", text)

    # Strip branding prefix "unsere"
    text = _UNSERE_RE.sub("", text).strip()

    text = re.sub(r"\s+", " ", text).strip()

    if text.endswith("filets"):
        text = text[:-1]
    elif text.endswith("steaks"):
        text = text[:-1]

    return text


def _is_excluded(name: str) -> bool:
    normalized = normalize_search_text(name)
    if not normalized:
        return True
    if normalized in {"original", "10 rabatt", "15", "20 rabatt"}:
        return True
    return normalized.startswith(_EXCLUDE_PREFIXES) or " rabatt" in normalized


def _pick_canonical_name(product_names: list[tuple[str, int]]) -> str:
    def sort_key(item: tuple[str, int]) -> tuple[int, int, int]:
        name, count = item
        normalized = normalize_search_text(name)
        has_prefix = int(normalized != _normalize_for_grouping(name))
        return (has_prefix, len(compact_text(name)), -count)

    return sorted(product_names, key=sort_key)[0][0]


def build_categories(db_path: Path | str = DB_PATH) -> dict[str, list[str]]:
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()
    cur.execute("SELECT product_name, COUNT(*) as cnt FROM offers GROUP BY product_name")
    all_names = cur.fetchall()
    conn.close()

    groups: dict[str, list[tuple[str, int]]] = defaultdict(list)
    excluded = 0
    for name, count in all_names:
        if not name or _is_excluded(name):
            excluded += 1
            continue
        key = _normalize_for_grouping(name)
        if not key:
            excluded += 1
            continue
        groups[key].append((str(name), int(count)))

    categories: dict[str, list[str]] = {}
    for members in groups.values():
        canonical = _pick_canonical_name(members)
        categories[canonical] = [member_name for member_name, _ in members]

    print(
        f"Built {len(categories)} categories from {len(all_names)} product names "
        f"(excluded {excluded} non-products)"
    )
    return categories


def populate_categories_table(db_path: Path | str = DB_PATH) -> None:
    categories = build_categories(db_path)

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS product_categories")
    cur.execute(
        """
        CREATE TABLE product_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            name_normalized TEXT NOT NULL,
            product_type TEXT NOT NULL,
            product_type_normalized TEXT NOT NULL,
            parent_category TEXT NOT NULL,
            parent_category_normalized TEXT NOT NULL,
            semantic_group TEXT NOT NULL,
            search_scope TEXT NOT NULL,
            ingredient_tags_json TEXT NOT NULL DEFAULT '[]',
            attributes_json TEXT NOT NULL DEFAULT '{}',
            classification_confidence REAL NOT NULL DEFAULT 0.0,
            classification_source TEXT NOT NULL DEFAULT 'rule',
            offer_count INTEGER NOT NULL DEFAULT 0,
            parent_id INTEGER,
            kind TEXT NOT NULL DEFAULT 'category',
            expanded_offer_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cur.execute("DROP TABLE IF EXISTS categories_fts")
    cur.execute(
        """
        CREATE VIRTUAL TABLE categories_fts USING fts5(
            name_normalized,
            content='product_categories',
            content_rowid='id',
            tokenize='unicode61'
        )
        """
    )

    cols = [row[1] for row in cur.execute("PRAGMA table_info(offers)").fetchall()]
    if "category_id" not in cols:
        cur.execute("ALTER TABLE offers ADD COLUMN category_id INTEGER")

    name_to_category_id: dict[str, int] = {}
    for canonical_name, member_names in sorted(categories.items()):
        classification = classify_category_name(canonical_name)
        cur.execute(
            """
            INSERT INTO product_categories (
                name,
                name_normalized,
                product_type,
                product_type_normalized,
                parent_category,
                parent_category_normalized,
                semantic_group,
                search_scope,
                ingredient_tags_json,
                attributes_json,
                classification_confidence,
                classification_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                classification.canonical_name,
                classification.name_normalized,
                classification.product_type,
                classification.product_type_normalized,
                classification.parent_category,
                classification.parent_category_normalized,
                classification.semantic_group,
                classification.search_scope,
                classification.ingredient_tags_json,
                classification.attributes_json,
                classification.classification_confidence,
                classification.classification_source,
            ),
        )
        cat_id = cur.lastrowid
        for member in member_names:
            name_to_category_id[member] = cat_id

    conn.commit()
    print(f"Inserted {len(categories)} categories. Assigning to offers...")

    cur.execute("DROP TABLE IF EXISTS _tmp_name_cat")
    cur.execute("CREATE TEMP TABLE _tmp_name_cat (product_name TEXT PRIMARY KEY, cat_id INTEGER)")
    cur.executemany("INSERT INTO _tmp_name_cat VALUES (?, ?)", name_to_category_id.items())
    cur.execute(
        """
        UPDATE offers SET category_id = (
            SELECT cat_id FROM _tmp_name_cat WHERE _tmp_name_cat.product_name = offers.product_name
        )
        WHERE product_name IN (SELECT product_name FROM _tmp_name_cat)
        """
    )
    cur.execute("DROP TABLE _tmp_name_cat")

    cur.execute(
        """
        UPDATE product_categories SET offer_count = (
            SELECT COUNT(*) FROM offers WHERE offers.category_id = product_categories.id
        )
        """
    )

    # --- Phase 2: Build taxonomy (parent_id, kind, expanded_offer_count) ---
    # Family nodes: categories where name_normalized == product_type_normalized
    # These are the canonical representatives of their product type group.
    cur.execute(
        """
        UPDATE product_categories
        SET kind = 'family'
        WHERE name_normalized = product_type_normalized
          AND search_scope IN ('broad', 'group')
        """
    )

    # Set parent_id: non-family nodes point to the family node of the same product_type_normalized.
    # Include 'exact' scope too — they are still part of the product family,
    # just not expanded when selected individually.
    cur.execute(
        """
        UPDATE product_categories
        SET parent_id = (
            SELECT f.id
            FROM product_categories f
            WHERE f.product_type_normalized = product_categories.product_type_normalized
              AND f.kind = 'family'
              AND f.id != product_categories.id
            LIMIT 1
        )
        WHERE kind != 'family'
        """
    )

    # Expanded offer count for family nodes = sum of own + all children's offer_count
    cur.execute(
        """
        UPDATE product_categories
        SET expanded_offer_count = (
            SELECT COALESCE(SUM(c.offer_count), 0)
            FROM product_categories c
            WHERE c.product_type_normalized = product_categories.product_type_normalized
        )
        WHERE kind = 'family'
        """
    )

    # For non-family nodes: expanded = own offer_count (no further expansion)
    cur.execute(
        """
        UPDATE product_categories
        SET expanded_offer_count = offer_count
        WHERE kind != 'family'
        """
    )

    cur.execute(
        """
        INSERT INTO categories_fts (rowid, name_normalized)
        SELECT id, name_normalized FROM product_categories
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_category_id ON offers(category_id)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_categories_type ON product_categories(product_type_normalized)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_categories_parent ON product_categories(parent_category_normalized)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_categories_group ON product_categories(semantic_group)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_categories_parent_id ON product_categories(parent_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_categories_kind ON product_categories(kind)"
    )

    conn.commit()

    cat_count = cur.execute("SELECT COUNT(*) FROM product_categories").fetchone()[0]
    family_count = cur.execute("SELECT COUNT(*) FROM product_categories WHERE kind='family'").fetchone()[0]
    with_parent = cur.execute("SELECT COUNT(*) FROM product_categories WHERE parent_id IS NOT NULL").fetchone()[0]
    assigned = cur.execute("SELECT COUNT(*) FROM offers WHERE category_id IS NOT NULL").fetchone()[0]
    unassigned = cur.execute("SELECT COUNT(*) FROM offers WHERE category_id IS NULL").fetchone()[0]
    total = cur.execute("SELECT COUNT(*) FROM offers").fetchone()[0]
    conn.close()

    print("\nDone!")
    print(f"  Categories created: {cat_count}")
    print(f"  Family nodes:       {family_count}")
    print(f"  With parent:        {with_parent}")
    print(f"  Offers assigned:    {assigned}/{total} ({100 * assigned / total:.1f}%)")
    print(f"  Offers unassigned:  {unassigned} (non-products/excluded)")


if __name__ == "__main__":
    populate_categories_table()
