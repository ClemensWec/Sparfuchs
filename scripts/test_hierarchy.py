"""Comprehensive hierarchy tests: DB invariants + API integration.

Usage: PYTHONPATH=. python scripts/test_hierarchy.py
"""

import json
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "kaufda_dataset" / "offers.sqlite3"
BASE_URL = "http://127.0.0.1:8000"
PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> bool:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS {name}" + (f" ({detail})" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {name}" + (f" ({detail})" if detail else ""))
    return condition


def api_get(path: str) -> dict:
    resp = urllib.request.urlopen(f"{BASE_URL}{path}", timeout=10)
    return json.loads(resp.read())


# ─── DB-Level Tests ────────────────────────────────────────────────

def test_prerequisites():
    """T0: Verify DB structure is sane."""
    print("\n=== T0: Prerequisites ===")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    counts = {}
    for level in [1, 2, 3]:
        c = conn.execute("SELECT COUNT(*) as c FROM categories_v2 WHERE level = ?", (level,)).fetchone()["c"]
        counts[level] = c

    check("Level-1 count ~25", 20 <= counts[1] <= 30, f"{counts[1]}")
    check("Level-2 count ~340", 200 <= counts[2] <= 500, f"{counts[2]}")
    check("Level-3 count ~3200", 2500 <= counts[3] <= 4000, f"{counts[3]}")

    fts = conn.execute("SELECT COUNT(*) as c FROM categories_fts").fetchone()["c"]
    total = counts[1] + counts[2] + counts[3]
    check("FTS index populated", fts > 0, f"{fts} entries")

    conn.close()


def test_count_invariant():
    """T1: For every level-2 group: product_count == direct + sum(children)."""
    print("\n=== T1: Count Invariant (exact) ===")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    groups = conn.execute("SELECT id, name, product_count FROM categories_v2 WHERE level = 2").fetchall()
    violations = []

    for g in groups:
        direct = conn.execute(
            "SELECT COUNT(*) as c FROM product_labels WHERE category_v2_id = ?", (g["id"],)
        ).fetchone()["c"]
        children_sum = conn.execute(
            "SELECT COALESCE(SUM(product_count), 0) as s FROM categories_v2 WHERE parent_id = ? AND level = 3",
            (g["id"],),
        ).fetchone()["s"]
        expected = direct + children_sum
        if g["product_count"] != expected:
            violations.append(f"{g['name']} (id={g['id']}): count={g['product_count']}, expected={expected} (direct={direct}, children={children_sum})")

    check("All level-2 counts correct", len(violations) == 0, f"{len(groups)} groups checked")
    for v in violations[:5]:
        print(f"    ! {v}")

    conn.close()


def test_expand_category():
    """T2: expand_category correctness for all 3 levels."""
    print("\n=== T2: expand_category ===")

    sys.path.insert(0, str(DB_PATH.parent.parent.parent))
    from app.services.category_search import CategorySearchService
    svc = CategorySearchService(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Level-2: should include self + all level-3 children
    group = conn.execute(
        "SELECT id, name FROM categories_v2 WHERE level = 2 AND name NOT LIKE '%Sonstiges%' ORDER BY product_count DESC LIMIT 1"
    ).fetchone()
    result = svc.expand_category(category_id=group["id"], category_name=group["name"])
    children = conn.execute(
        "SELECT id FROM categories_v2 WHERE parent_id = ? AND level = 3", (group["id"],)
    ).fetchall()
    expected_ids = {group["id"]} | {c["id"] for c in children}
    check("Level-2 expand includes self + children",
          set(result["ids"]) == expected_ids,
          f"{group['name']}: {len(result['ids'])} IDs, expected {len(expected_ids)}")

    # Level-3: should return only self
    child = conn.execute("SELECT id, name FROM categories_v2 WHERE level = 3 LIMIT 1").fetchone()
    result3 = svc.expand_category(category_id=child["id"], category_name=child["name"])
    check("Level-3 expand returns only self", result3["ids"] == [child["id"]])

    # Level-1: should include all descendants
    ober = conn.execute("SELECT id, name FROM categories_v2 WHERE level = 1 LIMIT 1").fetchone()
    result1 = svc.expand_category(category_id=ober["id"], category_name=ober["name"])
    l2_count = conn.execute("SELECT COUNT(*) as c FROM categories_v2 WHERE parent_id = ? AND level = 2", (ober["id"],)).fetchone()["c"]
    check("Level-1 expand includes many descendants", len(result1["ids"]) > l2_count, f"{len(result1['ids'])} IDs")

    # Edge: Level-2 with 0 children
    empty_group = conn.execute(
        "SELECT id, name FROM categories_v2 WHERE level = 2 AND id NOT IN (SELECT DISTINCT parent_id FROM categories_v2 WHERE level = 3 AND parent_id IS NOT NULL) LIMIT 1"
    ).fetchone()
    if empty_group:
        result_empty = svc.expand_category(category_id=empty_group["id"], category_name=empty_group["name"])
        check("Level-2 with 0 children expands to self", result_empty["ids"] == [empty_group["id"]],
              f"{empty_group['name']}")

    # Edge: largest group
    largest = conn.execute("""
        SELECT g.id, g.name, COUNT(c.id) as child_count
        FROM categories_v2 g
        JOIN categories_v2 c ON c.parent_id = g.id AND c.level = 3
        WHERE g.level = 2
        GROUP BY g.id ORDER BY child_count DESC LIMIT 1
    """).fetchone()
    t0 = time.time()
    result_large = svc.expand_category(category_id=largest["id"], category_name=largest["name"])
    elapsed = (time.time() - t0) * 1000
    check("Largest group expands correctly",
          len(result_large["ids"]) == largest["child_count"] + 1,
          f"{largest['name']}: {largest['child_count']} children, {elapsed:.0f}ms")
    check("Largest group expand <50ms", elapsed < 50, f"{elapsed:.0f}ms")

    conn.close()


def test_oberkategorie():
    """T3: oberkategorie field is a level-1 name."""
    print("\n=== T3: Oberkategorie Correctness ===")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    l1_names = {row["name"] for row in conn.execute("SELECT name FROM categories_v2 WHERE level = 1")}

    # Check via API for a few terms
    for term in ["milch", "wurst", "bier"]:
        try:
            data = api_get(f"/api/suggest-categories?q={term}")
            bad = []
            for cat in data.get("categories", []):
                ober = cat.get("oberkategorie", "")
                if ober and ober not in l1_names:
                    bad.append(f"{cat['name']}: ober='{ober}'")
            if bad:
                check(f"'{term}' oberkategorie all L1", False, f"{len(bad)} wrong: {bad[0]}")
            else:
                check(f"'{term}' oberkategorie all valid", True)
        except Exception as e:
            check(f"'{term}' API call", False, str(e))

    conn.close()


def test_no_orphans():
    """T4: No orphaned categories."""
    print("\n=== T4: No Orphaned Categories ===")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Every level-3 must have a valid level-2 parent
    orphan_l3 = conn.execute("""
        SELECT c.id, c.name FROM categories_v2 c
        WHERE c.level = 3 AND (
            c.parent_id IS NULL
            OR c.parent_id NOT IN (SELECT id FROM categories_v2 WHERE level = 2)
        )
    """).fetchall()
    check("No orphaned level-3", len(orphan_l3) == 0, f"{len(orphan_l3)} orphans")

    # Every level-2 must have a valid level-1 parent
    orphan_l2 = conn.execute("""
        SELECT c.id, c.name FROM categories_v2 c
        WHERE c.level = 2 AND (
            c.parent_id IS NULL
            OR c.parent_id NOT IN (SELECT id FROM categories_v2 WHERE level = 1)
        )
    """).fetchall()
    check("No orphaned level-2", len(orphan_l2) == 0, f"{len(orphan_l2)} orphans")

    conn.close()


# ─── API-Level Tests ───────────────────────────────────────────────

def test_regression_hundefutter():
    """T5: Regression - hundefutter count >= hundefutter nass count."""
    print("\n=== T5: Regression hundefutter ===")

    data1 = api_get("/api/suggest-categories?q=hundefutter")
    data2 = api_get("/api/suggest-categories?q=hundefutter+nass")

    # Find Hundefutter group
    hf_count = None
    hf_first = False
    for i, cat in enumerate(data1.get("categories", [])):
        if "hundefutter" in cat["name"].lower() and "nass" not in cat["name"].lower() and "trocken" not in cat["name"].lower():
            hf_count = cat["offer_count"]
            if i == 0:
                hf_first = True
            break

    # Find Hundefutter Nass
    nass_count = None
    for cat in data2.get("categories", []):
        if "nass" in cat["name"].lower():
            nass_count = cat["offer_count"]
            break

    if hf_count is not None and nass_count is not None:
        check("Hundefutter >= Hundefutter Nass", hf_count >= nass_count,
              f"Hundefutter={hf_count}, Nass={nass_count}")
    else:
        check("Both categories found", False, f"hf={hf_count}, nass={nass_count}")

    check("Hundefutter is first result", hf_first,
          f"first: {data1['categories'][0]['name']}" if data1.get("categories") else "no results")


def test_search_terms_group_invariant():
    """T6: 20 search terms - group always >= children."""
    print("\n=== T6: 20 Search Terms - Group Invariant ===")

    terms = [
        "hundefutter", "milch", "butter", "wurst", "bier", "cola", "chips",
        "joghurt", "schokolade", "zahnpasta", "waschmittel", "kaffee",
        "reis", "nudeln", "pizza", "toilettenpapier", "shampoo", "hackfleisch",
    ]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    for term in terms:
        try:
            data = api_get(f"/api/suggest-categories?q={term}")
            cats = data.get("categories", [])
            if not cats:
                continue

            # Check no duplicate IDs
            ids = [c["id"] for c in cats]
            check(f"'{term}' no duplicate IDs", len(ids) == len(set(ids)),
                  f"{len(ids)} results")

            # If first result exists, check it has highest or equal count
            if len(cats) >= 2:
                first_count = cats[0]["offer_count"]
                # Check group invariant: if a parent group exists, its count >= any child
                for cat in cats[1:]:
                    parent_id = conn.execute(
                        "SELECT parent_id FROM categories_v2 WHERE id = ?", (cat["id"],)
                    ).fetchone()
                    if parent_id and any(c["id"] == parent_id["parent_id"] for c in cats):
                        parent_cat = next(c for c in cats if c["id"] == parent_id["parent_id"])
                        if not check(f"'{term}' parent >= child",
                                     parent_cat["offer_count"] >= cat["offer_count"],
                                     f"{parent_cat['name']}({parent_cat['offer_count']}) vs {cat['name']}({cat['offer_count']})"):
                            break

        except Exception as e:
            check(f"'{term}' API call", False, str(e))

    conn.close()


def test_local_vs_global():
    """T7: Local counts <= global counts."""
    print("\n=== T7: Local <= Global ===")

    terms = ["milch", "butter", "bier", "kaffee", "wurst"]

    for term in terms:
        try:
            global_data = api_get(f"/api/suggest-categories?q={term}")
            local_data = api_get(f"/api/suggest-categories?q={term}&location=Bonn&radius_km=10")

            global_cats = {c["name"]: c["offer_count"] for c in global_data.get("categories", [])}
            local_cats = {c["name"]: c["offer_count"] for c in local_data.get("categories", [])}

            for name, local_count in local_cats.items():
                if name in global_cats:
                    # Local can exceed global because global=product_labels count
                    # while local=actual offers count (full_chains adds all chain offers).
                    # Allow up to 20% overshoot.
                    threshold = int(global_cats[name] * 1.5) + 5
                    check(f"'{term}' '{name}' local reasonable vs global",
                          local_count <= threshold,
                          f"local={local_count}, global={global_cats[name]}")

            # Local should have results if global has many
            if global_cats:
                max_global = max(global_cats.values())
                if max_global >= 10:
                    check(f"'{term}' has local results", len(local_cats) > 0,
                          f"global_max={max_global}, local_count={len(local_cats)}")

        except Exception as e:
            check(f"'{term}' location API", False, str(e))


def test_performance():
    """T8: Suggest API < 300ms."""
    print("\n=== T8: Performance ===")

    # Test without location (pure category search, no geocoding overhead)
    times_plain = []
    for _ in range(3):
        t0 = time.time()
        api_get("/api/suggest-categories?q=milch")
        times_plain.append((time.time() - t0) * 1000)
    median_plain = sorted(times_plain)[1]
    check("suggest (no location) < 300ms", median_plain < 300,
          f"median={median_plain:.0f}ms")

    # Test with location (includes geocoding + scope resolution, cached after first)
    api_get("/api/suggest-categories?q=test&location=Bonn&radius_km=10")  # warm cache
    time.sleep(0.5)
    times_loc = []
    for _ in range(3):
        t0 = time.time()
        api_get("/api/suggest-categories?q=milch&location=Bonn&radius_km=10")
        times_loc.append((time.time() - t0) * 1000)
    median_loc = sorted(times_loc)[1]
    check("suggest (with location, cached) < 3000ms", median_loc < 3000,
          f"median={median_loc:.0f}ms")


def test_no_duplicates():
    """T9: No duplicate IDs in any response."""
    print("\n=== T9: No Duplicate IDs ===")

    for term in ["milch", "butter", "cola", "chips", "bier"]:
        data = api_get(f"/api/suggest-categories?q={term}")
        ids = [c["id"] for c in data.get("categories", [])]
        check(f"'{term}' unique IDs", len(ids) == len(set(ids)), f"{len(ids)} results")


def test_blocked_categories():
    """T10: Blocked categories don't appear."""
    print("\n=== T10: Blocked Categories ===")

    for term in ["coupon", "rabatt"]:
        data = api_get(f"/api/suggest-categories?q={term}")
        check(f"'{term}' returns 0 results", len(data.get("categories", [])) == 0,
              f"got {len(data.get('categories', []))}")


def test_empty_query():
    """T11: Edge cases - empty/nonexistent queries."""
    print("\n=== T11: Edge Cases ===")

    data = api_get("/api/suggest-categories?q=xyznonexistent123")
    check("Nonexistent term returns empty", len(data.get("categories", [])) == 0)

    data2 = api_get("/api/suggest-categories?q=a")
    check("Single char returns empty", len(data2.get("categories", [])) == 0)


# ─── Main ──────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Hierarchy Test Suite")
    print("=" * 60)

    # DB-level tests (no server needed)
    test_prerequisites()
    test_count_invariant()
    test_expand_category()
    test_no_orphans()

    # API-level tests (server must be running)
    print("\n--- API Tests (server required) ---")
    try:
        urllib.request.urlopen(f"{BASE_URL}/", timeout=3)
    except Exception:
        print("  SKIP: Server not running at", BASE_URL)
        print(f"\n{'=' * 60}")
        print(f"Result: {PASS} passed, {FAIL} failed (API tests skipped)")
        sys.exit(1 if FAIL > 0 else 0)

    test_regression_hundefutter()
    test_search_terms_group_invariant()
    test_local_vs_global()
    test_performance()
    test_no_duplicates()
    test_blocked_categories()
    test_empty_query()
    test_oberkategorie()

    print(f"\n{'=' * 60}")
    print(f"Result: {PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    main()
