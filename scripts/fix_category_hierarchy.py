"""
Fix category hierarchy via OpenAI: create 3-level structure.

Current state:
  Level 1: 25 Oberkategorien (Milchprodukte & Käse, Fleisch & Wurst, ...)
  Level 2: 3,458 flat categories (all siblings)

Target:
  Level 1: Oberkategorien (unchanged)
  Level 2: Gruppen (new, e.g. "Milch", "Butter", "Joghurt")
  Level 3: Spezifisch (moved from old level-2, e.g. "Haltbare Milch", "Vollmilch")

OpenAI tasks per Oberkategorie:
  1. Group related categories into logical clusters
  2. Identify duplicates to merge (H-milch + Haltbare Milch)
  3. Assign singleton categories to appropriate groups

Usage:
  python scripts/fix_category_hierarchy.py export    # JSONL for OpenAI batch
  python scripts/fix_category_hierarchy.py submit    # Upload + start batch
  python scripts/fix_category_hierarchy.py poll      # Check status + download
  python scripts/fix_category_hierarchy.py apply     # Apply hierarchy to DB
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

from openai import OpenAI

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DB_PATH = PROJECT_DIR / "data" / "kaufda_dataset" / "offers.sqlite3"
DATA_DIR = PROJECT_DIR / "data" / "hierarchy_fix"
JSONL_FILE = DATA_DIR / "batch_input.jsonl"
BATCH_ID_FILE = DATA_DIR / "batch_id.txt"
RESULTS_FILE = DATA_DIR / "batch_results.jsonl"
GROUPING_FILE = DATA_DIR / "groupings.json"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    sys.exit("OPENAI_API_KEY environment variable not set")

MODEL = "gpt-4.1-mini"

SYSTEM_PROMPT = """Du bist ein Experte für Supermarkt-Produktkategorisierung.

Du bekommst eine Oberkategorie und alle ihre Unterkategorien mit Angebotsanzahlen.
Deine Aufgabe: Gruppiere die Unterkategorien in logische Zwischengruppen.

Regeln:
1. Jede Gruppe braucht einen klaren, kurzen deutschen Namen (z.B. "Milch", "Käse", "Joghurt")
2. JEDE Unterkategorie muss genau einer Gruppe zugeordnet werden
3. Identifiziere Duplikate: Kategorien die dasselbe meinen (z.B. "H-milch" und "Haltbare Milch", "Schokoladenmilch" und "Schokomilch")
4. Gruppen sollten 3-30 Mitglieder haben. Keine Gruppe mit nur 1 Mitglied (außer die Kategorie ist wirklich einzigartig)
5. Sehr kleine Kategorien (1-2 Angebote) sollten in passende größere Gruppen integriert werden
6. Der Gruppenname kann gleich sein wie eine der enthaltenen Unterkategorien (z.B. Gruppe "Milch" enthält "Milch", "Haltbare Milch", etc.)
7. Wenn eine Unterkategorie zu breit/generisch ist und besser als Gruppenname passt, nutze sie als Gruppennamen
8. Maximal 15-20 Gruppen pro Oberkategorie

Antworte NUR mit JSON:
[
  {
    "group": "Gruppenname",
    "children": ["Unterkategorie1", "Unterkategorie2", ...],
    "merge": [["Duplikat1", "Zielname"], ["Duplikat2", "Zielname"]]
  }
]

"merge" ist optional — nur wenn es echte Duplikate gibt.
Bei merge: erstes Element wird in zweites gemergt (erstes wird gelöscht, Angebote gehen zu zweitem)."""


def get_client() -> OpenAI:
    return OpenAI(api_key=OPENAI_API_KEY)


def export_jsonl() -> None:
    """Export categories grouped by Oberkategorie for OpenAI batch."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get all level-1 categories
    level1_rows = conn.execute(
        "SELECT id, name FROM categories_v2 WHERE level = 1 ORDER BY name"
    ).fetchall()

    requests = []
    for l1 in level1_rows:
        # Get all level-2 children
        children = conn.execute(
            """
            SELECT name, product_count
            FROM categories_v2
            WHERE parent_id = ? AND level = 2
            ORDER BY product_count DESC, name ASC
            """,
            (l1["id"],),
        ).fetchall()

        if not children:
            continue

        # Build user message
        lines = [f'Oberkategorie: {l1["name"]}', f"Unterkategorien ({len(children)}):"]
        for c in children:
            lines.append(f'  {c["name"]} ({c["product_count"]})')

        request = {
            "custom_id": f"ober-{l1['id']}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": MODEL,
                "temperature": 0.0,
                "max_tokens": 8192,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": "\n".join(lines)},
                ],
            },
        }
        requests.append(request)

    conn.close()

    with open(JSONL_FILE, "w", encoding="utf-8") as f:
        for req in requests:
            f.write(json.dumps(req, ensure_ascii=False) + "\n")

    print(f"Wrote {len(requests)} requests to {JSONL_FILE}")


def submit_batch() -> None:
    """Upload JSONL and start batch."""
    if not JSONL_FILE.exists():
        print("ERROR: Run 'export' first!")
        sys.exit(1)

    client = get_client()

    print("Uploading JSONL file...")
    with open(JSONL_FILE, "rb") as f:
        uploaded = client.files.create(file=f, purpose="batch")
    print(f"Uploaded: {uploaded.id}")

    print("Starting batch...")
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "Sparfuchs category hierarchy fix"},
    )
    print(f"Batch ID: {batch.id}")
    print(f"Status: {batch.status}")

    BATCH_ID_FILE.write_text(batch.id, encoding="utf-8")


def poll_batch() -> None:
    """Poll batch status, download results when done."""
    if not BATCH_ID_FILE.exists():
        print("ERROR: Run 'submit' first!")
        sys.exit(1)

    batch_id = BATCH_ID_FILE.read_text(encoding="utf-8").strip()
    client = get_client()

    while True:
        batch = client.batches.retrieve(batch_id)
        print(
            f"Status: {batch.status} | "
            f"completed: {batch.request_counts.completed}/{batch.request_counts.total} | "
            f"failed: {batch.request_counts.failed}"
        )

        if batch.status == "completed":
            print("\nDownloading results...")
            content = client.files.content(batch.output_file_id)
            RESULTS_FILE.write_bytes(content.read())
            print(f"Saved to {RESULTS_FILE}")
            _parse_results()
            return

        if batch.status in ("failed", "expired", "cancelled"):
            print(f"\nBatch {batch.status}!")
            if batch.errors:
                for err in batch.errors.data:
                    print(f"  Error: {err.message}")
            return

        print("Waiting 15s...")
        time.sleep(15)


def _parse_results() -> None:
    """Parse batch results into groupings.json."""
    if not RESULTS_FILE.exists():
        print("No results file!")
        return

    all_groupings: dict[str, list] = {}

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            result = json.loads(line)
            custom_id = result["custom_id"]
            ober_id = int(custom_id.split("-")[1])

            response = result.get("response", {})
            if response.get("status_code") != 200:
                print(f"WARNING: {custom_id} failed")
                continue

            content = response["body"]["choices"][0]["message"]["content"]
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1] if "\n" in content else content[3:]
                if content.endswith("```"):
                    content = content[:-3]
                content = content.strip()

            try:
                groups = json.loads(content)
                all_groupings[str(ober_id)] = groups
                total_children = sum(len(g["children"]) for g in groups)
                print(f"  Ober {ober_id}: {len(groups)} groups, {total_children} children")
            except json.JSONDecodeError:
                print(f"WARNING: Could not parse JSON for {custom_id}")
                print(f"  Content: {content[:200]}")

    with open(GROUPING_FILE, "w", encoding="utf-8") as f:
        json.dump(all_groupings, f, ensure_ascii=False, indent=2)

    print(f"\nSaved groupings to {GROUPING_FILE}")


def apply_hierarchy() -> None:
    """Apply groupings to DB: create level-2 groups, move old level-2 to level-3."""
    if not GROUPING_FILE.exists():
        print("ERROR: Run 'poll' first!")
        sys.exit(1)

    with open(GROUPING_FILE, "r", encoding="utf-8") as f:
        all_groupings = json.load(f)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Build lookup: category name (lowered) → id for existing level-2 categories
    existing = {}
    for row in conn.execute("SELECT id, name, parent_id, level FROM categories_v2 WHERE level = 2"):
        existing[row["name"].lower().strip()] = {
            "id": row["id"],
            "name": row["name"],
            "parent_id": row["parent_id"],
        }

    stats = {"groups_created": 0, "moved_to_l3": 0, "merges": 0, "unmatched": [], "orphans": 0}

    # Track which category IDs have been moved to level-3 or used as group
    moved_ids: set[int] = set()
    group_ids: set[int] = set()  # IDs used as level-2 groups

    from app.utils.text import normalize_search_text

    for ober_id_str, groups in all_groupings.items():
        ober_id = int(ober_id_str)

        for group in groups:
            group_name = group["group"]
            children_names = group.get("children", [])
            merges = group.get("merge", [])

            # Step 1: Process merges first
            for merge_pair in merges:
                if len(merge_pair) != 2:
                    continue
                source_name, target_name = merge_pair
                source = existing.get(source_name.lower().strip())
                target = existing.get(target_name.lower().strip())
                if source and target and source["id"] != target["id"]:
                    conn.execute(
                        "UPDATE product_labels SET category_v2_id = ? WHERE category_v2_id = ?",
                        (target["id"], source["id"]),
                    )
                    conn.execute(
                        "UPDATE offers SET category_id = ? WHERE category_id = ?",
                        (target["id"], source["id"]),
                    )
                    conn.execute("DELETE FROM categories_v2 WHERE id = ?", (source["id"],))
                    moved_ids.add(source["id"])
                    del existing[source_name.lower().strip()]
                    stats["merges"] += 1

            # Step 2: Check if group name matches an existing category
            group_existing = existing.get(group_name.lower().strip())

            if group_existing:
                new_group_id = group_existing["id"]
                conn.execute(
                    "UPDATE categories_v2 SET level = 2, parent_id = ? WHERE id = ?",
                    (ober_id, new_group_id),
                )
                group_ids.add(new_group_id)
            else:
                name_norm = normalize_search_text(group_name)
                # Check if a category with this normalized name already exists under same parent
                existing_norm = conn.execute(
                    "SELECT id FROM categories_v2 WHERE name_normalized = ? AND level = 2 AND parent_id = ?",
                    (name_norm, ober_id),
                ).fetchone()
                if existing_norm:
                    new_group_id = existing_norm["id"]
                    group_ids.add(new_group_id)
                else:
                    cursor = conn.execute(
                        "INSERT INTO categories_v2 (name, name_normalized, level, parent_id, product_count) VALUES (?, ?, 2, ?, 0)",
                        (group_name, name_norm, ober_id),
                    )
                    new_group_id = cursor.lastrowid
                    group_ids.add(new_group_id)
                    stats["groups_created"] += 1

            # Step 3: Move children to level-3 under new group
            for child_name in children_names:
                child = existing.get(child_name.lower().strip())
                if child and child["id"] != new_group_id:
                    conn.execute(
                        "UPDATE categories_v2 SET level = 3, parent_id = ? WHERE id = ?",
                        (new_group_id, child["id"]),
                    )
                    moved_ids.add(child["id"])
                    stats["moved_to_l3"] += 1
                elif not child and child_name.lower().strip() != group_name.lower().strip():
                    stats["unmatched"].append(child_name)

    # Step 3b: Handle orphaned level-2 categories (not assigned to any group)
    # Create a "Sonstiges" group per Oberkategorie for unassigned categories
    for ober_id_str in all_groupings.keys():
        ober_id = int(ober_id_str)
        orphans = conn.execute(
            "SELECT id FROM categories_v2 WHERE parent_id = ? AND level = 2 AND id NOT IN ({})".format(
                ",".join(str(gid) for gid in group_ids) if group_ids else "0"
            ),
            (ober_id,),
        ).fetchall()
        if not orphans:
            continue

        # Create or reuse "Sonstiges" group for this Oberkategorie
        name_norm = normalize_search_text("Sonstiges")
        existing_sonstiges = conn.execute(
            "SELECT id FROM categories_v2 WHERE name_normalized = ? AND level = 2 AND parent_id = ?",
            (name_norm, ober_id),
        ).fetchone()
        if existing_sonstiges:
            sonstiges_id = existing_sonstiges["id"]
        else:
            cursor = conn.execute(
                "INSERT INTO categories_v2 (name, name_normalized, level, parent_id, product_count) VALUES (?, ?, 2, ?, 0)",
                ("Sonstiges", name_norm, ober_id),
            )
            sonstiges_id = cursor.lastrowid
            stats["groups_created"] += 1
        group_ids.add(sonstiges_id)
        stats["groups_created"] += 1

        for orphan in orphans:
            conn.execute(
                "UPDATE categories_v2 SET level = 3, parent_id = ? WHERE id = ?",
                (sonstiges_id, orphan["id"]),
            )
            stats["orphans"] += 1

    # Step 4: Recalculate product_count for new level-2 groups
    # Level-2 count = sum of all level-3 children counts
    conn.execute("""
        UPDATE categories_v2
        SET product_count = (
            SELECT COALESCE(SUM(c.product_count), 0)
            FROM categories_v2 c
            WHERE c.parent_id = categories_v2.id AND c.level = 3
        ) + categories_v2.product_count
        WHERE level = 2
    """)

    # Step 5: Recalculate level-1 counts
    conn.execute("""
        UPDATE categories_v2
        SET product_count = (
            SELECT COALESCE(SUM(c.product_count), 0)
            FROM categories_v2 c
            WHERE c.parent_id = categories_v2.id
        )
        WHERE level = 1
    """)

    conn.commit()

    # Rebuild FTS
    try:
        conn.execute("DELETE FROM categories_fts")
        conn.execute("INSERT INTO categories_fts(categories_fts) VALUES('rebuild')")
        conn.commit()
        print("FTS index rebuilt")
    except Exception as e:
        print(f"FTS rebuild skipped: {e}")

    conn.close()

    print(f"\nDone!")
    print(f"  Groups created: {stats['groups_created']}")
    print(f"  Moved to level-3: {stats['moved_to_l3']}")
    print(f"  Merges: {stats['merges']}")
    print(f"  Orphans -> Sonstiges: {stats['orphans']}")
    if stats["unmatched"]:
        print(f"  Unmatched children (name mismatch): {len(stats['unmatched'])}")
        for name in stats["unmatched"][:20]:
            print(f"    - {name}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python fix_category_hierarchy.py [export|submit|poll|apply]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "export":
        export_jsonl()
    elif cmd == "submit":
        submit_batch()
    elif cmd == "poll":
        poll_batch()
    elif cmd == "apply":
        apply_hierarchy()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
