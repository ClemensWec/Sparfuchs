"""
Validiert search_labels via OpenAI Batch API.
Schickt alle Produkte mit ihren Labels an GPT und fragt,
welche Labels nicht zum Produkt passen (halluziniert/Bullshit).

Ablauf:
  1. Export: Produkte + Labels → JSONL batch requests
  2. Upload + Batch starten
  3. Pollt Status bis fertig
  4. Ergebnisse runterladen + parsen
  5. Bad labels als JSON speichern (zum späteren Löschen)

Usage:
  python scripts/validate_labels_openai.py export     # Step 1: create JSONL
  python scripts/validate_labels_openai.py submit      # Step 2+3: upload & start batch
  python scripts/validate_labels_openai.py poll         # Step 4: check status & download
  python scripts/validate_labels_openai.py apply        # Step 5: delete bad labels from DB
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
DATA_DIR = PROJECT_DIR / "data" / "label_validation"
JSONL_FILE = DATA_DIR / "batch_input.jsonl"
BATCH_ID_FILE = DATA_DIR / "batch_id.txt"
RESULTS_FILE = DATA_DIR / "batch_results.jsonl"
BAD_LABELS_FILE = DATA_DIR / "bad_labels.json"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    sys.exit("OPENAI_API_KEY environment variable not set")

BATCH_SIZE = 40  # products per request
MODEL = "gpt-4.1-mini"

SYSTEM_PROMPT = """Du bist ein Qualitätsprüfer für Supermarkt-Produktdaten.

Für jedes Produkt bekommst du den Produktnamen und eine Liste von Suchbegriffen (Labels).
Deine Aufgabe: Identifiziere Labels die NICHT zum Produkt passen.

Ein Label ist SCHLECHT wenn:
- Es ein komplett anderes Produkt beschreibt (z.B. "cornflakes pralinen" für Schoko-Pralinen)
- Es eine falsche Marke zuordnet (z.B. "haribo" für ein Nestlé-Produkt)
- Es eine falsche Kategorie zuordnet (z.B. "spirituosen" für Sprite-Limonade)
- Es sinnlose Wort-Fragmente sind (z.B. nur "waren", "süß", "frucht" als einzelnes Wort)
- Es halluzinierte Zusammensetzungen sind (z.B. "cornflakes alternative" für Müsli)
- Es irreführende Assoziationen sind (z.B. "eisenbahn" für Eis/Eiscreme)

Ein Label ist OK wenn:
- Es den Produktnamen oder Teile davon enthält
- Es eine korrekte Marke ist (auch wenn nicht im Namen)
- Es ein Synonym oder umgangssprachlicher Begriff ist
- Es eine korrekte Oberkategorie ist (z.B. "süßwaren" für Schokolade)
- Es einen typischen Suchbegriff beschreibt, den ein Kunde eingeben würde

Antworte NUR mit einem JSON-Array. Für jedes Produkt:
{
  "i": <index>,
  "bad": ["label1", "label2"]
}

Wenn ALLE Labels OK sind, gib ein leeres Array: {"i": 0, "bad": []}
Sei streng aber fair — lieber ein fragwürdiges Label entfernen als behalten."""


def get_client() -> OpenAI:
    return OpenAI(api_key=OPENAI_API_KEY)


def export_jsonl() -> None:
    """Step 1: Read all products+labels from DB, write batch JSONL."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get all products with their labels grouped
    rows = conn.execute("""
        SELECT product_name, GROUP_CONCAT(label, '||') AS labels
        FROM search_labels
        GROUP BY product_name
        ORDER BY product_name
    """).fetchall()
    conn.close()

    products = []
    for row in rows:
        name = row["product_name"]
        labels = [lb.strip() for lb in str(row["labels"]).split("||") if lb.strip()]
        # Deduplicate labels preserving order
        seen = set()
        unique_labels = []
        for lb in labels:
            if lb not in seen:
                seen.add(lb)
                unique_labels.append(lb)
        products.append({"name": name, "labels": unique_labels})

    print(f"Loaded {len(products)} products with labels")

    # Create batched requests
    request_count = 0
    with open(JSONL_FILE, "w", encoding="utf-8") as f:
        for batch_start in range(0, len(products), BATCH_SIZE):
            batch = products[batch_start : batch_start + BATCH_SIZE]
            # Build user message
            lines = []
            for idx, prod in enumerate(batch):
                labels_str = ", ".join(prod["labels"])
                lines.append(f'{idx}. "{prod["name"]}": [{labels_str}]')

            user_msg = "\n".join(lines)

            request = {
                "custom_id": f"batch-{batch_start}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "temperature": 0.0,
                    "max_tokens": 4096,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_msg},
                    ],
                },
            }
            f.write(json.dumps(request, ensure_ascii=False) + "\n")
            request_count += 1

    print(f"Wrote {request_count} batch requests to {JSONL_FILE}")
    print(f"Estimated tokens: ~{len(products) * 18 * 3} input + output")


def submit_batch() -> None:
    """Step 2+3: Upload JSONL and start batch."""
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
        metadata={"description": "Sparfuchs label validation"},
    )
    print(f"Batch ID: {batch.id}")
    print(f"Status: {batch.status}")

    BATCH_ID_FILE.write_text(batch.id, encoding="utf-8")
    print(f"Saved batch ID to {BATCH_ID_FILE}")


def poll_batch() -> None:
    """Step 4: Poll batch status, download results when done."""
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
            print("\nBatch completed! Downloading results...")
            output_file_id = batch.output_file_id
            content = client.files.content(output_file_id)
            RESULTS_FILE.write_bytes(content.read())
            print(f"Results saved to {RESULTS_FILE}")
            _parse_results()
            return

        if batch.status in ("failed", "expired", "cancelled"):
            print(f"\nBatch {batch.status}!")
            if batch.errors:
                for err in batch.errors.data:
                    print(f"  Error: {err.message}")
            return

        print("Waiting 30s...")
        time.sleep(30)


def _parse_results() -> None:
    """Parse batch results and extract bad labels."""
    if not RESULTS_FILE.exists():
        print("ERROR: No results file found!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Rebuild product list in same order as export
    rows = conn.execute("""
        SELECT product_name, GROUP_CONCAT(label, '||') AS labels
        FROM search_labels
        GROUP BY product_name
        ORDER BY product_name
    """).fetchall()
    conn.close()

    products = []
    for row in rows:
        name = row["product_name"]
        labels = [lb.strip() for lb in str(row["labels"]).split("||") if lb.strip()]
        seen = set()
        unique_labels = []
        for lb in labels:
            if lb not in seen:
                seen.add(lb)
                unique_labels.append(lb)
        products.append({"name": name, "labels": unique_labels})

    all_bad: list[dict] = []
    total_bad = 0

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            result = json.loads(line)
            custom_id = result["custom_id"]
            batch_start = int(custom_id.split("-")[1])

            response = result.get("response", {})
            if response.get("status_code") != 200:
                print(f"WARNING: {custom_id} failed with status {response.get('status_code')}")
                continue

            body = response.get("body", {})
            choices = body.get("choices", [])
            if not choices:
                continue

            content = choices[0].get("message", {}).get("content", "")
            # Parse JSON from response (handle markdown code blocks)
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1] if "\n" in content else content[3:]
                if content.endswith("```"):
                    content = content[:-3]
                content = content.strip()

            try:
                items = json.loads(content)
            except json.JSONDecodeError:
                print(f"WARNING: Could not parse JSON from {custom_id}: {content[:100]}")
                continue

            for item in items:
                idx = item.get("i", 0)
                bad_labels = item.get("bad", [])
                if bad_labels:
                    product_idx = batch_start + idx
                    if product_idx < len(products):
                        product = products[product_idx]
                        all_bad.append({
                            "product_name": product["name"],
                            "bad_labels": bad_labels,
                            "all_labels": product["labels"],
                        })
                        total_bad += len(bad_labels)

    # Save results
    with open(BAD_LABELS_FILE, "w", encoding="utf-8") as f:
        json.dump(all_bad, f, ensure_ascii=False, indent=2)

    print(f"\nFound {total_bad} bad labels across {len(all_bad)} products")
    print(f"Saved to {BAD_LABELS_FILE}")

    # Show some examples
    print("\n--- Examples ---")
    for entry in all_bad[:20]:
        print(f"  {entry['product_name']}: {entry['bad_labels']}")


def apply_cleanup() -> None:
    """Step 5: Remove bad labels from DB."""
    if not BAD_LABELS_FILE.exists():
        print("ERROR: Run 'poll' first to get results!")
        sys.exit(1)

    with open(BAD_LABELS_FILE, "r", encoding="utf-8") as f:
        bad_entries = json.load(f)

    if not bad_entries:
        print("No bad labels to remove!")
        return

    total_to_remove = sum(len(e["bad_labels"]) for e in bad_entries)
    print(f"Will remove {total_to_remove} bad labels from {len(bad_entries)} products")

    conn = sqlite3.connect(DB_PATH)
    removed = 0
    for entry in bad_entries:
        product_name = entry["product_name"]
        for label in entry["bad_labels"]:
            cursor = conn.execute(
                "DELETE FROM search_labels WHERE product_name = ? AND label = ?",
                [product_name, label],
            )
            removed += cursor.rowcount

    conn.commit()
    remaining = conn.execute("SELECT COUNT(*) FROM search_labels").fetchone()[0]
    conn.close()

    print(f"Removed {removed} labels from DB")
    print(f"Remaining labels: {remaining}")
    print("\nDon't forget to rebuild FTS index if needed!")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python validate_labels_openai.py [export|submit|poll|apply]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    if cmd == "export":
        export_jsonl()
    elif cmd == "submit":
        submit_batch()
    elif cmd == "poll":
        poll_batch()
    elif cmd == "apply":
        apply_cleanup()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
