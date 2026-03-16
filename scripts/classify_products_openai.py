"""
Klassifiziert alle Produkte via OpenAI Batch API (gpt-5-mini).
Ablauf:
  1. Erstellt JSONL-Datei mit allen Requests
  2. Lädt sie hoch
  3. Startet Batch
  4. Pollt Status
  5. Lädt Ergebnisse herunter
  6. Parsed und speichert als all_classified.json
"""
import json, os, sys, time
from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    sys.exit("OPENAI_API_KEY environment variable not set")

client = OpenAI(api_key=OPENAI_API_KEY)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
PRODUCTS_FILE = os.path.join(DATA_DIR, "all_products.json")
JSONL_FILE = os.path.join(DATA_DIR, "batch_input.jsonl")
OUTPUT_FILE = os.path.join(DATA_DIR, "classifications", "all_classified.json")
BATCH_ID_FILE = os.path.join(DATA_DIR, "batch_id.txt")

BATCH_SIZE = 50  # products per request
MODEL = "gpt-5-mini"

SYSTEM_PROMPT = """Du klassifizierst Supermarkt-Produkte. Antworte NUR mit einem JSON-Array.

Für jedes Produkt gibst du zurück:
{
  "i": <index>,
  "ok": "<Oberkategorie>",
  "uk": "<Unterkategorie>",
  "s": ["suchbegriff1", "suchbegriff2", ...],
  "m": "<Marke>" oder null,
  "g": "<Gattungsbegriff>" oder null,
  "f": true/false
}

Felder:
- i: Index des Produkts (wie im Input)
- ok: Oberkategorie aus: Alkohol & Getränke, Backen & Mehl, Backwaren & Brot, Bekleidung & Textilien, Drogerie & Gesundheit, Elektronik & Technik, Fertiggerichte & Suppen, Fisch & Meeresfrüchte, Fleisch & Wurst, Frühstück & Cerealien, Garten & Outdoor, Getränke, Haushalt & Küche, Haushalt & Reinigung, Milchprodukte & Käse, Obst & Gemüse, Öle Essig & Gewürze, Spielzeug & Freizeit, Süßwaren & Snacks, Tiefkühlprodukte, Tierbedarf, Werkzeug & Baumarkt, Aktionen & Promotions, Blumen & Pflanzen, Sonstiges
- uk: Spezifische Unterkategorie (z.B. "Bier", "Wurstwaren", "Kaffeepads")
- s: MINDESTENS 10 Suchbegriffe (kleingeschrieben) die ein Kunde eingeben würde. Vom Generischen zum Spezifischen. Synonyme, Verwendungszwecke, umgangssprachliche Begriffe einbeziehen.
- m: Markenname oder null
- g: Falls die Marke umgangssprachlich als Gattungsbegriff verwendet wird, den generischen Begriff angeben. Beispiele: Tempo→Taschentücher, Tesa→Klebeband, Nutella→Nuss-Nougat-Creme, UHU→Kleber, Zewa→Küchenrolle, Pampers→Windeln, Labello→Lippenpflege, Ohropax→Ohrstöpsel, Edding→Permanentmarker, Tesafilm→Klebefilm, Maggi→Würzsauce, Aspirin→Kopfschmerztablette. Wenn die Marke NICHT als Gattungsbegriff bekannt ist: null
- f: ist_lebensmittel (true/false)

Regeln:
- PAYBACK/Coupons/Rabatt-Einträge → ok:"Aktionen & Promotions", uk:"Coupon & Rabatt", kurze suchbegriffe, f:false
- Suchbegriffe IMMER kleingeschrieben, KEINE Mengenangaben/Gewichte als Suchbegriff
- Suchbegriffe sollen generisch sein (was ein Kunde suchen würde), nicht produktspezifisch. Z.B. "milch", "vollmilch", "trinkmilch", "frischmilch" — NICHT "1l" oder "3,5%"
- Antworte NUR mit einem JSON-Objekt: {"products": [...]}  wobei [...] das Array aller klassifizierten Produkte ist"""


def build_user_prompt(products, start_idx):
    lines = []
    for j, p in enumerate(products):
        idx = start_idx + j
        name = p["n"]
        desc = p["d"] if p["d"] else ""
        brand = p["b"] if p["b"] else ""
        parts = [f"[{idx}] {name}"]
        if desc:
            parts.append(f"Beschreibung: {desc}")
        if brand:
            parts.append(f"Marke: {brand}")
        lines.append(" | ".join(parts))
    return "Klassifiziere diese Produkte:\n\n" + "\n".join(lines)


def create_jsonl():
    """Erstellt die JSONL-Datei mit allen Batch-Requests."""
    with open(PRODUCTS_FILE, encoding="utf-8") as f:
        products = json.load(f)

    print(f"Produkte geladen: {len(products)}")

    with open(JSONL_FILE, "w", encoding="utf-8") as out:
        batch_num = 0
        for i in range(0, len(products), BATCH_SIZE):
            batch = products[i:i + BATCH_SIZE]
            user_prompt = build_user_prompt(batch, i)

            request = {
                "custom_id": f"batch_{batch_num:03d}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt}
                    ],
                    "max_completion_tokens": 30000,
                    "response_format": {"type": "json_object"}
                }
            }
            out.write(json.dumps(request, ensure_ascii=False) + "\n")
            batch_num += 1

    print(f"JSONL erstellt: {batch_num} Requests in {JSONL_FILE}")
    return batch_num


def upload_and_start_batch():
    """Lädt JSONL hoch und startet den Batch."""
    print("Lade Datei hoch...")
    with open(JSONL_FILE, "rb") as f:
        file_obj = client.files.create(file=f, purpose="batch")
    print(f"Datei hochgeladen: {file_obj.id}")

    print("Starte Batch...")
    batch = client.batches.create(
        input_file_id=file_obj.id,
        endpoint="/v1/chat/completions",
        completion_window="24h"
    )
    print(f"Batch gestartet: {batch.id}")

    with open(BATCH_ID_FILE, "w") as f:
        f.write(batch.id)

    return batch.id


def poll_batch(batch_id):
    """Pollt den Batch-Status bis fertig."""
    print(f"\nWarte auf Batch {batch_id}...")
    while True:
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        completed = batch.request_counts.completed if batch.request_counts else 0
        total = batch.request_counts.total if batch.request_counts else 0
        failed = batch.request_counts.failed if batch.request_counts else 0

        print(f"  Status: {status} | {completed}/{total} fertig | {failed} Fehler", flush=True)

        if status == "completed":
            return batch
        elif status in ("failed", "expired", "cancelled"):
            print(f"FEHLER: Batch {status}")
            if batch.errors:
                for err in batch.errors.data:
                    print(f"  {err.code}: {err.message}")
            sys.exit(1)

        time.sleep(30)


def download_results(batch):
    """Lädt Ergebnisse herunter und parsed sie."""
    output_file_id = batch.output_file_id
    if not output_file_id:
        print("Kein Output-File vorhanden!")
        sys.exit(1)

    print(f"Lade Ergebnisse herunter: {output_file_id}")
    content = client.files.content(output_file_id)

    all_results = []
    errors = []

    for line in content.text.strip().split("\n"):
        entry = json.loads(line)
        custom_id = entry["custom_id"]
        response = entry.get("response", {})

        if response.get("status_code") != 200:
            errors.append(f"{custom_id}: HTTP {response.get('status_code')}")
            continue

        body = response.get("body", {})
        choices = body.get("choices", [])
        if not choices:
            errors.append(f"{custom_id}: Keine Choices")
            continue

        content_str = choices[0].get("message", {}).get("content", "")
        try:
            parsed = json.loads(content_str)
            items = []
            # Handle: {"products": [...]} or {"results": [...]} or just [...]
            if isinstance(parsed, list):
                items = parsed
            elif isinstance(parsed, dict):
                # Try known keys first
                for key in ("products", "results", "data", "items"):
                    if key in parsed and isinstance(parsed[key], list):
                        items = parsed[key]
                        break
                if not items:
                    # Try any list value
                    for v in parsed.values():
                        if isinstance(v, list) and len(v) > 0:
                            items = v
                            break
                if not items:
                    # Try numbered keys: {"0": {...}, "1": {...}, ...}
                    for k, v in parsed.items():
                        if isinstance(v, dict) and "i" in v:
                            items.append(v)

            for item in items:
                if isinstance(item, dict) and "i" in item:
                    all_results.append(item)
        except json.JSONDecodeError as e:
            errors.append(f"{custom_id}: JSON-Fehler: {e}")

    # Sort by index
    all_results.sort(key=lambda x: x.get("i", 0))

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\nFERTIG: {len(all_results)} Produkte klassifiziert -> {OUTPUT_FILE}")
    if errors:
        print(f"\n{len(errors)} Fehler:")
        for e in errors:
            print(f"  {e}")

    return all_results


def main():
    # Resume support: check if batch ID exists
    if os.path.exists(BATCH_ID_FILE):
        with open(BATCH_ID_FILE) as f:
            batch_id = f.read().strip()
        print(f"Bestehende Batch-ID gefunden: {batch_id}")

        batch = client.batches.retrieve(batch_id)
        if batch.status == "completed":
            print("Batch bereits fertig! Lade Ergebnisse...")
            download_results(batch)
            return
        elif batch.status in ("validating", "in_progress", "finalizing"):
            print(f"Batch laeuft noch (Status: {batch.status}), warte...")
            batch = poll_batch(batch_id)
            download_results(batch)
            return
        else:
            print(f"Batch hat Status {batch.status}, starte neu...")

    # Step 1: Create JSONL
    num_batches = create_jsonl()

    # Step 2: Upload & start
    batch_id = upload_and_start_batch()

    # Step 3: Poll
    batch = poll_batch(batch_id)

    # Step 4: Download
    download_results(batch)


if __name__ == "__main__":
    main()
