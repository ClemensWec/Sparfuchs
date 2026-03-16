"""
Importiert die LLM-Klassifikationen (all_classified.json) in die SQLite-DB.

Pipeline-Dokumentation (für spätere Live-Pipeline):
=====================================================
1. Neue Produkte sammeln (product_name + description_text + brand_name aus offers)
2. An OpenAI Batch API senden (gpt-5-mini, 50 Produkte pro Request)
   - Skript: scripts/classify_products_openai.py
   - Input: data/all_products.json
   - Output: data/classifications/all_classified.json
3. Dieses Skript: Importiert Klassifikationen in die DB
   - Erstellt categories_v2, product_labels, search_labels + FTS5
   - Mapped category_id in offers-Tabelle auf neue IDs
4. Such-API und Pricing nutzen die neuen Tabellen

Tabellen:
=========
- categories_v2: Hierarchische Kategorien (Ober- + Unterkategorie)
- product_labels: Pro Produkt: Kategorie-Zuordnung, Marke, Gattungsbegriff, ist_lebensmittel
- search_labels: N Suchbegriffe pro Produkt (für FTS-Suche)
- search_labels_fts: FTS5-Index auf search_labels

Ablauf:
=======
1. categories_v2 aufbauen aus den unique ok+uk Kombinationen
2. product_labels befüllen (1 Zeile pro unique product_name)
3. search_labels befüllen (N Zeilen pro Produkt)
4. FTS5-Index erstellen
5. offers.category_id auf neue category_v2.id remappen
"""
import json
import os
import sqlite3
import sys
import time

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DB_PATH = os.path.join(DATA_DIR, "kaufda_dataset", "offers.sqlite3")
CLASSIFIED_PATH = os.path.join(DATA_DIR, "classifications", "all_classified.json")
PRODUCTS_PATH = os.path.join(DATA_DIR, "all_products.json")


# Triviale UK-Duplikate zusammenführen
UK_MERGE_MAP = {
    # ── Singular/Plural ──
    "fertiggericht": "Fertiggerichte",
    "erfrischungsgetränk": "Erfrischungsgetränke",
    "brote": "Brot",
    "salate": "Salat",
    "steaks": "Steak",
    "fischfilets": "Fischfilet",
    "koteletts": "Kotelett",
    "suppe": "Suppen",
    "lachsfilets": "Lachsfilet",
    "pflanzendrinks": "Pflanzendrink",
    "baguettes": "Baguette",
    "frikadelle": "Frikadellen",
    "avocados": "Avocado",
    "hundesnacks": "Hundesnack",
    "biermischgetränke": "Biermischgetränk",
    "bratpfannen": "Bratpfanne",
    "desserts": "Dessert",
    "fleischspieße": "Fleischspieß",
    "kiwis": "Kiwi",
    "roulade": "Rouladen",
    "smartphones": "Smartphone",
    "wurstkonserven": "Wurstkonserve",
    "croissants": "Croissant",
    "haxen": "Haxe",
    "donuts": "Donut",
    "sneakers": "Sneaker",
    "beinscheiben": "Beinscheibe",
    "solarleuchten": "Solarleuchte",
    "speiseöle": "Speiseöl",
    "kartenspiele": "Kartenspiel",
    "nackensteaks": "Nackensteak",
    "fertigsuppen": "Fertigsuppe",
    "malzgetränke": "Malzgetränk",
    "pfannen": "Pfanne",
    "spirituose": "Spirituosen",
    "brotaufstriche": "Brotaufstrich",
    "aufstriche": "Aufstrich",
    "wasserfilterkartuschen": "Wasserfilterkartusche",
    "waffeln & kekse": "Waffeln",
    "liköre": "Likör",
    "topfpflanze": "Topfpflanzen",
    "topf-pflanzen": "Topfpflanzen",
    "energy drinks": "Energy Drink",
    "energy-drink": "Energy Drink",
    # ── Zusammenschreibung / Schreibvarianten ──
    "katzennassfutter": "Katzenfutter Nass",
    "hundenassfutter": "Hundefutter Nass",
    "hundefutter nassfutter": "Hundefutter Nass",
    "hunde nassfutter": "Hundefutter Nass",
    "instant-nudeln": "Instantnudeln",
    "instant nudeln": "Instantnudeln",
    "energydrink": "Energy Drink",
    "tafel schokolade": "Tafelschokolade",
    "cola mix": "Cola-Mix",
    "cola-mix": "Cola-Mix",
    "cordon-bleu": "Cordon Bleu",
    "wc reiniger": "WC-Reiniger",
    "wc-reiniger": "WC-Reiniger",
    "käse-snack": "Käsesnack",
    "kaffee bohnen": "Kaffeebohnen",
    "kaffepads": "Kaffeepads",
    "entrecote": "Entrecôte",
    "belagte brötchen": "Belegte Brötchen",
    "belegtes brötchen": "Belegte Brötchen",
    # ── Rechtschreibvarianten ──
    "vodka": "Wodka",
    "whiskey": "Whisky",
    "kola": "Cola",
    "frikandellen": "Frikadellen",
    # ── Synonyme ──
    "pasta": "Nudeln",
    "pasta & teigwaren": "Nudeln",
    "nudeln / teigwaren": "Nudeln",
    "nudeln & teigwaren": "Nudeln",
    "nudeln & pasta": "Nudeln",
    "teigwaren & pasta": "Nudeln",
    "teigwaren": "Nudeln",
    "pasta trocken": "Nudeln",
    "pasta / penne": "Nudeln",
    "eiscreme": "Speiseeis",
    "eis": "Speiseeis",
    "eis am stiel": "Speiseeis",
    "wurst": "Wurstwaren",
    "kekse & gebäck": "Kekse",
    "kekse & cookies": "Kekse",
    "kekse & biscuits": "Kekse",
    "kekse & biscotti": "Kekse",
    "kekse & knuspergebäck": "Kekse",
    "backwaren & kekse": "Kekse",
    "schokolade & pralinen": "Schokolade",
    "schokolade & riegel": "Schokolade",
    "schokolade & konfekt": "Schokolade",
    "kuchen & torten": "Kuchen",
    "kuchen & gebäck": "Kuchen",
    "kuchen & süßgebäck": "Kuchen",
    "tiefkühlkuchen & blechkuchen": "Tiefkühlkuchen",
    "garnelen & shrimps": "Garnelen",
    "garnelen & krustentiere": "Garnelen",
    "garnelen & scampi": "Garnelen",
    "nüsse & kerne": "Nüsse",
    "saucen & dips": "Saucen",
    "saucen & gewürze": "Saucen",
    "saucen & dressing": "Saucen",
    "riegel & waffeln": "Schokoriegel",
    "weine": "Wein",
    "wein rosé": "Roséwein",
    "wein rot": "Rotwein",
    "wein rotwein": "Rotwein",
    "wein weiß": "Weißwein",
    "wein (weiß/halbtrocken)": "Weißwein",
    "wein (rot/weiß)": "Wein",
    "weisswein": "Weißwein",
    "konfitüre & marmelade": "Konfitüre",
    "marmelade & konfitüre": "Konfitüre",
    "marmelade & aufstrich": "Konfitüre",
    "marmelade & aufstriche": "Konfitüre",
    "marmelade & gelee": "Konfitüre",
    "konfitüre & gelees": "Konfitüre",
    "konfitüre & brotaufstrich": "Konfitüre",
    "aufstrich & marmelade": "Konfitüre",
    "fruchtaufstrich": "Konfitüre",
    "desserts & pudding": "Dessert & Pudding",
    "desserts & puddings": "Dessert & Pudding",
    "pralinen & konfekt": "Pralinen",
    "konfekt & pralinen": "Pralinen",
    "pralinen & schokolade": "Pralinen",
    "fruchtgummis & gummibärchen": "Fruchtgummi",
    "fruchtgummi & weingummis": "Fruchtgummi",
    "fruchtgummi & lakritz": "Fruchtgummi",
    "fruchtgummi / geleebonbons": "Fruchtgummi",
    "lakritz & bonbons": "Lakritz",
    "lakritz & fruchtgummi": "Lakritz",
    "bonbons & dragees": "Bonbons",
    "bonbons & drops": "Bonbons",
    "bonbons & konfekt": "Bonbons",
    "bonbons & karamell": "Bonbons",
    "fleischkäse & leberkäse": "Fleischkäse",
    "fleischkäse / leberkäse": "Fleischkäse",
    "schinken & speck": "Schinken",
    "chips & snacks": "Chips",
    "chips & knabbersnacks": "Chips",
    "chips & salziges": "Chips",
    "chips & knabberartikel": "Chips",
    "chips & knabbereien": "Chips",
    "kartoffelsnacks & chips": "Chips",
    "säfte & nektare": "Saft",
    "säfte": "Saft",
    "fruchtsaft & nektar": "Saft",
    "saft & nektar": "Saft",
    "säfte & schorlen": "Saft",
    "sahne / sprühsahne": "Sahne",
    "sahne & creme": "Sahne",
    "sahne & schlagrahm": "Sahne",
    "sahne & kochsahne": "Sahne",
    "quark / topfen": "Quark",
    "quark oder skyr": "Quark",
    "quark & quarkzubereitung": "Quark",
    "quark & frischkäse": "Quark",
    "butter & streichfett": "Butter",
    "butter / streichfett": "Butter",
    "butter & streichfette": "Butter",
    "butter & kräuterbutter": "Butter",
    "butter & fett": "Butter",
    "milch & milchgetränke": "Milch",
    "desserts & joghurts": "Joghurt",
    "duschgel & shampoo": "Shampoo",
    "shampoo & spülung": "Shampoo",
    "eis & desserts": "Speiseeis",
    "speiseeis & eisbeutel": "Speiseeis",
    "sekt & prosecco": "Sekt",
    "sekt & schaumwein": "Sekt",
    "bier / biermischgetränk": "Bier",
    "likör & spirituosen": "Spirituosen",
    "liköre & spirituosen": "Spirituosen",
    "deodorant-spray": "Deodorant",
    "deodorants": "Deodorant",
    "deo": "Deodorant",
    "deo-spray": "Deodorant",
    "deospray": "Deodorant",
    "deo bodyspray": "Deodorant",
    "bodyspray & deo": "Deodorant",
    "deodorant & body spray": "Deodorant",
    "dusch- & deoprodukte": "Deodorant",
    "eingelegte gemüse": "Eingelegtes Gemüse",
    "reis & getreide": "Reis",
    "pflanzliche joghurtalternativen": "Pflanzliche Joghurtalternative",
    "vegane fleischalternativen": "Vegane Fleischalternative",
}

# Oberkategorie-Normalisierung: Vereinheitlicht unterschiedliche OK-Zuordnungen
OK_MERGE_MAP = {
    # Alkohol gehört unter "Alkohol & Getränke", nicht "Getränke"
    ("getränke", "bier"): "Alkohol & Getränke",
    ("getränke", "wein"): "Alkohol & Getränke",
    ("getränke", "weißwein"): "Alkohol & Getränke",
    ("getränke", "rotwein"): "Alkohol & Getränke",
    ("getränke", "roséwein"): "Alkohol & Getränke",
    ("getränke", "weißbier"): "Alkohol & Getränke",
    ("getränke", "weizenbier"): "Alkohol & Getränke",
    ("getränke", "biermischgetränk"): "Alkohol & Getränke",
    ("getränke", "alkoholfreies bier"): "Alkohol & Getränke",
    ("getränke", "pflanzendrink"): "Milchprodukte & Käse",
    # Tiefkühl-Fisch gehört zu Fisch
    ("tiefkühlprodukte", "fischfilet"): "Fisch & Meeresfrüchte",
    ("tiefkühlprodukte", "lachsfilet"): "Fisch & Meeresfrüchte",
    ("tiefkühlprodukte", "garnelen"): "Fisch & Meeresfrüchte",
    # Sonstiges auflösen
    ("sonstiges", "nudeln"): "Backen & Mehl",
    ("sonstiges", "reis"): "Backen & Mehl",
    # Kekse/Kuchen/etc. Hauptkategorie
    ("backwaren & brot", "kekse"): "Süßwaren & Snacks",
    ("backwaren & brot", "waffeln"): "Süßwaren & Snacks",
    ("backwaren & brot", "donuts"): "Backwaren & Brot",
    ("süßwaren & snacks", "kuchen"): "Backwaren & Brot",
    ("süßwaren & snacks", "dessert"): "Milchprodukte & Käse",
    # Doppelte Zuordnungen
    ("milchprodukte & käse", "eier"): "Frühstück & Cerealien",
    ("öle essig & gewürze", "konfitüre"): "Frühstück & Cerealien",
    ("öle essig & gewürze", "fruchtaufstrich"): "Frühstück & Cerealien",
    ("süßwaren & snacks", "honig"): "Öle Essig & Gewürze",
    ("süßwaren & snacks", "brotaufstrich"): "Frühstück & Cerealien",
    ("milchprodukte & käse", "brotaufstrich"): "Frühstück & Cerealien",
    ("fleisch & wurst", "fertiggerichte"): "Fertiggerichte & Suppen",
    ("fertiggerichte & suppen", "salat"): "Obst & Gemüse",
    ("fertiggerichte & suppen", "gulasch"): "Fleisch & Wurst",
    ("drogerie & gesundheit", "toilettenpapier"): "Haushalt & Reinigung",
    ("haushalt & küche", "küchentücher"): "Haushalt & Reinigung",
    ("haushalt & reinigung", "taschentücher"): "Drogerie & Gesundheit",
    ("elektronik & technik", "staubsauger"): "Haushalt & Reinigung",
    ("elektronik & technik", "heißluftfritteuse"): "Haushalt & Küche",
    ("backwaren & brot", "knäckebrot"): "Backwaren & Brot",
    ("frühstück & cerealien", "knäckebrot"): "Backwaren & Brot",
    ("backen & mehl", "nüsse"): "Süßwaren & Snacks",
    ("obst & gemüse", "nüsse"): "Süßwaren & Snacks",
    # Zweite Runde Duplikate
    ("tiefkühlprodukte", "fertiggerichte"): "Fertiggerichte & Suppen",
    ("backwaren & brot", "nudeln"): "Backen & Mehl",
    ("fertiggerichte & suppen", "nudeln"): "Backen & Mehl",
    ("obst & gemüse", "konfitüre"): "Frühstück & Cerealien",
    ("frühstück & cerealien", "honig"): "Öle Essig & Gewürze",
    ("fleisch & wurst", "belegte brötchen"): "Backwaren & Brot",
    ("fleisch & wurst", "pfanne"): "Haushalt & Küche",
    ("süßwaren & snacks", "donut"): "Backwaren & Brot",
    ("backen & mehl", "trockenfrüchte"): "Obst & Gemüse",
    ("haushalt & küche", "kissen"): "Bekleidung & Textilien",
    ("öle essig & gewürze", "eingelegtes gemüse"): "Obst & Gemüse",
    ("werkzeug & baumarkt", "fahrradzubehör"): "Garten & Outdoor",
    ("spielzeug & freizeit", "fahrradzubehör"): "Garten & Outdoor",
    ("sonstiges", "fahrradzubehör"): "Garten & Outdoor",
    ("fertiggerichte & suppen", "saucen"): "Öle Essig & Gewürze",
    ("backwaren & brot", "knabbergebäck"): "Süßwaren & Snacks",
    ("süßwaren & snacks", "dessert & pudding"): "Milchprodukte & Käse",
    ("fisch & meeresfrüchte", "filet"): "Fleisch & Wurst",
    ("fleisch & wurst", "aufstrich"): "Frühstück & Cerealien",
    ("backen & mehl", "sirup"): "Getränke",
    ("süßwaren & snacks", "feingebäck"): "Backwaren & Brot",
}


def normalize_uk(uk: str) -> str:
    """Normalisiert Unterkategorie: Case + Merge-Map."""
    uk = uk.strip()
    # Title case normalisieren
    uk_title = uk[0].upper() + uk[1:] if uk else uk
    # Check merge map (case-insensitive)
    merged = UK_MERGE_MAP.get(uk.lower())
    if merged:
        return merged
    return uk_title


def main():
    start = time.time()

    # Load data
    print("Lade Klassifikationen...")
    with open(CLASSIFIED_PATH, encoding="utf-8") as f:
        classified = json.load(f)
    with open(PRODUCTS_PATH, encoding="utf-8") as f:
        products = json.load(f)

    print(f"  {len(classified)} Klassifikationen, {len(products)} Produkte")

    # Build index: product_name -> classification
    idx_map = {r["i"]: r for r in classified}

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    # =========================================
    # Step 1: Create categories_v2
    # =========================================
    print("\nSchritt 1: categories_v2 erstellen...")
    cur.execute("DROP TABLE IF EXISTS categories_v2")
    cur.execute("""
        CREATE TABLE categories_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            name_normalized TEXT NOT NULL,
            level INTEGER NOT NULL,  -- 1=ober, 2=unter
            parent_id INTEGER REFERENCES categories_v2(id),
            product_count INTEGER DEFAULT 0,
            UNIQUE(name_normalized, level, parent_id)
        )
    """)

    # Collect unique ober+unter combos
    ober_map = {}  # name_norm -> id
    unter_map = {}  # (ober_norm, unter_norm) -> id

    for i, p in enumerate(products):
        if i not in idx_map:
            continue
        r = idx_map[i]
        ok = r.get("ok", "Sonstiges").strip()
        uk_raw = r.get("uk", "Sonstiges").strip()
        uk = normalize_uk(uk_raw)

        ok_norm = ok.lower()
        uk_norm = uk.lower()

        # OK-Normalisierung: Verschiebt UK in die richtige Oberkategorie
        ok_override = OK_MERGE_MAP.get((ok_norm, uk_norm))
        if ok_override:
            ok = ok_override
            ok_norm = ok.lower()

        # Insert Oberkategorie
        if ok_norm not in ober_map:
            cur.execute(
                "INSERT OR IGNORE INTO categories_v2 (name, name_normalized, level, parent_id) VALUES (?, ?, 1, NULL)",
                (ok, ok_norm)
            )
            ober_map[ok_norm] = cur.lastrowid

        # Insert Unterkategorie
        key = (ok_norm, uk_norm)
        if key not in unter_map:
            parent_id = ober_map[ok_norm]
            cur.execute(
                "INSERT OR IGNORE INTO categories_v2 (name, name_normalized, level, parent_id) VALUES (?, ?, 2, ?)",
                (uk, uk_norm, parent_id)
            )
            unter_map[key] = cur.lastrowid

    # Re-read IDs (AUTOINCREMENT might differ from lastrowid on IGNORE)
    ober_map = {}
    unter_map = {}
    for row in cur.execute("SELECT id, name, name_normalized, level, parent_id FROM categories_v2"):
        cid, name, name_norm, level, parent_id = row
        if level == 1:
            ober_map[name_norm] = cid
        else:
            # Find parent's name_norm
            parent_norm = None
            for on, oid in ober_map.items():
                if oid == parent_id:
                    parent_norm = on
                    break
            if parent_norm:
                unter_map[(parent_norm, name_norm)] = cid

    print(f"  {len(ober_map)} Oberkategorien, {len(unter_map)} Unterkategorien")

    # =========================================
    # Step 2: Create product_labels
    # =========================================
    print("\nSchritt 2: product_labels erstellen...")
    cur.execute("DROP TABLE IF EXISTS product_labels")
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

    label_map = {}  # product_name -> product_labels.id
    cat_v2_map = {}  # product_name -> category_v2_id

    for i, p in enumerate(products):
        if i not in idx_map:
            continue
        r = idx_map[i]
        pname = p["n"]
        pname_norm = pname.lower().strip()

        ok_raw = r.get("ok", "Sonstiges").strip()
        uk = normalize_uk(r.get("uk", "Sonstiges").strip())
        ok_norm = ok_raw.lower()
        uk_norm = uk.lower()

        # OK-Normalisierung (gleich wie in Step 1)
        ok_override = OK_MERGE_MAP.get((ok_norm, uk_norm))
        if ok_override:
            ok_norm = ok_override.lower()

        cat_id = unter_map.get((ok_norm, uk_norm))
        if not cat_id:
            # Fallback: try to find by uk_norm alone
            for key, cid in unter_map.items():
                if key[1] == uk_norm:
                    cat_id = cid
                    break
        if not cat_id:
            # Last resort: map to parent ober
            cat_id = ober_map.get(ok_norm)

        marke = r.get("m") or None
        gattung = r.get("g") or None
        food = 1 if r.get("f", True) else 0

        cur.execute(
            "INSERT OR IGNORE INTO product_labels (product_name, product_name_normalized, category_v2_id, marke, gattungsbegriff, ist_lebensmittel) VALUES (?, ?, ?, ?, ?, ?)",
            (pname, pname_norm, cat_id, marke, gattung, food)
        )
        lid = cur.lastrowid
        label_map[pname] = lid
        cat_v2_map[pname] = cat_id

    print(f"  {len(label_map)} Product-Labels eingefügt")

    # =========================================
    # Step 3: Create search_labels + FTS5
    # =========================================
    print("\nSchritt 3: search_labels + FTS5 erstellen...")
    cur.execute("DROP TABLE IF EXISTS search_labels_fts")
    cur.execute("DROP TABLE IF EXISTS search_labels")
    cur.execute("""
        CREATE TABLE search_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            label TEXT NOT NULL,
            label_type TEXT NOT NULL DEFAULT 'suchbegriff'
        )
    """)

    total_labels = 0
    for i, p in enumerate(products):
        if i not in idx_map:
            continue
        r = idx_map[i]
        pname = p["n"]

        # Suchbegriffe
        for s in r.get("s", []):
            s = s.strip().lower()
            if s:
                cur.execute(
                    "INSERT INTO search_labels (product_name, label, label_type) VALUES (?, ?, 'suchbegriff')",
                    (pname, s)
                )
                total_labels += 1

        # Marke als separates Label
        marke = r.get("m")
        if marke:
            cur.execute(
                "INSERT INTO search_labels (product_name, label, label_type) VALUES (?, ?, 'marke')",
                (pname, marke.strip().lower())
            )
            total_labels += 1

        # Gattungsbegriff als separates Label
        gattung = r.get("g")
        if gattung:
            cur.execute(
                "INSERT INTO search_labels (product_name, label, label_type) VALUES (?, ?, 'gattung')",
                (pname, gattung.strip().lower())
            )
            total_labels += 1

    print(f"  {total_labels} Search-Labels eingefügt")

    # =========================================
    # Step 3b: Compound word decompounding
    # =========================================
    print("\n  Compound-Dekomposition...")
    decomp_labels = 0
    try:
        from compound_split import char_split

        # Manual fallback for words the splitter misses (umlauts, unusual compounds)
        MANUAL_SPLITS = {
            "sonnenblumenöl": ["sonnenblumen", "öl"],
            "vollkornbrot": ["vollkorn", "brot"],
            "spülmittel": ["spül", "mittel"],
            "gummibärchen": ["gummi", "bärchen"],
            "biermischgetränk": ["bier", "mischgetränk"],
            "roggenbrot": ["roggen", "brot"],
            "dinkelbrot": ["dinkel", "brot"],
            "mehrkornbrot": ["mehrkorn", "brot"],
            "schwarzbrot": ["schwarz", "brot"],
            "weißbrot": ["weiß", "brot"],
            "knäckebrot": ["knäcke", "brot"],
            "toastbrot": ["toast", "brot"],
            "fladenbrot": ["fladen", "brot"],
            "sandwichbrot": ["sandwich", "brot"],
            "sauerteigbrot": ["sauerteig", "brot"],
            "nussnougatcreme": ["nuss", "nougat", "creme"],
            "pflanzenmilch": ["pflanzen", "milch"],
            "hafermilch": ["hafer", "milch"],
            "sojamilch": ["soja", "milch"],
            "kokosmilch": ["kokos", "milch"],
            "mandelmilch": ["mandel", "milch"],
            "schokoladenmilch": ["schokoladen", "milch"],
            "frischmilch": ["frisch", "milch"],
            "vollmilch": ["voll", "milch"],
            "magermilch": ["mager", "milch"],
            "ziegenmilch": ["ziegen", "milch"],
            "apfelsaft": ["apfel", "saft"],
            "traubensaft": ["trauben", "saft"],
            "multivitaminsaft": ["multivitamin", "saft"],
            "gemüsesaft": ["gemüse", "saft"],
            "tomatensaft": ["tomaten", "saft"],
            "ananassaft": ["ananas", "saft"],
            "kirschsaft": ["kirsch", "saft"],
            "johannisbeersaft": ["johannisbeer", "saft"],
            "pflanzendrink": ["pflanzen", "drink"],
            "haferdrink": ["hafer", "drink"],
            "sojadrink": ["soja", "drink"],
            "mandeldrink": ["mandel", "drink"],
            "reisdrink": ["reis", "drink"],
            "olivenöl": ["oliven", "öl"],
            "rapsöl": ["raps", "öl"],
            "kürbiskernöl": ["kürbiskern", "öl"],
            "kokosöl": ["kokos", "öl"],
            "erdnussöl": ["erdnuss", "öl"],
            "sesamöl": ["sesam", "öl"],
            "walnussöl": ["walnuss", "öl"],
            "trüffelöl": ["trüffel", "öl"],
            "speiseöl": ["speise", "öl"],
            "bratöl": ["brat", "öl"],
            "rindfleisch": ["rind", "fleisch"],
            "schweinefleisch": ["schwein", "fleisch"],
            "putenfleisch": ["puten", "fleisch"],
            "geflügelfleisch": ["geflügel", "fleisch"],
            "hackfleisch": ["hack", "fleisch"],
            "fleischkäse": ["fleisch", "käse"],
            "schnittkäse": ["schnitt", "käse"],
            "weichkäse": ["weich", "käse"],
            "hartkäse": ["hart", "käse"],
            "frischkäse": ["frisch", "käse"],
            "scheibenkäse": ["scheiben", "käse"],
            "bergkäse": ["berg", "käse"],
            "ziegenkäse": ["ziegen", "käse"],
            "räucherkäse": ["räucher", "käse"],
            "streichkäse": ["streich", "käse"],
            "schmelzkäse": ["schmelz", "käse"],
            "hüttenkäse": ["hütten", "käse"],
        }

        # Get unique single-word labels that are long enough to be compounds
        existing_labels = set()
        for row in cur.execute("SELECT DISTINCT label FROM search_labels WHERE label NOT LIKE '% %' AND length(label) >= 8"):
            existing_labels.add(row[0])

        print(f"  {len(existing_labels)} einwort-Labels >= 8 Zeichen zum Dekomponieren")

        # Also get all (product_name, label) pairs to know what products have which labels
        label_products = {}
        for row in cur.execute("SELECT product_name, label FROM search_labels WHERE label NOT LIKE '% %' AND length(label) >= 8"):
            label_products.setdefault(row[1], set()).add(row[0])

        # Decompose and insert
        split_cache = {}
        for label in existing_labels:
            # Check manual dictionary first
            manual = MANUAL_SPLITS.get(label)
            if manual:
                split_cache[label] = manual
                continue

            # Try auto-split
            try:
                parts = char_split.split_compound(label)
                if parts and parts[0][0] > 0.3:
                    p1 = parts[0][1].lower()
                    p2 = parts[0][2].lower()
                    if len(p1) >= 3 and len(p2) >= 3:
                        split_cache[label] = [p1, p2]
            except Exception:
                pass

        print(f"  {len(split_cache)} Labels erfolgreich dekomponiert")

        # Insert decomposed parts as additional labels
        for label, parts in split_cache.items():
            products_with_label = label_products.get(label, set())
            for pname in products_with_label:
                for part in parts:
                    cur.execute(
                        "INSERT INTO search_labels (product_name, label, label_type) VALUES (?, ?, 'dekomposition')",
                        (pname, part),
                    )
                    decomp_labels += 1

        print(f"  {decomp_labels} Dekompositions-Labels eingefügt")

    except ImportError:
        print("  WARNUNG: compound_split nicht installiert, überspringe Dekomposition")

    total_labels += decomp_labels

    # FTS5 Index
    print("  FTS5-Index erstellen...")
    cur.execute("""
        CREATE VIRTUAL TABLE search_labels_fts USING fts5(
            product_name,
            label,
            label_type,
            content='search_labels',
            content_rowid='id',
            tokenize='unicode61'
        )
    """)
    cur.execute("INSERT INTO search_labels_fts(search_labels_fts) VALUES('rebuild')")
    print("  FTS5-Index fertig")

    # =========================================
    # Step 4: Update product_count in categories_v2
    # =========================================
    print("\nSchritt 4: Produkt-Counts aktualisieren...")
    cur.execute("""
        UPDATE categories_v2 SET product_count = (
            SELECT COUNT(*) FROM product_labels WHERE category_v2_id = categories_v2.id
        ) WHERE level = 2
    """)
    cur.execute("""
        UPDATE categories_v2 SET product_count = (
            SELECT COALESCE(SUM(c2.product_count), 0)
            FROM categories_v2 c2 WHERE c2.parent_id = categories_v2.id
        ) WHERE level = 1
    """)

    # =========================================
    # Step 5: Remap offers.category_id
    # =========================================
    print("\nSchritt 5: offers.category_id remappen...")

    # Build product_name -> category_v2_id from product_labels
    cur.execute("SELECT product_name, category_v2_id FROM product_labels")
    pname_to_cat = dict(cur.fetchall())

    # Update offers in batches
    updated = 0
    cur.execute("SELECT DISTINCT product_name FROM offers WHERE product_name IS NOT NULL")
    all_offer_names = [row[0] for row in cur.fetchall()]

    for pname in all_offer_names:
        cat_id = pname_to_cat.get(pname)
        if cat_id:
            cur.execute("UPDATE offers SET category_id = ? WHERE product_name = ?", (cat_id, pname))
            updated += cur.rowcount

    print(f"  {updated} Offers aktualisiert")

    # =========================================
    # Step 6: Create indexes
    # =========================================
    print("\nSchritt 6: Indexes erstellen...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_labels_product ON search_labels(product_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_search_labels_label ON search_labels(label)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_product_labels_cat ON product_labels(category_v2_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_product_labels_name ON product_labels(product_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_categories_v2_parent ON categories_v2(parent_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_categories_v2_norm ON categories_v2(name_normalized)")

    conn.commit()

    # =========================================
    # Stats
    # =========================================
    print(f"\n{'='*50}")
    print("FERTIG!")
    print(f"{'='*50}")

    stats = [
        ("categories_v2", cur.execute("SELECT COUNT(*) FROM categories_v2").fetchone()[0]),
        ("  davon Oberkategorien", cur.execute("SELECT COUNT(*) FROM categories_v2 WHERE level=1").fetchone()[0]),
        ("  davon Unterkategorien", cur.execute("SELECT COUNT(*) FROM categories_v2 WHERE level=2").fetchone()[0]),
        ("product_labels", cur.execute("SELECT COUNT(*) FROM product_labels").fetchone()[0]),
        ("search_labels", cur.execute("SELECT COUNT(*) FROM search_labels").fetchone()[0]),
        ("offers remapped", updated),
    ]
    for name, count in stats:
        print(f"  {name:30s} {count:>8,d}")

    # Quick validation
    print(f"\nValidierung:")
    row = cur.execute("""
        SELECT sl.product_name, sl.label, pl.marke, c.name as kategorie
        FROM search_labels sl
        JOIN product_labels pl ON pl.product_name = sl.product_name
        JOIN categories_v2 c ON c.id = pl.category_v2_id
        WHERE sl.label = 'müsli'
        LIMIT 5
    """).fetchall()
    print(f"  Suche 'müsli' -> {len(row)} Treffer")
    for r in row:
        print(f"    {r[0][:40]:40s} | marke={r[2]} | kat={r[3]}")

    elapsed = time.time() - start
    print(f"\nDauer: {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
