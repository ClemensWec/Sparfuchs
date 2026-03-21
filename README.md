# Sparfuchs – Preisvergleich für Supermarkt-Angebote

Web-App, die für einen Standort + Radius Supermärkte findet und einen Einkaufszettel **gegen aktuelle Angebote** vergleicht. Unterstützt 10 Ketten mit 188.000+ Angeboten und 42.000+ Filialen.

## Features

- **Kategorie-basierte Suche**: 5.340 Produktkategorien mit FTS5-Volltextsuche, Fuzzy-Matching und deutschen Kompositawörtern
- **Preisvergleich**: Warenkorb gegen alle Filialen im Radius vergleichen, sortiert nach Gesamtpreis
- **Exakte vs. ähnliche Produkte**: Markenprodukte werden exakt zugeordnet, andere Ketten zeigen ähnliche Alternativen (mit visuellem Trenner)
- **Produkt-Alternativen**: Klick auf ein Produkt zeigt alternative Angebote mit Bildern, Preisen und Grundpreisvergleich
- **Kategorie-Browser**: Stöbern durch Produktkategorien mit Angebotskarten
- **Ketten-Filter**: Ergebnisse nach Supermarktketten filtern
- **SparMix**: Günstigste Kombination aus mehreren Märkten berechnen
- **PWA**: Installierbar als App, offline-fähig mit Service Worker (Network-first)
- **Responsive**: Mobile Tab-Navigation, adaptive Grid-Layouts (2 Spalten auf Phones bis 7+ auf Ultra-Wide)

## Unterstützte Ketten

Aldi Nord, Aldi Süd, Edeka, Kaufland, Lidl, Netto, Norma, Penny, REWE, Globus

## Architektur

- **Backend**: FastAPI + Jinja2 Templates + Python 3.11
- **Frontend**: Vanilla JS (kein Framework), Single-Page mit dynamischen API-Calls
- **Datenbank**: SQLite mit 188K Angeboten, 42K Filialen, 5.3K Produktkategorien
- **Suche**: FTS5 (Prefix) → LIKE (Substring) → Fuzzy (Edit-Distance-1)
- **Filialen**: 6 Quellen — OSM Overpass, AllThePlaces, offizielle APIs (Aldi, Lidl, Kaufland, Globus, Penny), KaufDA, Norma Geo-Grid

## Quickstart

```bash
cd C:\Users\Clemens\Documents\Sparfuchs
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Server starten:

```bash
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Browser: `http://127.0.0.1:8000`

## Tests

```bash
# Unit-Tests (API, Parser, Pricing, Matching)
python -m pytest tests/unit/ -v

# E2E Selenium-Tests (Browser-Tests mit Screenshots)
python -m pytest tests/e2e/ -v

# Legacy Test-Suite
python tests/run_tests.py
```

## Relevante Dateien

| Datei | Beschreibung |
|-------|-------------|
| `app/main.py` | FastAPI Routes: UI + API Endpoints (compare, search, suggest, browse) |
| `app/services/pricing.py` | BasketPricer mit Kategorie- und Text-Matching, Exakt/Ähnlich-Erkennung |
| `app/services/catalog_search.py` | Dreischichtige Suche: FTS → LIKE → Fuzzy |
| `app/services/catalog_data.py` | DB-Zugriff, Geocoding, Store-Matching |
| `app/services/category_search.py` | Kategorie-Suche mit FTS5 |
| `app/services/categorize.py` | Produktkategorisierung (5.7K Namen → Kategorien) |
| `app/utils/matching.py` | Scoring: Kompositawörter, Umlaute, Tippfehler-Toleranz |
| `app/utils/text.py` | Textnormalisierung, Mojibake-Reparatur |
| `app/static/app.js` | Client: Kategorie-Autocomplete, Warenkorb, Preisvergleich, Alternativen |
| `app/static/app.css` | Alle Styles (responsive, mobile tabs, cards) |
| `data/kaufda_dataset/offers.sqlite3` | SQLite-Datenbank (188K Angebote, 42K Filialen) |
