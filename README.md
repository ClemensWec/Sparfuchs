# Sparfuchs (MVP) – Live KaufDA Angebote

Web-App, die für einen Standort + Radius Supermärkte findet und einen Einkaufszettel **gegen aktuelle Angebote** vergleicht.

Aktueller Stand (08.03.2026):
- **Einzige Datenquelle:** KaufDA (Bonial) – Live Fetching von Angebots-Suchergebnissen
- **Keine lokale Angebots-DB:** keine Vorab-Imports, kein `catalog.sqlite`
- **Standortfinder:** OpenStreetMap/Overpass (Filialen im Radius)

## Quickstart (Windows / PowerShell)

```powershell
cd C:\Users\Clemens\Documents\Sparfuchs
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Server starten:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

Browser: `http://127.0.0.1:8000`

## Wichtige Hinweise (Datenbasis)

- Live Fetching bedeutet: die App macht beim Vergleichen HTTP-Requests zu KaufDA. Das kann je nach Warenkorbgröße langsamer sein.
- Ergebnisse hängen vom Standort ab (KaufDA nutzt ein `location` Cookie, gesetzt über lat/lon).
- Wir vergleichen **nur Angebote**. Normale Regal-/Standardpreise sind nicht enthalten.

## Relevante Dateien

- `app/main.py` – FastAPI Web-App (UI + API), Live Fetching
- `app/connectors/kaufda.py` – KaufDA Parsing + `fetch_search_offers()` (Route `/angebote/<keyword>`)
- `app/services/overpass.py` – OSM/Overpass Store-Finder

