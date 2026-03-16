# Sparfuchs Testkonzept

## Übersicht

Dieses Dokument beschreibt das Testkonzept für die Sparfuchs-Anwendung.
Ziel ist es, alle kritischen Komponenten systematisch zu testen.

---

## 1. Komponenten-Tests

### 1.1 Keyword-Normalisierung (`app/utils/keywords.py`)

| Test-ID | Eingabe | Erwartete Varianten | Priorität |
|---------|---------|---------------------|-----------|
| KW-01 | `"Tomate"` | `["Tomate", "Tomaten"]` | Hoch |
| KW-02 | `"tomate"` | `["Tomate", "tomate", "Tomaten"]` | Hoch |
| KW-03 | `"Banane"` | `["Banane", "Bananen"]` | Hoch |
| KW-04 | `"Milch"` | `["Milch", "Milche", "Milchen"]` | Mittel |
| KW-05 | `"Käse"` | `["Käse", ...]` (Umlaut-Handling) | Hoch |
| KW-06 | `"Pizza"` | `["Pizza", "Pizzas"]` | Mittel |
| KW-07 | `"Apfel"` | `["Apfel"]` (kein -en) | Mittel |
| KW-08 | `""` | `[]` | Niedrig |
| KW-09 | `"   "` | `[]` | Niedrig |
| KW-10 | `"Coca-Cola"` | Markenname mit Bindestrich | Mittel |

### 1.2 Chain-Normalisierung (`app/utils/chains.py`)

| Test-ID | Eingabe | Erwartetes Ergebnis | Quelle |
|---------|---------|---------------------|--------|
| CH-01 | `"ALDI SÜD"` | `"Aldi"` | KaufDA |
| CH-02 | `"aldi nord"` | `"Aldi"` | KaufDA |
| CH-03 | `"REWE City"` | `"Rewe"` | KaufDA |
| CH-04 | `"Nahkauf"` | `"Rewe"` | OSM |
| CH-05 | `"EDEKA Center"` | `"Edeka"` | KaufDA |
| CH-06 | `"Netto Marken-Discount"` | `"Netto"` | KaufDA |
| CH-07 | `"Penny-Markt"` | `"Penny"` | OSM |
| CH-08 | `"Unbekannter Laden"` | `"Sonstige"` | OSM |
| CH-09 | `None` | `None` | - |
| CH-10 | `""` | `None` | - |

### 1.3 Fuzzy-Matching (`app/utils/matching.py`)

| Test-ID | Query | Angebot | Erwartet Match? | Min-Score |
|---------|-------|---------|-----------------|-----------|
| FM-01 | `"Milch"` | `"Milbona Haltbare Milch"` | Ja | >65 |
| FM-02 | `"Tomaten"` | `"Rispentomaten"` | Ja | >65 |
| FM-03 | `"Bananen"` | `"Chiquita Bananen"` | Ja | >65 |
| FM-04 | `"Butter"` | `"Kerrygold Butter"` | Ja | >65 |
| FM-05 | `"Milch"` | `"Milchschokolade"` | Nein | <65 |
| FM-06 | `"Apfel"` | `"Apfelsaft"` | Grenzfall | ~60-70 |
| FM-07 | `"Bio Milch"` | `"Bio Vollmilch"` | Ja | >65 |
| FM-08 | `"Coca Cola"` | `"Coca-Cola 1,5L"` | Ja | >65 |
| FM-09 | `"nutella"` | `"Nutella 450g"` | Ja | >65 |
| FM-10 | `"Hähnchen"` | `"Hähnchenbrustfilet"` | Ja | >65 |

### 1.4 Temporal-Filter (`app/utils/offers.py`)

| Test-ID | valid_from | valid_to | Referenz-Datum | Gültig? |
|---------|------------|----------|----------------|---------|
| TF-01 | `None` | `None` | heute | Ja |
| TF-02 | gestern | morgen | heute | Ja |
| TF-03 | morgen | übermorgen | heute | Nein |
| TF-04 | letzte Woche | gestern | heute | Nein |
| TF-05 | `None` | morgen | heute | Ja |
| TF-06 | gestern | `None` | heute | Ja |

---

## 2. API-Tests

### 2.1 KaufDA Connector (`app/connectors/kaufda.py`)

| Test-ID | Keyword | Standort | Erwartung |
|---------|---------|----------|-----------|
| KD-01 | `"Milch"` | Bonn | >10 Angebote |
| KD-02 | `"Tomaten"` | Bonn | >5 Angebote |
| KD-03 | `"Bananen"` | Bonn | >3 Angebote |
| KD-04 | `"Butter"` | Bonn | >5 Angebote |
| KD-05 | `"Brot"` | Bonn | >5 Angebote |
| KD-06 | `"Milch"` | Berlin | >10 Angebote |
| KD-07 | `"Milch"` | München | >10 Angebote |
| KD-08 | `"Milch"` | Hamburg | >10 Angebote |
| KD-09 | `"xyz123abc"` | Bonn | 0 Angebote |
| KD-10 | `""` | Bonn | 0 Angebote |

**Zu prüfende Felder pro Angebot:**
- `id` (nicht leer)
- `title` (nicht leer)
- `chain` (in KNOWN_CHAINS)
- `price_eur` (float oder None)
- `valid_from`, `valid_to` (date oder None)

### 2.2 Overpass/OSM (`app/services/overpass.py`)

| Test-ID | Standort | Radius | Erwartung |
|---------|----------|--------|-----------|
| OP-01 | Bonn Zentrum | 5km | >20 Stores |
| OP-02 | Berlin Mitte | 3km | >30 Stores |
| OP-03 | München Marienplatz | 2km | >15 Stores |
| OP-04 | Kleines Dorf | 2km | 0-5 Stores |
| OP-05 | Bonn Zentrum | 500m | >3 Stores |

**Zu prüfende Felder pro Store:**
- `osm_id` (int)
- `name` (nicht leer)
- `chain` (in KNOWN_CHAINS oder "Sonstige")
- `lat`, `lon` (valide Koordinaten)
- `address` (String oder None)

---

## 3. Integrations-Tests

### 3.1 End-to-End Warenkorb-Vergleich

| Test-ID | Standort | Warenkorb | Erwartung |
|---------|----------|-----------|-----------|
| E2E-01 | Bonn | `["Milch"]` | ≥1 Store mit Treffer |
| E2E-02 | Bonn | `["Milch", "Butter"]` | ≥1 Store mit 2 Treffern |
| E2E-03 | Bonn | `["Milch", "Butter", "Brot"]` | Ranking nach Treffern |
| E2E-04 | Bonn | `["Tomaten", "Bananen", "Äpfel"]` | Obst-Kategorie |
| E2E-05 | Berlin | `["Milch", "Käse"]` | Andere Region |

**Zu prüfende Aspekte:**
- Ranking: Store mit meisten Treffern zuerst
- Preisberechnung: Summe korrekt
- Missing-Count: Korrekt gezählt
- Distanz: Korrekt berechnet

### 3.2 Suggest-API (`/api/suggest`)

| Test-ID | Query | Erwartung |
|---------|-------|-----------|
| SG-01 | `"Mil"` | Autocomplete für "Milch" |
| SG-02 | `"Tom"` | Autocomplete für "Tomaten" |
| SG-03 | `"x"` | Zu kurz, keine Ergebnisse |
| SG-04 | `"Milch"` + chains=`["Lidl"]` | Nur Lidl-Angebote |

---

## 4. Produkt-Testmatrix

### 4.1 Standard-Produkte (Basis-Test)

| Kategorie | Produkte |
|-----------|----------|
| Milchprodukte | Milch, Butter, Käse, Joghurt, Quark, Sahne |
| Obst | Bananen, Äpfel, Orangen, Erdbeeren, Weintrauben |
| Gemüse | Tomaten, Gurken, Paprika, Karotten, Zwiebeln |
| Fleisch | Hähnchen, Hackfleisch, Schnitzel, Wurst |
| Backwaren | Brot, Brötchen, Toast, Croissant |
| Getränke | Wasser, Cola, Bier, Saft, Kaffee |
| Süßwaren | Schokolade, Chips, Kekse, Eis |
| Basics | Nudeln, Reis, Mehl, Zucker, Öl, Eier |

### 4.2 Marken-Produkte

| Marke | Produkte |
|-------|----------|
| Coca-Cola | Cola, Fanta, Sprite |
| Nutella | Nutella |
| Milka | Schokolade |
| Barilla | Nudeln |
| Kerrygold | Butter |
| Dr. Oetker | Pizza, Pudding |

### 4.3 Edge Cases

| Kategorie | Test-Fälle |
|-----------|------------|
| Umlaute | Käse, Müsli, Brötchen, Würstchen |
| Bindestriche | Coca-Cola, Dr. Oetker, H-Milch |
| Zahlen | 1,5L Wasser, 500g Hackfleisch |
| Plural/Singular | Ei/Eier, Apfel/Äpfel |
| Zusammengesetzte | Vollmilch, Hackfleisch, Orangensaft |

---

## 5. Standort-Testmatrix

### 5.1 Großstädte

| Stadt | PLZ | Lat/Lon | Erwartete Chains |
|-------|-----|---------|------------------|
| Berlin Mitte | 10117 | 52.52/13.405 | Alle |
| München | 80331 | 48.137/11.576 | Alle |
| Hamburg | 20095 | 53.551/9.993 | Alle |
| Köln | 50667 | 50.938/6.960 | Alle |
| Frankfurt | 60311 | 50.110/8.682 | Alle |
| Bonn | 53111 | 50.737/7.098 | Alle |

### 5.2 Mittelstädte

| Stadt | PLZ | Erwartung |
|-------|-----|-----------|
| Koblenz | 56068 | >10 Stores |
| Trier | 54290 | >10 Stores |
| Mainz | 55116 | >10 Stores |

### 5.3 Kleinstädte/Dörfer

| Ort | PLZ | Erwartung |
|-----|-----|-----------|
| Kleines Dorf | - | 0-3 Stores |
| Vorort | - | 3-10 Stores |

### 5.4 Grenzfälle

| Test | Beschreibung |
|------|--------------|
| Grenzregion | Nähe Frankreich/Belgien/Niederlande |
| Ostdeutschland | Andere Ketten-Verteilung |
| Touristengebiet | Saisonale Unterschiede |

---

## 6. Performance-Tests

### 6.1 Timeout-Handling

| Test-ID | Szenario | Timeout | Erwartung |
|---------|----------|---------|-----------|
| PT-01 | KaufDA langsam | 30s | Graceful timeout |
| PT-02 | Overpass überlastet | 25s | Fallback-URLs |
| PT-03 | Viele Stores | - | Max 50 Stores |

### 6.2 Caching

| Test-ID | Szenario | Erwartung |
|---------|----------|-----------|
| PC-01 | Gleiche Suche 2x | 2. Aufruf aus Cache |
| PC-02 | Gleicher Standort | Overpass-Cache nutzen |
| PC-03 | Cache-TTL | Nach 2h neu laden |

### 6.3 Parallelität

| Test-ID | Szenario | Erwartung |
|---------|----------|-----------|
| PP-01 | 3 Warenkorb-Items parallel | Alle fetchen parallel |
| PP-02 | 10 gleichzeitige Requests | Keine Race Conditions |

---

## 7. Test-Implementierung

### 7.1 Unit-Tests (pytest)

```
tests/
├── unit/
│   ├── test_keywords.py      # Keyword-Normalisierung
│   ├── test_chains.py        # Chain-Normalisierung
│   ├── test_matching.py      # Fuzzy-Matching
│   ├── test_offers.py        # Temporal-Filter
│   └── test_geo.py           # Haversine
├── integration/
│   ├── test_kaufda.py        # KaufDA API
│   ├── test_overpass.py      # Overpass API
│   └── test_pricing.py       # Basket-Pricing
└── e2e/
    ├── test_suggest.py       # /api/suggest
    └── test_results.py       # /results
```

### 7.2 Test-Fixtures

```python
# Standorte
LOCATIONS = {
    "bonn": {"lat": 50.7374, "lon": 7.0982, "plz": "53111"},
    "berlin": {"lat": 52.52, "lon": 13.405, "plz": "10117"},
    "muenchen": {"lat": 48.137, "lon": 11.576, "plz": "80331"},
}

# Standard-Warenkorb
STANDARD_BASKET = ["Milch", "Butter", "Brot", "Tomaten", "Bananen"]

# Alle bekannten Chains
KNOWN_CHAINS = ["Aldi", "Lidl", "Rewe", "Edeka", "Kaufland", "Penny", "Netto", "Norma", "Globus", "Marktkauf"]
```

### 7.3 Metriken

| Metrik | Ziel |
|--------|------|
| Keyword → Angebot Match-Rate | >80% |
| Chain-Normalisierung Accuracy | 100% |
| API-Verfügbarkeit | >95% |
| Durchschnittliche Response-Zeit | <3s |

---

## 8. Priorisierung

### Phase 1: Kritisch (sofort)
- [x] KW-01 bis KW-05 (Keyword-Varianten)
- [ ] KD-01 bis KD-05 (KaufDA Basis)
- [ ] FM-01 bis FM-05 (Matching Basis)
- [ ] E2E-01 bis E2E-03 (Warenkorb)

### Phase 2: Wichtig (diese Woche)
- [ ] CH-01 bis CH-10 (Chain-Normalisierung)
- [ ] OP-01 bis OP-05 (Overpass)
- [ ] Produkt-Matrix Basis

### Phase 3: Nice-to-have (später)
- [ ] Standort-Matrix komplett
- [ ] Performance-Tests
- [ ] Edge Cases

---

## 9. Ausführung

```bash
# Alle Unit-Tests
pytest tests/unit/ -v

# Integration-Tests (benötigt Netzwerk)
pytest tests/integration/ -v --network

# E2E-Tests
pytest tests/e2e/ -v

# Mit Coverage
pytest --cov=app --cov-report=html

# Nur kritische Tests
pytest -m critical -v
```

---

## 10. Reporting

Nach jedem Test-Lauf:
1. Match-Rate pro Produktkategorie
2. Fehlgeschlagene Keywords dokumentieren
3. Nicht erkannte Chains dokumentieren
4. Performance-Anomalien notieren
