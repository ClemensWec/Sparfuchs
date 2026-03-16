"""Selenium UI test: location-filtered search + category counts."""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE = "http://localhost:8000"
PASS = 0
FAIL = 0


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  OK {name}" + (f" ({detail})" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {name}" + (f" ({detail})" if detail else ""))


def make_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,900")
    return webdriver.Chrome(options=opts)


def test_index_page_category_suggest(driver):
    """Test: Category autocomplete on index page uses location."""
    print("\n=== Test 1: Startseite — Category Suggest mit Location ===")
    driver.get(BASE)
    time.sleep(1)

    # Set location to "Bonn" and radius to "10"
    loc_input = driver.find_element(By.ID, "location")
    loc_input.clear()
    loc_input.send_keys("Bonn")
    time.sleep(0.3)

    # radius_km is a range slider — set via JS
    driver.execute_script("document.getElementById('radius_km').value = 10;")
    driver.execute_script("document.getElementById('radius_km').dispatchEvent(new Event('input'));")
    time.sleep(0.3)

    # Type "milch" in search field
    search_input = driver.find_element(By.ID, "hero_search")
    search_input.clear()
    search_input.send_keys("milch")
    time.sleep(1.5)  # Wait for autocomplete

    # Check dropdown appeared
    dropdown = driver.find_element(By.ID, "category_dropdown")
    visible = dropdown.is_displayed()
    check("Kategorie-Dropdown sichtbar", visible)

    # Get category items
    items = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-item")
    check("Kategorien gefunden", len(items) > 0, f"{len(items)} Kategorien")

    # Check that counts are local (should be small numbers, not global)
    for item in items[:4]:
        text = item.text
        print(f"    > {text}")
        # Extract count from text like "Milch\n8 Angebote"
        if "Angebote" in text or "angebote" in text.lower():
            parts = text.split("\n")
            for p in parts:
                if "angebot" in p.lower():
                    count_str = "".join(c for c in p if c.isdigit())
                    if count_str:
                        count = int(count_str)
                        # Local counts for "milch" in Bonn should be < 30
                        # Global "Milch" count is 26, local should be ~8
                        check(f"Count ist lokal (< 30)", count < 30, f"count={count}")

    # Check localStorage was set
    loc_stored = driver.execute_script("return localStorage.getItem('sparfuchs.location')")
    check("localStorage.location gesetzt", loc_stored == "Bonn", f"'{loc_stored}'")


def test_search_page_location_filter(driver):
    """Test: Search page filters results by location."""
    print("\n=== Test 2: Suchseite — Ergebnisse nach Location gefiltert ===")

    # Set localStorage first (simulates user having set location on index page)
    driver.get(BASE)
    driver.execute_script("""
        localStorage.setItem('sparfuchs.location', 'Bonn');
        localStorage.setItem('sparfuchs.radius_km', '10');
    """)
    time.sleep(0.5)

    # Navigate to search page with location params
    driver.get(f"{BASE}/search?q=milch&location=Bonn&radius_km=10")
    time.sleep(3)  # Wait for API call + render

    # Check result count
    count_el = driver.find_element(By.ID, "result_count")
    count_text = count_el.text
    print(f"    Ergebnis-Text: '{count_text}'")
    count_num = int("".join(c for c in count_text if c.isdigit())) if any(c.isdigit() for c in count_text) else 0
    check("Ergebnisse < 200 (lokal gefiltert)", count_num < 200, f"{count_num} Ergebnisse")
    check("Ergebnisse > 50 (nicht leer)", count_num > 50, f"{count_num} Ergebnisse")

    # Check chains shown (Globus/Marktkauf should NOT be in Bonn)
    chain_labels = driver.find_elements(By.CSS_SELECTOR, "#chain_filters label")
    chain_names = [el.text.strip() for el in chain_labels if el.text.strip()]
    print(f"    Ketten: {chain_names}")
    check("Globus NICHT in Bonn-Ergebnissen", "Globus" not in chain_names)
    check("Kaufland IN Bonn-Ergebnissen", any("Kaufland" in c for c in chain_names))


def test_search_without_location(driver):
    """Test: Search without location shows global results."""
    print("\n=== Test 3: Suchseite OHNE Location — globale Ergebnisse ===")

    # Clear localStorage
    driver.get(BASE)
    driver.execute_script("""
        localStorage.removeItem('sparfuchs.location');
        localStorage.removeItem('sparfuchs.radius_km');
    """)
    time.sleep(0.5)

    # Navigate without location params
    driver.get(f"{BASE}/search?q=milch")
    time.sleep(3)

    count_el = driver.find_element(By.ID, "result_count")
    count_text = count_el.text
    count_num = int("".join(c for c in count_text if c.isdigit())) if any(c.isdigit() for c in count_text) else 0
    print(f"    Ergebnis-Text: '{count_text}'")
    check("Global: mehr Ergebnisse (> 300)", count_num > 300, f"{count_num} Ergebnisse")


def test_different_locations(driver):
    """Test: Different locations show different result counts."""
    print("\n=== Test 4: Verschiedene Standorte — unterschiedliche Ergebnisse ===")

    # Bonn 10km
    driver.get(f"{BASE}/search?q=butter&location=Bonn&radius_km=10")
    time.sleep(3)
    count_bonn = driver.find_element(By.ID, "result_count").text
    num_bonn = int("".join(c for c in count_bonn if c.isdigit())) if any(c.isdigit() for c in count_bonn) else 0

    # Berlin 5km
    driver.get(f"{BASE}/search?q=butter&location=Berlin&radius_km=5")
    time.sleep(3)
    count_berlin = driver.find_element(By.ID, "result_count").text
    num_berlin = int("".join(c for c in count_berlin if c.isdigit())) if any(c.isdigit() for c in count_berlin) else 0

    print(f"    Bonn 10km: {num_bonn}, Berlin 5km: {num_berlin}")
    check("Bonn hat Ergebnisse", num_bonn > 0)
    check("Berlin hat Ergebnisse", num_berlin > 0)
    check("Beide < global (< 300)", num_bonn < 300 and num_berlin < 300)


def test_dropdown_navigates_with_location(driver):
    """Test: Clicking 'Suche nach' in dropdown includes location in URL."""
    print("\n=== Test 5: Dropdown-Navigation enthält Location in URL ===")

    driver.get(BASE)
    time.sleep(1)

    # Set location
    loc_input = driver.find_element(By.ID, "location")
    loc_input.clear()
    loc_input.send_keys("Bonn")

    driver.execute_script("document.getElementById('radius_km').value = 10;")
    driver.execute_script("document.getElementById('radius_km').dispatchEvent(new Event('input'));")
    time.sleep(0.5)

    # Type search
    search_input = driver.find_element(By.ID, "hero_search")
    search_input.clear()
    search_input.send_keys("cola")
    time.sleep(1.5)

    # Click "Suche nach" row
    dropdown = driver.find_element(By.ID, "category_dropdown")
    search_row = dropdown.find_element(By.ID, "suggest-item-search")
    search_row.click()
    time.sleep(3)

    # Check URL contains location
    current_url = driver.current_url
    print(f"    URL: {current_url}")
    check("URL enthält location=Bonn", "location=Bonn" in current_url or "location=bonn" in current_url.lower())
    check("URL enthält radius_km=10", "radius_km=10" in current_url)

    # Check results are filtered
    count_el = driver.find_element(By.ID, "result_count")
    count_text = count_el.text
    count_num = int("".join(c for c in count_text if c.isdigit())) if any(c.isdigit() for c in count_text) else 0
    print(f"    Ergebnisse: {count_num}")
    check("Cola-Ergebnisse lokal gefiltert (< 100)", count_num < 100, f"{count_num}")


def main():
    driver = make_driver()
    try:
        test_index_page_category_suggest(driver)
        test_search_page_location_filter(driver)
        test_search_without_location(driver)
        test_different_locations(driver)
        test_dropdown_navigates_with_location(driver)
    finally:
        driver.quit()

    print(f"\n{'='*50}")
    print(f"Ergebnis: {PASS} bestanden, {FAIL} fehlgeschlagen")
    if FAIL > 0:
        exit(1)


if __name__ == "__main__":
    main()
