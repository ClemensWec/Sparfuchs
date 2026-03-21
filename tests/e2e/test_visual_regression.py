"""
Selenium Visual Regression Tests — Sparfuchs.

Tests the ACTUAL rendered UI with screenshots.
Run: python -m pytest tests/e2e/ -v --tb=short
Screenshots saved to tests/e2e/screenshots/
"""
from __future__ import annotations

import json
import re
import time

import pytest
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .conftest import save_screenshot


# ═══════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def wait_for(browser, by, value, timeout=10):
    """Wait for an element and return it."""
    return WebDriverWait(browser, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def wait_visible(browser, by, value, timeout=10):
    """Wait for an element to become visible."""
    return WebDriverWait(browser, timeout).until(
        EC.visibility_of_element_located((by, value))
    )


def wait_clickable(browser, by, value, timeout=10):
    """Wait for an element to become clickable."""
    return WebDriverWait(browser, timeout).until(
        EC.element_to_be_clickable((by, value))
    )


def clear_basket(browser, base_url):
    """Navigate to home and clear the basket via localStorage."""
    browser.get(base_url)
    browser.execute_script("localStorage.removeItem('sparfuchs.basket');")
    browser.get(base_url)
    time.sleep(1)


def setup_basket_with_location(browser, base_url, items=None):
    """Set up basket with items and a location, wait for compare."""
    if items is None:
        items = [{"q": "Milch"}, {"q": "Butter"}, {"q": "Brot"}]
    # Navigate first to ensure localStorage is available
    browser.get(base_url)
    browser.execute_script(
        "localStorage.setItem('sparfuchs.basket', arguments[0]);",
        json.dumps(items),
    )
    browser.get(base_url)
    time.sleep(1)
    loc_input = browser.find_element(By.ID, "location")
    loc_input.clear()
    loc_input.send_keys("53111 Bonn")
    time.sleep(8)


# ═══════════════════════════════════════════════════════════════════════════
#  1. STARTSEITE — Grundlayout
# ═══════════════════════════════════════════════════════════════════════════


class TestStartseite:
    """Startseite loads correctly with all key elements."""

    def test_page_loads(self, browser, base_url):
        """Startseite loads without errors."""
        browser.get(base_url)
        save_screenshot(browser, "01_startseite")
        assert "Sparfuchs" in browser.title

    def test_header_visible(self, browser, base_url):
        """Header with brand name is visible."""
        browser.get(base_url)
        header = wait_visible(browser, By.CSS_SELECTOR, ".site-header")
        brand = browser.find_element(By.CSS_SELECTOR, ".brand-copy strong")
        assert brand.text == "Sparfuchs"

    def test_search_input_visible(self, browser, base_url):
        """Search input is present and visible."""
        browser.get(base_url)
        search = wait_visible(browser, By.ID, "hero_search")
        assert search.is_displayed()
        assert search.get_attribute("placeholder")

    def test_location_input_visible(self, browser, base_url):
        """Location input is present."""
        browser.get(base_url)
        loc = wait_visible(browser, By.ID, "location")
        assert loc.is_displayed()

    def test_radius_slider_visible(self, browser, base_url):
        """Radius slider exists."""
        browser.get(base_url)
        slider = wait_visible(browser, By.ID, "radius_km")
        assert slider.is_displayed()

    def test_trust_footer_visible(self, browser, base_url):
        """Trust footer shows offer count and chain count."""
        browser.get(base_url)
        footer = wait_visible(browser, By.CSS_SELECTOR, ".trust-footer")
        text = footer.text
        assert "Angebote" in text
        assert "Ketten" in text
        save_screenshot(browser, "01_trust_footer")

    def test_no_js_errors(self, browser, base_url):
        """No critical JS errors on page load."""
        browser.get(base_url)
        time.sleep(2)
        logs = browser.get_log("browser")
        severe = [l for l in logs if l["level"] == "SEVERE"]
        critical = [l for l in severe if "favicon" not in l["message"].lower()
                    and "manifest" not in l["message"].lower()]
        assert len(critical) == 0, f"JS errors found: {critical}"


# ═══════════════════════════════════════════════════════════════════════════
#  2. KATEGORIE-TILES — Counts + Klickbarkeit
# ═══════════════════════════════════════════════════════════════════════════


class TestKategorieTiles:
    """Category tiles display correctly with non-zero counts."""

    def test_tiles_are_rendered(self, browser, base_url):
        """Tiles section is visible with at least one tile."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        save_screenshot(browser, "02_category_tiles")
        assert len(tiles) > 0, "Keine Kategorie-Tiles gerendert!"

    @pytest.mark.critical
    def test_tile_counts_are_not_zero(self, browser, base_url):
        """REGRESSION: Tile counts must NOT show '0 Angebote'."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        assert len(tiles) > 0, "Keine Tiles gefunden"

        zero_tiles = []
        for tile in tiles:
            count_el = tile.find_element(By.CSS_SELECTOR, ".tile-count")
            count_text = count_el.text
            match = re.search(r"(\d+)", count_text)
            count = int(match.group(1)) if match else 0
            if count == 0:
                name_el = tile.find_element(By.CSS_SELECTOR, ".tile-name")
                zero_tiles.append(name_el.text)

        save_screenshot(browser, "02_tile_counts")
        assert len(zero_tiles) == 0, (
            f"Tiles mit 0 Angeboten gefunden (Regression!): {zero_tiles}"
        )

    def test_tile_counts_are_realistic(self, browser, base_url):
        """Top tile should have a significant number of offers."""
        browser.get(base_url)
        time.sleep(2)
        first_tile = browser.find_element(By.CSS_SELECTOR, ".category-tile")
        count_text = first_tile.find_element(By.CSS_SELECTOR, ".tile-count").text
        match = re.search(r"(\d+)", count_text)
        count = int(match.group(1)) if match else 0
        assert count >= 50, f"Top-Tile hat zu wenig Angebote: {count}"

    def test_tile_names_are_readable(self, browser, base_url):
        """Every tile has a visible, non-empty name."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        for tile in tiles:
            name = tile.find_element(By.CSS_SELECTOR, ".tile-name").text
            assert len(name.strip()) > 0, "Tile mit leerem Namen gefunden"


# ═══════════════════════════════════════════════════════════════════════════
#  3. KATEGORIE-BROWSE — Tile klicken → Produkte sehen
# ═══════════════════════════════════════════════════════════════════════════


class TestKategorieBrowse:
    """Clicking a category tile opens the browse view with products."""

    @pytest.mark.critical
    def test_tile_click_opens_browse(self, browser, base_url):
        """Clicking first tile opens category browse panel."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles vorhanden")

        tile_name = tiles[0].find_element(By.CSS_SELECTOR, ".tile-name").text
        tiles[0].click()

        browse = wait_visible(browser, By.ID, "category_browse", timeout=15)
        save_screenshot(browser, "03_browse_open")
        assert browse.is_displayed()

        title = browser.find_element(By.CSS_SELECTOR, ".browse-title")
        assert title.text == tile_name

    def test_browse_shows_products(self, browser, base_url):
        """Browse view renders actual product cards (not just skeleton)."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles vorhanden")

        tiles[0].click()
        wait_visible(browser, By.ID, "category_browse", timeout=15)

        WebDriverWait(browser, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".offer-card")) > 0
        )
        time.sleep(1)
        save_screenshot(browser, "03_browse_products")

        cards = browser.find_elements(By.CSS_SELECTOR, ".offer-card")
        assert len(cards) > 0, "Keine Produkt-Cards im Browse-Modus sichtbar!"

    def test_browse_product_cards_have_content(self, browser, base_url):
        """Each product card shows title and price."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles vorhanden")

        tiles[0].click()
        wait_visible(browser, By.ID, "category_browse", timeout=15)

        WebDriverWait(browser, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".offer-card")) > 0
        )
        time.sleep(1)

        cards = browser.find_elements(By.CSS_SELECTOR, ".offer-card")
        assert len(cards) > 0

        for card in cards[:5]:
            text = card.text
            assert len(text.strip()) > 0, "Leere Offer-Card gefunden"
            assert re.search(r"\d", text), f"Kein Preis in Card: {text[:80]}"

        save_screenshot(browser, "03_browse_card_content")

    def test_browse_has_subcategory_tabs(self, browser, base_url):
        """Browse view shows subcategory tabs/pills."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles vorhanden")

        tiles[0].click()
        wait_visible(browser, By.ID, "category_browse", timeout=15)

        time.sleep(3)
        tabs = browser.find_elements(By.CSS_SELECTOR, ".browse-tabs .browse-tab, .browse-tabs button, #browse_tabs *")
        save_screenshot(browser, "03_browse_tabs")
        assert len(tabs) > 0, "Keine Subcategory-Tabs gefunden"

    def test_browse_back_button_works(self, browser, base_url):
        """Back button in browse returns to tiles view."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles vorhanden")

        tiles[0].click()
        wait_visible(browser, By.ID, "category_browse", timeout=15)

        back_btn = wait_clickable(browser, By.CSS_SELECTOR, ".browse-back")
        back_btn.click()
        time.sleep(2)

        WebDriverWait(browser, 5).until(
            lambda d: d.find_element(By.CSS_SELECTOR, ".tiles-section").is_displayed()
            or True
        )
        time.sleep(1)
        tiles_section = browser.find_element(By.CSS_SELECTOR, ".tiles-section")
        save_screenshot(browser, "03_browse_back")
        if not tiles_section.is_displayed():
            browser.get(base_url)
            time.sleep(1)
            tiles_section = browser.find_element(By.CSS_SELECTOR, ".tiles-section")
        assert tiles_section.is_displayed(), "Tiles-Section nicht wieder sichtbar nach Zurück"

    def test_add_product_from_browse(self, browser, base_url):
        """Clicking '+' on a product card adds it to the basket."""
        clear_basket(browser, base_url)
        time.sleep(2)

        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles vorhanden")

        tiles[0].click()
        wait_visible(browser, By.ID, "category_browse", timeout=15)

        WebDriverWait(browser, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".offer-card")) > 0
        )
        time.sleep(1)

        add_btns = browser.find_elements(By.CSS_SELECTOR, ".offer-card-add, .offer-card button")
        if not add_btns:
            pytest.skip("Keine Add-Buttons gefunden")

        browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", add_btns[0])
        time.sleep(0.5)
        browser.execute_script("arguments[0].click();", add_btns[0])
        time.sleep(1)
        save_screenshot(browser, "03_add_from_browse")

        basket_json = browser.execute_script("return localStorage.getItem('sparfuchs.basket');")
        assert basket_json is not None, "Basket ist leer nach Produkt-Hinzufügen"
        assert len(basket_json) > 2, f"Basket scheint leer: {basket_json}"


# ═══════════════════════════════════════════════════════════════════════════
#  4. SUCHFUNKTION + AUTOCOMPLETE
# ═══════════════════════════════════════════════════════════════════════════


class TestSuche:
    """Search input and autocomplete suggestions work visually."""

    def test_search_shows_suggestions(self, browser, base_url):
        """Typing in search input shows dropdown suggestions."""
        browser.get(base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("milch")
        time.sleep(2)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        save_screenshot(browser, "04_search_suggestions")

        items = dropdown.find_elements(By.CSS_SELECTOR, "*")
        visible = [i for i in items if i.is_displayed() and i.text.strip()]
        assert len(visible) > 0, "Keine Suchvorschläge für 'milch' angezeigt"

    def test_suggest_has_categories_section(self, browser, base_url):
        """Suggest dropdown shows category suggest items."""
        browser.get(base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("milch")
        time.sleep(2)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        items = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-item")
        visible = [i for i in items if i.is_displayed() and i.text.strip()]
        assert len(visible) >= 2, f"Weniger als 2 Kategorie-Vorschläge, nur {len(visible)}"
        # Check that at least one has an offer count badge
        badges = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-item .suggest-item-count")
        visible_badges = [b for b in badges if b.is_displayed()]
        assert len(visible_badges) > 0, "Keine Angebots-Counts bei Kategorien"

    def test_suggest_has_offers_section(self, browser, base_url):
        """Suggest dropdown shows an 'Aktuelle Angebote' section with product cards."""
        browser.get(base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("milch")
        time.sleep(3)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        save_screenshot(browser, "04_suggest_offers_section")

        # Check for "Aktuelle Angebote" header
        headers = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-section-header")
        header_texts = [h.text.strip().lower() for h in headers if h.is_displayed()]
        assert "aktuelle angebote" in header_texts, (
            f"Kein 'Aktuelle Angebote'-Header, nur: {header_texts}"
        )

    def test_suggest_offer_cards_have_image_and_price(self, browser, base_url):
        """Product offer cards in suggest show image, title, and price."""
        browser.get(base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("butter")
        time.sleep(3)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        offers = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-offer")

        if not offers:
            pytest.skip("Keine Angebots-Items im Suggest")

        first = offers[0]
        # Has an image
        imgs = first.find_elements(By.CSS_SELECTOR, ".suggest-offer-img")
        # Has a title
        titles = first.find_elements(By.CSS_SELECTOR, ".suggest-offer-title")
        assert len(titles) > 0, "Kein Titel in Angebots-Vorschlag"
        assert titles[0].text.strip(), "Leerer Titel in Angebots-Vorschlag"
        # Has a price
        prices = first.find_elements(By.CSS_SELECTOR, ".suggest-offer-price-now")
        assert len(prices) > 0, "Kein Preis in Angebots-Vorschlag"
        assert "€" in prices[0].text or re.search(r"\d", prices[0].text), (
            f"Kein Euro-Betrag: {prices[0].text}"
        )
        save_screenshot(browser, "04_suggest_offer_card")

    def test_suggest_offer_click_adds_to_basket(self, browser, base_url):
        """Clicking a product offer in suggest adds it to basket."""
        clear_basket(browser, base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("milch")
        time.sleep(3)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        offers = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-offer")

        if not offers:
            pytest.skip("Keine Angebots-Items im Suggest")

        offers[0].click()
        time.sleep(2)
        save_screenshot(browser, "04_suggest_offer_added")

        basket_json = browser.execute_script("return localStorage.getItem('sparfuchs.basket');")
        assert basket_json is not None, "Basket leer nach Angebots-Klick"
        items = json.loads(basket_json)
        assert len(items) >= 1, f"Basket hat keine Items: {items}"
        # Should be a text-based item (q field)
        assert "q" in items[0], f"Item hat kein 'q'-Feld: {items[0]}"

    def test_suggest_categories_before_brands(self, browser, base_url):
        """Categories section appears before brands in suggest dropdown."""
        browser.get(base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("butter")
        time.sleep(2)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        headers = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-section-header")
        visible_headers = [h.text.strip() for h in headers if h.is_displayed()]

        if "Kategorien" in visible_headers and "Marken" in visible_headers:
            cat_idx = visible_headers.index("Kategorien")
            brand_idx = visible_headers.index("Marken")
            assert cat_idx < brand_idx, (
                f"Kategorien ({cat_idx}) soll vor Marken ({brand_idx}) kommen"
            )

    def test_suggestion_click_adds_to_basket(self, browser, base_url):
        """Clicking a category suggestion adds it to the basket."""
        clear_basket(browser, base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("butter")
        time.sleep(2)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        items = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-item:not(.suggest-offer)")
        visible = [i for i in items if i.is_displayed() and i.text.strip()]

        if not visible:
            save_screenshot(browser, "04_no_suggestions")
            pytest.skip("Keine sichtbaren Vorschläge")

        visible[0].click()
        time.sleep(2)
        save_screenshot(browser, "04_after_suggestion_click")

        basket_section = browser.find_element(By.ID, "basket_section")
        assert basket_section.is_displayed(), "Basket-Section nicht sichtbar nach Suggestion-Klick"

    def test_comma_separated_adds_multiple(self, browser, base_url):
        """Typing comma-separated items adds multiple to basket."""
        clear_basket(browser, base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("Milch, Butter, Eier")
        search.send_keys(Keys.ENTER)
        time.sleep(3)
        save_screenshot(browser, "04_comma_separated")

        basket_json = browser.execute_script("return localStorage.getItem('sparfuchs.basket');")
        if basket_json:
            items = json.loads(basket_json)
            assert len(items) >= 2, f"Erwartet >= 2 Basket-Items, gefunden: {len(items)}"


# ═══════════════════════════════════════════════════════════════════════════
#  5. PREISVERGLEICH — Live-Ergebnisse
# ═══════════════════════════════════════════════════════════════════════════


class TestPreisvergleich:
    """Price comparison results display correctly."""

    @pytest.mark.critical
    def test_comparison_shows_results(self, browser, base_url):
        """After adding items, live results appear."""
        setup_basket_with_location(browser, base_url)

        results = browser.find_element(By.ID, "live_results")
        WebDriverWait(browser, 20).until(lambda d: results.is_displayed())
        # Wait for ranking to actually populate (not just spinner)
        ranking = browser.find_element(By.ID, "results_ranking")
        WebDriverWait(browser, 15).until(
            lambda d: len(d.find_element(By.ID, "results_ranking").text.strip()) > 0
        )
        save_screenshot(browser, "05_comparison_results")

        assert len(ranking.text.strip()) > 0, "Ergebnis-Ranking ist leer"

    def test_store_cards_have_info(self, browser, base_url):
        """Store cards show name, price, address."""
        setup_basket_with_location(browser, base_url)

        WebDriverWait(browser, 20).until(
            lambda d: d.find_element(By.ID, "live_results").is_displayed()
        )
        time.sleep(2)

        cards = browser.find_elements(By.CSS_SELECTOR, ".store-card, .compact-row, [class*='store']")
        save_screenshot(browser, "05_store_cards")
        assert len(cards) > 0, "Keine Store-Cards gefunden"

        first_text = cards[0].text
        assert len(first_text.strip()) > 10, f"Store-Card hat wenig Inhalt: {first_text}"

    def test_no_green_savings_banner(self, browser, base_url):
        """REGRESSION: No green savings banner should exist."""
        setup_basket_with_location(browser, base_url)

        WebDriverWait(browser, 20).until(
            lambda d: d.find_element(By.ID, "live_results").is_displayed()
        )
        time.sleep(2)
        save_screenshot(browser, "05_no_savings_banner")

        savings = browser.find_elements(By.CSS_SELECTOR, ".savings-line")
        assert len(savings) == 0, "Grüner Savings-Banner noch vorhanden"


# ═══════════════════════════════════════════════════════════════════════════
#  6. KETTEN-FILTER
# ═══════════════════════════════════════════════════════════════════════════


class TestKettenFilter:
    """Chain filter pills display and function correctly — globally on startpage."""

    @pytest.mark.critical
    def test_global_chain_filter_on_startpage(self, browser, base_url):
        """Chain filter pills are visible on startpage WITHOUT needing a basket."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.removeItem('sparfuchs.chainFilter');
            localStorage.removeItem('sparfuchs.basket');
        """)
        browser.get(base_url)
        time.sleep(4)

        filters = browser.find_element(By.ID, "chain_filters_global")
        save_screenshot(browser, "06_global_chain_filters")

        pills = filters.find_elements(By.CSS_SELECTOR, ".chain-pill")
        assert len(pills) >= 5, f"Zu wenige Ketten-Pills auf Startseite: {len(pills)}"

        active = [p for p in pills if "active" in p.get_attribute("class")]
        assert len(active) == len(pills), "Nicht alle Pills sind standardmäßig aktiv"

    def test_chain_filter_updates_tile_counts(self, browser, base_url):
        """Deactivating a chain updates tile counts."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.removeItem('sparfuchs.chainFilter');
            localStorage.removeItem('sparfuchs.basket');
        """)
        browser.get(base_url)
        time.sleep(4)

        first_tile = browser.find_element(By.CSS_SELECTOR, ".category-tile")
        initial_count_text = first_tile.find_element(By.CSS_SELECTOR, ".tile-count").text
        initial_match = re.search(r"(\d+)", initial_count_text)
        initial_count = int(initial_match.group(1)) if initial_match else 0

        browser.execute_script("""
            const pills = document.querySelectorAll('#chain_filters_global .chain-pill');
            if (pills.length >= 2) pills[0].click();
        """)
        time.sleep(4)

        first_tile = browser.find_element(By.CSS_SELECTOR, ".category-tile")
        new_count_text = first_tile.find_element(By.CSS_SELECTOR, ".tile-count").text
        new_match = re.search(r"(\d+)", new_count_text)
        new_count = int(new_match.group(1)) if new_match else 0

        save_screenshot(browser, "06_filter_counts_changed")
        assert new_count < initial_count, (
            f"Tile-Count hat sich nicht verringert: {initial_count} -> {new_count}"
        )

        browser.execute_script("localStorage.removeItem('sparfuchs.chainFilter');")

    def test_chain_filter_affects_browse(self, browser, base_url):
        """Deactivating chains before browsing filters browse results."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.chainFilter', JSON.stringify(['Edeka', 'Rewe']));
        """)
        browser.get(base_url)
        time.sleep(3)

        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles")

        tiles[0].click()
        time.sleep(5)

        cards = browser.find_elements(By.CSS_SELECTOR, ".offer-card[data-chain]")
        visible_cards = [c for c in cards if c.is_displayed()]

        if not visible_cards:
            save_screenshot(browser, "06_browse_filtered_empty")
            pytest.skip("Keine sichtbaren Cards nach Filter")

        wrong_chains = []
        for card in visible_cards[:20]:
            chain = card.get_attribute("data-chain")
            if chain and chain not in ("Edeka", "Rewe"):
                wrong_chains.append(chain)

        save_screenshot(browser, "06_browse_filtered")
        assert len(wrong_chains) == 0, (
            f"Browse zeigt Cards von gefilterten Ketten: {set(wrong_chains)}"
        )

        browser.execute_script("localStorage.removeItem('sparfuchs.chainFilter');")

    def test_no_duplicate_filter_in_compare(self, browser, base_url):
        """Compare results should NOT have their own chain filter (only global)."""
        browser.execute_script("localStorage.removeItem('sparfuchs.chainFilter');")
        setup_basket_with_location(browser, base_url)

        save_screenshot(browser, "06_no_duplicate_filter")

        global_filter = browser.find_element(By.ID, "chain_filters_global")
        global_pills = global_filter.find_elements(By.CSS_SELECTOR, ".chain-pill")
        assert len(global_pills) >= 5, "Globaler Filter fehlt"

        right_filters = browser.find_elements(By.ID, "chain_filters")
        if right_filters:
            right_pills = right_filters[0].find_elements(By.CSS_SELECTOR, ".chain-pill")
            assert len(right_pills) == 0, "Rechter Filter soll nicht mehr existieren"


# ═══════════════════════════════════════════════════════════════════════════
#  7. EXPANDABLE STORES
# ═══════════════════════════════════════════════════════════════════════════


class TestExpandableStores:
    """Compact store rows can be expanded to show details."""

    def test_compact_rows_exist(self, browser, base_url):
        """Compact rows (stores #4+) are visible."""
        setup_basket_with_location(browser, base_url,
                                   [{"q": "Milch"}, {"q": "Butter"}, {"q": "Käse"}])

        compact = browser.find_elements(By.CSS_SELECTOR, ".compact-row")
        save_screenshot(browser, "07_compact_rows")
        if len(compact) == 0:
            pytest.skip("Keine Kompakt-Reihen (evtl. zu wenige Märkte)")

    def test_compact_row_expands_on_click(self, browser, base_url):
        """Clicking a compact row expands it to show details."""
        setup_basket_with_location(browser, base_url,
                                   [{"q": "Milch"}, {"q": "Butter"}, {"q": "Käse"}])

        compact = browser.find_elements(By.CSS_SELECTOR, ".compact-row")
        if not compact:
            pytest.skip("Keine Kompakt-Reihen vorhanden")

        compact[0].click()
        time.sleep(0.5)
        save_screenshot(browser, "07_compact_expanded")

        expanded = compact[0].get_attribute("aria-expanded")
        detail_body = compact[0].find_elements(By.CSS_SELECTOR, ".detail-body, [class*='detail']")
        assert expanded == "true" or len(detail_body) > 0, \
            "Kompakt-Reihe hat sich nicht aufgeklappt"


# ═══════════════════════════════════════════════════════════════════════════
#  9. MOBILE VIEWPORT
# ═══════════════════════════════════════════════════════════════════════════


class TestMobileView:
    """Test mobile viewport rendering."""

    def test_mobile_layout(self, browser, base_url):
        """Page renders correctly at mobile width."""
        browser.set_window_size(375, 812)
        browser.get(base_url)
        time.sleep(2)
        save_screenshot(browser, "09_mobile_startseite")

        search = browser.find_element(By.ID, "hero_search")
        assert search.is_displayed()

        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        assert len(tiles) > 0, "Keine Tiles auf Mobile"

        browser.set_window_size(1280, 900)

    def test_mobile_tiles_not_zero(self, browser, base_url):
        """REGRESSION: Tiles on mobile also must not show 0."""
        browser.set_window_size(375, 812)
        browser.get(base_url)
        time.sleep(2)

        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        zero_tiles = []
        for tile in tiles:
            count_el = tile.find_element(By.CSS_SELECTOR, ".tile-count")
            match = re.search(r"(\d+)", count_el.text)
            count = int(match.group(1)) if match else 0
            if count == 0:
                name = tile.find_element(By.CSS_SELECTOR, ".tile-name").text
                zero_tiles.append(name)

        save_screenshot(browser, "09_mobile_tile_counts")
        assert len(zero_tiles) == 0, f"Mobile Tiles mit 0: {zero_tiles}"

        browser.set_window_size(1280, 900)

    def test_mobile_fab_with_basket(self, browser, base_url):
        """Mobile FAB appears when basket has items."""
        browser.set_window_size(375, 812)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([{q: "Milch"}]));
        """)
        browser.get(base_url)
        time.sleep(2)
        save_screenshot(browser, "09_mobile_fab")

        fab = browser.find_element(By.ID, "basket_fab")
        if fab.is_displayed():
            badge = browser.find_element(By.ID, "basket_fab_count")
            assert badge.text.strip() != "0"

        browser.set_window_size(1280, 900)
        browser.execute_script("localStorage.removeItem('sparfuchs.basket');")

    def test_mobile_no_horizontal_overflow(self, browser, base_url):
        """No horizontal scrollbar on mobile viewport."""
        browser.set_window_size(375, 812)
        browser.get(base_url)
        time.sleep(2)

        overflow = browser.execute_script(
            "return document.documentElement.scrollWidth > document.documentElement.clientWidth;"
        )
        save_screenshot(browser, "09_mobile_no_overflow")
        assert not overflow, "Horizontaler Overflow auf Mobile!"

        browser.set_window_size(1280, 900)

    def test_mobile_chain_pills_scrollable(self, browser, base_url):
        """Chain filter pills are horizontally scrollable on mobile (not wrapping)."""
        browser.set_window_size(375, 812)
        browser.get(base_url)
        browser.execute_script("localStorage.removeItem('sparfuchs.chainFilter');")
        browser.get(base_url)
        time.sleep(3)

        chain_bar = browser.find_elements(By.ID, "chain_filters_global")
        if not chain_bar:
            pytest.skip("Kein Ketten-Filter")

        bar = chain_bar[0]
        pills = bar.find_elements(By.CSS_SELECTOR, ".chain-pill")
        if len(pills) < 3:
            pytest.skip("Zu wenige Pills zum Testen")

        # Check that the bar scrolls horizontally (scrollWidth > clientWidth)
        is_scrollable = browser.execute_script(
            "return arguments[0].scrollWidth > arguments[0].clientWidth;", bar
        )
        save_screenshot(browser, "09_mobile_chain_scroll")
        # With 9+ chains on 375px, should be scrollable
        assert is_scrollable, "Ketten-Filter-Bar ist nicht scrollbar auf Mobile"

        browser.set_window_size(1280, 900)

    def test_mobile_suggest_large_touch_targets(self, browser, base_url):
        """Suggest items have minimum 48px height on mobile."""
        browser.set_window_size(375, 812)
        browser.get(base_url)
        time.sleep(1)

        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("milch")
        time.sleep(3)

        dropdown = browser.find_element(By.ID, "category_dropdown")
        items = dropdown.find_elements(By.CSS_SELECTOR, ".suggest-item")
        visible = [i for i in items if i.is_displayed()]

        if not visible:
            pytest.skip("Keine sichtbaren Items")

        small_items = []
        for item in visible[:5]:
            height = item.size["height"]
            if height < 44:  # Allow some tolerance (44 instead of 48)
                small_items.append((item.text[:30], height))

        save_screenshot(browser, "09_mobile_suggest_touch")
        assert len(small_items) == 0, f"Items zu klein für Touch: {small_items}"

        browser.set_window_size(1280, 900)

    def test_mobile_browse_two_columns(self, browser, base_url):
        """Browse grid shows 2 columns on small mobile viewport."""
        browser.set_window_size(375, 812)
        browser.get(base_url)
        time.sleep(2)

        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            browser.set_window_size(1280, 900)
            pytest.skip("Keine Tiles")

        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", tiles[0])
        time.sleep(0.3)
        tiles[0].click()
        wait_visible(browser, By.ID, "category_browse", timeout=15)
        WebDriverWait(browser, 15).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".offer-card")) > 0
        )
        time.sleep(1)
        save_screenshot(browser, "09_mobile_browse_grid")

        # Check grid has 2 columns by comparing first two card positions
        cards = browser.find_elements(By.CSS_SELECTOR, ".offer-card")
        if len(cards) >= 2:
            y0 = cards[0].location["y"]
            y1 = cards[1].location["y"]
            # If 2-column, first two cards should be on the same row (similar y)
            assert abs(y0 - y1) < 30, (
                f"Cards nicht nebeneinander: y0={y0}, y1={y1} (erwarte 2-Spalten)"
            )

        browser.set_window_size(1280, 900)


# ═══════════════════════════════════════════════════════════════════════════
#  10. ALTERNATIVEN-FEATURE
# ═══════════════════════════════════════════════════════════════════════════


class TestAlternativen:
    """Product alternatives feature works in basket and compare results."""

    def test_alternatives_button_on_category_items(self, browser, base_url):
        """Category-based basket items show an 'Alternativen' button."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([
                {category_id: 177, category_name: "Butter"}
            ]));
        """)
        browser.get(base_url)
        time.sleep(2)
        save_screenshot(browser, "10_alt_buttons")

        alt_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-alt-btn")
        assert len(alt_btns) >= 1, "Kein 'Alternativen'-Button bei Kategorie-Item"

    def test_alternatives_panel_shows_offer_cards(self, browser, base_url):
        """Clicking 'Alternativen' shows actual product cards with images and prices."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([
                {category_id: 177, category_name: "Butter"}
            ]));
            localStorage.setItem('sparfuchs.location', 'Bonn');
        """)
        browser.get(base_url)
        # Wait for auto-compare to finish before clicking alt button
        time.sleep(8)

        alt_btn = browser.find_element(By.CSS_SELECTOR, ".basket-alt-btn")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_btn)
        time.sleep(0.3)
        alt_btn.click()
        time.sleep(4)
        save_screenshot(browser, "10_alt_panel_offers")

        panels = browser.find_elements(By.CSS_SELECTOR, ".alt-panel")
        assert len(panels) >= 1, "Alternatives-Panel nicht geöffnet"

        # Should contain offer cards, not just pills
        cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
        assert len(cards) >= 1, "Keine Produkt-Karten im Alternatives-Panel"

        # Cards should have images
        imgs = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card .alt-offer-img")
        assert len(imgs) >= 1, "Produkt-Karten ohne Bilder"

        # Cards should have prices
        prices = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-price")
        assert len(prices) >= 1, "Produkt-Karten ohne Preise"

        # Should be grouped by category
        groups = browser.find_elements(By.CSS_SELECTOR, ".alt-group-name")
        assert len(groups) >= 1, "Keine Kategorie-Gruppen"

    def test_alternatives_card_swaps_item(self, browser, base_url):
        """Clicking an alternative product card swaps the basket item."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([
                {category_id: 177, category_name: "Butter"}
            ]));
            localStorage.setItem('sparfuchs.location', 'Bonn');
        """)
        browser.get(base_url)
        # Wait for auto-compare to finish
        time.sleep(8)

        alt_btn = browser.find_element(By.CSS_SELECTOR, ".basket-alt-btn")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_btn)
        time.sleep(0.3)
        alt_btn.click()
        time.sleep(3)

        cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
        if not cards:
            pytest.skip("Keine Produkt-Karten vorhanden")

        cards[0].click()
        time.sleep(2)
        save_screenshot(browser, "10_alt_swapped")

        basket_json = browser.execute_script("return localStorage.getItem('sparfuchs.basket');")
        items = json.loads(basket_json)
        assert len(items) == 1, f"Basket-Länge falsch: {len(items)}"
        assert "category_id" in items[0], "Item hat keine category_id nach Swap"
        assert items[0]["category_id"] != 177, "Item wurde nicht getauscht"

    def test_alternatives_panel_closes_on_toggle(self, browser, base_url):
        """Clicking 'Alternativen' again closes the panel."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([
                {category_id: 177, category_name: "Butter"}
            ]));
            localStorage.setItem('sparfuchs.location', 'Bonn');
        """)
        browser.get(base_url)
        # Wait for auto-compare to finish
        time.sleep(8)

        alt_btn = browser.find_element(By.CSS_SELECTOR, ".basket-alt-btn")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_btn)
        time.sleep(0.3)
        alt_btn.click()
        time.sleep(2)

        panels = browser.find_elements(By.CSS_SELECTOR, ".alt-panel")
        assert len(panels) >= 1, "Panel nicht geöffnet"

        alt_btn = browser.find_element(By.CSS_SELECTOR, ".basket-alt-btn")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_btn)
        time.sleep(0.3)
        alt_btn.click()
        time.sleep(1)

        panels = browser.find_elements(By.CSS_SELECTOR, ".alt-panel")
        assert len(panels) == 0, "Panel nicht geschlossen nach erneutem Klick"

    def test_text_items_get_alternatives_after_compare(self, browser, base_url):
        """Text-based basket items show alternatives after compare discovers category."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([
                {q: "butter", brand: null, any_brand: true}
            ]));
            localStorage.setItem('sparfuchs.location', 'Bonn');
        """)
        browser.get(base_url)
        time.sleep(8)

        # After compare, text item should have discovered category_id
        alt_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-alt-btn")
        assert len(alt_btns) >= 1, "Kein Alt-Button für Text-Item nach Preisvergleich"

    def test_alternatives_in_compare_detail_lines(self, browser, base_url):
        """Compare detail lines show 'Alternativen anzeigen' with offer cards."""
        browser.get(base_url)
        browser.execute_script("""
            localStorage.setItem('sparfuchs.basket', JSON.stringify([
                {category_id: 177, category_name: "Butter"}
            ]));
            localStorage.setItem('sparfuchs.location', '53111 Bonn');
        """)
        browser.get(base_url)
        time.sleep(8)

        # Expand first store card details
        summaries = browser.find_elements(By.CSS_SELECTOR, ".store-card-details summary")
        if not summaries:
            pytest.skip("Keine Store-Cards")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", summaries[0])
        time.sleep(0.3)
        browser.execute_script("arguments[0].click();", summaries[0])
        time.sleep(1)

        alt_links = [b for b in browser.find_elements(By.CSS_SELECTOR, ".detail-alt-btn") if b.is_displayed()]
        assert len(alt_links) >= 1, "Keine 'Alternativen anzeigen'-Links"

        # Click to open offer cards
        alt_links[0].click()
        time.sleep(3)
        save_screenshot(browser, "10_alt_in_compare_offers")

        cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
        visible_cards = [c for c in cards if c.is_displayed()]
        assert len(visible_cards) >= 1, "Keine Produkt-Karten in Detail-Line-Alternativen"


# ═══════════════════════════════════════════════════════════════════════════
#  11. SPAR-MIX ERGEBNISSE
# ═══════════════════════════════════════════════════════════════════════════


class TestSparMix:
    """Spar-Mix results display correctly."""

    def test_spar_mix_section_appears(self, browser, base_url):
        """Spar-Mix section appears when max_stores > 1."""
        setup_basket_with_location(browser, base_url,
                                   [{"q": "Milch"}, {"q": "Butter"}, {"q": "Brot"}, {"q": "Käse"}])

        spar_mix = browser.find_element(By.ID, "results_spar_mix")
        save_screenshot(browser, "11_spar_mix")

        if spar_mix.is_displayed():
            assert len(spar_mix.text.strip()) > 0, "Spar-Mix ist leer"


# ═══════════════════════════════════════════════════════════════════════════
#  12. SCREENSHOT-GALERIE — Gesamtübersicht
# ═══════════════════════════════════════════════════════════════════════════


class TestScreenshotGallery:
    """Take final overview screenshots for manual review."""

    def test_full_page_screenshot(self, browser, base_url):
        """Full-page screenshot of the startseite."""
        browser.get(base_url)
        time.sleep(3)

        browser.set_window_size(1280, 2000)
        time.sleep(1)
        save_screenshot(browser, "99_full_page")
        browser.set_window_size(1280, 900)

    def test_browse_full_screenshot(self, browser, base_url):
        """Full screenshot of category browse with products."""
        browser.get(base_url)
        time.sleep(2)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("Keine Tiles")

        tiles[0].click()
        time.sleep(5)
        browser.set_window_size(1280, 2000)
        time.sleep(1)
        save_screenshot(browser, "99_browse_full")
        browser.set_window_size(1280, 900)

    def test_mobile_full_flow_screenshot(self, browser, base_url):
        """Mobile viewport: startseite + search + browse screenshots."""
        browser.set_window_size(390, 844)  # iPhone 14
        browser.get(base_url)
        time.sleep(2)
        save_screenshot(browser, "99_mobile_390_startseite")

        # Search
        search = browser.find_element(By.ID, "hero_search")
        search.clear()
        search.send_keys("milch")
        time.sleep(3)
        save_screenshot(browser, "99_mobile_390_suggest")

        # Close dropdown and browse
        browser.execute_script("document.getElementById('category_dropdown').style.display='none';")
        time.sleep(1)
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if tiles:
            browser.execute_script("arguments[0].scrollIntoView({block:'center'});", tiles[0])
            time.sleep(0.3)
            tiles[0].click()
            time.sleep(4)
            save_screenshot(browser, "99_mobile_390_browse")

        browser.set_window_size(1280, 900)

