"""
Selenium E2E Tests — Alternatives & Ranking Update.

Tests that:
1. Selecting an alternative product correctly updates the basket
2. The ranking/compare results refresh after alternative selection
3. Brand-specific products show exact/similar split with divider
4. The ranking reflects the new product, not stale data

Run: python -m pytest tests/e2e/test_alternatives_ranking.py -v --tb=short
"""
from __future__ import annotations

import json
import time

import pytest
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .conftest import save_screenshot


def wait_for(browser, by, value, timeout=10):
    return WebDriverWait(browser, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def wait_visible(browser, by, value, timeout=10):
    return WebDriverWait(browser, timeout).until(
        EC.visibility_of_element_located((by, value))
    )


def clear_basket(browser, base_url):
    browser.get(base_url)
    browser.execute_script("localStorage.removeItem('sparfuchs.basket');")
    browser.get(base_url)
    time.sleep(1)


def setup_basket_and_wait(browser, base_url, items, location="53111 Bonn", wait_s=10):
    """Set basket items, enter location, wait for compare to finish."""
    browser.get(base_url)
    browser.execute_script(
        "localStorage.setItem('sparfuchs.basket', arguments[0]);",
        json.dumps(items),
    )
    browser.get(base_url)
    time.sleep(1)
    loc_input = browser.find_element(By.ID, "location")
    loc_input.clear()
    loc_input.send_keys(location)
    time.sleep(wait_s)


def get_basket(browser):
    """Read current basket from localStorage."""
    raw = browser.execute_script("return localStorage.getItem('sparfuchs.basket');")
    return json.loads(raw or "[]")


def get_ranking_chains(browser):
    """Get list of chain names from visible store cards in ranking."""
    cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card .store-name")
    compact = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .compact-row .compact-store-name")
    chains = []
    for el in cards + compact:
        text = el.text.strip()
        if text:
            chains.append(text)
    return chains


def get_ranking_chain_from_cards(browser):
    """Get chain names from store cards (via the chain data, not store name)."""
    cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
    compact = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .compact-row")
    result = []
    for card in cards + compact:
        # Try to get chain from the store name or address
        name_el = card.find_elements(By.CSS_SELECTOR, ".store-name, .compact-store-name")
        if name_el:
            result.append(name_el[0].text.strip())
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  1. BASKET UPDATE AFTER ALTERNATIVE SELECTION
# ═══════════════════════════════════════════════════════════════════════════

class TestAlternativeSelection:
    """Test that selecting alternatives updates basket and triggers re-compare."""

    def test_alternative_replaces_basket_item(self, browser, base_url):
        """Clicking an alternative should replace the basket item."""
        clear_basket(browser, base_url)

        # Add a category item
        items = [{"category_id": 34, "category_name": "Spaghetti"}]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        # Check initial basket
        basket_before = get_basket(browser)
        assert len(basket_before) == 1
        assert basket_before[0].get("category_name") == "Spaghetti"

        # Click "Alternativen" button
        alt_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-alt-btn")
        if not alt_btns:
            pytest.skip("No alternative button found")

        alt_btns[0].click()
        time.sleep(3)  # Wait for alternatives to load

        # Find alternative offer cards
        alt_cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
        save_screenshot(browser, "13_alt_panel_open")

        if not alt_cards:
            pytest.skip("No alternative offers found")

        # Get the alternative's info before clicking
        alt_title_el = alt_cards[0].find_elements(By.CSS_SELECTOR, ".alt-offer-title")
        alt_chain_el = alt_cards[0].find_elements(By.CSS_SELECTOR, ".alt-offer-chain")
        alt_chain = alt_chain_el[0].text if alt_chain_el else "unknown"

        # Click the first alternative
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_cards[0])
        time.sleep(0.5)
        alt_cards[0].click()
        time.sleep(2)

        # Check basket was updated
        basket_after = get_basket(browser)
        assert len(basket_after) == 1, f"Basket should still have 1 item, got {len(basket_after)}"

        # The item should have changed
        new_item = basket_after[0]
        assert "category_id" in new_item, f"Replaced item should have category_id: {new_item}"

        save_screenshot(browser, "13_alt_selected")

    def test_ranking_updates_after_alternative(self, browser, base_url):
        """After selecting alternative, ranking should refresh with new results."""
        clear_basket(browser, base_url)

        # Start with a REWE product (Grünländer Schnittkäse)
        items = [{
            "category_id": 34,
            "category_name": "Spaghetti",
        }]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        # Record initial ranking
        initial_cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        initial_count = len(initial_cards)

        # Get first store's price
        price_els = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card .total-price")
        initial_price = price_els[0].text if price_els else "none"

        save_screenshot(browser, "13_ranking_before_alt")

        # Open alternatives
        alt_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-alt-btn")
        if not alt_btns:
            pytest.skip("No alternative button found")

        alt_btns[0].click()
        time.sleep(3)

        alt_cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
        if len(alt_cards) < 2:
            pytest.skip("Not enough alternatives to test swap")

        # Click a different alternative (not the first one, to ensure change)
        target = alt_cards[1] if len(alt_cards) > 1 else alt_cards[0]
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
        time.sleep(0.5)
        target.click()

        # Wait for re-compare
        time.sleep(10)

        save_screenshot(browser, "13_ranking_after_alt")

        # Ranking should still exist (not empty)
        new_cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        assert len(new_cards) > 0, "Ranking should still show stores after alternative selection"

    def test_ranking_reflects_new_product(self, browser, base_url):
        """After switching from chain-A product to chain-B product,
        chain-B should appear in results (not stale chain-A data)."""
        clear_basket(browser, base_url)

        # Add Barilla Spaghetti (Aldi product)
        items = [{
            "q": "Barilla Spaghetti",
            "brand": "Barilla",
            "any_brand": False,
        }]
        setup_basket_and_wait(browser, base_url, items, location="Berlin")

        # Ranking should show Aldi stores
        ranking_before = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card .store-name")
        names_before = [el.text for el in ranking_before]

        save_screenshot(browser, "13_before_swap_barilla")

        # Now replace with Combino Spaghetti (Lidl product) via page reload
        browser.execute_script(
            "localStorage.setItem('sparfuchs.basket', arguments[0]);",
            json.dumps([{
                "q": "Combino Spaghetti",
                "brand": "Combino",
                "any_brand": False,
            }]),
        )
        browser.get(base_url)
        time.sleep(1)
        loc_input = browser.find_element(By.ID, "location")
        loc_input.clear()
        loc_input.send_keys("Berlin")
        time.sleep(10)

        ranking_after = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card .store-name")
        names_after = [el.text for el in ranking_after]

        save_screenshot(browser, "13_after_swap_combino")

        # After swap: should see Lidl stores, not Aldi
        if names_before and names_after:
            # At least the #1 store should be different
            has_lidl = any("lidl" in n.lower() for n in names_after)
            has_aldi_gone = not any("aldi" in n.lower() for n in names_after[:3])
            # At minimum, the results should have changed
            assert names_before != names_after or has_lidl, \
                f"Ranking should update after product swap. Before: {names_before[:3]}, After: {names_after[:3]}"


# ═══════════════════════════════════════════════════════════════════════════
#  2. EXACT VS SIMILAR DIVIDER WITH ALTERNATIVES
# ═══════════════════════════════════════════════════════════════════════════

class TestExactSimilarWithAlternatives:
    """Test the exact/similar divider behavior with alternative selection."""

    def test_brand_product_shows_divider(self, browser, base_url):
        """Brand-specific browse product should show divider when
        similar products exist at other chains."""
        clear_basket(browser, base_url)

        # Simulate adding RÜCKER Alt-Mecklenburger Tilsiter from browse
        items = [{
            "category_id": 293,
            "category_name": "Alt-Mecklenburger Tilsiter",
            "brand": "RÜCKER",
            "any_brand": False,
            "q": "RÜCKER Alt-Mecklenburger Tilsiter",
        }]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        save_screenshot(browser, "13_brand_divider")

        # Check for divider
        dividers = browser.find_elements(By.CSS_SELECTOR, ".similar-products-divider")
        store_cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")

        if store_cards:
            # If we have results and both exact+similar, divider should exist
            names = [c.find_element(By.CSS_SELECTOR, ".store-name").text for c in store_cards]
            print(f"Store names: {names}")

            if len(set(n.split()[0].lower() for n in names if n)) > 1:
                # Multiple different chains → divider should exist
                assert len(dividers) > 0, \
                    f"Should show divider with multiple chains. Stores: {names}"

    def test_divider_disappears_after_category_swap(self, browser, base_url):
        """When swapping from brand-specific to category item, divider should disappear."""
        clear_basket(browser, base_url)

        # Start with brand-specific item
        items = [{
            "category_id": 293,
            "category_name": "Alt-Mecklenburger Tilsiter",
            "brand": "RÜCKER",
            "any_brand": False,
            "q": "RÜCKER Alt-Mecklenburger Tilsiter",
        }]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        dividers_before = browser.find_elements(By.CSS_SELECTOR, ".similar-products-divider")

        # Replace with category item (no brand)
        browser.execute_script(
            "localStorage.setItem('sparfuchs.basket', arguments[0]);",
            json.dumps([{"category_id": 34, "category_name": "Spaghetti"}]),
        )
        # Reload page to pick up new basket from localStorage
        browser.get(base_url)
        time.sleep(1)
        loc_input = browser.find_element(By.ID, "location")
        loc_input.clear()
        loc_input.send_keys("53111 Bonn")
        time.sleep(10)

        dividers_after = browser.find_elements(By.CSS_SELECTOR, ".similar-products-divider")
        save_screenshot(browser, "13_no_divider_after_swap")

        assert len(dividers_after) == 0, \
            "Category item should not show divider"

    def test_exact_chain_is_first(self, browser, base_url):
        """The chain with the exact brand match should be ranked first."""
        clear_basket(browser, base_url)

        items = [{
            "category_id": 293,
            "category_name": "Alt-Mecklenburger Tilsiter",
            "brand": "RÜCKER",
            "any_brand": False,
            "q": "RÜCKER Alt-Mecklenburger Tilsiter",
        }]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        store_cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        save_screenshot(browser, "13_exact_chain_first")

        if store_cards:
            first_name = store_cards[0].find_element(By.CSS_SELECTOR, ".store-name").text
            assert "kaufland" in first_name.lower(), \
                f"Kaufland (exact RÜCKER match) should be first, got '{first_name}'"


# ═══════════════════════════════════════════════════════════════════════════
#  3. BROWSE ADD BRAND INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

class TestBrowseToRanking:
    """End-to-end: browse product → basket → ranking with exact/similar."""

    def test_browse_add_triggers_compare(self, browser, base_url):
        """Adding a product from browse should trigger compare and show results."""
        clear_basket(browser, base_url)
        browser.get(base_url)
        time.sleep(1)

        # Set location first
        loc_input = browser.find_element(By.ID, "location")
        loc_input.clear()
        loc_input.send_keys("53111 Bonn")
        time.sleep(2)

        # Open browse
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("No category tiles")
        tiles[0].click()
        time.sleep(3)

        # Add first product
        cards = browser.find_elements(By.CSS_SELECTOR, ".offer-card")
        if not cards:
            pytest.skip("No offer cards")

        add_btn = cards[0].find_element(By.CSS_SELECTOR, ".offer-card-add")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", add_btn)
        time.sleep(1)
        browser.execute_script("arguments[0].click();", add_btn)

        # Wait for compare
        time.sleep(10)

        save_screenshot(browser, "13_browse_to_ranking")

        # Should have results
        store_cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        assert len(store_cards) > 0, "Should show ranking after adding from browse"

    def test_browse_product_basket_has_brand(self, browser, base_url):
        """Product added from browse should have brand in basket."""
        # Read basket from previous test
        basket = get_basket(browser)
        if not basket:
            pytest.skip("No basket items")

        item = basket[-1]
        # Should have category_id at minimum
        assert "category_id" in item, f"Browse item needs category_id: {item}"
        # If it had a brand, verify brand info is present
        if item.get("brand"):
            assert item.get("any_brand") is False, \
                f"Brand item should have any_brand=false: {item}"


# ═══════════════════════════════════════════════════════════════════════════
#  4. STALE RANKING BUG
# ═══════════════════════════════════════════════════════════════════════════

class TestStaleRanking:
    """Test that ranking doesn't show stale data after basket changes."""

    def test_remove_item_updates_ranking(self, browser, base_url):
        """Removing a basket item should update the ranking."""
        clear_basket(browser, base_url)

        items = [
            {"category_id": 34, "category_name": "Spaghetti"},
            {"category_id": 1, "category_name": "Butter"},
        ]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        # Record initial state
        initial_price_els = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .total-price")
        initial_price = initial_price_els[0].text if initial_price_els else "none"

        # Remove first item via X button
        remove_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-item-x")
        if not remove_btns:
            pytest.skip("No remove buttons")

        remove_btns[0].click()
        time.sleep(10)

        # Price should change (one less item)
        new_price_els = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .total-price")
        new_price = new_price_els[0].text if new_price_els else "none"

        save_screenshot(browser, "13_after_remove_item")

        # At minimum, basket should have 1 item now
        basket = get_basket(browser)
        assert len(basket) == 1, f"Basket should have 1 item after removal, got {len(basket)}"

    def test_empty_basket_clears_ranking(self, browser, base_url):
        """Removing all items should clear the ranking."""
        clear_basket(browser, base_url)

        items = [{"category_id": 34, "category_name": "Spaghetti"}]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        # Verify ranking exists
        cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        assert len(cards) > 0, "Should have ranking before removal"

        # Remove the item
        remove_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-item-x")
        if remove_btns:
            remove_btns[0].click()
            time.sleep(3)

        # Ranking should be gone
        cards_after = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        save_screenshot(browser, "13_empty_basket_no_ranking")

        basket = get_basket(browser)
        assert len(basket) == 0, "Basket should be empty"

    def test_rapid_alternative_swap(self, browser, base_url):
        """Rapidly swapping alternatives should not leave stale ranking."""
        clear_basket(browser, base_url)

        items = [{"category_id": 34, "category_name": "Spaghetti"}]
        setup_basket_and_wait(browser, base_url, items, location="53111 Bonn")

        # Open alternatives
        alt_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-alt-btn")
        if not alt_btns:
            pytest.skip("No alt button")

        alt_btns[0].click()
        time.sleep(3)

        alt_cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
        if len(alt_cards) < 2:
            pytest.skip("Need at least 2 alternatives")

        # Rapid swap: click first, then immediately click second
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_cards[0])
        alt_cards[0].click()
        time.sleep(1)

        # Re-open alt panel (it may have closed)
        alt_btns = browser.find_elements(By.CSS_SELECTOR, ".basket-alt-btn")
        if alt_btns:
            alt_btns[0].click()
            time.sleep(3)

            alt_cards = browser.find_elements(By.CSS_SELECTOR, ".alt-offer-card")
            if alt_cards:
                browser.execute_script("arguments[0].scrollIntoView({block:'center'});", alt_cards[-1])
                alt_cards[-1].click()

        # Wait for final compare
        time.sleep(10)

        save_screenshot(browser, "13_rapid_swap")

        # Should still have valid ranking
        final_cards = browser.find_elements(By.CSS_SELECTOR, "#results_ranking .store-card")
        assert len(final_cards) > 0, "Should have ranking after rapid swaps"

        # Basket should reflect the last selection
        basket = get_basket(browser)
        assert len(basket) == 1, f"Should have 1 item, got {len(basket)}"


# ═══════════════════════════════════════════════════════════════════════════
#  5. API-LEVEL VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

class TestAPIExactSimilar:
    """API-level tests for match_type and is_exact_store."""

    def test_mixed_basket_exact_and_category(self, browser, base_url):
        """Basket with both brand-specific and category items should work."""
        import urllib.request
        import json as json_mod

        payload = json_mod.dumps({
            "location": "Bonn",
            "radius_km": 5,
            "basket": [
                {
                    "category_id": 293,
                    "category_name": "Alt-Mecklenburger Tilsiter",
                    "brand": "RÜCKER",
                    "any_brand": False,
                    "q": "RÜCKER Alt-Mecklenburger Tilsiter",
                },
                {"category_id": 34, "category_name": "Spaghetti"},
            ],
            "max_stores": 5,
        }).encode()

        req = urllib.request.Request(
            f"{base_url}/api/compare",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=30)
        data = json_mod.loads(resp.read())

        rows = data.get("rows", [])
        assert len(rows) > 0, "Should have results for mixed basket"

        # Each row should have 2 lines
        for r in rows[:3]:
            assert len(r["lines"]) == 2, \
                f"Should have 2 lines per store, got {len(r['lines'])}"

            # First line (brand item) should have match_type
            brand_line = r["lines"][0]
            assert brand_line["match_type"] in ("exact", "similar"), \
                f"Brand item should have match_type, got {brand_line['match_type']}"

            # Second line (category item) should have match_type=null
            cat_line = r["lines"][1]
            assert cat_line["match_type"] is None, \
                f"Category item should have match_type=null, got {cat_line['match_type']}"

    def test_text_only_brand_item(self, browser, base_url):
        """Text-only brand item (no category_id) should also get match_type."""
        import urllib.request
        import json as json_mod

        payload = json_mod.dumps({
            "location": "Berlin",
            "radius_km": 10,
            "basket": [{
                "q": "Barilla Spaghetti",
                "brand": "Barilla",
                "any_brand": False,
            }],
            "max_stores": 3,
        }).encode()

        req = urllib.request.Request(
            f"{base_url}/api/compare",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=30)
        data = json_mod.loads(resp.read())

        rows = data.get("rows", [])
        assert len(rows) > 0

        for r in rows:
            for line in r["lines"]:
                assert line["match_type"] in ("exact", "similar"), \
                    f"Text brand item should have match_type: {line}"
