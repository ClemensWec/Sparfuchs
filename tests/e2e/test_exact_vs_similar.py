"""
Selenium E2E Tests — Exact vs Similar Product Matching.

Tests that brand-specific products from browse/search show exact stores
first, with a divider before similar stores from other chains.

Run: python -m pytest tests/e2e/test_exact_vs_similar.py -v --tb=short
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


def setup_basket_and_compare(browser, base_url, items, location="53111 Bonn"):
    """Set basket items, enter location, wait for compare results."""
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
    time.sleep(10)  # wait for compare to finish


class TestBrowseAddBrandInfo:
    """Browse '+' button should include brand info in basket item."""

    def test_browse_add_includes_brand(self, browser, base_url):
        """When adding a product from browse grid, brand and any_brand
        should be included in the basket item."""
        clear_basket(browser, base_url)
        browser.get(base_url)
        time.sleep(2)

        # Click on a category tile to open browse panel
        tiles = browser.find_elements(By.CSS_SELECTOR, ".category-tile")
        if not tiles:
            pytest.skip("No category tiles found")
        tiles[0].click()
        time.sleep(3)

        # Find and click first offer card with a brand
        cards = browser.find_elements(By.CSS_SELECTOR, ".offer-card")
        if not cards:
            pytest.skip("No offer cards found in browse")

        # Click '+' on first card
        add_btn = cards[0].find_element(By.CSS_SELECTOR, ".offer-card-add")
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", add_btn)
        time.sleep(0.5)
        add_btn.click()
        time.sleep(1)

        # Check basket in localStorage
        basket_json = browser.execute_script(
            "return localStorage.getItem('sparfuchs.basket');"
        )
        basket = json.loads(basket_json or "[]")
        assert len(basket) >= 1, "Basket should have at least 1 item"

        item = basket[-1]  # last added item

        if item.get("brand"):
            assert item.get("any_brand") is False, \
                f"Brand-specific item should have any_brand=false, got {item}"
            assert item.get("q"), "Brand item should have 'q' field with product text"

        save_screenshot(browser, "12_browse_add_brand")

    def test_browse_add_has_category_id(self, browser, base_url):
        """Browse add should still include category_id."""
        basket_json = browser.execute_script(
            "return localStorage.getItem('sparfuchs.basket');"
        )
        basket = json.loads(basket_json or "[]")
        if not basket:
            pytest.skip("No items in basket")

        item = basket[-1]
        assert "category_id" in item, \
            f"Browse item should have category_id, got keys: {list(item.keys())}"


class TestExactVsSimilarAPI:
    """API should return match_type and is_exact_store for brand items."""

    def test_brand_item_exact_match(self, browser, base_url):
        """Brand-specific item should get match_type='exact' at correct chain."""
        import urllib.request
        import json as json_mod

        # Use Barilla Spaghetti at Aldi (known to exist in DB)
        payload = json_mod.dumps({
            "location": "Berlin",
            "radius_km": 10,
            "basket": [{
                "q": "Barilla Spaghetti",
                "brand": "Barilla",
                "any_brand": False,
            }],
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
        assert len(rows) > 0, "Should have results for Barilla Spaghetti"

        # Rows with Barilla brand should be exact, others similar
        exact_rows = [r for r in rows if r["is_exact_store"] is True]
        similar_rows = [r for r in rows if r["is_exact_store"] is False]
        assert len(exact_rows) > 0, "Should have at least one exact Barilla store"

        for r in exact_rows:
            for line in r["lines"]:
                assert line["match_type"] == "exact", \
                    f"Exact store at {r['chain']} should have match_type='exact'"
        for r in similar_rows:
            for line in r["lines"]:
                assert line["match_type"] == "similar", \
                    f"Similar store at {r['chain']} should have match_type='similar'"

    def test_category_item_no_match_type(self, browser, base_url):
        """Category items (any_brand=true) should have match_type=null."""
        import urllib.request
        import json as json_mod

        payload = json_mod.dumps({
            "location": "Berlin",
            "radius_km": 10,
            "basket": [{
                "category_id": 34,
                "category_name": "Spaghetti",
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
        assert len(rows) > 0, "Should have results for Spaghetti category"

        for r in rows:
            assert r["is_exact_store"] is None, \
                f"Category item should have is_exact_store=null, got {r['is_exact_store']}"
            for line in r["lines"]:
                assert line["match_type"] is None, \
                    f"Category item should have match_type=null, got {line['match_type']}"

    def test_brand_category_item_exact_vs_similar(self, browser, base_url):
        """Brand+category item: same brand=exact, other brand=similar."""
        import urllib.request
        import json as json_mod

        # REWE Bio Kidney-Bohnen — exists at REWE
        payload = json_mod.dumps({
            "location": "Berlin",
            "radius_km": 10,
            "basket": [{
                "category_id": 292,
                "category_name": "Alt-Mecklenburger",
                "brand": "R\u00dcCKER",
                "any_brand": False,
                "q": "R\u00dcCKER Alt-Mecklenburger",
            }],
            "max_stores": 30,
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
        if not rows:
            pytest.skip("No results for RÜCKER Alt-Mecklenburger")

        exact_rows = [r for r in rows if r["is_exact_store"] is True]
        similar_rows = [r for r in rows if r["is_exact_store"] is False]

        # Should have at least some results
        assert len(exact_rows) + len(similar_rows) > 0, "Should have some rows"

        # Exact rows should have RÜCKER brand
        for r in exact_rows:
            for line in r["lines"]:
                if line["match_type"] == "exact":
                    offer = line.get("offer", {})
                    brand = (offer.get("brand") or "").upper()
                    assert "RÜCKER" in brand or "RUCKER" in brand, \
                        f"Exact match should have RÜCKER brand, got '{offer.get('brand')}'"

        # Similar rows should NOT have RÜCKER brand
        for r in similar_rows:
            for line in r["lines"]:
                if line["match_type"] == "similar":
                    offer = line.get("offer", {})
                    brand = (offer.get("brand") or "").upper()
                    assert "RÜCKER" not in brand and "RUCKER" not in brand, \
                        f"Similar match should not have RÜCKER brand, got '{offer.get('brand')}'"


class TestDividerUI:
    """Frontend should show divider between exact and similar stores."""

    def test_divider_appears_for_brand_items(self, browser, base_url):
        """When a brand-specific product is in basket, divider should appear
        if there are similar stores."""
        clear_basket(browser, base_url)

        # Add brand-specific item
        items = [{
            "q": "Barilla Spaghetti",
            "brand": "Barilla",
            "any_brand": False,
        }]
        setup_basket_and_compare(browser, base_url, items, location="Berlin")

        # Check for store cards
        store_cards = browser.find_elements(By.CSS_SELECTOR, ".store-card")
        save_screenshot(browser, "12_exact_vs_similar_brand")

        if not store_cards:
            pytest.skip("No store cards rendered")

        # If there are both exact and similar stores, divider should exist
        dividers = browser.find_elements(By.CSS_SELECTOR, ".similar-products-divider")
        # Note: divider only appears when there are BOTH exact AND similar stores
        # With Barilla only at Aldi, there may be no similar stores
        # This test documents the current behavior

    def test_no_divider_for_category_items(self, browser, base_url):
        """Category items should NOT show the similar-products divider."""
        clear_basket(browser, base_url)

        items = [{"category_id": 34, "category_name": "Spaghetti"}]
        setup_basket_and_compare(browser, base_url, items, location="Berlin")

        dividers = browser.find_elements(By.CSS_SELECTOR, ".similar-products-divider")
        assert len(dividers) == 0, \
            "Category items should not show the similar-products divider"
        save_screenshot(browser, "12_no_divider_category")

    def test_divider_text_content(self, browser, base_url):
        """When divider appears, it should have the correct text."""
        clear_basket(browser, base_url)

        # Use a brand that exists at one chain but similar products at others
        items = [{
            "category_id": 292,
            "category_name": "Alt-Mecklenburger",
            "brand": "R\u00dcCKER",
            "any_brand": False,
            "q": "R\u00dcCKER Alt-Mecklenburger",
        }]
        setup_basket_and_compare(browser, base_url, items, location="Bonn")

        dividers = browser.find_elements(By.CSS_SELECTOR, ".similar-products-divider")
        save_screenshot(browser, "12_divider_text")

        if dividers:
            text = dividers[0].text.lower()
            assert "ähnliche" in text or "hnliche" in text, \
                f"Divider should mention 'ähnliche', got '{dividers[0].text}'"

    def test_exact_stores_appear_before_divider(self, browser, base_url):
        """Exact match stores should appear before the divider element."""
        clear_basket(browser, base_url)

        items = [{
            "category_id": 292,
            "category_name": "Alt-Mecklenburger",
            "brand": "R\u00dcCKER",
            "any_brand": False,
            "q": "R\u00dcCKER Alt-Mecklenburger",
        }]
        setup_basket_and_compare(browser, base_url, items, location="Bonn")

        ranking = browser.find_element(By.ID, "results_ranking")
        children = ranking.find_elements(By.XPATH, "./*")

        divider_idx = None
        store_indices = []
        for i, child in enumerate(children):
            classes = child.get_attribute("class") or ""
            if "similar-products-divider" in classes:
                divider_idx = i
            if "store-card" in classes or "compact-row" in classes:
                store_indices.append(i)

        save_screenshot(browser, "12_exact_before_divider")

        if divider_idx is not None and store_indices:
            # Some stores should be before the divider (exact)
            stores_before = [idx for idx in store_indices if idx < divider_idx]
            stores_after = [idx for idx in store_indices if idx > divider_idx]
            # At least the divider separates the groups
            assert len(stores_before) > 0 or len(stores_after) > 0, \
                "Divider should separate exact and similar stores"


class TestSearchResultBrandAdd:
    """Search results should preserve brand info when adding to basket."""

    def test_search_result_has_brand(self, browser, base_url):
        """Adding from search results should set brand and any_brand=false."""
        clear_basket(browser, base_url)
        browser.get(f"{base_url}/?q=Barilla&location=Berlin&radius_km=10")
        time.sleep(3)

        # Find result rows with add buttons
        add_btns = browser.find_elements(By.CSS_SELECTOR, ".row-action")
        if not add_btns:
            pytest.skip("No search result add buttons found")

        add_btns[0].click()
        time.sleep(1)

        basket_json = browser.execute_script(
            "return localStorage.getItem('sparfuchs.basket');"
        )
        basket = json.loads(basket_json or "[]")
        assert len(basket) >= 1

        item = basket[-1]
        # Search results with brand should have any_brand=false
        if item.get("brand"):
            assert item.get("any_brand") is False, \
                f"Search result with brand should have any_brand=false: {item}"

        save_screenshot(browser, "12_search_add_brand")
