function $(sel) { return document.querySelector(sel); }
function $all(sel) { return Array.from(document.querySelectorAll(sel)); }
function el(tag, attrs = {}, text = null) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") n.className = v;
    else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v);
  }
  if (text !== null) n.textContent = text;
  return n;
}

const STORAGE = {
  basket: "sparfuchs.basket",
  location: "sparfuchs.location",
  radiusKm: "sparfuchs.radius_km",
};

function loadJson(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch { return fallback; }
}

function saveJson(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch {}
}

function saveText(key, value) {
  try { localStorage.setItem(key, value); } catch {}
}

/* ── Basket Management ── */

const basket = [];

function loadBasket() {
  basket.length = 0;
  const saved = loadJson(STORAGE.basket, []);
  if (!Array.isArray(saved)) return;
  for (const it of saved) {
    if (!it || typeof it !== "object") continue;
    // Category-based items
    if (it.category_id != null) {
      basket.push({
        category_id: Number(it.category_id),
        category_name: String(it.category_name || ""),
      });
      continue;
    }
    // Legacy text-based items (ignored, migration path)
    const q = String(it.q || "").trim();
    if (!q) continue;
    basket.push({
      q,
      brand: it.brand == null ? null : (String(it.brand).trim() || null),
      any_brand: Boolean(it.any_brand),
    });
  }
}

function persistBasket() {
  saveJson(STORAGE.basket, basket);
  updateAllBasketViews();
}

function addToBasket(item) {
  basket.push(item);
  persistBasket();
}

function removeFromBasket(index) {
  basket.splice(index, 1);
  persistBasket();
}

function clearBasket() {
  basket.length = 0;
  persistBasket();
}

function basketCountLabel() {
  return basket.length === 1 ? "1 Artikel" : `${basket.length} Artikel`;
}

/* ── Basket UI Rendering ── */

function updateAllBasketViews() {
  // Update index page preview
  renderIndexBasketPreview();
  // Update search page sidebar
  renderSearchSidebar();
  // Update mobile FAB + bottom sheet
  updateMobileFab();
  renderBottomSheetBasket();
  // Update hidden form fields
  for (const el of $all("#basket_json, #sidebar_basket_json")) {
    if (el) el.value = JSON.stringify(basket);
  }
  // Update counts
  for (const el of $all("#basket_count, #sidebar_count")) {
    if (el) el.textContent = basket.length;
  }
  // Enable/disable compare buttons
  const btn = $("#sidebar_compare_btn");
  if (btn) btn.disabled = basket.length === 0;
}

function _basketItemLabel(it) {
  return it.category_name || it.q || "?";
}

function renderIndexBasketPreview() {
  const section = $("#basket_preview_section");
  const container = $("#basket_preview");
  if (!section || !container) return;

  if (basket.length === 0) {
    section.style.display = "none";
    return;
  }
  section.style.display = "";
  container.innerHTML = "";

  for (let i = 0; i < basket.length; i++) {
    const it = basket[i];
    const label = _basketItemLabel(it);
    const row = el("div", { class: "basket-item" });
    const info = el("div");
    info.appendChild(el("strong", {}, label));
    if (it.category_id != null) {
      info.appendChild(el("small", { class: "muted" }, "Produktkategorie"));
    } else {
      info.appendChild(el("small", { class: "muted" }, it.any_brand ? "Marke egal" : (it.brand || "Marke fixiert")));
    }
    const remove = el("button", {
      class: "x", type: "button",
      "aria-label": `${label} entfernen`,
      onclick: () => removeFromBasket(i),
    }, "\u2715");
    row.appendChild(info);
    row.appendChild(remove);
    container.appendChild(row);
  }
}

function renderSearchSidebar() {
  const container = $("#sidebar_basket");
  if (!container) return;
  container.innerHTML = "";

  if (basket.length === 0) {
    container.appendChild(el("div", { class: "muted", style: "padding:12px;font-size:0.9rem;" }, "Noch keine Artikel. Klicke auf Produkte, um sie hinzuzufuegen."));
    return;
  }

  for (let i = 0; i < basket.length; i++) {
    const it = basket[i];
    const label = _basketItemLabel(it);
    const row = el("div", { class: "sidebar-item" });
    const info = el("div");
    info.appendChild(el("strong", {}, label));
    if (it.category_id != null) {
      info.appendChild(el("small", { class: "muted" }, "Produktkategorie"));
    } else if (it.brand) {
      info.appendChild(el("small", { class: "muted" }, it.brand));
    }
    const remove = el("button", {
      class: "x", type: "button",
      onclick: () => removeFromBasket(i),
    }, "\u2715");
    row.appendChild(info);
    row.appendChild(remove);
    container.appendChild(row);
  }
}

/* ── Geolocation ── */

function wireGeolocation() {
  const btn = $("#use_geo");
  const locEl = $("#location");
  if (!btn || !locEl) return;
  btn.addEventListener("click", () => {
    if (!navigator.geolocation) { alert("Standort-Ermittlung nicht unterstuetzt."); return; }
    btn.disabled = true;
    btn.classList.add("is-loading");
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        locEl.value = `${pos.coords.latitude.toFixed(5)}, ${pos.coords.longitude.toFixed(5)}`;
        saveText(STORAGE.location, locEl.value.trim());
        btn.disabled = false;
        btn.classList.remove("is-loading");
      },
      () => { btn.disabled = false; btn.classList.remove("is-loading"); alert("Standort nicht ermittelbar."); },
      { enableHighAccuracy: false, timeout: 8000, maximumAge: 600000 },
    );
  });
}

function syncRadiusLabel() {
  const radiusEl = $("#radius_km");
  const output = $("#radius_value");
  if (!radiusEl || !output) return;
  output.textContent = `${String(radiusEl.value).trim()} km`;
}

/* ── Index Page ── */

function wireIndexPage() {
  const locEl = $("#location");
  const radiusEl = $("#radius_km");
  if (!locEl) return;

  // Restore
  const savedLoc = (localStorage.getItem(STORAGE.location) || "").trim();
  if (savedLoc) locEl.value = savedLoc;
  const savedRadius = (localStorage.getItem(STORAGE.radiusKm) || "").trim();
  if (radiusEl && savedRadius) radiusEl.value = savedRadius;
  syncRadiusLabel();

  locEl.addEventListener("input", () => saveText(STORAGE.location, locEl.value.trim()));
  if (radiusEl) {
    radiusEl.addEventListener("input", () => {
      saveText(STORAGE.radiusKm, radiusEl.value.trim());
      syncRadiusLabel();
    });
  }

  wireGeolocation();
  wireCategoryAutocomplete();

  // Compare form location sync
  const compareLoc = $("#compare_location");
  const compareRad = $("#compare_radius");
  if (compareLoc) compareLoc.value = locEl.value.trim();
  if (compareRad && radiusEl) compareRad.value = radiusEl.value;
  if (locEl) locEl.addEventListener("change", () => { if (compareLoc) compareLoc.value = locEl.value.trim(); });
  if (radiusEl) radiusEl.addEventListener("change", () => { if (compareRad) compareRad.value = radiusEl.value; });

  // Clear basket
  const clearBtn = $("#clear_basket");
  if (clearBtn) clearBtn.addEventListener("click", clearBasket);

  const maxStoresEl = $("#max_stores");
  const compareMaxStores = $("#compare_max_stores");
  if (maxStoresEl && compareMaxStores) {
    compareMaxStores.value = maxStoresEl.value;
    maxStoresEl.addEventListener("change", () => {
      compareMaxStores.value = maxStoresEl.value;
    });
  }

  // Overlay on compare submit
  const form = $("#compare_form");
  const overlay = $("#overlay");
  if (form && overlay) {
    form.addEventListener("submit", (e) => {
      if (basket.length === 0) { e.preventDefault(); alert("Bitte Artikel zur Liste hinzufuegen."); return; }
      const bjson = $("#basket_json");
      if (bjson) bjson.value = JSON.stringify(basket);
      const cl = $("#compare_location");
      if (cl) cl.value = locEl.value.trim();
      const cr = $("#compare_radius");
      if (cr && radiusEl) cr.value = radiusEl.value;
      if (compareMaxStores && maxStoresEl) compareMaxStores.value = maxStoresEl.value;
      overlay.classList.add("show");
    });
  }

  loadBasket();
  updateAllBasketViews();
}

/* ── Category Autocomplete ── */

let _catDebounce = null;
const _catCache = new Map(); // prefix -> {categories, timestamp}
const _CAT_CACHE_TTL = 300_000; // 5 min

function _getCachedOrFilter(query) {
  // Check exact cache hit
  const cached = _catCache.get(query);
  if (cached && Date.now() - cached.ts < _CAT_CACHE_TTL) {
    return { categories: cached.categories, brands: cached.brands || [] };
  }

  // Check if a shorter prefix has cached results we can filter client-side
  for (let i = query.length - 1; i >= 2; i--) {
    const prefix = query.slice(0, i);
    const prefixCache = _catCache.get(prefix);
    if (prefixCache && Date.now() - prefixCache.ts < _CAT_CACHE_TTL) {
      const filtered = prefixCache.categories.filter(c =>
        c.name.toLowerCase().includes(query.toLowerCase())
      );
      const filteredBrands = (prefixCache.brands || []).filter(b =>
        b.brand.toLowerCase().includes(query.toLowerCase())
      );
      if (filtered.length >= 3) return { categories: filtered, brands: filteredBrands };
      break; // not enough results — need server call
    }
  }
  return null;
}

function wireCategoryAutocomplete() {
  const input = $("#hero_search");
  const dropdown = $("#category_dropdown");
  if (!input || !dropdown) return;

  // ARIA: combobox pattern
  input.setAttribute("role", "combobox");
  input.setAttribute("aria-autocomplete", "list");
  input.setAttribute("aria-expanded", "false");
  input.setAttribute("aria-controls", "category_dropdown");
  dropdown.setAttribute("role", "listbox");
  dropdown.setAttribute("aria-label", "Suchvorschläge");

  input.addEventListener("input", () => {
    clearTimeout(_catDebounce);
    const q = input.value.trim();
    if (q.length < 2) {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
      return;
    }

    // Try client-side cache first
    const cached = _getCachedOrFilter(q);
    if (cached) {
      renderCategoryDropdown(cached.categories || cached, dropdown, input, q, null, cached.brands || []);
      return;
    }

    _catDebounce = setTimeout(() => fetchCategories(q, dropdown, input), 120);
  });

  // Close on outside click
  document.addEventListener("click", (e) => {
    if (!input.contains(e.target) && !dropdown.contains(e.target)) {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
    }
  });

  // Keyboard navigation
  input.addEventListener("keydown", (e) => {
    if (dropdown.style.display === "none") return;
    const items = Array.from(dropdown.querySelectorAll(".suggest-item"));
    const active = dropdown.querySelector(".suggest-item.active");
    let idx = items.indexOf(active);

    if (e.key === "ArrowDown") {
      e.preventDefault();
      idx = Math.min(idx + 1, items.length - 1);
      items.forEach((it, i) => {
        it.classList.toggle("active", i === idx);
        it.setAttribute("aria-selected", i === idx ? "true" : "false");
      });
      if (items[idx]) input.setAttribute("aria-activedescendant", items[idx].id);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      idx = Math.max(idx - 1, 0);
      items.forEach((it, i) => {
        it.classList.toggle("active", i === idx);
        it.setAttribute("aria-selected", i === idx ? "true" : "false");
      });
      if (items[idx]) input.setAttribute("aria-activedescendant", items[idx].id);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (active) active.click();
    } else if (e.key === "Escape") {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
      input.removeAttribute("aria-activedescendant");
    }
  });
}

async function fetchCategories(q, dropdown, input) {
  // Show loading indicator while waiting for server response
  dropdown.innerHTML = '<div class="suggest-loading" style="padding: 12px 16px; color: #888; font-size: 14px;">Suche...</div>';
  dropdown.style.display = 'block';
  try {
    const loc = (localStorage.getItem(STORAGE.location) || "").trim();
    const radius = (localStorage.getItem(STORAGE.radiusKm) || "10").trim();
    let catUrl = `/api/suggest-categories?q=${encodeURIComponent(q)}`;
    if (loc) catUrl += `&location=${encodeURIComponent(loc)}&radius_km=${encodeURIComponent(radius)}`;
    const resp = await fetch(catUrl);
    const data = await resp.json();
    const categories = data.categories || [];
    const brands = data.brands || [];
    // Cache server response
    _catCache.set(q, { categories, brands, ts: Date.now() });
    renderCategoryDropdown(categories, dropdown, input, q, data.corrected_to || null, brands);
    // Log search for analytics (fire-and-forget)
    fetch("/api/log-search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query: q,
        result_count: categories.length,
        corrected_from: data.corrected_to ? q : null,
        location: loc || null,
        radius_km: parseFloat(radius) || null
      })
    }).catch(() => {});
  } catch {
    dropdown.style.display = "none";
    input.setAttribute("aria-expanded", "false");
  }
}

function _highlightMatch(name, query) {
  const nameEl = el("span", {});
  const lowerName = name.toLowerCase();
  const lowerQuery = query.toLowerCase();
  const matchIdx = lowerName.indexOf(lowerQuery);
  if (matchIdx >= 0) {
    if (matchIdx > 0) nameEl.appendChild(document.createTextNode(name.slice(0, matchIdx)));
    nameEl.appendChild(el("span", { style: "color:var(--brand);" }, name.slice(matchIdx, matchIdx + query.length)));
    nameEl.appendChild(document.createTextNode(name.slice(matchIdx + query.length)));
  } else {
    nameEl.textContent = name;
  }
  return nameEl;
}

function _addAndFeedback(item, input, dropdown) {
  addToBasket(item);
  // Log category selection (fire-and-forget)
  fetch("/api/log-search", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: input.value,
      category_id: item.category_id,
      category_name: item.category_name,
      location: (localStorage.getItem(STORAGE.location) || "").trim() || null,
      radius_km: parseFloat(localStorage.getItem(STORAGE.radiusKm) || "10") || null
    })
  }).catch(() => {});
  input.value = "";
  dropdown.style.display = "none";
  input.setAttribute("aria-expanded", "false");
  input.placeholder = `\u2713 ${item.category_name} hinzugefuegt!`;
  setTimeout(() => { input.placeholder = "Was suchst du? z.B. Hähnchenbrust, Milch, Butter..."; }, 1500);
}

function _isInBasket(categoryId) {
  return basket.some(b => b.category_id === categoryId);
}

function renderCategoryDropdown(categories, dropdown, input, query, correctedTo, brands) {
  dropdown.innerHTML = "";
  input.removeAttribute("aria-activedescendant");

  // "Suche nach X" fallback row at top — always shown when user typed something
  if (query && query.length >= 2) {
    const searchRow = el("div", {
      class: "suggest-item suggest-search-fallback",
      role: "option",
      id: "suggest-item-search",
      style: "display:flex;align-items:center;gap:8px;color:var(--brand);font-weight:600;border-bottom:1px solid var(--line);"
    });
    searchRow.appendChild(el("span", { style: "font-size:1rem;" }, "\uD83D\uDD0D"));
    searchRow.appendChild(el("span", {}, `Suche nach „${query}"`));
    searchRow.addEventListener("click", () => {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
      const _loc = (localStorage.getItem(STORAGE.location) || "").trim();
      const _rad = (localStorage.getItem(STORAGE.radiusKm) || "10").trim();
      let _searchUrl = `/search?q=${encodeURIComponent(query)}`;
      if (_loc) _searchUrl += `&location=${encodeURIComponent(_loc)}&radius_km=${encodeURIComponent(_rad)}`;
      window.location.href = _searchUrl;
    });
    searchRow.addEventListener("mouseenter", () => { searchRow.style.background = "var(--hover-bg, #f0f0f0)"; });
    searchRow.addEventListener("mouseleave", () => { searchRow.style.background = ""; });
    dropdown.appendChild(searchRow);
  }

  // "Meinten Sie" banner for spell corrections
  if (correctedTo) {
    const corrRow = el("div", {
      style: "padding:6px 12px;font-size:0.85rem;color:var(--text-soft);background:var(--surface-soft);border-bottom:1px solid var(--line);"
    });
    corrRow.appendChild(document.createTextNode("Meinten Sie: "));
    corrRow.appendChild(el("strong", { style: "color:var(--brand);" }, correctedTo));
    corrRow.appendChild(document.createTextNode("?"));
    dropdown.appendChild(corrRow);
  }

  if (!categories.length && !(brands && brands.length) && !query) {
    dropdown.style.display = "none";
    return;
  }

  // ARIA live region for screen readers
  const liveRegion = document.getElementById("suggest-live");
  if (liveRegion) {
    const total = categories.length + (brands ? brands.length : 0);
    liveRegion.textContent = total
      ? `${total} Vorschläge verfügbar`
      : "Keine Vorschläge gefunden";
  }

  // Brand suggestions section
  if (brands && brands.length > 0) {
    const brandHeader = el("div", {
      class: "suggest-section-header",
      style: "padding:6px 16px;font-size:11px;color:#888;text-transform:uppercase;font-weight:600;"
    }, "Marken");
    dropdown.appendChild(brandHeader);

    for (const brand of brands) {
      const itemIdx = dropdown.querySelectorAll(".suggest-item").length;
      const item = el("div", {
        class: "suggest-item suggest-brand",
        role: "option",
        id: `suggest-item-${itemIdx}`,
        "aria-selected": "false"
      });
      const row = el("div", { style: "display:flex;justify-content:space-between;align-items:center;gap:8px;" });
      const left = el("div", {});
      const nameEl = el("strong", {});
      nameEl.appendChild(_highlightMatch(brand.brand, query));
      left.appendChild(nameEl);
      left.appendChild(el("span", { style: "color:#888;font-size:12px;margin-left:8px;" }, `in ${brand.top_category}`));
      row.appendChild(left);
      row.appendChild(el("span", { style: "color:#888;font-size:12px;white-space:nowrap;" }, `${brand.product_count} Produkte`));
      item.appendChild(row);

      // Click handler: trigger text search for the brand name
      item.addEventListener("click", () => {
        dropdown.style.display = "none";
        input.setAttribute("aria-expanded", "false");
        const _loc = (localStorage.getItem(STORAGE.location) || "").trim();
        const _rad = (localStorage.getItem(STORAGE.radiusKm) || "10").trim();
        let _searchUrl = `/search?q=${encodeURIComponent(brand.brand)}`;
        if (_loc) _searchUrl += `&location=${encodeURIComponent(_loc)}&radius_km=${encodeURIComponent(_rad)}`;
        window.location.href = _searchUrl;
      });
      dropdown.appendChild(item);
    }
  }

  // "Kategorien" section header when both brands and categories are present
  if (brands && brands.length > 0 && categories.length > 0) {
    const catHeader = el("div", {
      class: "suggest-section-header",
      style: "padding:6px 16px;font-size:11px;color:#888;text-transform:uppercase;font-weight:600;border-top:1px solid #eee;"
    }, "Kategorien");
    dropdown.appendChild(catHeader);
  }

  for (const cat of categories) {
    // Handle deal intent items
    if (cat.type === 'deal_intent') {
      const item = document.createElement('div');
      item.className = 'suggest-item suggest-deal';
      item.setAttribute('role', 'option');
      const dealIdx = dropdown.querySelectorAll('.suggest-item').length;
      item.id = `suggest-item-${dealIdx}`;
      item.setAttribute('aria-selected', 'false');
      item.style.cssText = 'padding: 12px 16px; cursor: pointer; background: #fff8e1; border-bottom: 1px solid #eee;';
      const dealRow = document.createElement('div');
      dealRow.style.cssText = 'display:flex; align-items:center; gap:8px;';
      dealRow.innerHTML = '<span style="font-size:18px;">\uD83D\uDD0D</span>';
      const dealText = document.createElement('div');
      const dealTitle = document.createElement('div');
      dealTitle.style.fontWeight = '600';
      dealTitle.textContent = cat.name;
      dealText.appendChild(dealTitle);
      const dealSub = document.createElement('div');
      dealSub.style.cssText = 'font-size:12px; color:#888;';
      dealSub.textContent = 'Alle aktuellen Prospekt-Angebote';
      dealText.appendChild(dealSub);
      dealRow.appendChild(dealText);
      item.appendChild(dealRow);
      item.addEventListener('click', () => {
        const _loc = (localStorage.getItem(STORAGE.location) || '').trim();
        const _rad = (localStorage.getItem(STORAGE.radiusKm) || '10').trim();
        let _url = cat.search_url || '/search?q=' + encodeURIComponent(query);
        if (_loc) _url += (_url.includes('?') ? '&' : '?') + 'location=' + encodeURIComponent(_loc) + '&radius_km=' + encodeURIComponent(_rad);
        window.location.href = _url;
      });
      item.addEventListener('mouseenter', () => { item.style.background = '#fff3cd'; });
      item.addEventListener('mouseleave', () => { item.style.background = '#fff8e1'; });
      dropdown.appendChild(item);
      continue;
    }

    const displayCount = Number(cat.display_offer_count ?? cat.offer_count ?? 0);
    const hasChildren = cat.kind === "family" && Array.isArray(cat.children) && cat.children.length > 0;

    const inBasket = _isInBasket(cat.id);
    const itemIdx = dropdown.querySelectorAll(".suggest-item").length;
    const item = el("div", {
      class: "suggest-item" + (hasChildren ? " has-children" : "") + (inBasket ? " in-basket" : ""),
      role: "option",
      id: `suggest-item-${itemIdx}`,
      "aria-selected": "false"
    });

    // Top row: name + Oberkategorie + count
    const topRow = el("div", { style: "display:flex;align-items:center;gap:6px;" });
    if (inBasket) {
      topRow.appendChild(el("span", { style: "color:var(--brand);font-size:0.85rem;flex-shrink:0;", title: "Bereits im Warenkorb" }, "\u2713"));
    }
    const nameCol = el("div", { style: "flex:1;min-width:0;" });
    const nameEl = el("div", { style: "font-weight:700;font-size:0.95rem;" + (inBasket ? "color:var(--brand);" : "") });
    nameEl.appendChild(_highlightMatch(cat.name, query));
    nameCol.appendChild(nameEl);
    if (cat.oberkategorie) {
      nameCol.appendChild(el("div", { style: "font-size:0.75rem;color:var(--text-soft);margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" }, cat.oberkategorie));
    }
    topRow.appendChild(nameCol);

    const countText = cat.not_local
        ? `${displayCount} Angebote deutschlandweit`
        : (displayCount === 1 ? '1 Angebot vergleichen' : `${displayCount} Angebote vergleichen`);
    const countEl = el("div", { style: "font-size:0.8rem;color:var(--text-soft);white-space:nowrap;" },
      countText);
    if (cat.not_local) {
      countEl.style.color = '#999';
      countEl.style.fontStyle = 'italic';
    }
    topRow.appendChild(countEl);

    if (hasChildren) {
      const arrow = el("span", { class: "expand-arrow", style: "margin-left:4px;font-size:0.7rem;color:var(--text-soft);transition:transform 0.2s;" }, "\u25B6");
      topRow.appendChild(arrow);
    }
    item.appendChild(topRow);

    if (hasChildren) {
      // Family node: click toggles children
      const childrenContainer = el("div", { class: "suggest-children", style: "display:none;padding-left:12px;margin-top:4px;border-left:2px solid var(--brand-light, #e0e0e0);" });

      for (const child of cat.children) {
        const childItem = el("div", { class: "suggest-child-item", style: "padding:4px 8px;cursor:pointer;border-radius:4px;display:flex;align-items:center;gap:6px;" });
        const childName = el("span", { style: "flex:1;font-size:0.9rem;" });
        childName.appendChild(_highlightMatch(child.name, query));
        childItem.appendChild(childName);
        const childCountText = child.offer_count === 1 ? '1 Angebot vergleichen' : `${child.offer_count} Angebote vergleichen`;
        childItem.appendChild(el("span", { style: "font-size:0.75rem;color:var(--text-soft);white-space:nowrap;" }, childCountText));

        childItem.addEventListener("click", (e) => {
          e.stopPropagation();
          _addAndFeedback({ category_id: child.id, category_name: child.name }, input, dropdown);
        });
        childItem.addEventListener("mouseenter", () => { childItem.style.background = "var(--hover-bg, #f0f0f0)"; });
        childItem.addEventListener("mouseleave", () => { childItem.style.background = ""; });
        childrenContainer.appendChild(childItem);
      }

      // Also add the family itself as "Alle [name]" option at the top
      const allItem = el("div", { class: "suggest-child-item", style: "padding:4px 8px;cursor:pointer;border-radius:4px;display:flex;align-items:center;gap:6px;font-weight:600;" });
      allItem.appendChild(el("span", { style: "flex:1;font-size:0.9rem;" }, `Alle ${cat.name}`));
      const allCountText = displayCount === 1 ? '1 Angebot vergleichen' : `${displayCount} Angebote vergleichen`;
      allItem.appendChild(el("span", { style: "font-size:0.75rem;color:var(--text-soft);white-space:nowrap;" }, allCountText));
      allItem.addEventListener("click", (e) => {
        e.stopPropagation();
        _addAndFeedback({ category_id: cat.id, category_name: cat.name }, input, dropdown);
      });
      allItem.addEventListener("mouseenter", () => { allItem.style.background = "var(--hover-bg, #f0f0f0)"; });
      allItem.addEventListener("mouseleave", () => { allItem.style.background = ""; });
      childrenContainer.insertBefore(allItem, childrenContainer.firstChild);

      item.appendChild(childrenContainer);

      item.addEventListener("click", () => {
        const visible = childrenContainer.style.display !== "none";
        childrenContainer.style.display = visible ? "none" : "block";
        const arrow = item.querySelector(".expand-arrow");
        if (arrow) arrow.textContent = visible ? "\u25B6" : "\u25BC";
      });
    } else {
      // Leaf node: click adds directly
      item.addEventListener("click", () => {
        _addAndFeedback({ category_id: cat.id, category_name: cat.name }, input, dropdown);
      });
    }

    dropdown.appendChild(item);
  }
  dropdown.style.display = "block";
  input.setAttribute("aria-expanded", "true");
}

/* ── Search Page ── */

let searchState = { hits: [], allHits: [], offset: 0, total: 0, loading: false };

function wireSearchPage() {
  const input = $("#search_input");
  if (!input) return;

  loadBasket();
  updateAllBasketViews();

  const clearBtn = $("#sidebar_clear");
  if (clearBtn) clearBtn.addEventListener("click", clearBasket);

  // Compare form overlay
  const form = $("#sidebar_compare_form");
  const overlay = $("#overlay");
  if (form && overlay) {
    form.addEventListener("submit", (e) => {
      if (basket.length === 0) { e.preventDefault(); return; }
      const bjson = $("#sidebar_basket_json");
      if (bjson) bjson.value = JSON.stringify(basket);
      overlay.classList.add("show");
    });
  }

  // Initial search if q is provided
  const params = new URLSearchParams(window.location.search);
  const q = params.get("q") || "";
  if (q) {
    doSearch(q);
  }

  // Sort change
  const sortSel = $("#sort_select");
  if (sortSel) sortSel.addEventListener("change", () => renderFilteredResults());

  // Load more
  const loadMoreBtn = $("#load_more");
  if (loadMoreBtn) loadMoreBtn.addEventListener("click", () => loadMore());
}

async function doSearch(q) {
  const grid = $("#product_grid");
  if (!grid) return;
  grid.innerHTML = '<div class="muted" style="grid-column:1/-1;text-align:center;padding:40px;">Lade Ergebnisse...</div>';

  const params = new URLSearchParams(window.location.search);
  const location = params.get("location") || (localStorage.getItem(STORAGE.location) || "").trim();
  const radius = params.get("radius_km") || (localStorage.getItem(STORAGE.radiusKm) || "10").trim();

  try {
    const url = `/api/search?q=${encodeURIComponent(q)}&location=${encodeURIComponent(location)}&radius_km=${encodeURIComponent(radius)}&limit=60`;
    const resp = await fetch(url);
    const data = await resp.json();

    searchState.hits = data.hits || [];
    searchState.allHits = data.hits || [];
    searchState.total = data.total || 0;
    searchState.offset = data.hits ? data.hits.length : 0;

    // Build chain filters
    buildChainFilters(data.available_chains || []);

    renderFilteredResults();
    $("#result_count").textContent = `${data.total} Ergebnisse`;

    // Show load more if there are more results
    const wrap = $("#load_more_wrap");
    if (wrap) wrap.style.display = searchState.offset < searchState.total ? "" : "none";
  } catch (e) {
    grid.innerHTML = '<div class="notice error" style="grid-column:1/-1;">Fehler beim Laden der Ergebnisse.</div>';
  }
}

function buildChainFilters(chains) {
  const container = $("#chain_filters");
  if (!container) return;
  container.innerHTML = "";
  for (const chain of chains) {
    const label = el("label", { class: "filter-check" });
    const cb = el("input", { type: "checkbox", value: chain, checked: "checked" });
    cb.addEventListener("change", () => renderFilteredResults());
    label.appendChild(cb);
    label.appendChild(el("span", {}, chain));
    container.appendChild(label);
  }
}

function getActiveChainFilters() {
  return $all("#chain_filters input:checked").map(cb => cb.value);
}

function renderFilteredResults() {
  const activeChains = new Set(getActiveChainFilters());
  const sort = ($("#sort_select") || {}).value || "relevance";

  let filtered = searchState.allHits.filter(h => activeChains.size === 0 || activeChains.has(h.chain));

  if (sort === "price_asc") filtered.sort((a, b) => (a.price_eur ?? 9999) - (b.price_eur ?? 9999));
  else if (sort === "price_desc") filtered.sort((a, b) => (b.price_eur ?? 0) - (a.price_eur ?? 0));
  else if (sort === "discount") filtered.sort((a, b) => (b.discount_percent ?? 0) - (a.discount_percent ?? 0));

  renderProductGrid(filtered);
}

function renderProductGrid(hits) {
  const grid = $("#product_grid");
  if (!grid) return;
  grid.innerHTML = "";

  if (!hits.length) {
    grid.innerHTML = '<div class="muted" style="grid-column:1/-1;text-align:center;padding:40px;">Keine Ergebnisse fuer diese Filter.</div>';
    return;
  }

  for (const h of hits) {
    const card = el("div", { class: "product-card" });

    // Image
    if (h.image_url) {
      const img = el("img", { class: "product-img", src: h.image_url, alt: h.title, loading: "lazy" });
      img.onerror = function() { this.style.display = "none"; };
      card.appendChild(img);
    } else {
      card.appendChild(el("div", { class: "product-img-placeholder" }));
    }

    const body = el("div", { class: "product-body" });

    // Chain pill
    const meta = el("div", { class: "product-meta" });
    meta.appendChild(el("span", { class: "pill" }, h.chain));
    if (h.discount_percent) meta.appendChild(el("span", { class: "pill offer" }, `-${h.discount_percent}%`));
    body.appendChild(meta);

    // Title
    const title = h.brand ? `${h.brand} - ${h.title}` : h.title;
    body.appendChild(el("div", { class: "product-title" }, title));

    // Price
    const priceRow = el("div", { class: "product-price-row" });
    if (h.price_eur != null) {
      priceRow.appendChild(el("strong", { class: "product-price" }, `${Number(h.price_eur).toFixed(2)} \u20AC`));
      if (h.was_price_eur && h.was_price_eur > h.price_eur) {
        priceRow.appendChild(el("span", { class: "product-was-price" }, `statt ${Number(h.was_price_eur).toFixed(2)} \u20AC`));
      }
    } else {
      priceRow.appendChild(el("span", { class: "muted" }, "Kein Preis"));
    }
    body.appendChild(priceRow);

    // Base price
    if (h.base_price_eur && h.base_unit) {
      body.appendChild(el("div", { class: "product-base-price muted" }, `${Number(h.base_price_eur).toFixed(2)} \u20AC/${h.base_unit}`));
    }

    // Validity
    if (h.valid_from || h.valid_until) {
      const validity = [h.valid_from, h.valid_until].filter(Boolean).join(" - ");
      body.appendChild(el("div", { class: "product-validity muted" }, validity));
    }

    // Add to list button
    const addBtn = el("button", {
      class: "add-to-list-btn",
      type: "button",
      onclick: () => {
        addToBasket({ q: h.title, brand: h.brand || null, any_brand: !h.brand });
        addBtn.textContent = "\u2713 Hinzugefuegt";
        addBtn.classList.add("added");
        setTimeout(() => { addBtn.textContent = "In die Liste"; addBtn.classList.remove("added"); }, 1500);
      },
    }, "In die Liste");
    body.appendChild(addBtn);

    card.appendChild(body);
    grid.appendChild(card);
  }
}

async function loadMore() {
  const params = new URLSearchParams(window.location.search);
  const q = params.get("q") || "";
  const location = params.get("location") || (localStorage.getItem(STORAGE.location) || "").trim();
  const radius = params.get("radius_km") || (localStorage.getItem(STORAGE.radiusKm) || "10").trim();

  const btn = $("#load_more");
  if (btn) { btn.disabled = true; btn.textContent = "Lade..."; }

  try {
    const url = `/api/search?q=${encodeURIComponent(q)}&location=${encodeURIComponent(location)}&radius_km=${encodeURIComponent(radius)}&limit=60&offset=${searchState.offset}`;
    const resp = await fetch(url);
    const data = await resp.json();
    const newHits = data.hits || [];
    searchState.allHits = searchState.allHits.concat(newHits);
    searchState.offset += newHits.length;
    renderFilteredResults();
    const wrap = $("#load_more_wrap");
    if (wrap) wrap.style.display = searchState.offset < searchState.total ? "" : "none";
  } catch {}

  if (btn) { btn.disabled = false; btn.textContent = "Mehr laden"; }
}

/* ── Results Page Tabs ── */

function wireResultsTabs() {
  const tabs = $all(".tab-btn");
  if (!tabs.length) return;

  for (const tab of tabs) {
    tab.addEventListener("click", () => {
      const target = tab.dataset.tab;
      for (const t of tabs) t.classList.toggle("active", t === tab);
      for (const c of $all(".tab-content")) {
        c.style.display = c.id === `tab-${target}` ? "" : "none";
      }
    });
  }
}

/* ── Mobile FAB + Bottom Sheet ── */

function updateMobileFab() {
  const fab = $("#basket_fab");
  if (!fab) return;
  const badge = $("#basket_fab_count");
  if (badge) badge.textContent = basket.length;
  fab.classList.toggle("has-items", basket.length > 0);
  if (basket.length === 0) fab.style.display = "none";
}

function renderBottomSheetBasket() {
  const container = $("#sheet_basket_items");
  if (!container) return;
  container.innerHTML = "";

  if (basket.length === 0) {
    container.appendChild(el("div", { class: "muted", style: "padding:12px;font-size:0.9rem;" }, "Noch keine Artikel in der Liste."));
    return;
  }

  for (let i = 0; i < basket.length; i++) {
    const it = basket[i];
    const label = _basketItemLabel(it);
    const row = el("div", { class: "sidebar-item" });
    const info = el("div");
    info.appendChild(el("strong", {}, label));
    if (it.category_id != null) {
      info.appendChild(el("small", {}, "Produktkategorie"));
    } else if (it.brand) {
      info.appendChild(el("small", {}, it.brand));
    }
    const remove = el("button", {
      class: "x", type: "button",
      "aria-label": `${label} entfernen`,
      onclick: () => { removeFromBasket(i); if (basket.length === 0) closeBottomSheet(); },
    }, "\u2715");
    row.appendChild(info);
    row.appendChild(remove);
    container.appendChild(row);
  }
}

function openBottomSheet() {
  const sheet = $("#basket_sheet");
  if (!sheet) return;
  sheet.classList.add("open");
  sheet.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";
}

function closeBottomSheet() {
  const sheet = $("#basket_sheet");
  if (!sheet) return;
  sheet.classList.remove("open");
  sheet.setAttribute("aria-hidden", "true");
  document.body.style.overflow = "";
}

function wireBottomSheet() {
  const fab = $("#basket_fab");
  if (fab) fab.addEventListener("click", openBottomSheet);

  const backdrop = $("#sheet_backdrop");
  if (backdrop) backdrop.addEventListener("click", closeBottomSheet);

  const closeBtn = $("#sheet_close");
  if (closeBtn) closeBtn.addEventListener("click", closeBottomSheet);

  const compareBtn = $("#sheet_compare_btn");
  if (compareBtn) {
    compareBtn.addEventListener("click", () => {
      closeBottomSheet();
      // Trigger the main compare form submission
      const form = $("#compare_form");
      if (form) {
        // Sync values before submit
        const locEl = $("#location");
        const radiusEl = $("#radius_km");
        const cl = $("#compare_location");
        const cr = $("#compare_radius");
        const bjson = $("#basket_json");
        const maxStoresEl = $("#max_stores");
        const compareMaxStores = $("#compare_max_stores");
        if (bjson) bjson.value = JSON.stringify(basket);
        if (cl && locEl) cl.value = locEl.value.trim();
        if (cr && radiusEl) cr.value = radiusEl.value;
        if (compareMaxStores && maxStoresEl) compareMaxStores.value = maxStoresEl.value;
        if (basket.length > 0) form.requestSubmit();
        else alert("Bitte Artikel zur Liste hinzufuegen.");
      }
    });
  }
}

/* ── Init ── */

window.addEventListener("DOMContentLoaded", () => {
  // Detect page and wire accordingly
  if ($("#hero_search")) wireIndexPage();
  else if ($("#search_input")) wireSearchPage();
  wireResultsTabs();
  wireBottomSheet();
});
