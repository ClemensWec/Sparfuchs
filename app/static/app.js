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

// Weight/volume units get normalized to a standard display unit for comparison.
// All other units (wl, tab, stk, …) are shown with their original qty label.
const _WEIGHT_UNITS  = new Set(["g", "kg"]);
const _VOLUME_UNITS  = new Set(["ml", "l", "liter"]);

/**
 * Format a human-readable base price normalized to a standard comparison unit:
 *   Weight  → always €/kg   (e.g. 100g@1,29€  → "12,90 €/kg")
 *   Volume  → always €/l    (e.g. 330ml@0,79€ → "2,39 €/l")
 *   Others  → original qty  (e.g. 1 WL@0,03€  → "0,03 €/WL")
 *                           (e.g. 1 St.@3,30€ → "3,30 €/Stück")
 */
function formatBasePrice(offer) {
  const unit  = offer.base_unit || offer.unit;
  const qty   = offer.quantity != null ? Number(offer.quantity) : null;
  const price = offer.base_price_eur != null ? Number(offer.base_price_eur) : null;
  if (price == null || unit == null) return null;

  const fmt = v => v.toFixed(2).replace(".", ",");

  if (_WEIGHT_UNITS.has(unit)) {
    // Normalize to €/kg
    const multiplier = unit === "kg" ? 1 : 0.001;   // g→kg: /1000
    const perKg = (qty != null && qty > 0)
      ? (price / qty) / multiplier
      : (unit === "kg" ? price : null);
    if (perKg == null) return null;
    return `${fmt(perKg)}\u00a0€/kg`;
  }

  if (_VOLUME_UNITS.has(unit)) {
    // Normalize to €/l
    const mlPerUnit = unit === "ml" ? 1 : 1000;     // ml→l: /1000
    const perL = (qty != null && qty > 0)
      ? (price / qty) * (1000 / mlPerUnit)
      : (unit === "l" || unit === "liter" ? price : null);
    if (perL == null) return null;
    return `${fmt(perL)}\u00a0€/l`;
  }

  // Other units: show original price with qty label
  const unitLabel = {
    wl: "WL", tab: "Tab", blatt: "Blatt",
    tuecher: "Tuch", tuch: "Tuch",
    stueck: "Stück", st: "Stück", stk: "Stück",
    dose: "Dose", m: "m",
  }[unit] || unit;

  const qtyLabel = (qty != null && qty !== 1)
    ? `${Number.isInteger(qty) ? qty : qty}\u00a0${unitLabel}`
    : unitLabel;
  return `${fmt(price)}\u00a0€/${qtyLabel}`;
}

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
    if (it.category_id != null) {
      const catItem = {
        category_id: Number(it.category_id),
        category_name: String(it.category_name || ""),
      };
      // Preserve brand info for brand-specific products
      if (it.brand != null) {
        catItem.brand = String(it.brand).trim() || null;
        catItem.any_brand = Boolean(it.any_brand);
        catItem.q = String(it.q || it.category_name || "").trim();
      }
      basket.push(catItem);
      continue;
    }
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
  triggerLiveCompare();
}

let _basketJustAdded = false;

function addToBasket(item) {
  basket.push(item);
  _basketJustAdded = true;
  persistBasket();
  _basketJustAdded = false;
  _switchToBasketTabOnMobile();
}

function _switchToBasketTabOnMobile() {
  if (window.innerWidth >= 768) return;
  const tabBar = $("#mobile_tab_bar");
  if (!tabBar) return;
  const basketTab = tabBar.querySelector('[data-tab="basket"]');
  if (basketTab && !basketTab.classList.contains("active")) {
    basketTab.click();
  }
}

function removeFromBasket(index) {
  basket.splice(index, 1);
  persistBasket();
}

function clearBasket() {
  basket.length = 0;
  persistBasket();
}

function _basketItemLabel(it) {
  return it.category_name || it.q || "?";
}

/* ── Basket UI Rendering ── */

function updateAllBasketViews() {
  renderBasketPanel();
  updateMobileFab();
  renderBottomSheetBasket();

  for (const e of $all("#basket_count")) {
    if (e) e.textContent = basket.length;
  }
}

function renderBasketPanel() {
  const section = $("#basket_section");
  const container = $("#basket_items");
  const emptyHint = $("#basket_empty_hint");
  const liveResults = $("#live_results");

  if (!section || !container) return;

  if (basket.length === 0) {
    section.style.display = "none";
    if (emptyHint) emptyHint.style.display = "";
    if (liveResults) liveResults.style.display = "none";
    const chainBar = $("#chain_filters");
    if (chainBar) chainBar.style.display = "none";
    _lastCompareData = null;
    _lastChainList = null;
    return;
  }

  section.style.display = "";
  if (emptyHint) emptyHint.style.display = "none";

  container.innerHTML = "";
  for (let i = 0; i < basket.length; i++) {
    const it = basket[i];
    const label = _basketItemLabel(it);
    const wrapper = el("div", { class: "basket-item-wrapper" });
    const row = el("div", { class: "basket-item" });
    row.appendChild(el("span", { class: "basket-item-name" }, label));
    const actions = el("div", { class: "basket-item-actions" });
    if (_effectiveCategoryId(i) != null) {
      const altBtn = el("button", {
        class: "basket-alt-btn", type: "button",
        "aria-label": `Alternativen f\u00fcr ${label}`,
      }, "\u2194 Alternativen");
      altBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        toggleAlternativesPanel(wrapper, i);
      });
      actions.appendChild(altBtn);
    }
    const remove = el("button", {
      class: "basket-item-x", type: "button",
      "aria-label": `${label} entfernen`,
      onclick: () => removeFromBasket(i),
    }, "\u2715");
    actions.appendChild(remove);
    row.appendChild(actions);
    wrapper.appendChild(row);
    container.appendChild(wrapper);
  }

  if (_basketJustAdded) {
    const lastWrapper = container.lastElementChild;
    if (lastWrapper) {
      const lastItem = lastWrapper.querySelector('.basket-item');
      if (lastItem) {
        lastItem.classList.add('basket-item-new');
        lastItem.addEventListener('animationend', () => lastItem.classList.remove('basket-item-new'), { once: true });
      }
    }
  }
}

/* ── Alternatives Panel ── */

let _altAbort = null;
let _discoveredCategoryIds = {};  // basketIndex → category_id from compare results

function _getLocationParams() {
  const loc = (localStorage.getItem(STORAGE.location) || "").trim();
  const radius = (localStorage.getItem(STORAGE.radiusKm) || "10").trim();
  return { loc, radius };
}

function _effectiveCategoryId(basketIndex) {
  const it = basket[basketIndex];
  if (it && it.category_id != null) return it.category_id;
  return _discoveredCategoryIds[basketIndex] || null;
}

function toggleAlternativesPanel(wrapper, basketIndex) {
  const existing = wrapper.querySelector('.alt-panel');
  if (existing) {
    existing.remove();
    return;
  }
  for (const p of $all('.alt-panel')) p.remove();

  const catId = _effectiveCategoryId(basketIndex);
  if (catId == null) return;

  const panel = el("div", { class: "alt-panel" });
  panel.appendChild(el("div", { class: "alt-panel-loading" }, "Lade Alternativen\u2026"));
  wrapper.appendChild(panel);

  fetchAlternatives(catId, panel, basketIndex);
}

async function fetchAlternatives(categoryId, panel, basketIndex) {
  if (_altAbort) _altAbort.abort();
  _altAbort = new AbortController();

  const { loc, radius } = _getLocationParams();
  let url = `/api/alternative-offers?category_id=${categoryId}`;
  if (loc) url += `&location=${encodeURIComponent(loc)}&radius_km=${encodeURIComponent(radius)}`;
  // Pass active chain filter
  if (_activeChains.size > 0) url += `&chains=${[..._activeChains].join(",")}`;

  try {
    const resp = await fetch(url, { signal: _altAbort.signal });
    const data = await resp.json();
    const groups = data.groups || [];

    panel.innerHTML = "";

    if (groups.length === 0 || groups.every(g => g.offers.length === 0)) {
      panel.appendChild(el("div", { class: "alt-panel-empty" }, "Keine Alternativen verf\u00fcgbar."));
      return;
    }

    const header = el("div", { class: "alt-panel-header" });
    header.appendChild(el("span", { class: "alt-panel-title" }, "Alternativen:"));
    const closeBtn = el("button", { class: "alt-panel-close", type: "button", "aria-label": "Schlie\u00dfen" }, "\u2715");
    closeBtn.addEventListener("click", () => panel.remove());
    header.appendChild(closeBtn);
    panel.appendChild(header);

    for (const group of groups) {
      if (group.offers.length === 0) continue;
      const section = el("div", { class: "alt-group" });
      section.appendChild(el("div", { class: "alt-group-name" }, group.category_name));
      const grid = el("div", { class: "alt-offer-grid" });
      for (const offer of group.offers) {
        grid.appendChild(_buildAltOfferCard(offer, basketIndex));
      }
      section.appendChild(grid);
      panel.appendChild(section);
    }
  } catch (err) {
    if (err.name === 'AbortError') return;
    panel.innerHTML = "";
    panel.appendChild(el("div", { class: "alt-panel-empty" }, "Fehler beim Laden."));
  }
}

function _buildAltOfferCard(offer, basketIndex) {
  const card = el("div", { class: "alt-offer-card" });

  if (offer.image_url) {
    const img = el("img", { class: "alt-offer-img", loading: "lazy", src: offer.image_url, alt: offer.title || "" });
    img.addEventListener("error", () => img.replaceWith(el("div", { class: "alt-offer-img-placeholder" })));
    card.appendChild(img);
  } else {
    card.appendChild(el("div", { class: "alt-offer-img-placeholder" }));
  }

  const body = el("div", { class: "alt-offer-body" });
  body.appendChild(el("div", { class: "alt-offer-chain" }, offer.chain));

  const titleText = (offer.brand ? offer.brand + " " : "") + (offer.title || "");
  body.appendChild(el("div", { class: "alt-offer-title" }, titleText));

  const priceRow = el("div", { class: "alt-offer-price-row" });
  if (offer.price_eur != null) {
    priceRow.appendChild(el("strong", { class: "alt-offer-price" },
      offer.price_eur.toFixed(2).replace(".", ",") + " \u20AC"));
  }
  if (offer.was_price_eur != null && offer.was_price_eur > (offer.price_eur || 0)) {
    priceRow.appendChild(el("span", { class: "was-price" },
      offer.was_price_eur.toFixed(2).replace(".", ",") + " \u20AC"));
  }
  body.appendChild(priceRow);

  const bpLabel = formatBasePrice(offer);
  if (bpLabel) body.appendChild(el("div", { class: "alt-offer-base" }, bpLabel));

  card.appendChild(body);

  card.addEventListener("click", () => {
    // Visual feedback
    card.style.opacity = "0.5";
    card.style.pointerEvents = "none";

    // Close all alt panels before updating basket (prevents DOM destruction race)
    for (const p of $all('.alt-panel')) p.remove();

    // Replace basket item with selected alternative
    const newItem = { category_id: offer.category_id, category_name: offer.category_name || offer.title };
    if (offer.brand) {
      newItem.brand = offer.brand;
      newItem.any_brand = false;
      newItem.q = ((offer.brand || '') + ' ' + (offer.title || '')).trim();
    }
    basket[basketIndex] = newItem;
    persistBasket();
  });

  return card;
}

function showDetailLineAlternatives(lineEl, basketIndex, categoryId) {
  const existing = lineEl.querySelector('.alt-panel');
  if (existing) {
    existing.remove();
    return;
  }
  const parent = lineEl.closest('.store-card-details-body, .compact-detail-body');
  if (parent) {
    for (const p of parent.querySelectorAll('.alt-panel')) p.remove();
  }

  const catId = categoryId || _effectiveCategoryId(basketIndex);
  if (catId == null) return;

  const panel = el("div", { class: "alt-panel" });
  panel.appendChild(el("div", { class: "alt-panel-loading" }, "Lade Alternativen\u2026"));
  lineEl.appendChild(panel);

  fetchAlternatives(catId, panel, basketIndex);
}

/* ── Live Compare ── */

let _compareDebounce = null;
let _compareAbort = null;
let _compareVersion = 0;
let _activeChains = new Set();
let _lastCompareData = null;
let _lastChainList = null;
const CHAIN_FILTER_KEY = "sparfuchs.chainFilter";

function triggerLiveCompare() {
  clearTimeout(_compareDebounce);

  if (basket.length === 0) {
    const liveResults = $("#live_results");
    if (liveResults) liveResults.style.display = "none";
    return;
  }

  // Debounce 300ms to batch rapid additions
  _compareDebounce = setTimeout(() => doLiveCompare(), 300);
}

async function doLiveCompare() {
  const liveResults = $("#live_results");
  const loading = $("#results_loading");
  const errorEl = $("#results_error");
  const ranking = $("#results_ranking");
  const sparMix = $("#results_spar_mix");
  const freshness = $("#results_freshness");

  if (!liveResults || !ranking) return;

  const location = ($("#location") || {}).value || localStorage.getItem(STORAGE.location) || "";
  const radiusKm = ($("#radius_km") || {}).value || localStorage.getItem(STORAGE.radiusKm) || "5";
  const maxStores = ($("#max_stores") || {}).value || "2";

  if (!location.trim()) {
    liveResults.style.display = "";
    if (loading) loading.style.display = "none";
    if (errorEl) { errorEl.style.display = ""; errorEl.textContent = "Bitte Standort eingeben, um Preise zu vergleichen."; }
    ranking.innerHTML = "";
    if (sparMix) sparMix.innerHTML = "";
    return;
  }

  // Show loading
  liveResults.style.display = "";
  if (loading) loading.style.display = "";
  if (errorEl) errorEl.style.display = "none";

  // Abort previous request
  if (_compareAbort) _compareAbort.abort();
  _compareAbort = new AbortController();
  const version = ++_compareVersion;

  try {
    const resp = await fetch("/api/compare", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        location: location.trim(),
        radius_km: parseFloat(radiusKm) || 5,
        basket: basket,
        max_stores: parseInt(maxStores) || 0,
      }),
      signal: _compareAbort.signal,
    });

    // Check if this is still the latest request
    if (version !== _compareVersion) {
      if (loading) loading.style.display = "none";
      return;
    }

    const data = await resp.json();
    if (loading) loading.style.display = "none";

    if (data.error) {
      if (errorEl) { errorEl.style.display = ""; errorEl.textContent = data.error; }
      ranking.innerHTML = "";
      if (sparMix) sparMix.innerHTML = "";
      return;
    }

    if (errorEl) errorEl.style.display = "none";

    _lastCompareData = data;

    // Extract discovered category IDs from compare results (for text-based items)
    _discoveredCategoryIds = {};
    if (data.rows && data.rows.length > 0) {
      const bestRow = data.rows[0];
      for (let li = 0; li < (bestRow.lines || []).length; li++) {
        const offer = (bestRow.lines[li] || {}).offer;
        if (offer && offer.category_id != null) {
          _discoveredCategoryIds[li] = offer.category_id;
        }
      }
    }

    // Re-render basket to show alt buttons for text-based items with discovered categories
    // BUT only if no alt-panel is currently open (to avoid destroying it mid-interaction)
    if (!document.querySelector('.alt-panel')) {
      updateAllBasketViews();
    }

    // Apply the global chain filter to compare results
    applyChainFilter();
    if (freshness) freshness.innerHTML = '';

  } catch (err) {
    if (err.name === 'AbortError') {
      if (loading) loading.style.display = "none";
      return;
    }
    if (version !== _compareVersion) {
      if (loading) loading.style.display = "none";
      return;
    }
    if (loading) loading.style.display = "none";
    if (errorEl) { errorEl.style.display = ""; errorEl.textContent = "Fehler beim Laden der Preise."; }
  }
}

function _mapsLink(lat, lon, address) {
  if (lat && lon) return `https://www.google.com/maps/dir/?api=1&destination=${lat},${lon}`;
  if (address) return `https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(address)}`;
  return null;
}

function _addressHtml(address, lat, lon, distKm) {
  const kmStr = `${distKm.toFixed(1).replace('.', ',')} km`;
  const link = _mapsLink(lat, lon, address);
  if (address && link) {
    return `<a href="${link}" target="_blank" rel="noopener">${address}</a> \u00B7 ${kmStr}`;
  } else if (address) {
    return `${address} \u00B7 ${kmStr}`;
  }
  return kmStr;
}

function buildDetailLines(lines) {
  const frag = document.createDocumentFragment();
  for (let li = 0; li < lines.length; li++) {
    const line = lines[li];
    const lineEl = el("div", { class: "detail-line" + (!line.offer ? " is-missing" : "") });

    if (line.offer && line.offer.image_url) {
      lineEl.appendChild(el("img", { class: "detail-line-img", src: line.offer.image_url, alt: "", loading: "lazy" }));
    }

    const lineInfo = el("div", { class: "detail-line-info" });
    lineInfo.appendChild(el("div", { class: "detail-line-wanted" }, line.wanted));

    if (line.offer) {
      const foundText = [line.offer.brand, line.offer.title].filter(Boolean).join(" ");
      lineInfo.appendChild(el("div", { class: "detail-line-found" }, foundText));
    } else {
      lineInfo.appendChild(el("div", { class: "detail-line-found muted" }, "Kein passendes Angebot"));
    }

    // "Alternativen" link — works for category-based AND text-based items
    const lineCatId = (line.offer && line.offer.category_id != null)
      ? line.offer.category_id
      : _effectiveCategoryId(li);
    if (lineCatId != null) {
      const altLink = el("button", {
        class: "detail-alt-btn", type: "button",
      }, "\u2194 Alternativen anzeigen");
      const idx = li;
      const capturedCatId = lineCatId;
      altLink.addEventListener("click", (e) => {
        e.stopPropagation();
        showDetailLineAlternatives(lineEl, idx, capturedCatId);
      });
      lineInfo.appendChild(altLink);
    }

    lineEl.appendChild(lineInfo);

    const linePrice = el("div", { class: "detail-line-price" });
    if (line.offer && line.offer.price_eur != null) {
      linePrice.appendChild(el("strong", {}, `${line.offer.price_eur.toFixed(2).replace('.', ',')} \u20AC`));
      if (line.offer.was_price_eur && line.offer.was_price_eur > line.offer.price_eur) {
        linePrice.appendChild(el("span", { class: "was-price" }, `${line.offer.was_price_eur.toFixed(2).replace('.', ',')} \u20AC`));
      }
      const bpLabel = formatBasePrice(line.offer);
      if (bpLabel) linePrice.appendChild(el("span", { class: "base-price" }, bpLabel));
    } else if (line.offer) {
      linePrice.appendChild(el("span", { class: "muted" }, "\u2014"));
    } else {
      linePrice.appendChild(el("span", { class: "pill pill-danger" }, "Fehlt"));
    }
    lineEl.appendChild(linePrice);

    frag.appendChild(lineEl);
  }
  return frag;
}

function renderLiveRanking(rows, container, sparMixHtml) {
  container.innerHTML = "";

  if (rows.length === 0) {
    container.appendChild(el("div", { class: "no-results-text" }, "Keine M\u00e4rkte mit Angeboten im Umkreis gefunden."));
    return;
  }

  // Check if there are brand-specific items → split exact vs similar
  const hasBrandItems = rows.some(r => r.is_exact_store !== null && r.is_exact_store !== undefined);
  let exactRows = rows;
  let similarRows = [];

  if (hasBrandItems) {
    exactRows = rows.filter(r => r.is_exact_store === true);
    similarRows = rows.filter(r => r.is_exact_store !== true);
  }

  // Render exact rows (or all rows if no brand items)
  function _renderGroup(group, startRank, showBest) {
    for (let i = 0; i < group.length; i++) {
      const row = Object.assign({}, group[i], { rank: startRank + i });
      if (i < 3) {
        container.appendChild(buildStoreCard(row, showBest && i === 0));
      } else {
        container.appendChild(buildCompactRow(row));
      }
      // Spar-Mix after #1
      if (i === 0 && sparMixHtml && showBest) {
        container.appendChild(sparMixHtml);
      }
    }
    return group.length;
  }

  const exactCount = _renderGroup(exactRows, 1, true);

  // Insert SparMix after #1 if no exact rows but we have similar
  if (exactCount === 0 && sparMixHtml) {
    // Will be added after first similar row instead
  }

  // Divider + similar rows
  if (hasBrandItems && similarRows.length > 0) {
    const divider = el("div", { class: "similar-products-divider" });
    divider.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" style="flex-shrink:0"><path d="M8 1v14M1 8h14" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg> Folgende M\u00e4rkte bieten \u00e4hnliche Produkte';
    container.appendChild(divider);
    _renderGroup(similarRows, exactCount + 1, exactCount === 0);
  }

  // If no exact rows existed but we have sparMix, insert after first similar
  if (exactCount === 0 && sparMixHtml && similarRows.length > 0 && !sparMixHtml.parentNode) {
    const firstCard = container.querySelector('.store-card');
    if (firstCard && firstCard.nextSibling) {
      container.insertBefore(sparMixHtml, firstCard.nextSibling);
    }
  }
}

function buildStoreCard(row, isBest) {
  const card = el("div", { class: "store-card" + (isBest ? " is-best" : "") });

  // Header
  const head = el("div", { class: "store-card-head" });
  const left = el("div", { class: "store-card-info" });

  const rankBadge = el("span", { class: "rank-badge" }, `#${row.rank}`);
  left.appendChild(rankBadge);

  const nameWrap = el("div");
  if (isBest) {
    nameWrap.appendChild(el("span", { class: "best-flag" }, "G\u00fcnstigste Option"));
  }
  nameWrap.appendChild(el("strong", { class: "store-name" }, row.store_name));
  const metaSpan = el("span", { class: "store-meta" });
  metaSpan.innerHTML = _addressHtml(row.address, row.lat, row.lon, row.distance_km);
  nameWrap.appendChild(metaSpan);
  left.appendChild(nameWrap);

  const right = el("div", { class: "store-card-price" });
  if (row.total_eur != null) {
    right.appendChild(el("strong", { class: "total-price" }, `${row.total_eur.toFixed(2).replace('.', ',')} \u20AC`));
  } else {
    right.appendChild(el("span", { class: "muted" }, "Nicht genug Daten"));
  }
  head.appendChild(left);
  head.appendChild(right);
  card.appendChild(head);

  // Progress bar
  const pct = row.total_items > 0 ? Math.round(row.found / row.total_items * 100) : 0;
  const progWrap = el("div", { class: "progress-wrap" });
  const progBar = el("div", { class: "progress-bar" });
  const progFill = el("div", { class: "progress-fill" + (row.found < row.total_items ? " incomplete" : "") });
  progFill.style.width = `${pct}%`;
  progBar.appendChild(progFill);
  progWrap.appendChild(progBar);
  progWrap.appendChild(el("span", { class: "progress-text" }, `${row.found}/${row.total_items} Artikel`));
  card.appendChild(progWrap);

  // Context line
  const ctx = el("div", { class: "store-card-context" });
  if (isBest) {
    if (row.missing_count === 0) {
      ctx.innerHTML = '<span class="context-good">Alles da \u2713</span>';
    } else {
      ctx.innerHTML = `<span class="context-warn">${row.missing_count} Artikel nicht gefunden</span>`;
    }
  } else if (row.diff_eur != null) {
    ctx.innerHTML = `+${row.diff_eur.toFixed(2).replace('.', ',')} \u20AC mehr`;
    if (row.missing_count > 0) {
      ctx.innerHTML += ` \u00B7 <span class="context-warn">${row.missing_count} fehlen</span>`;
    }
  } else if (row.missing_count > 0) {
    ctx.innerHTML = `<span class="context-warn">${row.missing_count} Artikel nicht gefunden</span>`;
  }
  card.appendChild(ctx);

  // Detail accordion (uses shared buildDetailLines)
  const details = el("details", { class: "store-card-details" });
  details.appendChild(el("summary", {}, "Details"));
  const body = el("div", { class: "store-card-details-body" });
  body.appendChild(buildDetailLines(row.lines));
  details.appendChild(body);
  card.appendChild(details);

  return card;
}

function buildCompactRow(row) {
  const wrapper = el("div", { class: "compact-store-wrapper" });

  // Clickable summary row
  const r = el("div", { class: "compact-store-row", "aria-expanded": "false" });
  r.appendChild(el("span", { class: "rank-badge rank-small" }, `#${row.rank}`));

  const nameBlock = el("div", { style: "min-width:0" });
  nameBlock.appendChild(el("span", { class: "compact-name" }, row.store_name));
  const meta = el("div", { class: "compact-meta" });
  meta.innerHTML = _addressHtml(row.address, row.lat, row.lon, row.distance_km);
  nameBlock.appendChild(meta);
  r.appendChild(nameBlock);

  if (row.total_eur != null) {
    r.appendChild(el("span", { class: "compact-price" }, `${row.total_eur.toFixed(2).replace('.', ',')} \u20AC`));
  } else {
    r.appendChild(el("span", { class: "compact-price muted" }, "Keine Daten"));
  }

  if (row.missing_count > 0) {
    r.appendChild(el("span", { class: "compact-missing" }, `${row.missing_count} fehlen`));
  }

  const icon = el("span", { class: "compact-expand-icon" }, "\u25BC");
  r.appendChild(icon);

  // Hidden detail body (lazy-rendered)
  const detailBody = el("div", { class: "compact-detail-body", style: "display:none" });
  let detailRendered = false;

  r.addEventListener("click", () => {
    const expanded = r.getAttribute("aria-expanded") === "true";
    r.setAttribute("aria-expanded", expanded ? "false" : "true");
    detailBody.style.display = expanded ? "none" : "";
    if (!expanded && !detailRendered) {
      detailBody.appendChild(buildDetailLines(row.lines));
      detailRendered = true;
    }
  });

  wrapper.appendChild(r);
  wrapper.appendChild(detailBody);
  return wrapper;
}

function renderLiveSparMix(sparMix, container, rows) {
  if (container) container.innerHTML = "";

  if (!sparMix || !sparMix.lines || sparMix.lines.length === 0) return null;
  if (sparMix.total_eur == null) return null;

  const card = el("div", { class: "spar-mix-card-inline" });

  // Clickable summary header
  const head = el("div", { class: "spar-mix-head", style: "cursor:pointer" });
  head.setAttribute("aria-expanded", "false");
  head.innerHTML = `<span class="spar-mix-icon">\uD83D\uDCA1</span> <strong>Spar-Mix</strong>`;

  const priceRow = el("div", { class: "spar-mix-price-row" });
  priceRow.appendChild(el("strong", { class: "spar-mix-total" }, `${sparMix.total_eur.toFixed(2).replace('.', ',')} \u20AC`));
  priceRow.appendChild(el("span", { class: "spar-mix-store-count" }, `bei ${sparMix.store_count} L\u00e4den`));
  head.appendChild(priceRow);

  if (sparMix.saving_vs_best != null && sparMix.saving_vs_best > 0 && rows.length > 0) {
    head.appendChild(el("div", { class: "spar-mix-saving" },
      `${sparMix.saving_vs_best.toFixed(2).replace('.', ',')} \u20AC weniger als nur bei ${rows[0].chain}`));
  }

  const expandIcon = el("span", { class: "compact-expand-icon" }, "\u25BC");
  head.appendChild(expandIcon);
  card.appendChild(head);

  // Collapsible store breakdown (hidden initially)
  const storeBreakdown = el("div", { class: "spar-mix-breakdown", style: "display:none" });

  // Group lines by store_name for address display
  const byStore = {};
  const missing = [];
  for (const line of sparMix.lines) {
    if (line.chain && line.store_name) {
      const key = `${line.chain}|${line.store_name}`;
      if (!byStore[key]) byStore[key] = { chain: line.chain, store_name: line.store_name, address: line.address, lat: line.lat, lon: line.lon, items: [] };
      byStore[key].items.push(line.wanted);
    } else if (line.chain) {
      const key = line.chain;
      if (!byStore[key]) byStore[key] = { chain: line.chain, store_name: null, address: null, lat: null, lon: null, items: [] };
      byStore[key].items.push(line.wanted);
    } else {
      missing.push(line.wanted);
    }
  }
  for (const key of Object.keys(byStore)) {
    const s = byStore[key];
    const storeLine = el("div", { class: "spar-mix-store-line" });
    const namePart = s.store_name ? `${s.chain} (${s.store_name})` : s.chain;
    let metaHtml = '';
    if (s.address) {
      const link = _mapsLink(s.lat, s.lon, s.address);
      metaHtml = link
        ? ` <span class="compact-meta"><a href="${link}" target="_blank" rel="noopener">${s.address}</a></span>`
        : ` <span class="compact-meta">${s.address}</span>`;
    }
    storeLine.innerHTML = `<strong>${namePart}</strong>${metaHtml}<br>${s.items.join(', ')} (${s.items.length} Artikel)`;
    storeBreakdown.appendChild(storeLine);
  }
  if (missing.length) {
    const missLine = el("div", { class: "spar-mix-store-line spar-mix-missing" });
    missLine.textContent = `Nicht gefunden: ${missing.join(', ')}`;
    storeBreakdown.appendChild(missLine);
  }
  card.appendChild(storeBreakdown);

  // Toggle breakdown on head click
  head.addEventListener("click", () => {
    const expanded = head.getAttribute("aria-expanded") === "true";
    head.setAttribute("aria-expanded", expanded ? "false" : "true");
    storeBreakdown.style.display = expanded ? "none" : "";
  });

  return card;
}

/* ── Chain Filter (Live Compare) ── */

function buildChainFilterBar(chains) {
  const container = document.getElementById("chain_filters");
  if (!container) return;
  container.innerHTML = "";

  if (chains.length === 0) {
    container.style.display = "none";
    return;
  }

  // Use existing _activeChains (already initialized by global filter)
  // Only re-init if _activeChains is empty (no global filter was loaded)
  if (_activeChains.size === 0) {
    const saved = JSON.parse(localStorage.getItem(CHAIN_FILTER_KEY) || "null");
    if (saved && Array.isArray(saved)) {
      for (const c of chains) {
        if (saved.includes(c)) _activeChains.add(c);
      }
      if (_activeChains.size === 0) {
        for (const c of chains) _activeChains.add(c);
      }
    } else {
      for (const c of chains) _activeChains.add(c);
    }
  }

  for (const chain of chains) {
    const isActive = _activeChains.has(chain);
    const btn = el("button", {
      class: "chain-pill" + (isActive ? " active" : ""),
      type: "button",
      "aria-pressed": isActive ? "true" : "false",
    }, chain);

    btn.addEventListener("click", () => {
      const nowActive = _activeChains.has(chain);
      if (nowActive) {
        if (_activeChains.size <= 1) return;
        _activeChains.delete(chain);
      } else {
        _activeChains.add(chain);
      }
      _syncAllChainPills();
      localStorage.setItem(CHAIN_FILTER_KEY, JSON.stringify([..._activeChains]));
      applyChainFilter();
      // Also reload tiles to update counts
      loadCategoryTiles();
      _refreshBrowseIfOpen();
    });

    container.appendChild(btn);
  }

  container.style.display = "";
}

function applyChainFilter() {
  if (!_lastCompareData) {
    _applyChainFilterToBrowse();
    return;
  }

  const ranking = $("#results_ranking");
  const sparMixEl = $("#results_spar_mix");
  if (!ranking) return;

  const allRows = _lastCompareData.rows || [];
  const sparMix = _lastCompareData.spar_mix;

  // Filter rows by active chains
  const filteredRows = allRows.filter(r => _activeChains.has(r.chain));

  // Re-number ranks
  for (let i = 0; i < filteredRows.length; i++) {
    filteredRows[i] = Object.assign({}, filteredRows[i], { rank: i + 1 });
  }

  // Build spar mix card
  let sparMixCard = null;
  if (sparMix && sparMix.lines) {
    const filteredLines = sparMix.lines.filter(l => !l.chain || _activeChains.has(l.chain));
    const filteredTotal = filteredLines.reduce((sum, l) => {
      if (l.offer && l.offer.price_eur != null) return sum + l.offer.price_eur;
      return sum;
    }, 0);
    const filteredStores = [...new Set(filteredLines.map(l => l.chain).filter(Boolean))];

    const filteredSparMix = {
      total_eur: filteredTotal > 0 ? filteredTotal : null,
      store_count: filteredStores.length,
      stores_used: filteredStores,
      lines: filteredLines,
      saving_vs_best: filteredRows.length > 0 && filteredRows[0].total_eur != null && filteredTotal > 0
        ? filteredRows[0].total_eur - filteredTotal
        : null,
    };

    sparMixCard = renderLiveSparMix(filteredSparMix, sparMixEl, filteredRows);
  } else {
    sparMixCard = renderLiveSparMix(sparMix, sparMixEl, filteredRows);
  }

  renderLiveRanking(filteredRows, ranking, sparMixCard);

  // Also filter browse cards if browse is open
  _applyChainFilterToBrowse();
}

function _applyChainFilterToBrowse() {
  const browse = $('#category_browse');
  if (!browse || browse.style.display === 'none') return;
  const cards = browse.querySelectorAll('.offer-card[data-chain]');
  cards.forEach(card => {
    const chain = card.getAttribute('data-chain');
    if (_activeChains.size === 0 || !chain || _activeChains.has(chain)) {
      card.style.display = '';
    } else {
      card.style.display = 'none';
    }
  });
}

/* ── Geolocation ── */

function wireGeolocation() {
  const btn = $("#use_geo");
  const locEl = $("#location");
  if (!btn || !locEl) return;
  btn.addEventListener("click", () => {
    if (!navigator.geolocation) { alert("Standort-Ermittlung nicht unterst\u00fctzt."); return; }
    btn.disabled = true;
    btn.classList.add("is-loading");
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        locEl.value = `${pos.coords.latitude.toFixed(5)}, ${pos.coords.longitude.toFixed(5)}`;
        saveText(STORAGE.location, locEl.value.trim());
        btn.disabled = false;
        btn.classList.remove("is-loading");
        triggerLiveCompare(); // Re-compare with new location
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

function wireIndexPage(skipLoadBasket) {
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
  // Re-compare + refresh tiles when location changes (debounced)
  let _locDebounce = null;
  locEl.addEventListener("change", () => {
    clearTimeout(_locDebounce);
    _locDebounce = setTimeout(() => {
      triggerLiveCompare();
      loadCategoryTiles();
      _refreshBrowseIfOpen();
    }, 500);
  });

  if (radiusEl) {
    radiusEl.addEventListener("input", () => {
      saveText(STORAGE.radiusKm, radiusEl.value.trim());
      syncRadiusLabel();
    });
    // Re-compare + refresh tiles when radius changes
    let _radiusTileDebounce = null;
    radiusEl.addEventListener("change", () => {
      triggerLiveCompare();
      clearTimeout(_radiusTileDebounce);
      _radiusTileDebounce = setTimeout(() => {
        loadCategoryTiles();
        _refreshBrowseIfOpen();
      }, 300);
    });
  }

  const maxStoresEl = $("#max_stores");
  if (maxStoresEl) {
    maxStoresEl.addEventListener("change", () => triggerLiveCompare());
  }

  wireGeolocation();
  wireCategoryAutocomplete();

  // Clear basket
  const clearBtn = $("#clear_basket");
  if (clearBtn) clearBtn.addEventListener("click", clearBasket);

  if (!skipLoadBasket) {
    loadBasket();
  }
  updateAllBasketViews();

  // Trigger initial compare if basket has items
  if (basket.length > 0) {
    triggerLiveCompare();
  }
}

/* ── Category Autocomplete ── */

let _catDebounce = null;
const _catCache = new Map();
const _CAT_CACHE_TTL = 300_000;

function _getCachedOrFilter(query) {
  const cached = _catCache.get(query);
  if (cached && Date.now() - cached.ts < _CAT_CACHE_TTL) {
    return { categories: cached.categories, brands: cached.brands || [], products: cached.products || [], products_total: cached.products_total || 0 };
  }

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
      const filteredProducts = (prefixCache.products || []).filter(p =>
        ((p.brand || "") + " " + p.title).toLowerCase().includes(query.toLowerCase())
      );
      if (filtered.length >= 3 || filteredProducts.length >= 1) return { categories: filtered, brands: filteredBrands, products: filteredProducts, products_total: filteredProducts.length };
      break;
    }
  }
  return null;
}

/* ── Popular Items (on focus when empty) ── */

let _popularItemsCache = null;
let _popularItemsLoading = false;

async function loadPopularItems() {
  if (_popularItemsCache !== null) return _popularItemsCache;
  if (_popularItemsLoading) return [];
  _popularItemsLoading = true;
  try {
    const resp = await fetch('/api/popular-items?limit=8');
    const data = await resp.json();
    _popularItemsCache = data.items || [];
    return _popularItemsCache;
  } catch { _popularItemsCache = []; return []; }
  finally { _popularItemsLoading = false; }
}

function showPopularSuggestions(input, dropdown) {
  loadPopularItems().then(items => {
    if (items.length === 0) return;
    if (input.value.trim()) return;

    dropdown.innerHTML = '';

    const header = el('div', { class: 'suggest-section-header' }, 'Beliebte Artikel');
    dropdown.appendChild(header);

    for (const item of items) {
      const row = el('div', { class: 'suggest-item', role: 'option' });
      const rowInner = el('div', { class: 'suggest-item-row' });
      const nameDiv = el('div', { class: 'suggest-item-name' });
      const nameText = el('div');
      nameText.textContent = item.name;
      nameDiv.appendChild(nameText);
      rowInner.appendChild(nameDiv);

      const countSpan = el('span', { class: 'suggest-item-count' });
      countSpan.textContent = `${item.searches}\u00d7 gesucht`;
      rowInner.appendChild(countSpan);

      row.appendChild(rowInner);
      row.addEventListener('click', () => {
        addToBasket({ category_id: item.id, category_name: item.name });
        dropdown.style.display = 'none';
        input.value = '';
        input.placeholder = `\u2713 ${item.name} hinzugef\u00fcgt!`;
        setTimeout(() => { input.placeholder = "Was brauchst du? Butter, Milch, Eier..."; }, 1500);
        input.focus();
      });
      dropdown.appendChild(row);
    }

    dropdown.style.display = 'block';
    input.setAttribute('aria-expanded', 'true');
  });
}

function wireCategoryAutocomplete() {
  const input = $("#hero_search");
  const dropdown = $("#category_dropdown");
  if (!input || !dropdown) return;

  input.setAttribute("role", "combobox");
  input.setAttribute("aria-autocomplete", "list");
  input.setAttribute("aria-expanded", "false");
  input.setAttribute("aria-controls", "category_dropdown");
  dropdown.setAttribute("role", "listbox");

  input.addEventListener("focus", () => {
    if (!input.value.trim()) {
      showPopularSuggestions(input, dropdown);
    }
  });

  input.addEventListener("input", () => {
    clearTimeout(_catDebounce);
    const q = input.value.trim();
    if (q.length < 2) {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
      return;
    }

    const cached = _getCachedOrFilter(q);
    if (cached) {
      renderCategoryDropdown(cached.categories || cached, dropdown, input, q, null, cached.brands || [], cached.products || [], cached.products_total || 0);
      return;
    }

    _catDebounce = setTimeout(() => fetchCategories(q, dropdown, input), 120);
  });

  document.addEventListener("click", (e) => {
    if (!input.contains(e.target) && !dropdown.contains(e.target)) {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
    }
  });

  input.addEventListener("keydown", (e) => {
    const dropdownHidden = dropdown.style.display === "none";
    if (dropdownHidden && e.key !== "Enter") return;
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
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      idx = Math.max(idx - 1, 0);
      items.forEach((it, i) => {
        it.classList.toggle("active", i === idx);
        it.setAttribute("aria-selected", i === idx ? "true" : "false");
      });
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (active) {
        active.click();
      } else {
        const raw = input.value.trim();
        if (!raw) return;
        const isMobile = window.innerWidth <= 820;
        const segments = isMobile ? [raw] : raw.split(',').map(s => s.trim()).filter(Boolean);
        dropdown.style.display = "none";
        input.setAttribute("aria-expanded", "false");
        input.value = '';
        for (const seg of segments) {
          batchAddItem(seg);
        }
        // On mobile, blur input to dismiss keyboard
        if (isMobile) input.blur();
      }
    } else if (e.key === "Escape") {
      dropdown.style.display = "none";
      input.setAttribute("aria-expanded", "false");
    }
  });
}

async function fetchCategories(q, dropdown, input) {
  dropdown.innerHTML = '<div class="suggest-loading">Suche...</div>';
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
    const products = data.products || [];
    const products_total = data.products_total || 0;
    _catCache.set(q, { categories, brands, products, products_total, ts: Date.now() });
    renderCategoryDropdown(categories, dropdown, input, q, data.corrected_to || null, brands, products, products_total);
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
    nameEl.appendChild(el("mark", {}, name.slice(matchIdx, matchIdx + query.length)));
    nameEl.appendChild(document.createTextNode(name.slice(matchIdx + query.length)));
  } else {
    nameEl.textContent = name;
  }
  return nameEl;
}

function _addAndFeedback(item, input, dropdown) {
  addToBasket(item);
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
  input.placeholder = `\u2713 ${item.category_name} hinzugef\u00fcgt!`;
  setTimeout(() => { input.placeholder = "Was brauchst du? Butter, Milch, Eier..."; }, 1500);

  // On mobile, blur input to dismiss the keyboard
  if (window.innerWidth <= 820) {
    input.blur();
  }
}

function _isInBasket(categoryId) {
  return basket.some(b => b.category_id === categoryId);
}

function renderCategoryDropdown(categories, dropdown, input, query, correctedTo, brands, products, productsTotal) {
  dropdown.innerHTML = "";
  input.removeAttribute("aria-activedescendant");
  products = products || [];
  productsTotal = productsTotal || 0;

  if (correctedTo) {
    const corrRow = el("div", { class: "suggest-correction" });
    corrRow.appendChild(document.createTextNode("Meinten Sie: "));
    corrRow.appendChild(el("strong", {}, correctedTo));
    corrRow.appendChild(document.createTextNode("?"));
    dropdown.appendChild(corrRow);
  }

  if (!categories.length && !products.length && !query) {
    dropdown.style.display = "none";
    return;
  }

  const liveRegion = document.getElementById("suggest-live");
  if (liveRegion) {
    const total = categories.length + products.length;
    liveRegion.textContent = total ? `${total} Vorschl\u00e4ge verf\u00fcgbar` : "Keine Vorschl\u00e4ge gefunden";
  }

  // Product suggestions — grouped by product (shows cheapest price + chain list)
  if (products.length > 0) {
    dropdown.appendChild(el("div", { class: "suggest-section-header" }, "Produkte"));
    for (const prod of products) {
      const itemIdx = dropdown.querySelectorAll(".suggest-item").length;
      const item = el("div", {
        class: "suggest-item suggest-product",
        role: "option",
        id: `suggest-item-${itemIdx}`,
        "aria-selected": "false"
      });
      const row = el("div", { class: "suggest-product-row" });

      // Product image
      if (prod.image_url) {
        row.appendChild(el("img", { class: "suggest-product-img", src: prod.image_url, alt: "", loading: "lazy" }));
      } else {
        row.appendChild(el("div", { class: "suggest-product-img suggest-product-img-empty" }));
      }

      // Info column: title + chain list + base price
      const infoCol = el("div", { class: "suggest-product-info" });
      const titleLine = el("div", { class: "suggest-product-title" });
      const displayTitle = prod.brand ? `${prod.brand} \u2013 ${prod.title}` : prod.title;
      titleLine.appendChild(_highlightMatch(displayTitle, query));
      infoCol.appendChild(titleLine);

      // Compact chain list as mini pills
      const chains = prod.chains || [prod.cheapest_chain || prod.chain];
      if (chains.length > 0) {
        const chainsRow = el("div", { class: "suggest-product-chains-list" });
        for (const ch of chains.slice(0, 6)) {
          chainsRow.appendChild(el("span", { class: "suggest-product-chain-mini" }, ch));
        }
        if (chains.length > 6) {
          chainsRow.appendChild(el("span", { class: "suggest-product-chain-mini" }, `+${chains.length - 6}`));
        }
        infoCol.appendChild(chainsRow);
      }

      // Base price line
      const basePriceLabel = formatBasePrice(prod);
      if (basePriceLabel) {
        infoCol.appendChild(el("div", { class: "suggest-product-meta" }, basePriceLabel));
      }
      row.appendChild(infoCol);

      // Price column — "ab X €" when multiple chains
      const priceCol = el("div", { class: "suggest-product-price" });
      if (prod.price_eur != null) {
        const chainCount = prod.chain_count || 1;
        const prefix = chainCount > 1 ? "ab " : "";
        priceCol.appendChild(el("span", { class: "suggest-product-price-value" }, `${prefix}${Number(prod.price_eur).toFixed(2).replace(".", ",")} \u20ac`));
        // Show cheapest chain name below price
        const cheapestChain = prod.cheapest_chain || (chains.length === 1 ? chains[0] : null);
        if (cheapestChain) {
          priceCol.appendChild(el("span", { class: "suggest-product-cheapest-chain" }, cheapestChain));
        }
        // was_price only meaningful for single-chain products
        if (chainCount === 1 && prod.was_price_eur != null && prod.was_price_eur > prod.price_eur) {
          priceCol.appendChild(el("span", { class: "suggest-product-was-price" }, `${Number(prod.was_price_eur).toFixed(2).replace(".", ",")} \u20ac`));
        }
      }
      row.appendChild(priceCol);

      // Add button
      const addBtn = el("button", { class: "suggest-product-add", title: "Zur Einkaufsliste" }, "+");
      addBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        addToBasket({ q: prod.title, brand: prod.brand || null, any_brand: !prod.brand });
        addBtn.textContent = "\u2713";
        addBtn.classList.add("added");
        input.value = "";
        dropdown.style.display = "none";
        input.setAttribute("aria-expanded", "false");
        input.placeholder = `\u2713 ${prod.brand ? prod.brand + " " : ""}${prod.title} hinzugef\u00fcgt!`;
        setTimeout(() => { input.placeholder = "Was brauchst du? Butter, Milch, Eier..."; }, 1500);
      });
      row.appendChild(addBtn);

      item.appendChild(row);
      item.addEventListener("click", () => { addBtn.click(); });
      dropdown.appendChild(item);
    }

    // "Show all results" link if more than 5 grouped products
    if (productsTotal > 5) {
      const showAllItem = el("div", { class: "suggest-item suggest-show-all" });
      const _loc = (localStorage.getItem(STORAGE.location) || "").trim();
      const _rad = (localStorage.getItem(STORAGE.radiusKm) || "10").trim();
      showAllItem.textContent = `Alle ${productsTotal} Ergebnisse anzeigen \u203a`;
      showAllItem.addEventListener("click", () => {
        dropdown.style.display = "none";
        input.setAttribute("aria-expanded", "false");
        let _searchUrl = `/search?q=${encodeURIComponent(query)}`;
        if (_loc) _searchUrl += `&location=${encodeURIComponent(_loc)}&radius_km=${encodeURIComponent(_rad)}`;
        window.location.href = _searchUrl;
      });
      dropdown.appendChild(showAllItem);
    }
  }

  if (products.length > 0 && categories.length > 0) {
    dropdown.appendChild(el("div", { class: "suggest-section-header" }, "Kategorien"));
  }

  for (const cat of categories) {
    if (cat.type === 'deal_intent') continue; // Skip deal intents

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

    const topRow = el("div", { class: "suggest-item-row" });
    if (inBasket) {
      topRow.appendChild(el("span", { class: "suggest-item-check", title: "Bereits im Warenkorb" }, "\u2713"));
    }
    const nameCol = el("div", { class: "suggest-item-name" });
    const nameEl = el("div", {});
    nameEl.appendChild(_highlightMatch(cat.name, query));
    nameCol.appendChild(nameEl);
    if (cat.oberkategorie) {
      nameCol.appendChild(el("div", { class: "suggest-item-meta" }, cat.oberkategorie));
    }
    topRow.appendChild(nameCol);

    const countText = cat.not_local
        ? `${displayCount} deutschlandweit`
        : (displayCount === 1 ? '1 Angebot' : `${displayCount} Angebote`);
    topRow.appendChild(el("div", { class: "suggest-item-count" + (cat.not_local ? " not-local" : "") }, countText));

    if (hasChildren) {
      topRow.appendChild(el("span", { class: "expand-arrow" }, "\u25B6"));
    }
    item.appendChild(topRow);

    if (hasChildren) {
      const childrenContainer = el("div", { class: "suggest-children", style: "display:none;" });

      const allItem = el("div", { class: "suggest-child-item", style: "font-weight:600;" });
      allItem.appendChild(el("span", {}, `Alle ${cat.name}`));
      allItem.appendChild(el("span", {}, displayCount === 1 ? '1 Angebot' : `${displayCount} Angebote`));
      allItem.addEventListener("click", (e) => {
        e.stopPropagation();
        _addAndFeedback({ category_id: cat.id, category_name: cat.name }, input, dropdown);
      });
      childrenContainer.appendChild(allItem);

      for (const child of cat.children) {
        const childItem = el("div", { class: "suggest-child-item" });
        const childName = el("span", {});
        childName.appendChild(_highlightMatch(child.name, query));
        childItem.appendChild(childName);
        childItem.appendChild(el("span", {}, child.offer_count === 1 ? '1 Angebot' : `${child.offer_count} Angebote`));
        childItem.addEventListener("click", (e) => {
          e.stopPropagation();
          _addAndFeedback({ category_id: child.id, category_name: child.name }, input, dropdown);
        });
        childrenContainer.appendChild(childItem);
      }

      item.appendChild(childrenContainer);
      item.addEventListener("click", () => {
        const visible = childrenContainer.style.display !== "none";
        childrenContainer.style.display = visible ? "none" : "block";
        const arrow = item.querySelector(".expand-arrow");
        if (arrow) arrow.textContent = visible ? "\u25B6" : "\u25BC";
      });
    } else {
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

  const params = new URLSearchParams(window.location.search);
  const q = params.get("q") || "";
  if (q) doSearch(q);

  const sortSel = $("#sort_select");
  if (sortSel) sortSel.addEventListener("change", () => renderFilteredResults());

  const loadMoreBtn = $("#load_more");
  if (loadMoreBtn) loadMoreBtn.addEventListener("click", () => loadMore());
}

async function doSearch(q) {
  const grid = $("#product_grid");
  if (!grid) return;

  let skeletonHtml = "";
  for (let i = 0; i < 6; i++) {
    skeletonHtml += '<div class="skeleton-row"><div class="skeleton-block sk-chain"></div><div class="skeleton-block sk-info"></div><div class="skeleton-block sk-price"></div><div class="skeleton-block sk-btn"></div></div>';
  }
  grid.innerHTML = skeletonHtml;

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

    buildChainFilters(data.available_chains || []);
    renderFilteredResults();

    const chainCount = (data.available_chains || []).length;
    const chainLabel = chainCount === 1 ? "Kette" : "Ketten";
    const counter = $("#result_count");
    if (counter) counter.innerHTML = `<strong>${data.total}</strong> Angebote \u00B7 <strong>${chainCount}</strong> ${chainLabel} \u00B7 <strong>${radius}</strong> km`;

    const wrap = $("#load_more_wrap");
    if (wrap) wrap.style.display = searchState.offset < searchState.total ? "" : "none";
  } catch {
    grid.innerHTML = '<div class="notice error">Fehler beim Laden der Ergebnisse.</div>';
  }
}

function buildChainFilters(chains) {
  const container = $("#chain_filters");
  if (!container) return;
  container.innerHTML = "";
  for (const chain of chains) {
    const chip = el("button", { class: "filter-chip active", type: "button", "data-chain": chain }, chain);
    chip.addEventListener("click", () => { chip.classList.toggle("active"); renderFilteredResults(); });
    container.appendChild(chip);
  }
}

function getActiveChainFilters() {
  return $all("#chain_filters .filter-chip.active").map(btn => btn.dataset.chain);
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
    grid.innerHTML = '<div class="empty-state-text">Keine Ergebnisse f\u00fcr diese Filter.</div>';
    return;
  }

  for (const h of hits) {
    const row = el("div", { class: "product-row" });

    if (h.image_url) {
      row.appendChild(el("img", { class: "row-image", src: h.image_url, alt: h.title || "", loading: "lazy" }));
    }

    const chainCol = el("div", { class: "row-chain" });
    chainCol.appendChild(el("span", { class: "pill" }, h.chain));
    if (h.discount_percent) {
      chainCol.appendChild(el("span", { class: "pill offer" }, `-${h.discount_percent}%`));
    }
    row.appendChild(chainCol);

    const info = el("div", { class: "row-info" });
    const titleText = h.brand ? `${h.brand} \u2013 ${h.title}` : h.title;
    info.appendChild(el("div", { class: "row-title" }, titleText));
    const meta = el("div", { class: "row-meta" });
    const bpLabel = formatBasePrice(h);
    if (bpLabel) meta.appendChild(el("span", {}, bpLabel));
    if (h.valid_from || h.valid_until) {
      meta.appendChild(el("span", {}, [h.valid_from, h.valid_until].filter(Boolean).join(" \u2013 ")));
    }
    if (meta.children.length) info.appendChild(meta);
    row.appendChild(info);

    const priceCol = el("div", { class: "row-price" });
    if (h.price_eur != null) {
      priceCol.appendChild(el("strong", {}, `${Number(h.price_eur).toFixed(2)} \u20AC`));
      if (h.was_price_eur && h.was_price_eur > h.price_eur) {
        priceCol.appendChild(el("span", { class: "was-price" }, `${Number(h.was_price_eur).toFixed(2)} \u20AC`));
      }
    } else {
      priceCol.appendChild(el("span", { class: "muted" }, "\u2014"));
    }
    row.appendChild(priceCol);

    const addBtn = el("button", { class: "row-action", type: "button" }, "+");
    addBtn.addEventListener("click", () => {
      addToBasket({ q: h.title, brand: h.brand || null, any_brand: !h.brand });
      addBtn.textContent = "\u2713";
      addBtn.classList.add("added");
      setTimeout(() => { addBtn.textContent = "+"; addBtn.classList.remove("added"); }, 1500);
    });
    row.appendChild(addBtn);
    grid.appendChild(row);
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

/* ── Batch Add Item ── */

async function batchAddItem(query) {
  try {
    const loc = document.getElementById('location')?.value || '';
    const radius = (localStorage.getItem(STORAGE.radiusKm) || '10').trim();
    let url = `/api/suggest-categories?q=${encodeURIComponent(query)}`;
    if (loc) url += `&location=${encodeURIComponent(loc)}&radius_km=${encodeURIComponent(radius)}`;
    const resp = await fetch(url);
    const data = await resp.json();
    const cats = (data.categories || []).filter(c => c.id != null && c.type !== 'deal_intent');
    if (cats.length > 0) {
      addToBasket({ category_id: cats[0].id, category_name: cats[0].name });
    } else {
      addToBasket({ q: query, brand: null, any_brand: true });
    }
  } catch {
    addToBasket({ q: query, brand: null, any_brand: true });
  }
}

/* ── Load Shared Basket from URL ── */

function loadSharedBasket() {
  const params = new URLSearchParams(window.location.search);
  const items = params.get('items');
  const loc = params.get('loc');
  const radius = params.get('r');

  if (!items) return false;

  if (loc) {
    const locInput = document.getElementById('location');
    if (locInput) locInput.value = loc;
    saveText(STORAGE.location, loc);
  }
  if (radius) {
    const radiusInput = document.getElementById('radius_km');
    const radiusOutput = document.getElementById('radius_value');
    if (radiusInput) {
      radiusInput.value = radius;
      saveText(STORAGE.radiusKm, radius);
      if (radiusOutput) radiusOutput.textContent = radius + ' km';
    }
  }

  const segments = items.split(',').map(s => s.trim()).filter(Boolean);
  if (segments.length === 0) return false;

  basket.length = 0;

  async function batchAddSilent(query) {
    try {
      const locVal = document.getElementById('location')?.value || '';
      const radVal = (localStorage.getItem(STORAGE.radiusKm) || '10').trim();
      let url = `/api/suggest-categories?q=${encodeURIComponent(query)}`;
      if (locVal) url += `&location=${encodeURIComponent(locVal)}&radius_km=${encodeURIComponent(radVal)}`;
      const resp = await fetch(url);
      const data = await resp.json();
      const cats = (data.categories || []).filter(c => c.id != null && c.type !== 'deal_intent');
      if (cats.length > 0) {
        basket.push({ category_id: cats[0].id, category_name: cats[0].name });
      } else {
        basket.push({ q: query, brand: null, any_brand: true });
      }
    } catch {
      basket.push({ q: query, brand: null, any_brand: true });
    }
  }

  Promise.all(segments.map(seg => batchAddSilent(seg))).then(() => {
    persistBasket();
  });

  return true;
}

/* ── Category Tiles ── */

let _allAvailableChains = [];

async function loadCategoryTiles() {
  const container = document.getElementById('category_tiles');
  if (!container) return;

  try {
    const loc = localStorage.getItem(STORAGE.location) || ($('#location') ? $('#location').value : '');
    const radius = localStorage.getItem(STORAGE.radiusKm) || ($('#radius_km') ? $('#radius_km').value : '15');

    const params = new URLSearchParams();
    if (_activeChains.size > 0 && _activeChains.size < _allAvailableChains.length) {
      params.set('chains', [..._activeChains].join(','));
    }
    if (loc.trim()) {
      params.set('location', loc);
      params.set('radius_km', radius);
    }

    const qs = params.toString();
    const resp = await fetch('/api/category-tiles' + (qs ? '?' + qs : ''));
    const data = await resp.json();

    // Build global chain filter bar from available_chains
    if (data.available_chains && data.available_chains.length > 0) {
      _allAvailableChains = data.available_chains;
      buildGlobalChainFilter(data.available_chains);
    }

    if (!data.tiles || data.tiles.length === 0) {
      const section = container.closest('.tiles-section');
      if (section) section.style.display = 'none';
      return;
    }

    const section = container.closest('.tiles-section');
    if (section) section.style.display = '';

    container.innerHTML = '';
    for (const tile of data.tiles) {
      const btn = el('button', { class: 'category-tile', type: 'button' });
      btn.addEventListener('click', () => {
        openCategoryBrowse(tile.id, tile.name);
      });
      btn.innerHTML = `<span class="tile-name">${tile.name}</span><span class="tile-count">${tile.count} Angebote</span>`;
      container.appendChild(btn);
    }
  } catch {
    const section = container.closest('.tiles-section');
    if (section) section.style.display = 'none';
  }
}

function buildGlobalChainFilter(chains) {
  const container = document.getElementById('chain_filters_global');
  const section = document.getElementById('chain_section');
  if (!container || !section) return;

  container.innerHTML = '';
  if (chains.length === 0) {
    section.style.display = 'none';
    return;
  }

  // Load saved filter from localStorage
  const saved = JSON.parse(localStorage.getItem(CHAIN_FILTER_KEY) || "null");
  if (_activeChains.size === 0) {
    if (saved && Array.isArray(saved)) {
      for (const c of chains) {
        if (saved.includes(c)) _activeChains.add(c);
      }
      if (_activeChains.size === 0) {
        for (const c of chains) _activeChains.add(c);
      }
    } else {
      for (const c of chains) _activeChains.add(c);
    }
  }

  for (const chain of chains) {
    const isActive = _activeChains.has(chain);
    const btn = el("button", {
      class: "chain-pill" + (isActive ? " active" : ""),
      type: "button",
      "aria-pressed": isActive ? "true" : "false",
    }, chain);

    btn.addEventListener("click", () => {
      const nowActive = _activeChains.has(chain);
      if (nowActive) {
        if (_activeChains.size <= 1) return;
        _activeChains.delete(chain);
      } else {
        _activeChains.add(chain);
      }
      // Update all pills (global + compare)
      _syncAllChainPills();
      localStorage.setItem(CHAIN_FILTER_KEY, JSON.stringify([..._activeChains]));
      // Reload tiles with new filter
      loadCategoryTiles();
      // Re-filter browse if open
      _refreshBrowseIfOpen();
      // Re-filter compare results if present
      if (_lastCompareData) applyChainFilter();
    });

    container.appendChild(btn);
  }

  section.style.display = '';
}

function _syncAllChainPills() {
  // Sync visual state of all chain pills (global + compare filter bars)
  for (const bar of ['#chain_filters_global', '#chain_filters']) {
    const container = document.querySelector(bar);
    if (!container) continue;
    const pills = container.querySelectorAll('.chain-pill');
    pills.forEach(pill => {
      const chain = pill.textContent;
      if (_activeChains.has(chain)) {
        pill.classList.add('active');
        pill.setAttribute('aria-pressed', 'true');
      } else {
        pill.classList.remove('active');
        pill.setAttribute('aria-pressed', 'false');
      }
    });
  }
}

function _refreshBrowseIfOpen() {
  const browse = $('#category_browse');
  if (!browse || browse.style.display === 'none') return;
  const title = browse.querySelector('.browse-title');
  if (!title) return;
  // Re-open the current category browse (will re-fetch with new chain filter)
  const state = history.state;
  if (state && state.browse) {
    // Clear cache for this category
    _browseCache.clear();
    openCategoryBrowse(state.browse, state.name || title.textContent);
  }
}

/* ── Mobile FAB + Bottom Sheet ── */

function updateMobileFab() {
  const fab = $("#basket_fab");
  if (fab) {
    const badge = $("#basket_fab_count");
    if (badge) badge.textContent = basket.length;
    fab.classList.toggle("has-items", basket.length > 0);
    fab.style.display = basket.length > 0 ? "" : "none";
  }
  // Update tab bar badge
  const tabBadge = $("#tab_basket_badge");
  if (tabBadge) {
    tabBadge.textContent = basket.length;
    tabBadge.style.display = basket.length > 0 ? "" : "none";
  }
}

function wireMobileTabBar() {
  const tabBar = $("#mobile_tab_bar");
  if (!tabBar) return;

  const tabs = tabBar.querySelectorAll(".mobile-tab");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.tab;
      tabs.forEach(t => t.classList.toggle("active", t === tab));
      if (target === "basket") {
        document.body.classList.add("mobile-tab-basket");
      } else {
        document.body.classList.remove("mobile-tab-basket");
      }
    });
  });
}

function renderBottomSheetBasket() {
  const container = $("#sheet_basket_items");
  if (!container) return;
  container.innerHTML = "";

  if (basket.length === 0) {
    container.appendChild(el("div", { class: "sidebar-empty" }, "Noch keine Artikel in der Liste."));
    return;
  }

  for (let i = 0; i < basket.length; i++) {
    const it = basket[i];
    const label = _basketItemLabel(it);
    const row = el("div", { class: "sidebar-item" });
    const info = el("div");
    info.appendChild(el("strong", {}, label));
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

  // Swipe-to-close gesture
  _wireBottomSheetSwipe();
}

/* ── Bottom Sheet Swipe-to-Close ── */

function _wireBottomSheetSwipe() {
  const sheet = $("#basket_sheet");
  if (!sheet) return;

  const panel = sheet.querySelector(".bottom-sheet-panel");
  const handle = sheet.querySelector(".bottom-sheet-handle");
  if (!panel) return;

  let startY = 0;
  let startTime = 0;
  let currentY = 0;
  let isDragging = false;

  function onTouchStart(e) {
    const touch = e.touches[0];
    const panelRect = panel.getBoundingClientRect();
    const touchRelY = touch.clientY - panelRect.top;

    const body = panel.querySelector(".bottom-sheet-body");
    const isAtTop = !body || body.scrollTop <= 0;
    const isInHandleZone = touchRelY < 60;

    if (!isInHandleZone && !isAtTop) return;

    startY = touch.clientY;
    startTime = Date.now();
    currentY = 0;
    isDragging = true;
    panel.classList.add("is-dragging");
  }

  function onTouchMove(e) {
    if (!isDragging) return;

    const touch = e.touches[0];
    const deltaY = touch.clientY - startY;

    if (deltaY < 0) {
      currentY = 0;
      panel.style.transform = "translateY(0)";
      return;
    }

    currentY = deltaY;
    panel.style.transform = `translateY(${deltaY}px)`;

    const backdropEl = sheet.querySelector(".bottom-sheet-backdrop");
    if (backdropEl) {
      const progress = Math.min(deltaY / 300, 1);
      backdropEl.style.opacity = String(1 - progress * 0.6);
    }

    e.preventDefault();
  }

  function onTouchEnd() {
    if (!isDragging) return;
    isDragging = false;
    panel.classList.remove("is-dragging");

    const elapsed = Date.now() - startTime;
    const velocity = currentY / Math.max(elapsed, 1);

    const shouldClose = currentY > 100 || (velocity > 0.5 && currentY > 30);

    if (shouldClose) {
      panel.style.transition = "transform 0.25s cubic-bezier(0.32, 0.72, 0, 1)";
      panel.style.transform = "translateY(100%)";
      const backdropEl = sheet.querySelector(".bottom-sheet-backdrop");
      if (backdropEl) {
        backdropEl.style.transition = "opacity 0.25s ease";
        backdropEl.style.opacity = "0";
      }
      setTimeout(() => {
        closeBottomSheet();
        panel.style.transition = "";
        panel.style.transform = "";
        if (backdropEl) {
          backdropEl.style.transition = "";
          backdropEl.style.opacity = "";
        }
      }, 260);
    } else {
      panel.style.transition = "transform 0.2s cubic-bezier(0.32, 0.72, 0, 1)";
      panel.style.transform = "translateY(0)";
      const backdropEl = sheet.querySelector(".bottom-sheet-backdrop");
      if (backdropEl) {
        backdropEl.style.transition = "opacity 0.2s ease";
        backdropEl.style.opacity = "";
      }
      setTimeout(() => {
        panel.style.transition = "";
      }, 210);
    }

    currentY = 0;
  }

  const targets = handle ? [handle, panel] : [panel];
  for (const target of targets) {
    target.addEventListener("touchstart", onTouchStart, { passive: true });
    target.addEventListener("touchmove", onTouchMove, { passive: false });
    target.addEventListener("touchend", onTouchEnd, { passive: true });
  }
}

/* ── Toast ── */

function showToast(message) {
  let toast = document.getElementById('toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = 'toast';
    toast.style.cssText = 'position:fixed;bottom:80px;left:50%;transform:translateX(-50%);padding:10px 20px;border-radius:8px;background:#1A3A32;color:white;font-size:14px;font-weight:500;z-index:200;opacity:0;transition:opacity 200ms;';
    document.body.appendChild(toast);
  }
  toast.textContent = message;
  toast.style.opacity = '1';
  setTimeout(() => { toast.style.opacity = '0'; }, 2000);
}

/* ── Category Browse ── */

const _browseCache = new Map();
let _browseAbort = null;
let _browseScrollSpy = null;

function openCategoryBrowse(categoryId, categoryName) {
  // Only push a new history entry if we're not already in browse mode.
  // Re-opening (e.g. after chain filter change) uses replaceState to avoid stacking.
  const alreadyBrowsing = history.state && history.state.browse;
  if (alreadyBrowsing) {
    history.replaceState({ browse: categoryId, name: categoryName }, '', '');
  } else {
    history.pushState({ browse: categoryId, name: categoryName }, '', '');
  }

  const tilesSection = $('.tiles-section');
  if (tilesSection) tilesSection.style.display = 'none';

  // Also hide the search + location cards to give browse full space
  const cards = document.querySelectorAll('.index-col-left > .index-card');

  let panel = $('#category_browse');
  if (!panel) {
    panel = el('div', { id: 'category_browse', class: 'category-browse' });
    const colLeft = $('.index-col-left');
    if (colLeft) colLeft.appendChild(panel);
  }
  panel.style.display = '';
  panel.innerHTML = '';

  // Header
  const header = el('div', { class: 'browse-header' });
  const backBtn = el('button', { class: 'browse-back', type: 'button' });
  backBtn.innerHTML = '\u2190 Zur\u00fcck';
  backBtn.addEventListener('click', () => {
    closeCategoryBrowse();
    // Clean up history: go back to the state before browse was opened.
    // Use replaceState to avoid stacking additional entries.
    history.replaceState(null, '', location.pathname);
  });
  header.appendChild(backBtn);
  header.appendChild(el('h2', { class: 'browse-title' }, categoryName));
  panel.appendChild(header);

  // Tabs container (sticky, populated after fetch)
  const tabs = el('div', { class: 'browse-tabs', id: 'browse_tabs' });
  panel.appendChild(tabs);

  // Scrollable content area
  const scrollArea = el('div', { class: 'browse-scroll', id: 'browse_scroll' });
  // Skeleton
  const skeleton = _buildBrowseSkeleton();
  scrollArea.appendChild(skeleton);
  panel.appendChild(scrollArea);

  // Fetch all subcategories with their offers
  _fetchAllSubcategories(categoryId, categoryName, tabs, scrollArea);
}

function _buildBrowseSkeleton() {
  const grid = el('div', { class: 'browse-skeleton' });
  for (let i = 0; i < 6; i++) {
    const card = el('div', { class: 'skeleton-card' });
    card.appendChild(el('div', { class: 'skeleton-img' }));
    const body = el('div', { class: 'skeleton-body' });
    body.appendChild(el('div', { class: 'skeleton-line' }));
    body.appendChild(el('div', { class: 'skeleton-line short' }));
    card.appendChild(body);
    grid.appendChild(card);
  }
  return grid;
}

async function _fetchAllSubcategories(parentId, parentName, tabsContainer, scrollArea) {
  if (_browseAbort) _browseAbort.abort();
  _browseAbort = new AbortController();

  const loc = localStorage.getItem(STORAGE.location) || ($('#location') ? $('#location').value : '');
  const radius = localStorage.getItem(STORAGE.radiusKm) || ($('#radius_km') ? $('#radius_km').value : '15');
  const chainsParam = (_activeChains.size > 0 && _activeChains.size < _allAvailableChains.length)
    ? [..._activeChains].join(',') : '';
  const cacheKey = `all:${parentId}:${loc}:${radius}:${chainsParam}`;

  if (_browseCache.has(cacheKey)) {
    _renderAllSections(_browseCache.get(cacheKey), tabsContainer, scrollArea);
    return;
  }

  const params = new URLSearchParams({
    category_id: parentId,
    location: loc,
    radius_km: radius,
    limit: 200,
  });
  if (chainsParam) params.set('chains', chainsParam);

  try {
    const resp = await fetch(`/api/offers-by-category?${params}`, { signal: _browseAbort.signal });
    const data = await resp.json();

    // If there are subcategories, also fetch each subcategory's offers individually
    if (data.subcategories && data.subcategories.length > 0) {
      const sections = [];

      // Fetch each subcategory
      const fetches = data.subcategories.map(async (sub) => {
        const subParams = new URLSearchParams({
          category_id: sub.id,
          location: loc,
          radius_km: radius,
          limit: 30,
        });
        if (chainsParam) subParams.set('chains', chainsParam);
        try {
          const subResp = await fetch(`/api/offers-by-category?${subParams}`, { signal: _browseAbort.signal });
          const subData = await subResp.json();
          return { name: sub.name, id: sub.id, offers: subData.offers || [], count: sub.count };
        } catch (e) {
          if (e.name === 'AbortError') throw e;
          return { name: sub.name, id: sub.id, offers: [], count: sub.count };
        }
      });

      const results = await Promise.all(fetches);
      // Only keep sections that have offers
      for (const r of results) {
        if (r.offers.length > 0) sections.push(r);
      }

      const cacheData = { parentName, sections };
      _browseCache.set(cacheKey, cacheData);
      _renderAllSections(cacheData, tabsContainer, scrollArea);
    } else {
      // No subcategories — just show the offers flat
      const cacheData = { parentName, sections: [{ name: parentName, id: parentId, offers: data.offers || [], count: data.total }] };
      _browseCache.set(cacheKey, cacheData);
      _renderAllSections(cacheData, tabsContainer, scrollArea);
    }
  } catch (err) {
    if (err.name === 'AbortError') return;
    scrollArea.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:32px 0;">Fehler beim Laden.</div>';
  }
}

function _renderAllSections(data, tabsContainer, scrollArea) {
  scrollArea.innerHTML = '';
  tabsContainer.innerHTML = '';

  const sections = data.sections;
  if (!sections || sections.length === 0) {
    scrollArea.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:32px 0;">Keine Angebote in deiner N\u00e4he gefunden.</div>';
    return;
  }

  // Build tabs
  const sectionEls = [];
  sections.forEach((sec, idx) => {
    const tab = el('button', {
      class: 'browse-tab' + (idx === 0 ? ' active' : ''),
      type: 'button',
      'data-section-idx': String(idx),
    }, sec.name);
    tab.addEventListener('click', () => {
      const target = scrollArea.querySelector(`[data-section="${idx}"]`);
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    tabsContainer.appendChild(tab);
  });

  // Build sections with grids
  sections.forEach((sec, idx) => {
    const sectionDiv = el('div', { class: 'browse-section', 'data-section': String(idx) });
    sectionDiv.appendChild(el('h3', { class: 'browse-section-header' }, sec.name));

    const grid = el('div', { class: 'browse-grid' });
    for (const offer of sec.offers) {
      const card = buildOfferCard(offer);
      card.setAttribute('data-chain', offer.chain || '');
      // Hide if chain filter is active and chain not selected
      if (_activeChains.size > 0 && offer.chain && !_activeChains.has(offer.chain)) {
        card.style.display = 'none';
      }
      grid.appendChild(card);
    }
    sectionDiv.appendChild(grid);
    scrollArea.appendChild(sectionDiv);
    sectionEls.push(sectionDiv);
  });

  // Scroll-spy: highlight active tab as user scrolls
  if (_browseScrollSpy) {
    scrollArea.removeEventListener('scroll', _browseScrollSpy);
  }
  _browseScrollSpy = () => {
    const scrollTop = scrollArea.scrollTop;
    let activeIdx = 0;
    for (let i = sectionEls.length - 1; i >= 0; i--) {
      if (sectionEls[i].offsetTop - scrollArea.offsetTop <= scrollTop + 60) {
        activeIdx = i;
        break;
      }
    }
    tabsContainer.querySelectorAll('.browse-tab').forEach((t, i) => {
      t.classList.toggle('active', i === activeIdx);
      if (i === activeIdx) {
        t.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
      }
    });
  };
  scrollArea.addEventListener('scroll', _browseScrollSpy, { passive: true });
}

function buildOfferCard(offer) {
  const card = el('div', { class: 'offer-card' });

  // Image
  if (offer.image_url) {
    const img = el('img', { class: 'offer-card-img', loading: 'lazy', src: offer.image_url, alt: offer.title || '' });
    img.addEventListener('error', () => {
      const placeholder = el('div', { class: 'offer-card-img-placeholder' });
      img.replaceWith(placeholder);
    });
    card.appendChild(img);
  } else {
    card.appendChild(el('div', { class: 'offer-card-img-placeholder' }));
  }

  // Body
  const body = el('div', { class: 'offer-card-body' });

  if (offer.chain) {
    body.appendChild(el('div', { class: 'offer-card-chain' }, offer.chain));
  }

  const titleText = (offer.brand ? offer.brand + ' ' : '') + (offer.title || '');
  body.appendChild(el('div', { class: 'offer-card-title' }, titleText));

  // Price row
  const priceRow = el('div', { class: 'offer-card-price-row' });
  if (offer.price_eur != null) {
    priceRow.appendChild(el('strong', { class: 'offer-card-price' }, _formatPrice(offer.price_eur)));
  }
  if (offer.was_price_eur != null && offer.was_price_eur > (offer.price_eur || 0)) {
    priceRow.appendChild(el('span', { class: 'was-price' }, _formatPrice(offer.was_price_eur)));
  }
  body.appendChild(priceRow);

  // Base price
  const cardBpLabel = formatBasePrice(offer);
  if (cardBpLabel) {
    body.appendChild(el('div', { class: 'offer-card-base' }, cardBpLabel));
  }

  card.appendChild(body);

  // Add button
  const addBtn = el('button', { class: 'offer-card-add', type: 'button', 'aria-label': 'Zum Warenkorb' }, '+');
  addBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    const basketItem = { category_id: offer.category_id, category_name: offer.category_name || offer.title };
    if (offer.brand) {
      basketItem.brand = offer.brand;
      basketItem.any_brand = false;
      basketItem.q = (offer.brand + ' ' + (offer.title || '')).trim();
    }
    addToBasket(basketItem);

    addBtn.textContent = '\u2713';
    addBtn.classList.add('added');
    setTimeout(() => {
      addBtn.textContent = '+';
      addBtn.classList.remove('added');
    }, 1500);

    // Update browse header badge
  });
  card.appendChild(addBtn);

  return card;
}

function _formatPrice(val) {
  return val.toFixed(2).replace('.', ',') + ' \u20AC';
}


function closeCategoryBrowse() {
  const panel = $('#category_browse');
  if (panel) panel.style.display = 'none';

  const tilesSection = $('.tiles-section');
  if (tilesSection) tilesSection.style.display = '';

  if (_browseAbort) {
    _browseAbort.abort();
    _browseAbort = null;
  }
  _browseScrollSpy = null;
}

/* ── Init ── */

window.addEventListener("DOMContentLoaded", () => {
  if ($("#hero_search")) {
    const hasSharedBasket = loadSharedBasket();
    wireIndexPage(hasSharedBasket);
    loadCategoryTiles();
  }
  else if ($("#search_input")) wireSearchPage();

  wireBottomSheet();
  wireMobileTabBar();

  window.addEventListener('popstate', (e) => {
    const panel = $('#category_browse');
    if (panel && panel.style.display !== 'none') {
      if (e.state && e.state.browse) {
        openCategoryBrowse(e.state.browse, e.state.name);
      } else {
        closeCategoryBrowse();
      }
    }
  });
});

// PWA Service Worker registration
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/sw.js', { scope: '/' }).catch(() => {});
}
