from __future__ import annotations

import json
import re
from dataclasses import dataclass

from app.utils.text import compact_text, normalize_search_text


@dataclass(frozen=True)
class CategoryClassification:
    canonical_name: str
    name_normalized: str
    product_type: str
    product_type_normalized: str
    parent_category: str
    parent_category_normalized: str
    semantic_group: str
    search_scope: str
    ingredient_tags_json: str
    attributes_json: str
    classification_confidence: float
    classification_source: str


_NON_FOOD_OVERRIDES = {
    "scheuermilch": ("Reiniger", "Haushalt", "reiniger", "exact"),
    "reiniger": ("Reiniger", "Haushalt", "reiniger", "broad"),
}

_SPECIAL_FOOD_OVERRIDES = {
    "butterchicken": ("Chicken", "Fertiggerichte", "chicken", "exact", ["butter"]),
    "erdnussbutter": ("Erdnussbutter", "Aufstriche", "erdnussbutter", "group", []),
    "peanutbutter": ("Erdnussbutter", "Aufstriche", "erdnussbutter", "group", []),
    "buttertoast": ("Toast", "Backwaren", "toast", "group", ["butter"]),
}

_HEAD_RULES: list[tuple[str, str, str, str]] = [
    ("brustfilet", "Brustfilet", "Gefluegel", "haehnchenbrust"),
    ("croissant", "Croissant", "Backwaren", "croissant"),
    ("toast", "Toast", "Backwaren", "toast"),
    ("broetchen", "Broetchen", "Backwaren", "broetchen"),
    ("baguette", "Baguette", "Backwaren", "baguette"),
    ("schnitte", "Schnitte", "Suesswaren", "schnitte"),
    ("riegel", "Riegel", "Suesswaren", "riegel"),
    ("dessert", "Dessert", "Molkerei", "dessert"),
    ("joghurt", "Joghurt", "Molkerei", "joghurt"),
    ("camembert", "Camembert", "Kaese", "camembert"),
    ("kaese", "Kaese", "Kaese", "kaese"),
    ("schokolade", "Schokolade", "Suesswaren", "schokolade"),
    ("schorle", "Schorle", "Getraenke", "schorle"),
    ("saft", "Saft", "Getraenke", "saft"),
    ("wurst", "Wurst", "Wurst", "wurst"),
    ("mortadella", "Mortadella", "Wurst", "mortadella"),
    ("salami", "Salami", "Wurst", "salami"),
    ("schenkel", "Schenkel", "Gefluegel", "haehnchenschenkel"),
    ("schnitzel", "Schnitzel", "Gefluegel", "haehnchenschnitzel"),
    ("geschnetzeltes", "Geschnetzeltes", "Gefluegel", "haehnchen_geschnetzeltes"),
    ("nuggets", "Nuggets", "Gefluegel", "haehnchen_nuggets"),
    ("milch", "Milch", "Molkerei", "trinkmilch"),
    ("butter", "Butter", "Molkerei", "butter"),
]

_INGREDIENT_CANDIDATES = {
    "butter",
    "milch",
    "kaese",
    "honig",
    "vanille",
    "schokolade",
    "tomate",
}

_ATTRIBUTE_PATTERNS: list[tuple[str, str, object]] = [
    (r"\bbio\b", "bio", True),
    (r"\bdemeter\b", "demeter", True),
    (r"\blaktosefrei\b", "laktosefrei", True),
    (r"\bh milch\b|\bhmilch\b", "haltbar", True),
    (r"\bhaltbare?\b", "haltbar", True),
    (r"\b1 5\b|\b1,5\b", "fett", "1.5"),
    (r"\b3 5\b|\b3,5\b", "fett", "3.5"),
]

_POULTRY_TOKENS = {"haehnchen", "huhn", "gefluegel", "maishaehnchen"}


def classify_category_name(name: str) -> CategoryClassification:
    canonical_name = compact_text(name)
    normalized = normalize_search_text(canonical_name)
    compact = normalized.replace(" ", "")
    ingredients: set[str] = set()
    attributes: dict[str, object] = {}

    for pattern, key, value in _ATTRIBUTE_PATTERNS:
        if re.search(pattern, normalized):
            attributes[key] = value

    product_type = canonical_name
    parent_category = "Sonstige"
    semantic_group = normalized or "sonstige"
    search_scope = "exact"
    confidence = 0.55

    for marker, (ptype, parent, group, scope) in _NON_FOOD_OVERRIDES.items():
        if marker in compact:
            product_type = ptype
            parent_category = parent
            semantic_group = group
            search_scope = scope
            confidence = 0.98
            return _build_classification(
                canonical_name=canonical_name,
                normalized=normalized,
                product_type=product_type,
                parent_category=parent_category,
                semantic_group=semantic_group,
                search_scope=search_scope,
                ingredients=sorted(ingredients),
                attributes=attributes,
                confidence=confidence,
            )

    for marker, (ptype, parent, group, scope, fixed_ingredients) in _SPECIAL_FOOD_OVERRIDES.items():
        if marker in compact:
            product_type = ptype
            parent_category = parent
            semantic_group = group
            search_scope = scope
            ingredients.update(fixed_ingredients)
            confidence = 0.97
            return _build_classification(
                canonical_name=canonical_name,
                normalized=normalized,
                product_type=product_type,
                parent_category=parent_category,
                semantic_group=semantic_group,
                search_scope=search_scope,
                ingredients=sorted(ingredients),
                attributes=attributes,
                confidence=confidence,
            )

    head = _detect_head(normalized, compact)
    if head is not None:
        product_type, parent_category, semantic_group = head
        search_scope = _infer_search_scope(
            normalized=normalized,
            product_type=product_type,
            semantic_group=semantic_group,
        )
        confidence = 0.86

    if _contains_poultry(normalized):
        product_type, semantic_group, search_scope = _classify_poultry(
            normalized=normalized,
            default_type=product_type,
            default_group=semantic_group,
        )
        parent_category = "Gefluegel" if parent_category == "Sonstige" else parent_category
        attributes["tierart"] = "haehnchen"
        confidence = max(confidence, 0.9)

    if product_type == "Milch":
        semantic_group = _milk_group(normalized, compact)
        search_scope = _infer_search_scope(normalized=normalized, product_type=product_type, semantic_group=semantic_group)
        confidence = max(confidence, 0.92)
    elif product_type == "Butter":
        semantic_group = "butter"
        search_scope = _infer_search_scope(normalized=normalized, product_type=product_type, semantic_group=semantic_group)
        confidence = max(confidence, 0.94)

    for ingredient in _INGREDIENT_CANDIDATES:
        if ingredient == normalize_search_text(product_type):
            continue
        if f" {ingredient} " in f" {normalized} " or compact.startswith(ingredient):
            ingredients.add(ingredient)

    if product_type == "Croissant" and "butter" in compact:
        ingredients.add("butter")
    if product_type in {"Schnitte", "Riegel", "Dessert", "Joghurt", "Schokolade"} and "milch" in compact:
        ingredients.add("milch")
    if product_type == "Butter":
        ingredients.discard("butter")
    if product_type == "Milch":
        ingredients.discard("milch")

    return _build_classification(
        canonical_name=canonical_name,
        normalized=normalized,
        product_type=product_type,
        parent_category=parent_category,
        semantic_group=semantic_group,
        search_scope=search_scope,
        ingredients=sorted(ingredients),
        attributes=attributes,
        confidence=confidence,
    )


def _build_classification(
    *,
    canonical_name: str,
    normalized: str,
    product_type: str,
    parent_category: str,
    semantic_group: str,
    search_scope: str,
    ingredients: list[str],
    attributes: dict[str, object],
    confidence: float,
) -> CategoryClassification:
    return CategoryClassification(
        canonical_name=canonical_name,
        name_normalized=normalized,
        product_type=product_type,
        product_type_normalized=normalize_search_text(product_type),
        parent_category=parent_category,
        parent_category_normalized=normalize_search_text(parent_category),
        semantic_group=semantic_group,
        search_scope=search_scope,
        ingredient_tags_json=json.dumps(ingredients, ensure_ascii=False),
        attributes_json=json.dumps(attributes, ensure_ascii=False, sort_keys=True),
        classification_confidence=round(confidence, 3),
        classification_source="rule",
    )


def _detect_head(normalized: str, compact: str) -> tuple[str, str, str] | None:
    for suffix, product_type, parent_category, semantic_group in _HEAD_RULES:
        if compact.endswith(suffix) or f" {suffix} " in f" {normalized} " or normalized.endswith(" " + suffix):
            return (product_type, parent_category, semantic_group)
    return None


def _contains_poultry(normalized: str) -> bool:
    return any(token in normalized.split() or token in normalized.replace(" ", "") for token in _POULTRY_TOKENS)


def _classify_poultry(
    *,
    normalized: str,
    default_type: str,
    default_group: str,
) -> tuple[str, str, str]:
    compact = normalized.replace(" ", "")
    if "brust" in normalized or compact.endswith("brustfilet"):
        if "filet" in normalized or compact.endswith("brustfilet"):
            return ("Haehnchenbrustfilet", "haehnchenbrust", "exact")
        return ("Haehnchenbrust", "haehnchenbrust", "broad")
    if "schenkel" in normalized or "keule" in normalized:
        return ("Haehnchenschenkel", "haehnchenschenkel", "group")
    if "schnitzel" in normalized:
        return ("Haehnchenschnitzel", "haehnchenschnitzel", "group")
    if "mortadella" in normalized or "wurst" in normalized or "salami" in normalized:
        return ("Gefluegelwurst", "gefluegelwurst", "group")
    if default_group != normalized:
        return (default_type, default_group, "group")
    return ("Haehnchen", "haehnchen", "broad")


def _milk_group(normalized: str, compact: str) -> str:
    if "h milch" in normalized or "hmilch" in compact:
        return "hmilch"
    if "haltbare milch" in normalized:
        return "haltbare_milch"
    return "trinkmilch"


def _infer_search_scope(*, normalized: str, product_type: str, semantic_group: str) -> str:
    if any(char.isdigit() for char in normalized):
        return "exact"
    if normalized == normalize_search_text(product_type):
        return "broad"
    if semantic_group in {"hmilch", "haltbare_milch", "haehnchenschenkel", "haehnchenschnitzel", "gefluegelwurst"}:
        return "group"
    if "filet" in normalized or "kirschtomate" in normalized or "curry" in normalized:
        return "exact"
    return "group"
