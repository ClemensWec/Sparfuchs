from app.services.category_classifier import classify_category_name


def test_buttercroissant_is_croissant_not_butter() -> None:
    result = classify_category_name("Buttercroissant")
    assert result.product_type_normalized == "croissant"
    assert result.parent_category_normalized == "backwaren"
    assert "butter" in result.ingredient_tags_json


def test_butter_chicken_is_not_classified_as_butter() -> None:
    result = classify_category_name("Butter Chicken")
    assert result.product_type_normalized == "chicken"
    assert result.parent_category_normalized == "fertiggerichte"


def test_h_milch_is_group_but_h_milch_15_is_exact() -> None:
    generic = classify_category_name("H-Milch")
    specific = classify_category_name("H-Milch 1,5% Fett")
    assert generic.semantic_group == "hmilch"
    assert generic.search_scope == "group"
    assert specific.semantic_group == "hmilch"
    assert specific.search_scope == "exact"
