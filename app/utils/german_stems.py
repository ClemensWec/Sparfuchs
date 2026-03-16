"""German singular/plural expansion for grocery search.

Uses a curated stem map for common grocery terms (especially umlaut changes
that rules can't handle) plus rule-based suffix stripping as fallback.

All forms are in normalized space (post normalize_search_text: ae/oe/ue/ss).
"""
from __future__ import annotations

# Each tuple groups all known forms of a word (normalized).
# First entry is canonical but all directions are supported.
_GROCERY_FORMS: list[tuple[str, ...]] = [
    # Obst
    ("apfel", "aepfel"),
    ("tafelapfel", "tafelaepfel"),
    ("banane", "bananen"),
    ("orange", "orangen"),
    ("zitrone", "zitronen"),
    ("traube", "trauben"),
    ("kirsche", "kirschen"),
    ("birne", "birnen"),
    ("pflaume", "pflaumen"),
    ("himbeere", "himbeeren"),
    ("erdbeere", "erdbeeren"),
    ("blaubeere", "blaubeeren"),
    ("heidelbeere", "heidelbeeren"),
    ("mango", "mangos"),
    ("avocado", "avocados"),
    ("kiwi", "kiwis"),
    ("clementine", "clementinen"),
    ("mandarine", "mandarinen"),
    ("nektarine", "nektarinen"),
    ("ananas", "ananasse"),
    # Gemuese
    ("tomate", "tomaten"),
    ("kartoffel", "kartoffeln"),
    ("zwiebel", "zwiebeln"),
    ("gurke", "gurken"),
    ("karotte", "karotten"),
    ("moehre", "moehren"),
    ("paprika", "paprikas"),
    ("zucchini", "zucchinis"),
    ("aubergine", "auberginen"),
    ("bohne", "bohnen"),
    ("erbse", "erbsen"),
    ("linse", "linsen"),
    ("olive", "oliven"),
    ("radieschen", "radieschen"),
    ("champignon", "champignons"),
    ("pilz", "pilze"),
    # Fleisch / Wurst
    ("wurst", "wuerste", "wuerstchen"),
    ("bratwurst", "bratwuerste"),
    ("rostbratwurst", "rostbratwuerste"),
    ("currywurst", "currywuerste"),
    ("steak", "steaks"),
    ("filet", "filets"),
    ("schnitzel", "schnitzel"),
    ("haehnchen", "haehnchen", "huhn", "huehnchen", "huehner"),
    ("haehnchenbrustfilet", "haehnchenbrustfilets"),
    ("schinken", "schinken"),
    ("wuerstchen", "wuerstchen"),
    ("frikadelle", "frikadellen"),
    ("bulette", "buletten"),
    ("garnele", "garnelen"),
    # Milchprodukte
    ("joghurt", "joghurts", "jogurt", "jogurts", "yoghurt", "yoghurts"),
    ("kaese", "kaese"),
    ("milch", "milch"),
    ("butter", "butter"),
    ("sahne", "sahne"),
    ("quark", "quark"),
    # Backwaren
    ("brot", "brote", "broetchen"),
    ("broetchen", "broetchen"),
    ("brezel", "brezeln"),
    ("croissant", "croissants"),
    ("kuchen", "kuchen"),
    ("torte", "torten"),
    ("semmel", "semmeln"),
    # Grundnahrungsmittel
    ("nudel", "nudeln"),
    ("spaghetti", "spaghetti"),
    ("reis", "reis"),
    ("mehl", "mehl"),
    ("zucker", "zucker"),
    ("salz", "salz"),
    ("oel", "oele"),
    ("essig", "essig"),
    ("nuss", "nuesse"),
    ("haselnuss", "haselnuesse"),
    ("walnuss", "walnuesse"),
    ("erdnuss", "erdnuesse"),
    ("mandel", "mandeln"),
    # Getraenke
    ("wasser", "waesser"),
    ("saft", "saefte"),
    ("apfelsaft", "apfelsaefte"),
    ("orangensaft", "orangensaefte"),
    ("limonade", "limonaden"),
    ("bier", "biere"),
    ("wein", "weine"),
    ("flasche", "flaschen"),
    ("dose", "dosen"),
    # Verpackung / Mengeneinheiten
    ("packung", "packungen"),
    ("beutel", "beutel"),
    ("stueck", "stueck"),
    ("scheibe", "scheiben"),
    ("glas", "glaeser"),
    ("becher", "becher"),
    ("tafel", "tafeln"),
    ("riegel", "riegel"),
    # Sonstiges
    ("ei", "eier"),
    ("pfeffer", "pfeffer"),
    ("gewuerz", "gewuerze"),
    ("chips", "chips"),
    ("keks", "kekse"),
    ("praline", "pralinen"),
    ("schokolade", "schokoladen"),
    ("bonbon", "bonbons"),
    ("muesli", "mueslis"),
    ("pizza", "pizzen", "pizzas"),
    ("tiefkuehlpizza", "tiefkuehlpizzen"),
    ("waffel", "waffeln"),
    ("pfannkuchen", "pfannkuchen"),
]

# Build lookup: normalized_form -> set of all sibling forms
_FORM_MAP: dict[str, set[str]] = {}
for group in _GROCERY_FORMS:
    all_forms = set(group)
    for form in group:
        if form in _FORM_MAP:
            _FORM_MAP[form] |= all_forms
        else:
            _FORM_MAP[form] = set(all_forms)


def _rule_based_variants(token: str) -> set[str]:
    """Generate candidate forms by common German plural suffix rules."""
    variants: set[str] = set()
    n = len(token)

    # Strip plural suffixes to find potential singular
    if n > 4 and token.endswith("en"):
        variants.add(token[:-2])       # tomaten -> tomat (needs map) / kartoffeln -> kartoffel
        variants.add(token[:-1])       # nudeln -> nudel (if -n rule)
    elif n > 3 and token.endswith("n"):
        variants.add(token[:-1])       # nudeln -> nudel, brezeln -> brezel
    if n > 3 and token.endswith("e"):
        variants.add(token[:-1])       # biere -> bier, pilze -> pilz
    if n > 4 and token.endswith("er"):
        variants.add(token[:-2])       # eier -> ei, glaeser -> glas (needs map)
    if n > 3 and token.endswith("s"):
        variants.add(token[:-1])       # joghurts -> joghurt, steaks -> steak

    # Add plural suffixes to find potential plural
    variants.add(token + "n")          # nudel -> nudeln, birne -> birnen
    variants.add(token + "en")         # tomate -> tomaten (if doesn't already end in e)
    variants.add(token + "e")          # bier -> biere, pilz -> pilze
    variants.add(token + "er")         # ei -> eier
    variants.add(token + "s")          # steak -> steaks, joghurt -> joghurts

    # Remove self and too-short candidates
    variants.discard(token)
    return {v for v in variants if len(v) >= 2}


def get_token_variants(token: str) -> set[str]:
    """Get all known forms for a normalized token.

    Returns a set including the token itself plus any known variants.
    Map lookup first, then rule-based fallback (max 4 variants total).
    """
    result = {token}

    # 1. Check curated map (handles umlaut changes like apfel<->aepfel)
    if token in _FORM_MAP:
        result |= _FORM_MAP[token]
        return result

    # 2. Check if token is a compound ending with a mapped word
    #    e.g. "tafelaepfel" -> check "aepfel" in map
    for mapped_form, siblings in _FORM_MAP.items():
        if token.endswith(mapped_form) and len(token) > len(mapped_form):
            prefix = token[: len(token) - len(mapped_form)]
            for sibling in siblings:
                result.add(prefix + sibling)
            if len(result) > 1:
                return result

    # 3. Rule-based fallback
    rule_variants = _rule_based_variants(token)
    # Limit to avoid query explosion
    for v in sorted(rule_variants, key=len)[:4]:
        result.add(v)

    return result


def expand_query_tokens(normalized_query: str) -> list[set[str]]:
    """Expand each token in the query to its variant set.

    Returns a list of sets, one per token.
    E.g. "aepfel kuchen" -> [{"aepfel", "apfel"}, {"kuchen"}]
    """
    tokens = normalized_query.split()
    return [get_token_variants(t) for t in tokens if t]
