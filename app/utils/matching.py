"""
Verbessertes Fuzzy-Matching für Produktsuche.

Strategien:
1. Token-basiertes Matching (für Wort-Übereinstimmungen)
2. Substring-Erkennung (für deutsche Komposita: "milch" in "Buttermilch")
3. Umlaut-aware Matching (Originaltext + normalisiert + stripped prüfen)
4. Levenshtein-basierte Tippfehler-Toleranz
"""
from __future__ import annotations

import re
import unicodedata

from rapidfuzz import fuzz
from rapidfuzz.distance import Levenshtein


# Schwellenwerte für Matching
MIN_SCORE_WITH_PRICE = 55
MIN_SCORE_WITHOUT_PRICE = 60
SUBSTRING_BONUS = 25
TYPO_BONUS = 20


def normalize_text(text: str) -> str:
    """Normalize with umlaut expansion: ä→ae, ö→oe, ü→ue, ß→ss."""
    if not text:
        return ""
    s = text.lower().strip()
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _strip_umlauts(text: str) -> str:
    """Strip umlauts to base vowels: ä→a, ö→o, ü→u, ß→ss. No expansion."""
    if not text:
        return ""
    s = text.lower().strip()
    s = s.replace("ä", "a").replace("ö", "o").replace("ü", "u").replace("ß", "ss")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_substring_match(query: str, offer_text: str,
                         *, q_norm: str = "", o_norm: str = "",
                         q_strip: str = "", o_strip: str = "") -> bool:
    """Check if query appears as substring in offer text.

    Tests three ways:
    1. Original lowercase (catches "apfel" in "Apfelmark")
    2. Normalized with expansion (catches "kaese" in "Frischkäse" → "frischkaese")
    3. Stripped umlauts (catches "apfel" in "Tafeläpfel" → "tafelapfel")

    Pre-computed normalized/stripped forms can be passed to avoid redundant work.
    """
    q_lower = query.lower().strip()
    o_lower = offer_text.lower().strip()
    if not q_lower or not o_lower:
        return False

    # 1. Original text
    if q_lower in o_lower:
        return True

    # 2. Normalized (umlaut expanded) — use pre-computed if available
    qn = q_norm or normalize_text(query)
    on = o_norm or normalize_text(offer_text)
    if qn and on and qn in on:
        return True

    # 3. Stripped umlauts (ä→a, not ä→ae) — catches "apfel" in "Tafeläpfel"
    qs = q_strip or _strip_umlauts(query)
    os_ = o_strip or _strip_umlauts(offer_text)
    if qs and os_ and qs in os_:
        return True

    return False


def _has_close_typo(query_words: list[str], offer_words: set[str]) -> bool:
    """Check if query words are within typo distance of offer words.

    Allows 1 edit for words up to 5 chars, 2 edits for longer words.
    Sliding window only for compound words where query covers >50% of the word.
    """
    for qw in query_words:
        if len(qw) < 5:
            # Words under 5 chars are too short for reliable typo matching
            # (e.g. "eier" vs "bier" = distance 1 but different products)
            return False
        max_dist = 1 if len(qw) <= 7 else 2
        found = False
        for ow in offer_words:
            # Direct word comparison (similar length words)
            if abs(len(qw) - len(ow)) <= max_dist:
                if Levenshtein.distance(qw, ow, score_cutoff=max_dist) <= max_dist:
                    found = True
                    break

            # Sliding window for compound words
            # Only if query covers > 85% of the offer word (prevents false positives
            # like "biere" matching "biene" in "bienen" or "beere" in "waldbeere")
            if len(ow) > len(qw) and len(qw) / len(ow) > 0.85:
                for i in range(len(ow) - len(qw) + 1):
                    window = ow[i:i + len(qw)]
                    if Levenshtein.distance(qw, window, score_cutoff=max_dist) <= max_dist:
                        found = True
                        break
            if found:
                break
        if not found:
            return False
    return True


def _score_single_query(q_norm: str, o_norm: str, query_raw: str, offer_raw: str,
                         *, q_strip: str = "", o_strip: str = "") -> float:
    """Score a single normalized query against a normalized offer."""
    # Fuzzy scores
    token_score = fuzz.token_set_ratio(q_norm, o_norm)
    partial_score = fuzz.partial_ratio(q_norm, o_norm)

    # Weighted base score (partial_ratio is better for compounds)
    base_score = token_score * 0.5 + partial_score * 0.5

    # Substring bonus: query is contained in offer text (handles German compounds)
    is_substr = _is_substring_match(query_raw, offer_raw,
                                     q_norm=q_norm, o_norm=o_norm,
                                     q_strip=q_strip, o_strip=o_strip)
    if is_substr:
        base_score += SUBSTRING_BONUS

    # Word-level match bonus (all query words found as prefixes/substrings in normalized)
    query_words = q_norm.split()
    offer_words = set(o_norm.split())
    all_words_found = all(
        any(qw in ow for ow in offer_words)
        for qw in query_words
    )
    if all_words_found and not is_substr:
        base_score += 15

    # Compound-only penalty: when query is found only as part of a compound
    # word (e.g. "butter" in "Buttercroissant") but not as a standalone word
    # (e.g. "Butter" in "Irische Butter"), reduce score so real products
    # outscore compound matches in the pricer.
    exact_word_match = all(
        any(qw == ow for ow in offer_words)
        for qw in query_words
    )
    if (is_substr or all_words_found) and not exact_word_match:
        base_score -= 8

    # Typo tolerance: if no exact match found, check Levenshtein distance
    if not is_substr and not all_words_found:
        if _has_close_typo(query_words, offer_words):
            base_score += TYPO_BONUS

    # Multi-word coverage penalty: when query has 2+ words and not all are
    # found in the offer, penalize proportionally to missing words.
    # This prevents "Milch-Schnitte" matching "haltbare milch" (only 1/2 words).
    if len(query_words) >= 2 and not all_words_found and not is_substr:
        words_found = sum(
            1 for qw in query_words if any(qw in ow for ow in offer_words)
        )
        missing_ratio = (len(query_words) - words_found) / len(query_words)
        base_score -= missing_ratio * 20

    # Pure fuzzy cap: when no substring, no word match, and no typo was found,
    # the match is based only on fuzzy string similarity which is unreliable
    # for short queries (e.g. "bananen" vs "Orangen"). Cap these at 50.
    has_any_structural_match = is_substr or all_words_found
    if not has_any_structural_match:
        has_typo = _has_close_typo(query_words, offer_words) if not is_substr and not all_words_found else False
        if not has_typo:
            base_score = min(base_score, 50.0)

    return min(base_score, 100.0)


# Abbreviation expansion: short form -> long form only.
# We only expand short abbreviations to their full form so that
# "H-Milch" can match "haltbare milch" queries. We do NOT map
# long form -> short form to avoid false positives ("h" in "schnitte").
_ABBREVIATION_EXPAND: dict[str, str] = {
    "h milch": "haltbare milch",
    "tk": "tiefkuehl",
    "bio milch": "biomilch",
    # Compound word splits for common grocery terms
    "tiefkuehlpizza": "tiefkuehl pizza",
    "tiefkuehlkost": "tiefkuehl kost",
    "tiefkuehlgemuese": "tiefkuehl gemuese",
    "aufbackbroetchen": "aufback broetchen",
    "orangensaft": "orangen saft",
    "apfelsaft": "apfel saft",
    "hundefutter": "hunde futter",
    "katzenfutter": "katzen futter",
    "alufolie": "alu folie",
    "frischhaltefolie": "frischhalte folie",
}

# Bidirectional consumer synonyms: common search terms ↔ DB terms.
# Both directions are tried during scoring.
_CONSUMER_SYNONYMS: list[tuple[str, str]] = [
    # Existing
    ("marmelade", "konfituere"),
    ("zahnpasta", "zahncreme"),
    ("weintrauben", "trauben"),
    ("weintraube", "traube"),
    ("wodka", "vodka"),
    # Regional variants
    ("broetchen", "semmel"),
    ("broetchen", "schrippe"),
    ("quark", "topfen"),
    ("sahne", "rahm"),
    ("sahne", "obers"),
    ("hackfleisch", "faschiertes"),
    ("hackfleisch", "gehacktes"),
    ("kartoffel", "erdapfel"),
    ("tomate", "paradeiser"),
    ("aprikose", "marille"),
    ("blumenkohl", "karfiol"),
    ("meerrettich", "kren"),
    ("pfannkuchen", "eierkuchen"),
    ("pfannkuchen", "palatschinken"),
    # Spelling variants
    ("joghurt", "jogurt"),
    ("joghurt", "yoghurt"),
    ("ketchup", "ketschup"),
    ("thunfisch", "tunfisch"),
    ("mayonnaise", "majonnaese"),
    ("mozzarella", "mozzarela"),
    # Colloquial / short forms
    ("klopapier", "toilettenpapier"),
    ("spueli", "spuelmittel"),
    ("limo", "limonade"),
    ("pommes", "pommes frites"),
    ("wuerstchen", "wiener"),
]


def _expand_abbreviations(text: str) -> str:
    """Expand known product abbreviations in normalized text."""
    for abbrev, full in _ABBREVIATION_EXPAND.items():
        # Replace abbreviation with full form (whole-word match via split/join)
        words = text.split()
        abbrev_words = abbrev.split()
        result_words: list[str] = []
        i = 0
        while i < len(words):
            if words[i:i + len(abbrev_words)] == abbrev_words:
                result_words.extend(full.split())
                i += len(abbrev_words)
            else:
                result_words.append(words[i])
                i += 1
        expanded = " ".join(result_words)
        if expanded != text:
            return expanded
    return text


def calculate_match_score(query: str, offer_text: str) -> float:
    """Berechnet Match-Score mit Compound-Word, Typo- und Plural-Unterstützung."""
    if not query or not offer_text:
        return 0.0

    q_norm = normalize_text(query)
    o_norm = normalize_text(offer_text)

    if not q_norm or not o_norm:
        return 0.0

    # Pre-compute stripped-umlaut forms once (avoids redundant calls in _is_substring_match)
    q_strip = _strip_umlauts(query)
    o_strip = _strip_umlauts(offer_text)

    # Score with original query
    best = _score_single_query(q_norm, o_norm, query, offer_text,
                                q_strip=q_strip, o_strip=o_strip)
    if best >= 100.0:
        return best

    # Expand abbreviations in offer text (e.g. "h milch" -> "haltbare milch")
    # so that query "haltbare milch" matches offer "H-Milch 1,5% Fett".
    o_expanded = _expand_abbreviations(o_norm)
    if o_expanded != o_norm:
        s = _score_single_query(q_norm, o_expanded, query, offer_text,
                                 q_strip=q_strip, o_strip=o_strip)
        if s > best:
            best = s
            if best >= 100.0:
                return best

    # Also expand abbreviations in query (e.g. query "h milch" -> "haltbare milch")
    q_expanded = _expand_abbreviations(q_norm)
    if q_expanded != q_norm:
        s = _score_single_query(q_expanded, o_norm, query, offer_text,
                                 q_strip=q_strip, o_strip=o_strip)
        if s > best:
            best = s
            if best >= 100.0:
                return best

    # Try consumer synonyms bidirectionally (marmelade↔konfitüre, zahnpasta↔zahncreme)
    for term_a, term_b in _CONSUMER_SYNONYMS:
        for src, dst in [(term_a, term_b), (term_b, term_a)]:
            if src in q_norm:
                q_syn = q_norm.replace(src, dst)
                s = _score_single_query(q_syn, o_norm, query, offer_text,
                                         q_strip=q_strip, o_strip=o_strip)
                if s > best:
                    best = s
                    if best >= 100.0:
                        return best
            if src in o_norm:
                o_syn = o_norm.replace(src, dst)
                s = _score_single_query(q_norm, o_syn, query, offer_text,
                                         q_strip=q_strip, o_strip=o_strip)
                if s > best:
                    best = s
                    if best >= 100.0:
                        return best

    # Try plural/singular variants — single-token substitution instead of cross-product.
    # For 3 tokens × 5 variants each: 15 calls instead of 125.
    from app.utils.german_stems import expand_query_tokens
    token_variant_sets = expand_query_tokens(q_norm)
    has_variants = any(len(vs) > 1 for vs in token_variant_sets)
    if has_variants:
        base_tokens = q_norm.split()
        variant_lists = [sorted(v for v in vs if len(v) >= 3) for vs in token_variant_sets]
        # Single-token substitution: replace one token at a time
        for i, variants in enumerate(variant_lists):
            if i >= len(base_tokens):
                break
            for v in variants:
                if v == base_tokens[i]:
                    continue
                trial = list(base_tokens)
                trial[i] = v
                alt_q = " ".join(trial)
                s = _score_single_query(alt_q, o_norm, query, offer_text,
                                         q_strip=q_strip, o_strip=o_strip)
                if s > best:
                    best = s
                    if best >= 100.0:
                        return best

    return best


def is_good_match(query: str, offer_text: str, has_price: bool = True) -> tuple[bool, float]:
    score = calculate_match_score(query, offer_text)
    threshold = MIN_SCORE_WITH_PRICE if has_price else MIN_SCORE_WITHOUT_PRICE
    return (score >= threshold, score)
