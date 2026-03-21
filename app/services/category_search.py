"""Category search service using LLM-generated search labels + categories_v2.

Search flow:
  1. FTS5 prefix search on search_labels_fts (118K labels)
  2. LIKE fallback for substring matches
  3. Group results by category (categories_v2 level 2=Gruppe, level 3=Spezifisch)
  4. Aggregate level-3 hits into level-2 group results
  5. Rank by match quality + offer count

Tables used:
  - search_labels_fts: FTS5 index on search_labels (product_name, label, label_type)
  - search_labels: 118K labels (suchbegriff, marke, gattung) per product
  - product_labels: product_name → category_v2_id, marke, gattungsbegriff
  - categories_v2: 3-level hierarchy (level 1=Ober, level 2=Gruppe, level 3=Spezifisch)
"""

from __future__ import annotations

import enum
import math
import re
import sqlite3
from pathlib import Path


class MatchTier(enum.IntEnum):
    """Match quality tiers for category search ranking.

    Lower value = better match. Within the same tier, offer_count decides.
    This bucketing approach avoids the fragility of fine-grained numeric scores
    where a 1-point difference can flip rankings (see IR literature on bucketed
    ranking vs. fine-grained scoring).
    """
    EXACT = 1       # Category name == query exactly
    STRONG = 2      # Word-prefix, compound-suffix, multi-token intersection
    PARTIAL = 3     # Compound-prefix (long), word-match in name
    WEAK = 4        # Short compound-prefix, random substring
    INDIRECT = 5    # Label-only match (category name doesn't contain query)

from app.services.spell_check import SpellCheckService
from app.utils.german_stems import expand_query_tokens
from app.utils.text import normalize_search_text

# Bidirectional synonym pairs for search (regex pattern, replacement).
_SEARCH_SYNONYMS: list[tuple[str, str]] = [
    (r"\bh\s*milch", "haltbare milch"),
    (r"\bhmilch", "haltbare milch"),
    (r"\bh\s*vollmilch", "haltbare vollmilch"),
]

# Simple keyword synonyms: each group maps bidirectionally.
# When user searches any term, we also search all other terms in the group.
_KEYWORD_SYNONYMS: list[tuple[str, ...]] = [
    ("klopapier", "toilettenpapier"),
    ("spüli", "spülmittel"),
    ("spueli", "spuelmittel"),  # normalized form
    ("limo", "limonade"),
    ("pommes", "pommes frites", "fritten"),
    ("brötchen", "semmel", "schrippe"),
    ("broetchen", "semmel", "schrippe"),  # normalized
    ("pfannkuchen", "eierkuchen", "palatschinken"),
    ("tk", "tiefkühl", "tiefkuehl"),
    ("joghurt", "jogurt"),
    ("ketchup", "ketschup"),
    ("hackfleisch", "gehacktes", "faschiertes"),
    ("sahne", "rahm"),
    ("quark", "topfen"),
    ("marmelade", "konfitüre", "konfituere"),
    ("paprika", "peperoni"),
    ("aprikose", "marille"),
    ("blumenkohl", "karfiol"),
    ("tomate", "paradeiser"),
    ("pfifferlinge", "eierschwammerl"),
    ("meerrettich", "kren"),
    ("pflaumen", "zwetschgen", "zwetschken"),
    ("berliner", "krapfen"),
    ("feldsalat", "rapunzel", "nüsslisalat", "nuessisalat"),
    ("aubergine", "melanzani"),
]

# Directed synonyms: specific → general (Recall expansion).
# "schlagrahm" → expand to "sahne" (user searching specific should also find general)
# But "sahne" should NOT expand to "schlagrahm" (too specific).
_DIRECTED_SYNONYMS: list[tuple[str, list[str]]] = [
    # (specific_term, [broader_terms])
    ("schlagrahm", ["sahne", "rahm"]),
    ("vollmilch", ["milch"]),
    ("buttermilch", ["milch"]),
    ("hafermilch", ["milch", "haferdrink"]),
    ("sojamilch", ["milch", "sojadrink"]),
    ("espresso", ["kaffee"]),
    ("cappuccino", ["kaffee"]),
    ("latte macchiato", ["kaffee"]),
    ("weizenbier", ["bier"]),
    ("pils", ["bier"]),
    ("rotwein", ["wein"]),
    ("weisswein", ["wein"]),
    ("orangensaft", ["saft"]),
    ("apfelsaft", ["saft"]),
    ("multivitaminsaft", ["saft"]),
    ("schwarztee", ["tee"]),
    ("gruentee", ["tee"]),
    ("fruechtetee", ["tee"]),
    ("cheddar", ["kaese"]),
    ("gouda", ["kaese"]),
    ("mozzarella", ["kaese"]),
    ("emmentaler", ["kaese"]),
    ("salami", ["wurst"]),
    ("leberwurst", ["wurst"]),
    ("bratwurst", ["wurst"]),
    ("spaghetti", ["nudeln", "pasta"]),
    ("penne", ["nudeln", "pasta"]),
    ("fusilli", ["nudeln", "pasta"]),
    ("basmati", ["reis"]),
    ("jasminreis", ["reis"]),
]


def _umlaut_variants(text: str) -> list[str]:
    """Generate umlaut variants: ae→ä, oe→ö, ue→ü, ss→ß and vice versa.

    Produces all combinations so 'nuesse' → ['nüsse', 'nueße', 'nüße']
    rather than only the all-at-once replacement.

    A negative list prevents false positives where digraphs are NOT umlauts:
    'abenteuer' must NOT become 'abentüer', 'steuer' must NOT become 'stür'.
    """
    _PAIRS = [("ae", "ä"), ("oe", "ö"), ("ue", "ü"), ("ss", "ß")]

    # Stems where the digraph is NOT an umlaut — skip that specific pair only.
    _NEGATIVE_STEMS: dict[str, frozenset[str]] = {
        "ue": frozenset([
            "abenteuer", "steuer", "feuer", "teuer", "ungeheuer", "heuer",
            "scheuer", "treue", "neue", "reue", "bauer", "lauer", "mauer",
            "sauer", "trauer", "dauer", "schauer", "knauer",
        ]),
        "ae": frozenset([
            "israel", "michael", "raphael", "aegypten",
        ]),
        "oe": frozenset([
            "poet", "boeing", "aloe", "canoe", "oboe",
        ]),
    }

    # Find which replacements apply (forward and reverse)
    applicable: list[tuple[str, str]] = []
    for ascii_form, umlaut_form in _PAIRS:
        if ascii_form in text:
            # Check negative list: skip this pair if text contains a blocked stem
            blocked = _NEGATIVE_STEMS.get(ascii_form)
            if blocked and any(stem in text for stem in blocked):
                continue
            applicable.append((ascii_form, umlaut_form))
        if umlaut_form in text:
            applicable.append((umlaut_form, ascii_form))

    if not applicable:
        return []

    # Cap applicable pairs at 4 to limit combinatorial explosion (2^4 = 16 max)
    applicable = applicable[:4]

    # Generate all 2^N combinations (skip the 0-replacement case = original)
    results: set[str] = set()
    for mask in range(1, 1 << len(applicable)):
        candidate = text
        for i, (src, dst) in enumerate(applicable):
            if mask & (1 << i):
                candidate = candidate.replace(src, dst)
        if candidate != text:
            results.add(candidate)

    # Cap total variants at 7, preferring single replacements (lower mask values)
    return list(results)[:7]


def _synonym_variants(normalized: str) -> list[str]:
    """Return alternative query forms via synonym expansion."""
    variants: list[str] = []

    # Regex-based synonyms (H-Milch etc.)
    for pattern, replacement in _SEARCH_SYNONYMS:
        expanded = re.sub(pattern, replacement, normalized)
        if expanded != normalized:
            variants.append(expanded)
        collapsed = re.sub(
            re.escape(replacement),
            pattern.replace(r"\b", "").replace(r"\s*", " "),
            normalized,
        )
        if collapsed != normalized and collapsed not in variants:
            variants.append(collapsed)

    # Keyword synonyms: check each token individually and generate variants
    query_tokens = normalized.lower().split()
    if len(query_tokens) >= 1:
        for i, token in enumerate(query_tokens):
            for group in _KEYWORD_SYNONYMS:
                if token in group:
                    for replacement in group:
                        if replacement != token:
                            # Replace this token and rebuild query
                            new_tokens = query_tokens.copy()
                            new_tokens[i] = replacement
                            variant = " ".join(new_tokens)
                            if variant != normalized and variant not in variants:
                                variants.append(variant)

    # Directed synonyms: specific → broader terms
    # Only expands in one direction (specific→general), never general→specific.
    for i, token in enumerate(query_tokens):
        for specific, broader_list in _DIRECTED_SYNONYMS:
            if token == specific:
                for broader in broader_list:
                    new_tokens = query_tokens.copy()
                    new_tokens[i] = broader
                    variant = " ".join(new_tokens)
                    if variant != normalized and variant not in variants:
                        variants.append(variant)

    # Directed synonyms: multi-word specific terms (e.g. "latte macchiato")
    full_query = " ".join(query_tokens)
    for specific, broader_list in _DIRECTED_SYNONYMS:
        if " " in specific and specific in full_query:
            for broader in broader_list:
                variant = full_query.replace(specific, broader, 1)
                if variant != normalized and variant not in variants:
                    variants.append(variant)

    return variants


class CategorySearchService:
    def __init__(self, db_path: Path | str) -> None:
        self._db_path = str(db_path)
        self._spell = SpellCheckService(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA cache_size = -32768")  # 32MB cache
        conn.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
        return conn

    def available(self) -> bool:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) as c FROM sqlite_master "
                    "WHERE type='table' AND name='categories_v2'"
                ).fetchone()
            return bool(row and row["c"] > 0)
        except Exception:
            return False

    def search(
        self,
        query: str,
        limit: int = 8,
        *,
        _corrected: bool = False,
        local_category_counts: dict[int, int] | None = None,
    ) -> list[dict]:
        normalized = normalize_search_text(query)
        if not normalized:
            return []

        # The DB labels have original umlauts (müsli, brötchen) but
        # normalize_search_text converts ü→ue, ö→oe etc.
        # We must search BOTH forms to get matches.
        raw_lower = (query or "").strip().lower()

        queries = [normalized]
        # Add raw query (with umlauts) if different from normalized
        if raw_lower != normalized and raw_lower:
            queries.append(raw_lower)
        # Add umlaut variants (ae↔ä, oe↔ö, ue↔ü, ss↔ß)
        for q in [normalized, raw_lower]:
            queries.extend(v for v in _umlaut_variants(q) if v not in queries)
        # Add synonym variants for all forms so far
        for q in list(queries):
            queries.extend(v for v in _synonym_variants(q) if v not in queries)

        # Collect (category_v2_id -> best score, product_count, hit_count)
        cat_scores: dict[int, tuple[int, int]] = {}  # cat_id -> (best_score, hit_count)
        cat_info: dict[int, dict] = {}  # cat_id -> category row info

        with self._connect() as conn:
            for q in queries:
                self._search_labels(conn, q, cat_scores, cat_info)

            # Multi-token intersection: "chips paprika" → products with BOTH labels
            raw_tokens = raw_lower.split()
            if len(raw_tokens) >= 2:
                self._multi_token_search(conn, raw_tokens, cat_scores, cat_info)
                # Also try normalized tokens
                norm_tokens = normalized.split()
                if norm_tokens != raw_tokens:
                    self._multi_token_search(conn, norm_tokens, cat_scores, cat_info)

        # Aggregate level-3 hits into level-2 group results.
        # When searching "milch", we find level-3 cats like "Haltbare Milch", "Vollmilch".
        # Their parent (level-2 group "Milch & Spezialmilch") should also appear as a result.
        self._aggregate_to_groups(cat_scores, cat_info)
        self._resolve_group_info(cat_info)

        # SymSpell fallback: try spell correction when zero results OR only
        # weak/few results (e.g. "milc" finds "Milch-Alternative" via FTS
        # prefix but correction "milch" would give much better results).
        should_try_correction = (
            not cat_scores  # Zero results
            or (len(cat_scores) <= 3 and all(
                score < 100 for score, _ in cat_scores.values()
            ))  # Few results, all WEAK or INDIRECT
        )

        if should_try_correction and not _corrected:
            corrected = self._spell.correct(raw_lower)
            if corrected and corrected != raw_lower and corrected != normalized:
                corrected_results = self.search(
                    corrected, limit=limit, _corrected=True,
                    local_category_counts=local_category_counts,
                )
                # Only use correction if it produces BETTER results
                if corrected_results:
                    current_best = max(
                        (score for score, _ in cat_scores.values()),
                        default=0,
                    ) if cat_scores else 0
                    corrected_best_score = max(
                        (r.get("_score", r.get("offer_count", 0))
                         for r in corrected_results),
                        default=0,
                    )
                    if corrected_best_score > current_best or not cat_scores:
                        for r in corrected_results:
                            r["corrected_from"] = query
                            r["corrected_to"] = corrected
                        return corrected_results

            if not cat_scores:
                return []

        return self._rank_results(cat_scores, cat_info, limit, local_category_counts)

    def _rank_results(
        self,
        cat_scores: dict[int, tuple[int, int]],
        cat_info: dict[int, dict],
        limit: int,
        local_category_counts: dict[int, int] | None,
    ) -> list[dict]:
        """Pass 2: Rank, filter, deduplicate, and diversify results."""
        # Build ranked results
        # Sort tuple: (tier, score_bucket, log_count, hit_count)
        # - tier (from cat_info): lower = better match quality
        # - score_bucket: score // 5 so 1-2 point diffs don't flip rankings
        # - log_count: log2(offer_count) as popularity tiebreaker
        # - hit_count: internal DB artifact, final tiebreaker
        ranked: list[tuple[tuple[int, int, float, int], dict]] = []
        strongest = 0

        for cat_id, (best_score, hit_count) in cat_scores.items():
            info = cat_info[cat_id]
            product_count = info["product_count"]
            # Use local count if available (location-filtered)
            if local_category_counts is not None:
                display_count = local_category_counts.get(cat_id, 0)
                if display_count == 0:
                    continue  # No local offers → skip
            else:
                display_count = product_count

            # Skip non-food / promotional categories
            cat_name_lower = (info["name"] or "").lower()
            if self._is_blocked_category(cat_name_lower):
                continue
            # Also block categories whose Oberkategorie is "Sonstiges"
            ober_lower = (info.get("parent_name") or "").lower()
            if "sonstig" in ober_lower:
                continue

            # Skip very low-count categories unless they're a strong match.
            # Categories with ≤2 offers are usually junk/overly-specific
            # (e.g. "Zahnpasta kräuter" with 1 offer).
            if display_count <= 2 and best_score < 140:
                continue

            # Sub-tier bucketing: within same tier and score bucket,
            # only popularity (log2 offer_count) decides ranking.
            tier = cat_info[cat_id].get("_tier", MatchTier.INDIRECT)
            log_count = math.log2(max(1, display_count))

            payload = {
                "id": cat_id,
                "name": info["name"],
                "offer_count": display_count,
                "display_offer_count": display_count,
                "direct_offer_count": display_count,
                "expanded_offer_count": info.get("parent_count", display_count),
                "kind": "category",
                "oberkategorie": info.get("parent_name", ""),
                "_level": info.get("level", 3),
                "_score": best_score,  # raw score for filtering
                "_features": {
                    "tier": int(tier),
                    "score": best_score,
                    "score_bucket": best_score // 5,
                    "log_count": round(log_count, 2),
                    "hit_count": hit_count,
                    "coverage_bonus": int(math.log2(max(1, hit_count))),
                    "level": info.get("level", 3),
                    "has_direct_hit": 1 if cat_id in cat_scores and info.get("level") == 2 else 0,
                },
            }
            ranked.append((
                (-tier, best_score // 5, log_count, hit_count),
                payload,
            ))
            strongest = max(strongest, best_score)

        # Adaptive threshold: filter results relative to strongest match.
        # Uses a percentage of the best score with an absolute floor.
        # This avoids fragile hardcoded breakpoints where a 1-point score
        # difference would change the threshold by 50 points.
        adaptive_min = max(int(strongest * 0.65), 50)
        ranked = [
            item for item in ranked
            if item[1]["_score"] >= adaptive_min
        ]

        # When strong matches exist (EXACT/STRONG tier), aggressively filter
        # INDIRECT results — they're label-only matches that cause noise
        # (e.g. "Küchenmaschinen" when searching "saft" via "Entsafter" labels).
        if strongest >= 155:
            ranked = [
                item for item in ranked
                if item[1]["_score"] >= 85  # At least WEAK tier
                or item[1]["offer_count"] >= 20  # Very popular INDIRECT results survive
            ]

        # When only weak indirect matches exist, require minimum offer count
        if strongest < 100:
            ranked = [
                item for item in ranked
                if item[1]["offer_count"] >= 3 or item[1]["_score"] >= 80
            ]

        # Label coverage bonus: categories with many matching labels get a small boost.
        # This differentiates "Milch" (30 label hits) from "Kaffeesahne" (1 label hit)
        for i, (sort_key, payload) in enumerate(ranked):
            cat_id = payload["id"]
            _, hit_count = cat_scores.get(cat_id, (0, 0))
            # Coverage bonus: log2(hits) added to score bucket
            coverage_bonus = int(math.log2(max(1, hit_count)))
            tier, score_bucket, log_count, hits = sort_key
            ranked[i] = ((tier, score_bucket + coverage_bonus, log_count, hits), payload)

        ranked.sort(key=lambda item: item[0], reverse=True)

        # Deduplicate by normalized category name.
        # When a level-2 group and level-3 child share the same name
        # (e.g. both "Milch"), always prefer the group (broader, more offers).
        ranked.sort(key=lambda item: item[0], reverse=True)
        seen_names: dict[str, int] = {}
        deduped: list[dict] = []
        for _, payload in ranked:
            norm = normalize_search_text(payload["name"])
            tokens = sorted(t for t in re.split(r"[\s&/,]+", norm) if t)
            name_key = " ".join(tokens)
            if name_key in seen_names:
                existing = deduped[seen_names[name_key]]
                existing_level = existing.get("_level", 3)
                new_level = payload.get("_level", 3)
                if existing_level == 2 and new_level == 3:
                    # Group already present, absorb child's count
                    continue
                if new_level == 2 and existing_level == 3:
                    # New entry is the group — replace child with group,
                    # keep the higher count (group aggregates children)
                    idx = seen_names[name_key]
                    payload["offer_count"] = max(payload["offer_count"], existing["offer_count"])
                    payload["display_offer_count"] = max(payload["display_offer_count"], existing["display_offer_count"])
                    deduped[idx] = payload
                    continue
                # Same level duplicates: merge counts
                existing["offer_count"] += payload["offer_count"]
                existing["display_offer_count"] += payload["display_offer_count"]
                continue
            seen_names[name_key] = len(deduped)
            deduped.append(payload)

        # Diversity cap: limit results per Oberkategorie.
        # When results span ≥3 Oberkategorien, cap at 2 per Ober for diversity.
        # When only 1-2 Obers exist, allow up to 4 (narrow query, show depth).
        distinct_obers = len(set(item.get("oberkategorie", "") for item in deduped))
        MAX_PER_OBER = 2 if distinct_obers >= 3 else 4
        ober_counts: dict[str, int] = {}
        diverse: list[dict] = []
        for item in deduped:
            ober = item.get("oberkategorie", "")
            count = ober_counts.get(ober, 0)
            if count >= MAX_PER_OBER:
                continue
            ober_counts[ober] = count + 1
            diverse.append(item)

        result = diverse[:limit]
        # Strip internal scoring fields before returning
        for item in result:
            item.pop("_features", None)
            item.pop("_score", None)
            item.pop("_level", None)
        return result

    @staticmethod
    def _aggregate_to_groups(
        cat_scores: dict[int, tuple[int, int]],
        cat_info: dict[int, dict],
    ) -> None:
        """Add level-2 group entries by aggregating their level-3 children hits."""
        # Collect level-3 hits grouped by their parent (level-2 group)
        group_children: dict[int, list[int]] = {}  # parent_id → [child cat_ids]
        for cat_id, info in cat_info.items():
            if info.get("level") == 3 and info.get("parent_id"):
                parent_id = info["parent_id"]
                group_children.setdefault(parent_id, []).append(cat_id)

        for group_id, child_ids in group_children.items():
            best_child_score = max(cat_scores[cid][0] for cid in child_ids)
            total_hits = sum(cat_scores[cid][1] for cid in child_ids)
            # Sum children's product_count for L2 groups (DB product_count is 0
            # because product_labels only attach at L3 level)
            children_product_count = sum(
                cat_info[cid].get("product_count", 0) for cid in child_ids
            )

            # Group score: slightly below best child so exact matches rank first.
            # "Orangensaft" (200) should appear before "Saft & Schorlen" (185).
            group_score = best_child_score - 15

            # Inherit best child's tier but downgrade by 1 (group is less specific)
            best_child_tier = min(
                cat_info[cid].get("_tier", MatchTier.INDIRECT) for cid in child_ids
            )
            group_tier = min(best_child_tier + 1, MatchTier.INDIRECT)

            # Enforce tier floor so group score stays consistent with tier system.
            # Without this, WEAK children (score 85) → group_score 70, which is
            # below INDIRECT floor (80), making groups rank under INDIRECT matches.
            _TIER_FLOORS = {
                MatchTier.EXACT: 200, MatchTier.STRONG: 155,
                MatchTier.PARTIAL: 125, MatchTier.WEAK: 85, MatchTier.INDIRECT: 50,
            }
            group_score = max(group_score, _TIER_FLOORS.get(group_tier, 50))

            if group_id in cat_scores:
                # Group already has a direct hit — boost ABOVE children so the
                # umbrella term ranks first (e.g. "milch" → "Milch & Spezialmilch").
                existing_score = cat_scores[group_id][0]
                boosted_score = max(best_child_score + 5, existing_score)
                cat_scores[group_id] = (boosted_score, total_hits + cat_scores[group_id][1])
                # Direct-hit group gets the BETTER tier (not downgraded)
                if group_id in cat_info:
                    existing_tier = cat_info[group_id].get("_tier", MatchTier.INDIRECT)
                    cat_info[group_id]["_tier"] = min(group_tier, existing_tier)
                    # Fix product_count if DB has 0 (L2 nodes don't have direct labels)
                    if cat_info[group_id].get("product_count", 0) == 0:
                        cat_info[group_id]["product_count"] = children_product_count
                continue

            cat_scores[group_id] = (group_score, total_hits)

            # We need group info — use first child's parent info
            first_child = cat_info[child_ids[0]]
            cat_info[group_id] = {
                "name": f"_group_{group_id}",  # placeholder, resolved below
                "product_count": children_product_count,  # sum of children (DB has 0 for L2)
                "parent_name": first_child.get("parent_name", ""),
                "parent_count": 0,
                "level": 2,
                "parent_id": None,
                "_tier": group_tier,
            }

    def _resolve_group_info(self, cat_info: dict[int, dict]) -> None:
        """Fetch real name/count for synthesized level-2 group entries."""
        placeholder_ids = [
            cid for cid, info in cat_info.items()
            if isinstance(info.get("name"), str) and info["name"].startswith("_group_")
        ]
        if not placeholder_ids:
            return
        with self._connect() as conn:
            placeholders = ",".join("?" for _ in placeholder_ids)
            rows = conn.execute(
                f"""SELECT c.id, c.name, c.product_count, c.parent_id,
                           p.name AS parent_name
                    FROM categories_v2 c
                    LEFT JOIN categories_v2 p ON p.id = c.parent_id
                    WHERE c.id IN ({placeholders})""",
                placeholder_ids,
            ).fetchall()
            for row in rows:
                info = cat_info.get(row["id"])
                if info:
                    info["name"] = row["name"]
                    # Keep children sum if DB product_count is 0 (L2 nodes)
                    db_count = row["product_count"] or 0
                    if db_count > 0:
                        info["product_count"] = db_count
                    # else: keep the children_product_count set in _aggregate_to_groups
                    info["parent_name"] = row["parent_name"] or ""

    def _multi_token_search(
        self,
        conn: sqlite3.Connection,
        tokens: list[str],
        cat_scores: dict[int, tuple[int, int]],
        cat_info: dict[int, dict],
    ) -> None:
        """Find products that have ALL tokens as separate labels, boost their categories."""
        if len(tokens) < 2 or len(tokens) > 5:
            return

        # Build intersection: products that have a label matching each token
        # Use LIKE for flexibility (handles substrings)
        conditions = []
        params = []
        for i, token in enumerate(tokens):
            conditions.append(
                f"pl.product_name IN (SELECT product_name FROM search_labels WHERE label LIKE ?)"
            )
            params.append(f"%{token}%")

        query = f"""
            SELECT pl.category_v2_id,
                   c.name AS cat_name, c.product_count, c.level AS cat_level,
                   c.parent_id,
                   p.name AS parent_name, p.product_count AS parent_count, p.level AS parent_level,
                   p.parent_id AS grandparent_id,
                   gp.name AS grandparent_name,
                   COUNT(DISTINCT pl.product_name) as match_count
            FROM product_labels pl
            JOIN categories_v2 c ON c.id = pl.category_v2_id
            LEFT JOIN categories_v2 p ON p.id = c.parent_id
            LEFT JOIN categories_v2 gp ON gp.id = p.parent_id
            WHERE {' AND '.join(conditions)}
            GROUP BY pl.category_v2_id
            ORDER BY match_count DESC
            LIMIT 20
        """

        try:
            rows = conn.execute(query, params).fetchall()
        except Exception:
            return

        combined_query = " ".join(tokens)
        for row in rows:
            cat_id = row["category_v2_id"]
            if cat_id is None:
                continue

            cat_name_lower = (row["cat_name"] or "").lower()
            cat_name_norm = normalize_search_text(cat_name_lower)

            # Score based on how well combined query matches category name
            if combined_query in cat_name_norm or combined_query in cat_name_lower:
                score = 170  # Combined query found in name → STRONG
            elif any(t in cat_name_norm or t in cat_name_lower for t in tokens):
                score = 140  # At least one token in name → PARTIAL
            else:
                score = 80   # Only label matches → INDIRECT

            self._update_cat(cat_scores, cat_info, cat_id, score, row, combined_query)

    def _search_labels(
        self,
        conn: sqlite3.Connection,
        normalized: str,
        cat_scores: dict[int, tuple[int, int]],
        cat_info: dict[int, dict],
    ) -> None:
        """Search search_labels via FTS5 + LIKE, aggregate by category_v2_id."""

        # --- FTS5 prefix search with plural/singular variants ---
        token_variant_sets = expand_query_tokens(normalized)
        parts = []
        for variant_set in token_variant_sets:
            if len(variant_set) == 1:
                parts.append(f'"{next(iter(variant_set))}"*')
            else:
                or_terms = " OR ".join(f'"{v}"*' for v in sorted(variant_set))
                parts.append(f"({or_terms})")
        fts_query = " AND ".join(parts)
        try:
            fts_rows = conn.execute(
                """
                SELECT sl.product_name, sl.label, sl.label_type,
                       pl.category_v2_id,
                       c.name AS cat_name, c.product_count, c.level AS cat_level,
                       c.parent_id,
                       p.name AS parent_name, p.product_count AS parent_count, p.level AS parent_level,
                       p.parent_id AS grandparent_id,
                       gp.name AS grandparent_name
                FROM search_labels_fts fts
                JOIN search_labels sl ON sl.id = fts.rowid
                JOIN product_labels pl ON pl.product_name = sl.product_name
                JOIN categories_v2 c ON c.id = pl.category_v2_id
                LEFT JOIN categories_v2 p ON p.id = c.parent_id
                LEFT JOIN categories_v2 gp ON gp.id = p.parent_id
                WHERE fts.label MATCH ?
                ORDER BY rank
                LIMIT 500
                """,
                (fts_query,),
            ).fetchall()
        except Exception:
            fts_rows = []

        for row in fts_rows:
            cat_id = row["category_v2_id"]
            if cat_id is None:
                continue
            score = self._score_label_match(normalized, row["label"], row["label_type"])
            self._update_cat(cat_scores, cat_info, cat_id, score, row, normalized)

        # --- LIKE fallback only when FTS found very few categories ---
        # With 47K compound-decomposition labels in the FTS index,
        # FTS covers nearly all queries. Empirically, only 1/50 common
        # queries find < 3 FTS categories (and those are usually
        # false-positive substrings like "oel" in "voelkel").
        # Threshold lowered from 5→3 to eliminate unnecessary LIKE scans.
        fts_cat_count = len(cat_scores)
        if fts_cat_count < 3:
            like_pattern = f"%{normalized}%"
            like_rows = conn.execute(
                """
                SELECT sl.product_name, sl.label, sl.label_type,
                       pl.category_v2_id,
                       c.name AS cat_name, c.product_count, c.level AS cat_level,
                       c.parent_id,
                       p.name AS parent_name, p.product_count AS parent_count, p.level AS parent_level,
                       p.parent_id AS grandparent_id,
                       gp.name AS grandparent_name
                FROM search_labels sl
                JOIN product_labels pl ON pl.product_name = sl.product_name
                JOIN categories_v2 c ON c.id = pl.category_v2_id
                LEFT JOIN categories_v2 p ON p.id = c.parent_id
                LEFT JOIN categories_v2 gp ON gp.id = p.parent_id
                WHERE sl.label LIKE ?
                LIMIT 500
                """,
                (like_pattern,),
            ).fetchall()

            for row in like_rows:
                cat_id = row["category_v2_id"]
                if cat_id is None:
                    continue
                score = self._score_label_match(normalized, row["label"], row["label_type"])
                self._update_cat(cat_scores, cat_info, cat_id, score, row, normalized)

    def _update_cat(
        self,
        cat_scores: dict[int, tuple[int, int]],
        cat_info: dict[int, dict],
        cat_id: int,
        score: int,
        row: sqlite3.Row,
        query: str,
    ) -> None:
        cat_name_lower = (row["cat_name"] or "").lower()
        cat_name_norm = normalize_search_text(cat_name_lower)
        # Check both normalized (umlaut-expanded) and raw lowercase
        query_in_cat = query in cat_name_norm or query in cat_name_lower

        is_prefix_n = cat_name_norm.startswith(query)
        is_prefix_l = cat_name_lower.startswith(query)
        is_word_pfx = self._is_word_prefix(cat_name_norm, query) or self._is_word_prefix(cat_name_lower, query)

        # Determine match tier (lower = better) based on how query relates to category name.
        # Within the same tier, offer_count decides ranking.
        if cat_name_norm == query or cat_name_lower == query:
            tier = MatchTier.EXACT
        elif is_word_pfx:
            # Clean word-boundary prefix: "reis" in "reis & getreide"
            tier = MatchTier.STRONG
        elif (is_prefix_n or is_prefix_l) and len(query) >= 5:
            # Compound prefix for longer queries: "wurst" in "wurstwaren"
            tier = MatchTier.STRONG
        elif self._has_word_match(cat_name_norm, query) or self._has_word_match(cat_name_lower, query):
            # Word appears at word boundary: "reis" in "basmati reis"
            tier = MatchTier.PARTIAL
        elif query_in_cat:
            if self._is_compound_suffix(cat_name_lower, query) or self._is_compound_suffix(cat_name_norm, query):
                # German compound suffix: "eis" in "Speiseeis" (Grundwort match)
                tier = MatchTier.STRONG
            elif (is_prefix_n or is_prefix_l):
                # Short compound prefix (<5 chars): "reis" in "reisbeilage"
                # Can't distinguish valid compounds from false positives
                tier = MatchTier.WEAK
            else:
                # Random substring: "reis" in "Dreisatz"
                tier = MatchTier.WEAK
        else:
            # Category name does NOT contain query — only labels matched
            tier = MatchTier.INDIRECT

        # The tier CAPS the score — a label-only match (INDIRECT) should never
        # score as high as a category-name match, regardless of label quality.
        tier_to_max_score = {
            MatchTier.EXACT: 200,
            MatchTier.STRONG: 170,
            MatchTier.PARTIAL: 140,
            MatchTier.WEAK: 100,
            MatchTier.INDIRECT: 80,
        }
        tier_floor = {
            MatchTier.EXACT: 200,
            MatchTier.STRONG: 155,
            MatchTier.PARTIAL: 125,
            MatchTier.WEAK: 85,
            MatchTier.INDIRECT: min(20 + int(score * 0.55), 80),
        }
        score = min(max(score, tier_floor.get(tier, 50)), tier_to_max_score.get(tier, 200))

        prev = cat_scores.get(cat_id, (0, 0))
        cat_scores[cat_id] = (max(prev[0], score), prev[1] + 1)
        if cat_id not in cat_info:
            cat_level = int(row["cat_level"] or 2)
            # Resolve oberkategorie name: for level-3, it's the grandparent (level-1)
            # For level-2, it's the parent (level-1)
            if cat_level == 3:
                ober_name = row["grandparent_name"] or ""
            else:
                ober_name = row["parent_name"] or ""
            cat_info[cat_id] = {
                "name": row["cat_name"],
                "product_count": row["product_count"] or 0,
                "parent_name": ober_name,
                "parent_count": row["parent_count"] or 0,
                "level": cat_level,
                "parent_id": row["parent_id"],
                "_tier": tier,
            }
        else:
            # Upgrade tier if this match is stronger
            existing_tier = cat_info[cat_id].get("_tier", MatchTier.INDIRECT)
            if tier < existing_tier:  # lower = better
                cat_info[cat_id]["_tier"] = tier

    # Non-food / promotional categories that should never appear in autocomplete
    _BLOCKED_TERMS = frozenset([
        "coupon", "gutschein", "aktion", "promotion",
        "eisenbahn", "puppen", "spielzeug", "gartenspritze", "spritzbeutel",
        "rindenmulch", "wasserfilter", "eismaschine", "eiswürfelbeutel",
        "eiswuerfelbeutel", "eiswürfelbereiter", "eiswuerfelbereiter",
        "seifenblasen", "reisegepäck", "reisegepaeck", "reisekoffer",
    ])

    @staticmethod
    def _is_blocked_category(name_lower: str) -> bool:
        """Check if a category name contains a blocked non-food term or is a junk-drawer."""
        name_norm = normalize_search_text(name_lower)
        # Block "Sonstiges"/"Sonstige" categories — junk drawers with no informational value
        if "sonstig" in name_lower or "sonstig" in name_norm:
            return True
        for term in CategorySearchService._BLOCKED_TERMS:
            if term in name_lower or term in name_norm:
                return True
        return False

    @staticmethod
    def _is_word_prefix(text: str, query: str) -> bool:
        """Check if query matches the start of text at a word boundary.

        'reis' is a word prefix of 'reis & getreide' but NOT of 'reisegepäck'.
        """
        if not text.startswith(query):
            return False
        if len(text) == len(query):
            return True  # exact match
        # Next char after query must be a word boundary (space, hyphen, &)
        next_char = text[len(query)]
        return next_char in (" ", "-", "&", "/", ",")

    @staticmethod
    def _has_word_match(text: str, query: str) -> bool:
        """Check if query appears in text at word boundaries.

        'reis' matches 'basmati reis' but NOT 'reisegepäck'.
        Handles multi-word queries: 'chips paprika' in 'chips paprika style'.
        Checks ALL occurrences, not just the first.
        """
        if query not in text:
            return False
        _BOUNDARY = frozenset(" -&/,")
        start = 0
        while True:
            idx = text.find(query, start)
            if idx == -1:
                return False
            end = idx + len(query)
            left_ok = idx == 0 or text[idx - 1] in _BOUNDARY
            right_ok = end >= len(text) or text[end] in _BOUNDARY
            if left_ok and right_ok:
                return True
            start = idx + 1

    @staticmethod
    def _is_compound_suffix(text: str, query: str) -> bool:
        """Check if query is a compound word suffix in text.

        'eis' is a suffix of 'speiseeis' (compound: Speise+Eis).
        'reis' is NOT considered a suffix of 'milchreis' for the word 'eis'
        because we check text words, not query substrings.

        Requires the prefix part to be at least 3 chars to avoid
        false positives like 'reis' matching 'eis'.
        """
        for word in text.split():
            if word.endswith(query) and word != query and len(word) >= len(query) + 3:
                return True
        return False

    @staticmethod
    def _score_label_match(query: str, label: str, label_type: str) -> int:
        """Score a single label match against the query.

        Key principle: labels like "müsli" (exact) score much higher than
        "müsli zutaten" or "milch fürs müsli" (associative labels).
        """
        label = (label or "").lower().strip()
        query = query.lower().strip()

        # Type bonus
        type_bonus = 0
        if label_type == "gattung":
            type_bonus = 5
        elif label_type == "marke":
            type_bonus = 15

        # Exact match: "müsli" == "müsli"
        if label == query:
            return 140 + type_bonus

        query_tokens = set(query.split())
        label_tokens = set(label.split())

        # Multi-word query exact token match: "vollkorn müsli" == all tokens in label
        if len(query_tokens) > 1 and query_tokens == label_tokens:
            return 135 + type_bonus

        # All query tokens present in label: "müsli" in {"müsli", "zutaten"}
        if query_tokens <= label_tokens:
            # Penalize heavily if label has extra words (associative labels)
            extra = len(label_tokens) - len(query_tokens)
            if extra == 0:
                return 130 + type_bonus
            # "müsli riegel" when searching "müsli" → ok but lower
            return max(60, 95 - extra * 15) + type_bonus

        # Query starts with label: "vollkorn müsli" starts with "vollkorn"
        if query.startswith(label + " "):
            return 90 + type_bonus

        # Label is a single-token prefix: "müsli" startswith "müs"
        if len(query_tokens) == 1 and len(label_tokens) == 1 and label.startswith(query):
            return 85 + type_bonus

        # Query appears as substring in label: "müsli" in "milch fürs müsli"
        # This is an associative match — score low
        if query in label:
            return 50 + type_bonus

        # Label appears as substring in query
        if label in query:
            return 55 + type_bonus

        # Partial token overlap
        overlap = len(query_tokens & label_tokens)
        if overlap > 0:
            return 40 + overlap * 5 + type_bonus

        return 20 + type_bonus

    def search_brands(self, query: str, limit: int = 3) -> list[dict]:
        """Search for brand names matching the query."""
        normalized = normalize_search_text(query)
        if not normalized or len(normalized) < 2:
            return []

        with self._connect() as conn:
            # Normalize hyphens: "coca cola" should match "Coca-Cola"
            brand_query = query.lower().replace("-", " ")

            # Search brands in product_labels — prefix match first
            rows = conn.execute(
                """
                SELECT pl.marke, COUNT(DISTINCT pl.product_name) as product_count,
                       c.name as category_name, c.id as category_id
                FROM product_labels pl
                JOIN categories_v2 c ON c.id = pl.category_v2_id
                WHERE REPLACE(LOWER(pl.marke), '-', ' ') LIKE ? AND pl.marke != ''
                GROUP BY pl.marke
                HAVING product_count >= 2
                ORDER BY product_count DESC
                LIMIT ?
                """,
                (f"{brand_query}%", limit),
            ).fetchall()

            # Also try normalized / case-insensitive substring if not enough
            if len(rows) < limit:
                brand_normalized = normalized.replace("-", " ")
                more = conn.execute(
                    """
                    SELECT pl.marke, COUNT(DISTINCT pl.product_name) as product_count,
                           c.name as category_name, c.id as category_id
                    FROM product_labels pl
                    JOIN categories_v2 c ON c.id = pl.category_v2_id
                    WHERE REPLACE(LOWER(pl.marke), '-', ' ') LIKE ? AND pl.marke != ''
                    GROUP BY pl.marke
                    HAVING product_count >= 2
                    ORDER BY product_count DESC
                    LIMIT ?
                    """,
                    (f"%{brand_normalized}%", limit - len(rows)),
                ).fetchall()
                existing_brands = {r["marke"] for r in rows}
                rows.extend(r for r in more if r["marke"] not in existing_brands)

            return [
                {
                    "brand": row["marke"],
                    "product_count": row["product_count"],
                    "top_category": row["category_name"],
                    "category_id": row["category_id"],
                    "type": "brand",
                }
                for row in rows[:limit]
            ]

    def expand_category(self, *, category_id: int, category_name: str) -> dict[str, object]:
        """Expand a category_v2 ID to all IDs that should match for pricing.

        3-level hierarchy:
        - Level 1 (Oberkategorie): expand to all level-2 + level-3 descendants
        - Level 2 (Gruppe): expand to all level-3 children + self
        - Level 3 (Spezifisch): returns just this ID
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, level, product_count FROM categories_v2 WHERE id = ?",
                (category_id,),
            ).fetchone()
            if row is None:
                return {"ids": [category_id], "offer_count": 0}

            level = int(row["level"])
            if level == 1:
                # Oberkategorie: expand to all level-2 and level-3 descendants
                l2_children = conn.execute(
                    "SELECT id FROM categories_v2 WHERE parent_id = ? AND level = 2",
                    (category_id,),
                ).fetchall()
                ids = set()
                for l2 in l2_children:
                    ids.add(int(l2["id"]))
                    l3_children = conn.execute(
                        "SELECT id FROM categories_v2 WHERE parent_id = ? AND level = 3",
                        (l2["id"],),
                    ).fetchall()
                    for l3 in l3_children:
                        ids.add(int(l3["id"]))
                if ids:
                    return {"ids": sorted(ids), "offer_count": int(row["product_count"])}

            elif level == 2:
                # Gruppe: expand to all level-3 children + self
                children = conn.execute(
                    "SELECT id FROM categories_v2 WHERE parent_id = ? AND level = 3",
                    (category_id,),
                ).fetchall()
                ids = [category_id] + [int(c["id"]) for c in children]
                return {"ids": sorted(ids), "offer_count": int(row["product_count"])}

            # Level 3: just this category
            return {"ids": [category_id], "offer_count": int(row["product_count"])}
