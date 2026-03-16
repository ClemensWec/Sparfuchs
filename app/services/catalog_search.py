from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from math import cos, radians

from app.services.matching import Suggestion, _discount_percent
from app.utils.matching import _ABBREVIATION_EXPAND, _CONSUMER_SYNONYMS
from app.utils.german_stems import expand_query_tokens
from app.utils.matching import MIN_SCORE_WITHOUT_PRICE, MIN_SCORE_WITH_PRICE, calculate_match_score
from app.utils.text import compact_text, normalize_search_text


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _base_price_parts(value: str | None) -> tuple[float | None, str | None]:
    text = compact_text(value)
    if not text:
        return (None, None)

    match = re.search(r"([A-Za-z]+)\s*=\s*([0-9]+(?:[.,][0-9]+)?)", text)
    if not match:
        match = re.search(r"([0-9]+(?:[.,][0-9]+)?)\s*(?:€|EUR)?\s*/\s*([A-Za-z]+)", text, re.IGNORECASE)
        if match:
            price_str, unit = match.group(1), match.group(2)
            return (_parse_float(price_str), unit.lower())
        return (None, None)

    unit, price_str = match.group(1), match.group(2)
    return (_parse_float(price_str), unit.lower())


def _parse_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


@dataclass(frozen=True)
class CatalogSearchService:
    db_path: Path

    def available(self) -> bool:
        return self.db_path.exists()

    def search(
        self,
        q: str,
        chains: list[str] | None = None,
        *,
        lat: float | None = None,
        lon: float | None = None,
        radius_km: float | None = None,
        limit: int = 0,
        local_offer_ids: frozenset[int] | None = None,
    ) -> list[Suggestion]:
        query = compact_text(q)
        query_normalized = normalize_search_text(query)
        if len(query_normalized) < 2 or not self.available():
            return []

        with _connect(self.db_path) as conn:
            # Determine nearby chains from stores table
            nearby_chains: list[str] | None = None
            if lat is not None and lon is not None and radius_km is not None:
                nearby_chains = self._nearby_chains(conn, lat, lon, radius_km)

            # Effective chain filter: intersection of user-selected chains and nearby chains
            effective_chains = chains
            if nearby_chains is not None:
                if chains:
                    effective_chains = [c for c in chains if c in nearby_chains]
                else:
                    effective_chains = nearby_chains

            # Expand query tokens with plural/singular variants
            token_variant_sets = expand_query_tokens(query_normalized)

            # Build list of query variants to search (original + compound splits + consumer synonyms)
            query_variants = [token_variant_sets]

            # Compound word splits (e.g. "orangensaft" → "orangen saft")
            for abbrev, expanded in _ABBREVIATION_EXPAND.items():
                if abbrev in query_normalized:
                    split_q = query_normalized.replace(abbrev, expanded)
                    query_variants.append(expand_query_tokens(split_q))

            # Consumer synonyms (e.g. "marmelade" → "konfituere")
            for term_a, term_b in _CONSUMER_SYNONYMS:
                for src, dst in [(term_a, term_b), (term_b, term_a)]:
                    if src in query_normalized:
                        syn_q = query_normalized.replace(src, dst)
                        query_variants.append(expand_query_tokens(syn_q))

            # Run FTS for all variants (fast index lookup)
            fts_rows: list[sqlite3.Row] = []
            seen_fts: set[int] = set()
            for tv in query_variants:
                try:
                    for row in self._search_fts(conn, token_variants=tv, chains=effective_chains):
                        rid = row["id"]
                        if rid not in seen_fts:
                            seen_fts.add(rid)
                            fts_rows.append(row)
                except sqlite3.Error:
                    pass

            # Run LIKE: merge all variants into minimal number of queries.
            # Single-token variants combine into ONE query (1 table scan, not N).
            # Multi-token variants stay separate (need AND between tokens).
            like_rows: list[sqlite3.Row] = self._search_like_combined(
                conn, all_variants=query_variants, chains=effective_chains
            )

            # Merge by offer ID — FTS rows get priority (have bm25 rank)
            seen_ids: set[int] = set()
            rows: list[sqlite3.Row] = []
            for row in fts_rows:
                rid = row["id"]
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    rows.append(row)
            for row in like_rows:
                rid = row["id"]
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    rows.append(row)

            # Fuzzy fallback: only when zero results (fuzzy is expensive — full table scan)
            if len(rows) == 0:
                fuzzy_rows = self._search_fuzzy(conn, query_normalized=query_normalized, token_variants=token_variant_sets, chains=effective_chains)
                for row in fuzzy_rows:
                    rid = row["id"]
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        rows.append(row)

            # Category boost: find category IDs matching the query
            boost_cat_ids = self._lookup_category_ids(conn, query_normalized)

        if not rows:
            return []

        suggestions_by_key: dict[tuple[str, str, str], Suggestion] = {}
        for row in rows:
            product_name = compact_text(row["product_name"])
            brand_name = compact_text(row["brand_name"])
            primary_text = " ".join(part for part in [brand_name, product_name] if part)
            search_text = compact_text(row["search_text"])
            offer_text = primary_text or search_text
            # Score against both primary text and search_text, use best.
            # Cap description-only matches so they rank below direct title matches.
            score1 = calculate_match_score(query, offer_text) if offer_text else 0.0
            score2 = calculate_match_score(query, search_text) if search_text and search_text != offer_text else 0.0
            description_only = score2 > score1 and score1 < MIN_SCORE_WITH_PRICE
            if description_only:
                # Query matches description/ingredients but not the product name —
                # cap so it ranks below products whose *name* matches.
                score = min(score2, 79.0)
            else:
                score = max(score1, score2)

            # Category boost: +15 for offers whose category matches the query.
            # Skip boost for description-only matches to preserve the 79-cap intent.
            offer_cat_id = row["category_id"]
            if not description_only and offer_cat_id and boost_cat_ids and offer_cat_id in boost_cat_ids:
                score += 15.0

            has_price = row["sales_price_eur"] is not None
            if score < (MIN_SCORE_WITH_PRICE if has_price else MIN_SCORE_WITHOUT_PRICE):
                continue

            base_price_eur, base_unit = _base_price_parts(row["base_price_text"])
            suggestion = Suggestion(
                offer_id=str(row["id"]),
                title=product_name,
                brand=brand_name or None,
                chain=str(row["chain"]),
                price_eur=_parse_float(str(row["sales_price_eur"])) if row["sales_price_eur"] is not None else None,
                was_price_eur=_parse_float(str(row["regular_price_eur"])) if row["regular_price_eur"] is not None else None,
                is_offer=True,
                discount_percent=_discount_percent(row["sales_price_eur"], row["regular_price_eur"]),
                base_price_eur=base_price_eur,
                base_unit=base_unit,
                score=score,
                image_url=row["offer_image_url"] if "offer_image_url" in row.keys() else None,
                valid_from=compact_text(row["valid_from"])[:10] if row["valid_from"] else None,
                valid_until=compact_text(row["valid_until"])[:10] if row["valid_until"] else None,
            )
            dedupe_key = (
                suggestion.chain,
                normalize_search_text(suggestion.brand or ""),
                normalize_search_text(suggestion.title),
            )
            current = suggestions_by_key.get(dedupe_key)
            if current is None or self._sort_key(suggestion) > self._sort_key(current):
                suggestions_by_key[dedupe_key] = suggestion

        suggestions = list(suggestions_by_key.values())

        # Location post-filter: keep only offers available in the user's radius
        if local_offer_ids is not None:
            suggestions = [
                s for s in suggestions
                if int(s.offer_id) in local_offer_ids
            ]

        suggestions.sort(key=self._sort_key, reverse=True)
        if limit > 0:
            suggestions = suggestions[:limit]

        return suggestions

    def _nearby_chains(
        self, conn: sqlite3.Connection, lat: float, lon: float, radius_km: float,
    ) -> list[str] | None:
        """Find chains that have stores within radius using the stores table."""
        bbox = self._bbox(lat, lon, radius_km)
        if bbox is None:
            return None
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT chain FROM stores
                WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
                """,
                [bbox[0], bbox[1], bbox[2], bbox[3]],
            ).fetchall()
            if rows:
                return [row["chain"] for row in rows]
        except sqlite3.Error:
            pass
        return None

    @staticmethod
    def _bbox(lat: float | None, lon: float | None, radius_km: float | None) -> tuple[float, float, float, float] | None:
        if lat is None or lon is None or radius_km is None or radius_km <= 0:
            return None
        r = max(0.5, float(radius_km))
        lat_delta = r / 111.0
        lon_divisor = max(0.1, cos(radians(lat)) * 111.0)
        lon_delta = r / lon_divisor
        return (lat - lat_delta, lat + lat_delta, lon - lon_delta, lon + lon_delta)

    def _search_fts(
        self,
        conn: sqlite3.Connection,
        *,
        token_variants: list[set[str]],
        chains: list[str] | None,
    ) -> list[sqlite3.Row]:
        if not token_variants:
            return []
        # FTS MATCH with OR for plural variants: (apfel OR aepfel) AND (rot*)
        parts = []
        for variant_set in token_variants:
            if len(variant_set) == 1:
                parts.append(f"{next(iter(variant_set))}*")
            else:
                or_terms = " OR ".join(f"{v}*" for v in sorted(variant_set))
                parts.append(f"({or_terms})")
        match_expr = " AND ".join(parts)

        params: list[object] = [match_expr]
        sql = """
            SELECT MIN(r.id) AS id, r.chain, r.product_name, r.brand_name,
                   r.search_text, r.search_text_normalized,
                   MIN(r.sales_price_eur) AS sales_price_eur,
                   MIN(r.regular_price_eur) AS regular_price_eur,
                   r.base_price_text,
                   MAX(r.offer_image_url) AS offer_image_url,
                   MAX(r.valid_from) AS valid_from,
                   MAX(r.valid_until) AS valid_until,
                   MIN(r.fts_rank) AS fts_rank,
                   r.category_id
            FROM (
                SELECT o.*, bm25(offers_fts, 8.0, 5.0, 1.5, 3.0, 0.5) AS fts_rank
                FROM offers_fts
                JOIN offers o ON o.id = offers_fts.rowid
                WHERE offers_fts MATCH ?
        """
        if chains:
            placeholders = ", ".join("?" for _ in chains)
            sql += f" AND o.chain IN ({placeholders})"
            params.extend(chains)
        sql += """
                LIMIT 2000
            ) r
            GROUP BY r.chain, r.product_name, r.brand_name, r.category_id
            ORDER BY fts_rank ASC
            LIMIT 500
        """
        return list(conn.execute(sql, params))

    _DEDUP_SELECT = """
        MIN(o.id) AS id, o.chain, o.product_name, o.brand_name,
        o.search_text, o.search_text_normalized,
        MIN(o.sales_price_eur) AS sales_price_eur,
        MIN(o.regular_price_eur) AS regular_price_eur,
        o.base_price_text,
        MAX(o.offer_image_url) AS offer_image_url,
        MAX(o.valid_from) AS valid_from,
        MAX(o.valid_until) AS valid_until,
        o.category_id
    """

    def _search_like_combined(
        self,
        conn: sqlite3.Connection,
        *,
        all_variants: list[list[set[str]]],
        chains: list[str] | None,
    ) -> list[sqlite3.Row]:
        """Run LIKE search for all query variants with minimal table scans.

        Single-token variants are merged into ONE query (1 table scan, not N).
        Multi-token variants stay as separate queries (need AND between tokens).
        """
        single_token_patterns: set[str] = set()
        multi_token_variants: list[list[set[str]]] = []

        for tv in all_variants:
            if len(tv) == 1:
                single_token_patterns.update(tv[0])
            else:
                multi_token_variants.append(tv)

        rows: list[sqlite3.Row] = []
        seen: set[int] = set()

        # One combined query for all single-token patterns (1 scan instead of N)
        if single_token_patterns:
            for row in self._search_like(conn, token_variants=[single_token_patterns], chains=chains):
                rid = row["id"]
                if rid not in seen:
                    seen.add(rid)
                    rows.append(row)

        # Separate queries for multi-token variants
        for tv in multi_token_variants:
            for row in self._search_like(conn, token_variants=tv, chains=chains):
                rid = row["id"]
                if rid not in seen:
                    seen.add(rid)
                    rows.append(row)

        return rows

    def _search_like(
        self,
        conn: sqlite3.Connection,
        *,
        token_variants: list[set[str]],
        chains: list[str] | None,
    ) -> list[sqlite3.Row]:
        if not token_variants:
            return []

        params: list[object] = []
        sql = f"""
            SELECT {self._DEDUP_SELECT}, 0.0 AS fts_rank FROM offers o
            WHERE 1=1
        """
        for variant_set in token_variants:
            or_clauses = " OR ".join("o.search_text_normalized LIKE ?" for _ in variant_set)
            sql += f" AND ({or_clauses})"
            params.extend(f"%{v}%" for v in sorted(variant_set))
        if chains:
            placeholders = ", ".join("?" for _ in chains)
            sql += f" AND o.chain IN ({placeholders})"
            params.extend(chains)
        sql += " GROUP BY o.chain, o.product_name, o.brand_name, o.category_id"
        sql += " ORDER BY sales_price_eur ASC LIMIT 500"
        return list(conn.execute(sql, params))

    @staticmethod
    def _fuzzy_variants(token: str, max_patterns: int = 20) -> list[str]:
        """Generate edit-distance-1 LIKE patterns for a token.

        Three strategies:
        1. Deletion: remove each char → catches extra-char typos
        2. Substitution: replace each char with _ wildcard → catches wrong-char typos
        3. Insertion: insert _ between each pair → catches missing-char typos

        Limits total patterns to avoid expensive full-table scans on long tokens.
        Skips tokens >12 chars (compound words — already found by compound split).
        """
        if len(token) < 3:
            return [token]
        if len(token) > 12:
            # Long compound words generate too many patterns for too little benefit.
            # They're already found by compound split or FTS.
            return []
        variants: list[str] = []
        seen: set[str] = set()
        # 1. Deletions: "apfl" → "pfl", "afl", "apl", "apf"
        for i in range(len(token)):
            v = token[:i] + token[i + 1:]
            if len(v) >= 2 and v not in seen:
                seen.add(v)
                variants.append(v)
                if len(variants) >= max_patterns:
                    return variants
        # 2. Substitutions: "schocolade" → "s_hocolade", "sc_ocolade", ...
        for i in range(len(token)):
            v = token[:i] + "_" + token[i + 1:]
            if v not in seen:
                seen.add(v)
                variants.append(v)
                if len(variants) >= max_patterns:
                    return variants
        # 3. Insertions: "kartffel" → "_kartffel", "k_artffel", ..., "kart_ffel", ...
        for i in range(len(token) + 1):
            v = token[:i] + "_" + token[i:]
            if v not in seen:
                seen.add(v)
                variants.append(v)
                if len(variants) >= max_patterns:
                    return variants
        return variants

    def _search_fuzzy(
        self,
        conn: sqlite3.Connection,
        *,
        query_normalized: str,
        token_variants: list[set[str]],
        chains: list[str] | None,
    ) -> list[sqlite3.Row]:
        """Typo-tolerant search using edit-distance-1 LIKE patterns + plural variants."""
        tokens = [token for token in query_normalized.split() if token]
        if not tokens:
            return []

        # Combine typo variants with plural variants for each token
        combined_variants: list[list[str]] = []
        for i, token in enumerate(tokens):
            typo_vars = self._fuzzy_variants(token)
            # Also add typo variants for plural forms
            plural_forms = token_variants[i] if i < len(token_variants) else {token}
            all_vars = set(typo_vars)
            for form in plural_forms:
                if form != token:
                    all_vars.update(self._fuzzy_variants(form))
            if all_vars:
                combined_variants.append(sorted(all_vars))

        if not combined_variants:
            return []

        params: list[object] = []
        sql = f"""
            SELECT {self._DEDUP_SELECT}, 0.0 AS fts_rank FROM offers o
            WHERE 1=1
        """
        for variants in combined_variants:
            or_clauses = " OR ".join("o.search_text_normalized LIKE ?" for _ in variants)
            sql += f" AND ({or_clauses})"
            params.extend(f"%{v}%" for v in variants)
        if chains:
            placeholders = ", ".join("?" for _ in chains)
            sql += f" AND o.chain IN ({placeholders})"
            params.extend(chains)
        sql += " GROUP BY o.chain, o.product_name, o.brand_name, o.category_id"
        sql += " ORDER BY sales_price_eur ASC LIMIT 300"
        return list(conn.execute(sql, params))

    @staticmethod
    def _lookup_category_ids(
        conn: sqlite3.Connection, query_normalized: str,
    ) -> set[int]:
        """Find category IDs matching the query via categories_v2 FTS.

        Returns a set of category IDs (level 2+3) so offers with matching
        category_id can receive a relevance boost.
        - Level-1 match: expand to all level-2 + level-3 descendants
        - Level-2 match: expand to all level-3 children + self
        - Level-3 match: just this ID
        """
        token_variant_sets = expand_query_tokens(query_normalized)
        if not token_variant_sets:
            return set()
        parts = []
        for variant_set in token_variant_sets:
            if len(variant_set) == 1:
                parts.append(f'"{next(iter(variant_set))}"*')
            else:
                or_terms = " OR ".join(f'"{v}"*' for v in sorted(variant_set))
                parts.append(f"({or_terms})")
        fts_expr = " AND ".join(parts)
        try:
            rows = conn.execute(
                """
                SELECT c.id, c.level FROM categories_fts fts
                JOIN categories_v2 c ON c.rowid = fts.rowid
                WHERE fts.name_normalized MATCH ?
                LIMIT 50
                """,
                (fts_expr,),
            ).fetchall()
            result: set[int] = set()
            expand_l1: list[int] = []
            expand_l2: list[int] = []
            for row in rows:
                level = row["level"]
                result.add(row["id"])
                if level == 1:
                    expand_l1.append(row["id"])
                elif level == 2:
                    expand_l2.append(row["id"])
            # Expand level-1 → all level-2 children → all level-3 grandchildren
            if expand_l1:
                placeholders = ",".join("?" for _ in expand_l1)
                l2_children = conn.execute(
                    f"SELECT id FROM categories_v2 WHERE parent_id IN ({placeholders}) AND level = 2",
                    expand_l1,
                ).fetchall()
                for child in l2_children:
                    result.add(child["id"])
                    expand_l2.append(child["id"])
            # Expand level-2 → all level-3 children
            if expand_l2:
                placeholders = ",".join("?" for _ in expand_l2)
                l3_children = conn.execute(
                    f"SELECT id FROM categories_v2 WHERE parent_id IN ({placeholders}) AND level = 3",
                    expand_l2,
                ).fetchall()
                for child in l3_children:
                    result.add(child["id"])
            return result
        except sqlite3.Error:
            return set()

    @staticmethod
    def _sort_key(suggestion: Suggestion) -> tuple[float, float, float]:
        known_price = 1.0 if suggestion.price_eur is not None else 0.0
        price_sort = -float(suggestion.price_eur) if suggestion.price_eur is not None else -1e9
        return (suggestion.score + known_price, known_price, price_sort)
