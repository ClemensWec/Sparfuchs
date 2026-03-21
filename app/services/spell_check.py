"""SymSpell-based spell correction for German grocery search.

Builds a dictionary from category names + product names in the database.
Used as fallback when FTS + LIKE return zero results.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from symspellpy import SymSpell, Verbosity


class SpellCheckService:
    def __init__(self, db_path: Path | str) -> None:
        self._db_path = str(db_path)
        self._sym: SymSpell | None = None
        self._loaded = False

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            self._sym = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
            conn = sqlite3.connect(self._db_path)
            # Load category names
            rows = conn.execute(
                "SELECT name FROM categories_v2 WHERE level = 2"
            ).fetchall()
            for (name,) in rows:
                words = name.lower().split()
                for w in words:
                    if len(w) >= 3:
                        self._sym.create_dictionary_entry(w, 1)
                if len(words) > 1:
                    self._sym.create_dictionary_entry(name.lower(), 1)

            # Load unique search labels (high frequency = higher dictionary count)
            rows = conn.execute(
                "SELECT label, COUNT(*) as cnt FROM search_labels "
                "WHERE label_type IN ('suchbegriff', 'gattung') "
                "GROUP BY label ORDER BY cnt DESC LIMIT 20000"
            ).fetchall()
            for label, cnt in rows:
                label = label.lower().strip()
                if len(label) >= 3:
                    self._sym.create_dictionary_entry(label, max(1, cnt))

            conn.close()
        except Exception:
            self._sym = None

    def correct(self, query: str, max_edit_distance: int = 2) -> str | None:
        """Return corrected query, or None if no correction found/needed.

        Only returns a correction if it differs from the input.
        """
        self._ensure_loaded()
        if not self._sym or not query or not query.strip():
            return None

        q = query.lower().strip()
        words = q.split()

        if len(words) == 1:
            # Single word: lookup with ALL suggestions at closest distance,
            # then re-rank by shared prefix length to prefer "butter" over
            # "bier" for query "butr" (both distance 2, but "butter" shares
            # more prefix characters).
            suggestions = self._sym.lookup(
                q, Verbosity.ALL, max_edit_distance=max_edit_distance
            )
            if suggestions:
                best_dist = suggestions[0].distance
                # Keep only suggestions at the best distance
                tied = [s for s in suggestions if s.distance == best_dist]
                # Re-rank: longest shared prefix wins, then higher frequency
                def _prefix_len(term: str) -> int:
                    i = 0
                    while i < len(q) and i < len(term) and q[i] == term[i]:
                        i += 1
                    return i
                tied.sort(key=lambda s: (-_prefix_len(s.term), -s.count))
                if tied[0].term != q:
                    return tied[0].term
        else:
            # Multi-word: lookup_compound handles word segmentation + correction
            suggestions = self._sym.lookup_compound(
                q, max_edit_distance=max_edit_distance
            )
            if suggestions and suggestions[0].term != q:
                return suggestions[0].term

        return None
