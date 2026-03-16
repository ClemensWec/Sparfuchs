from __future__ import annotations

import re
import unicodedata


def repair_mojibake(value: str | None) -> str:
    text = str(value or "")
    if not text:
        return ""

    # Common UTF-8-as-latin1 mojibake from KaufDA payloads.
    if any(marker in text for marker in ("Ã", "â", "€", "™")):
        try:
            repaired = text.encode("latin-1").decode("utf-8")
            if repaired:
                text = repaired
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return text


def compact_text(value: str | None) -> str:
    return " ".join(repair_mojibake(value).split()).strip()


def normalize_search_text(value: str | None) -> str:
    text = compact_text(value).lower()
    if not text:
        return ""

    text = (
        text.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
