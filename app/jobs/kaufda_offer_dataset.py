from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from app.utils.text import compact_text


@dataclass
class DatasetStats:
    brochures: int = 0
    pages: int = 0
    offers: int = 0


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_brochure_dirs(downloads_dir: Path) -> Iterable[Path]:
    if not downloads_dir.exists():
        return []
    for chain_dir in sorted(p for p in downloads_dir.iterdir() if p.is_dir()):
        for brochure_dir in sorted(p for p in chain_dir.iterdir() if p.is_dir()):
            if (brochure_dir / "metadata.json").exists() and (brochure_dir / "pages.json").exists():
                yield brochure_dir


def normalize_text(value: str | None) -> str:
    return compact_text(value)


def parse_decimal(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(Decimal(str(value).replace(",", ".")))
    except (InvalidOperation, ValueError):
        return None


def extract_deal_values(deals: list[dict[str, Any]]) -> dict[str, Any]:
    sales_price = None
    regular_price = None
    currency_code = None
    base_price_text = None

    for deal in deals:
        deal_type = str(deal.get("type") or "").upper()
        price_min = parse_decimal(deal.get("min"))
        if currency_code is None:
            currency_code = deal.get("currencyCode")
        if base_price_text is None and deal.get("priceByBaseUnit"):
            base_price_text = normalize_text(deal.get("priceByBaseUnit"))

        if deal_type == "SALES_PRICE" and sales_price is None:
            sales_price = price_min
        elif deal_type == "REGULAR_PRICE" and regular_price is None:
            regular_price = price_min

    discount_amount = None
    discount_percent = None
    if sales_price is not None and regular_price is not None and regular_price > sales_price:
        discount_amount = round(regular_price - sales_price, 2)
        discount_percent = round((discount_amount / regular_price) * 100, 2)

    return {
        "sales_price_eur": sales_price,
        "regular_price_eur": regular_price,
        "discount_amount_eur": discount_amount,
        "discount_percent": discount_percent,
        "currency_code": currency_code,
        "base_price_text": base_price_text,
    }


def page_image_path(brochure_dir: Path, page_number: int) -> Path:
    return brochure_dir / f"page_{page_number + 1:03d}.jpg"


def build_page_manifest_entry(brochure_dir: Path, metadata: dict[str, Any], page: dict[str, Any]) -> dict[str, Any]:
    content = metadata["content"]
    page_number = int(page["number"])
    chain_key = brochure_dir.parent.name
    image_path = page_image_path(brochure_dir, page_number)
    largest_image = _largest_image_url(page.get("images", []))
    return {
        "chain_key": chain_key,
        "brochure_content_id": content["id"],
        "brochure_legacy_id": content.get("legacyId"),
        "brochure_title": normalize_text(content.get("title")),
        "publisher_name": content.get("publisher", {}).get("name"),
        "valid_from": content.get("validFrom"),
        "valid_until": content.get("validUntil"),
        "page_number": page_number + 1,
        "page_index": page_number,
        "page_image_path": str(image_path),
        "page_image_exists": image_path.exists(),
        "page_image_url": largest_image,
        "offers_count": len(page.get("offers", [])),
        "linkouts_count": len(page.get("linkOuts", [])),
        "metadata_path": str(brochure_dir / "metadata.json"),
        "pages_path": str(brochure_dir / "pages.json"),
    }


def extract_offer_records_from_brochure(brochure_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    metadata = load_json(brochure_dir / "metadata.json")
    pages_payload = load_json(brochure_dir / "pages.json")
    page_manifest: list[dict[str, Any]] = []
    offer_records: list[dict[str, Any]] = []
    brochure_content = metadata["content"]

    for page in pages_payload.get("contents", []):
        page_manifest.append(build_page_manifest_entry(brochure_dir, metadata, page))
        for offer in page.get("offers", []):
            content = offer.get("content") or {}
            products = content.get("products") or []
            first_product = products[0] if products else {}
            parent_page = (content.get("parentContent") or {}).get("page") or {}
            page_area = parent_page.get("area") or {}
            deal_values = extract_deal_values(content.get("deals") or [])
            description_items = first_product.get("description") or []
            description_text = " ".join(
                normalize_text(item.get("paragraph"))
                for item in description_items
                if normalize_text(item.get("paragraph"))
            )
            category_names = [
                normalize_text(category.get("name"))
                for category in (first_product.get("categoryPaths") or [])
                if normalize_text(category.get("name"))
            ]
            search_text = normalize_text(
                " ".join(
                    part
                    for part in [
                        first_product.get("brandName"),
                        first_product.get("name"),
                        description_text,
                    ]
                    if normalize_text(part)
                )
            )

            offer_records.append(
                {
                    "chain_key": brochure_dir.parent.name,
                    "brochure_content_id": brochure_content["id"],
                    "brochure_legacy_id": brochure_content.get("legacyId"),
                    "brochure_title": normalize_text(brochure_content.get("title")),
                    "publisher_name": brochure_content.get("publisher", {}).get("name"),
                    "valid_from": brochure_content.get("validFrom"),
                    "valid_until": brochure_content.get("validUntil"),
                    "page_number": int(page["number"]) + 1,
                    "page_index": int(page["number"]),
                    "page_image_path": str(page_image_path(brochure_dir, int(page["number"]))),
                    "offer_content_id": content.get("id"),
                    "offer_type": content.get("type"),
                    "placement": offer.get("placement"),
                    "ad_format": offer.get("adFormat"),
                    "product_name": normalize_text(first_product.get("name")),
                    "brand_name": normalize_text(first_product.get("brandName")),
                    "description_text": description_text,
                    "category_names": category_names,
                    "search_text": search_text,
                    "sales_price_eur": deal_values["sales_price_eur"],
                    "regular_price_eur": deal_values["regular_price_eur"],
                    "discount_amount_eur": deal_values["discount_amount_eur"],
                    "discount_percent": deal_values["discount_percent"],
                    "currency_code": deal_values["currency_code"],
                    "base_price_text": deal_values["base_price_text"],
                    "discount_label_type": (content.get("discountLabel") or {}).get("type"),
                    "discount_label_value": parse_decimal((content.get("discountLabel") or {}).get("value")),
                    "offer_image_url": content.get("image"),
                    "bbox_top_left_x": page_area.get("topLeft", {}).get("x"),
                    "bbox_top_left_y": page_area.get("topLeft", {}).get("y"),
                    "bbox_bottom_right_x": page_area.get("bottomRight", {}).get("x"),
                    "bbox_bottom_right_y": page_area.get("bottomRight", {}).get("y"),
                    "raw_deals": content.get("deals") or [],
                }
            )

    return page_manifest, offer_records


def _largest_image_url(images: list[dict[str, Any]]) -> str | None:
    best_url = None
    best_area = -1
    for image in images:
        size = str(image.get("size") or "")
        width = 0
        height = 0
        if "x" in size:
            try:
                width_str, height_str = size.split("x", 1)
                width = int(width_str)
                height = int(height_str)
            except ValueError:
                width = 0
                height = 0
        area = width * height
        if area > best_area:
            best_area = area
            best_url = image.get("url")
    return best_url


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def build_dataset(downloads_dir: Path, output_dir: Path, limit_brochures: int | None = None) -> dict[str, Any]:
    page_manifest_rows: list[dict[str, Any]] = []
    offer_rows: list[dict[str, Any]] = []
    stats = DatasetStats()

    for index, brochure_dir in enumerate(iter_brochure_dirs(downloads_dir), start=1):
        if limit_brochures is not None and index > limit_brochures:
            break
        pages, offers = extract_offer_records_from_brochure(brochure_dir)
        page_manifest_rows.extend(pages)
        offer_rows.extend(offers)
        stats.brochures += 1
        stats.pages += len(pages)
        stats.offers += len(offers)

    page_manifest_path = output_dir / "page_manifest.jsonl"
    offers_path = output_dir / "offers.jsonl"
    summary_path = output_dir / "summary.json"
    write_jsonl(page_manifest_path, page_manifest_rows)
    write_jsonl(offers_path, offer_rows)

    summary = {
        "generated_at": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
        "downloads_dir": str(downloads_dir),
        "brochures": stats.brochures,
        "pages": stats.pages,
        "offers": stats.offers,
        "page_manifest_path": str(page_manifest_path),
        "offers_path": str(offers_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def build_page_manifest(downloads_dir: Path, output_path: Path, limit_brochures: int | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    brochures = 0
    for index, brochure_dir in enumerate(iter_brochure_dirs(downloads_dir), start=1):
        if limit_brochures is not None and index > limit_brochures:
            break
        pages, _offers = extract_offer_records_from_brochure(brochure_dir)
        rows.extend(pages)
        brochures += 1
    count = write_jsonl(output_path, rows)
    return {
        "generated_at": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
        "downloads_dir": str(downloads_dir),
        "brochures": brochures,
        "pages": count,
        "page_manifest_path": str(output_path),
    }


def build_offer_dataset(downloads_dir: Path, output_path: Path, limit_brochures: int | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    brochures = 0
    pages = 0
    for index, brochure_dir in enumerate(iter_brochure_dirs(downloads_dir), start=1):
        if limit_brochures is not None and index > limit_brochures:
            break
        page_rows, offer_rows = extract_offer_records_from_brochure(brochure_dir)
        rows.extend(offer_rows)
        pages += len(page_rows)
        brochures += 1
    count = write_jsonl(output_path, rows)
    return {
        "generated_at": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
        "downloads_dir": str(downloads_dir),
        "brochures": brochures,
        "pages": pages,
        "offers": count,
        "offers_path": str(output_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a structured KaufDA offer dataset from downloaded brochure pages.")
    parser.add_argument("--downloads-dir", default="downloads/kaufda_full/downloads", help="Brochure download root.")
    parser.add_argument("--output-dir", default="data/kaufda_dataset", help="Output directory for JSONL dataset files.")
    parser.add_argument("--limit-brochures", type=int, default=None, help="Optional limit for test runs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_dataset(
        downloads_dir=Path(args.downloads_dir),
        output_dir=Path(args.output_dir),
        limit_brochures=args.limit_brochures,
    )
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
