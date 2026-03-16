from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.catalog_data import CatalogDataService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a local coverage report from the KaufDA SQLite dataset.")
    parser.add_argument(
        "--db-path",
        default="data/kaufda_dataset/offers.sqlite3",
        help="Path to the local KaufDA SQLite database.",
    )
    parser.add_argument(
        "--output-path",
        default="data/kaufda_dataset/coverage.summary.json",
        help="Where to write the generated JSON summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = CatalogDataService(db_path=Path(args.db_path))
    if not service.available():
        raise SystemExit(f"Database not found: {args.db_path}")

    summary = service.local_coverage_summary()
    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
