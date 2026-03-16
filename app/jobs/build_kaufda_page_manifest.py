import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.jobs.kaufda_offer_dataset import build_page_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a page manifest from downloaded KaufDA brochures.")
    parser.add_argument("--downloads-dir", default="downloads/kaufda_full/downloads", help="Brochure download root.")
    parser.add_argument(
        "--output-path",
        default="data/kaufda_dataset/page_manifest.jsonl",
        help="Output JSONL file for page manifest rows.",
    )
    parser.add_argument("--limit-brochures", type=int, default=None, help="Optional limit for test runs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_page_manifest(
        downloads_dir=Path(args.downloads_dir),
        output_path=Path(args.output_path),
        limit_brochures=args.limit_brochures,
    )
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
