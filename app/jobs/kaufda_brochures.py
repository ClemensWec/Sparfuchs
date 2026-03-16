import argparse
import concurrent.futures
import json
import re
import time
from collections import deque
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CHAIN_TARGETS = {
    "aldi-nord": {"display_name": "ALDI Nord", "global_slug": "Aldi-Nord"},
    "aldi-sued": {"display_name": "ALDI Sued", "global_slug": "Aldi-Sued"},
    "lidl": {"display_name": "Lidl", "global_slug": "Lidl"},
    "rewe": {"display_name": "REWE", "global_slug": "REWE"},
    "edeka": {"display_name": "EDEKA", "global_slug": "Edeka"},
    "kaufland": {"display_name": "Kaufland", "global_slug": "Kaufland"},
    "penny": {"display_name": "Penny", "global_slug": "Penny-Markt"},
    "netto": {"display_name": "Netto Marken-Discount", "global_slug": "Netto-Marken-Discount"},
    "norma": {"display_name": "Norma", "global_slug": "Norma"},
    "globus": {"display_name": "Globus", "global_slug": "Globus"},
    "marktkauf": {"display_name": "Marktkauf", "global_slug": "Marktkauf"},
}

USER_AGENT = "Mozilla/5.0"
KAUFDA_BASE = "https://www.kaufda.de"
CONTENT_VIEWER_BASE = "https://content-viewer-be.kaufda.de"
VIEWER_HEADERS = {"Bonial-Api-Consumer": "web-content-viewer-fe"}
VIEWER_QUERY_DEFAULTS = {
    "partner": "kaufda_web",
    "lat": "52.5200065",
    "lng": "13.40495399",
}


def make_session() -> requests.Session:
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16))
    return session


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned.strip("_") or "item"


def fetch_global_page_info(session: requests.Session, global_slug: str) -> dict[str, Any]:
    url = f"{KAUFDA_BASE}/Geschaefte/{global_slug}"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', response.text)
    if not match:
        raise RuntimeError(f"__NEXT_DATA__ missing on {url}")
    payload = json.loads(match.group(1))
    page_information = payload["props"]["pageProps"]["pageInformation"]
    publisher = page_information["publisher"]
    default_city = publisher["defaultCity"]
    return {
        "build_id": payload["buildId"],
        "global_slug": global_slug,
        "local_url": publisher["localUrl"],
        "local_id": str(publisher["localId"]),
        "default_city_url": default_city["url"],
        "default_city_name": default_city["displayName"],
        "default_lat": default_city["lat"],
        "default_lng": default_city["lng"],
        "publisher_name": publisher["name"],
    }


def fetch_store_overview_json(
    session: requests.Session,
    build_id: str,
    store_path: str,
) -> tuple[dict[str, Any] | None, str]:
    url = f"{KAUFDA_BASE}/_next/data/{build_id}/{store_path}.json"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json(), response.text


def crawl_chain(
    key: str,
    chain_cfg: dict[str, Any],
    max_workers: int,
    page_limit: int | None = None,
) -> dict[str, Any]:
    session = make_session()
    page_info = fetch_global_page_info(session, chain_cfg["global_slug"])
    local_url = page_info["local_url"]
    local_id = page_info["local_id"]
    build_id = page_info["build_id"]
    default_city_url = page_info["default_city_url"]
    seed = f"Filialen/{default_city_url}/{local_url}/v-r{local_id}"
    path_pattern = re.compile(rf"/Filialen/([^\"\\]+/{re.escape(local_url)}/v-r{local_id})")

    queue: deque[str] = deque([seed])
    queued = {seed}
    visited: set[str] = set()
    brochures: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, str]] = []
    started = time.time()

    def fetch_path(store_path: str) -> tuple[str, dict[str, Any] | None, str | None]:
        worker_session = make_session()
        try:
            data, raw_text = fetch_store_overview_json(worker_session, build_id, store_path)
            return store_path, data, raw_text
        except Exception as exc:  # pragma: no cover - network-dependent
            return store_path, None, str(exc)

    while queue and (page_limit is None or len(visited) < page_limit):
        batch: list[str] = []
        while queue and len(batch) < max_workers and (page_limit is None or len(visited) + len(batch) < page_limit):
            path = queue.popleft()
            queued.discard(path)
            if path in visited:
                continue
            visited.add(path)
            batch.append(path)

        if not batch:
            continue

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for store_path, payload, raw_text in executor.map(fetch_path, batch):
                if payload is None:
                    failures.append({"store_path": store_path, "error": raw_text or "unknown"})
                    continue

                page_information = payload["pageProps"]["pageInformation"]
                location = page_information.get("location") or {}
                city_name = location.get("city") or page_information.get("city", {}).get("displayName") or ""
                lat = location.get("lat") or page_info["default_lat"]
                lng = location.get("lng") or page_info["default_lng"]

                for bucket in ("viewer", "publisher"):
                    for brochure in page_information["brochures"].get(bucket, []):
                        content_id = brochure["contentId"]
                        record = brochures.setdefault(
                            content_id,
                            {
                                "content_id": content_id,
                                "legacy_id": brochure.get("id"),
                                "title": brochure.get("title"),
                                "page_count": brochure.get("pageCount"),
                                "valid_from": brochure.get("validFrom"),
                                "valid_until": brochure.get("validUntil"),
                                "chain_key": key,
                                "chain_name": chain_cfg["display_name"],
                                "publisher_name": brochure.get("publisher", {}).get("name"),
                                "discoveries": [],
                                "query": {
                                    "partner": VIEWER_QUERY_DEFAULTS["partner"],
                                    "lat": str(lat),
                                    "lng": str(lng),
                                },
                            },
                        )
                        record["discoveries"].append(
                            {
                                "bucket": bucket,
                                "store_path": store_path,
                                "city_name": city_name,
                                "lat": lat,
                                "lng": lng,
                            }
                        )

                if isinstance(raw_text, str):
                    for match in path_pattern.findall(raw_text):
                        next_path = f"Filialen/{match}"
                        if next_path not in visited and next_path not in queued:
                            queue.append(next_path)
                            queued.add(next_path)

    return {
        "chain_key": key,
        "chain_name": chain_cfg["display_name"],
        "global_slug": chain_cfg["global_slug"],
        "publisher_name": page_info["publisher_name"],
        "build_id": page_info["build_id"],
        "local_url": page_info["local_url"],
        "local_id": page_info["local_id"],
        "seed_city": page_info["default_city_name"],
        "store_pages_crawled": len(visited),
        "brochure_count": len(brochures),
        "brochures": brochures,
        "failures": failures,
        "elapsed_seconds": round(time.time() - started, 2),
    }


def fetch_brochure(session: requests.Session, content_id: str, query: dict[str, str]) -> dict[str, Any]:
    response = session.get(
        f"{CONTENT_VIEWER_BASE}/v1/brochures/{content_id}",
        params=query,
        headers=VIEWER_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_brochure_pages(session: requests.Session, content_id: str, query: dict[str, str]) -> dict[str, Any]:
    response = session.get(
        f"{CONTENT_VIEWER_BASE}/v1/brochures/{content_id}/pages",
        params=query,
        headers=VIEWER_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def choose_largest_image(images: list[dict[str, Any]]) -> str | None:
    if not images:
        return None

    def image_score(item: dict[str, Any]) -> tuple[int, int]:
        size = item.get("size", "")
        match = re.match(r"(\d+)x(\d+)", size)
        if match:
            width, height = int(match.group(1)), int(match.group(2))
            return width * height, width
        url = item.get("url", "")
        return (1 if "zoomlarge" in url else 0, 0)

    best = max(images, key=image_score)
    return best.get("url")


def download_file(session: requests.Session, url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, timeout=60, stream=True) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 128):
                if chunk:
                    handle.write(chunk)


def enrich_and_download(chain_result: dict[str, Any], output_dir: Path, download_pages: bool) -> dict[str, Any]:
    session = make_session()
    chain_dir = output_dir / safe_name(chain_result["chain_key"])
    chain_dir.mkdir(parents=True, exist_ok=True)
    enriched: dict[str, Any] = {}

    for brochure in chain_result["brochures"].values():
        content_id = brochure["content_id"]
        metadata = fetch_brochure(session, content_id, brochure["query"])
        brochure_type = metadata["content"]["type"]
        brochure_record = {**brochure, "viewer_metadata": metadata, "brochure_type": brochure_type}

        if brochure_type == "static_brochure":
            pages_payload = fetch_brochure_pages(session, content_id, brochure["query"])
            brochure_record["pages_payload"] = pages_payload

            if download_pages:
                brochure_dir = chain_dir / safe_name(content_id)
                brochure_dir.mkdir(parents=True, exist_ok=True)
                (brochure_dir / "metadata.json").write_text(
                    json.dumps(metadata, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                (brochure_dir / "pages.json").write_text(
                    json.dumps(pages_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                for page in pages_payload.get("contents", []):
                    image_url = choose_largest_image(page.get("images", []))
                    if not image_url:
                        continue
                    destination = brochure_dir / f"page_{page['number'] + 1:03d}.jpg"
                    if not destination.exists():
                        download_file(session, image_url, destination)

        enriched[content_id] = brochure_record

    chain_result["brochures"] = enriched
    return chain_result


def write_outputs(results: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "chains": [
            {
                "chain_key": result["chain_key"],
                "chain_name": result["chain_name"],
                "brochure_count": result["brochure_count"],
                "store_pages_crawled": result["store_pages_crawled"],
                "failures": len(result["failures"]),
                "elapsed_seconds": result["elapsed_seconds"],
            }
            for result in results
        ],
        "total_brochures": sum(result["brochure_count"] for result in results),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    chain_payload = []
    for result in results:
        brochures = result["brochures"]
        if isinstance(brochures, dict):
            brochures = list(brochures.values())
        chain_payload.append(
            {
                key: value
                for key, value in result.items()
                if key not in {"brochures"}
            }
            | {"brochures": brochures}
        )
    (output_dir / "brochures.json").write_text(json.dumps(chain_payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl KaufDA brochure metadata and optionally download brochure pages.")
    parser.add_argument(
        "--chains",
        nargs="*",
        default=list(CHAIN_TARGETS.keys()),
        help=f"Chain keys: {', '.join(CHAIN_TARGETS.keys())}",
    )
    parser.add_argument("--output-dir", default="data/kaufda_brochures", help="Where to write JSON and downloads.")
    parser.add_argument("--max-workers", type=int, default=6, help="Concurrent requests per chain crawl.")
    parser.add_argument("--page-limit", type=int, default=None, help="Optional cap for store overview pages per chain.")
    parser.add_argument("--download-pages", action="store_true", help="Download brochure page images for static brochures.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    invalid = [chain for chain in args.chains if chain not in CHAIN_TARGETS]
    if invalid:
        raise SystemExit(f"Unknown chain keys: {', '.join(invalid)}")

    output_dir = Path(args.output_dir)
    results: list[dict[str, Any]] = []

    for chain_key in args.chains:
        result = crawl_chain(
            key=chain_key,
            chain_cfg=CHAIN_TARGETS[chain_key],
            max_workers=args.max_workers,
            page_limit=args.page_limit,
        )
        if args.download_pages:
            result = enrich_and_download(result, output_dir / "downloads", download_pages=True)
        results.append(result)
        write_outputs(results, output_dir)
        print(
            json.dumps(
                {
                    "chain_key": result["chain_key"],
                    "chain_name": result["chain_name"],
                    "brochure_count": result["brochure_count"],
                    "store_pages_crawled": result["store_pages_crawled"],
                    "failures": len(result["failures"]),
                    "elapsed_seconds": result["elapsed_seconds"],
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
