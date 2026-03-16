import argparse
import json
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from requests import Session

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.jobs.kaufda_brochures import (
    CHAIN_TARGETS,
    choose_largest_image,
    crawl_chain,
    fetch_brochure,
    fetch_brochure_pages,
    make_session,
    safe_name,
    write_outputs,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    for attempt in range(1, 11):
        tmp_path.write_text(content, encoding="utf-8")
        try:
            tmp_path.replace(path)
            return
        except PermissionError:
            if attempt == 10:
                raise
            time.sleep(min(2.0, attempt * 0.2))
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


@contextmanager
def run_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise SystemExit(
            f"Another download process already uses this output directory. Remove {lock_path} only if no run is active."
        ) from exc

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "created_at": utc_now()}, ensure_ascii=False, indent=2))
        yield
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def ensure_catalog(
    output_dir: Path,
    chain_keys: list[str],
    max_workers: int,
    page_limit: int | None,
    refresh_catalog: bool,
    catalog_path: Path | None,
) -> tuple[list[dict[str, Any]], Path]:
    path = catalog_path or output_dir / "brochures.json"
    summary_path = path.with_name("summary.json")

    existing_results: list[dict[str, Any]] = []
    if not refresh_catalog and path.exists():
        existing_results = load_json(path, default=[]) or []

    result_by_chain = {item["chain_key"]: item for item in existing_results}
    completed = [chain_key for chain_key in chain_keys if chain_key in result_by_chain]
    for chain_key in completed:
        print(f"[catalog] reuse {chain_key} from {path}")

    for chain_key in chain_keys:
        if chain_key in result_by_chain:
            continue
        print(f"[catalog] crawl {chain_key}")
        result = crawl_chain(
            key=chain_key,
            chain_cfg=CHAIN_TARGETS[chain_key],
            max_workers=max_workers,
            page_limit=page_limit,
        )
        result_by_chain[chain_key] = result
        ordered = [result_by_chain[key] for key in chain_keys if key in result_by_chain]
        write_outputs(ordered, path.parent)
        if path.parent != output_dir:
            atomic_write_json(path, ordered)
            if summary_path.exists():
                atomic_write_text(output_dir / "summary.json", summary_path.read_text(encoding="utf-8"))
        print(
            f"[catalog] done {chain_key}: brochures={result['brochure_count']} "
            f"store_pages={result['store_pages_crawled']} failures={len(result['failures'])}"
        )

    ordered_results = [result_by_chain[key] for key in chain_keys if key in result_by_chain]
    if catalog_path is not None:
        atomic_write_json(path, ordered_results)
        write_outputs(ordered_results, path.parent)
    return ordered_results, path


def iter_catalog_brochures(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for chain in catalog:
        brochure_values = chain["brochures"].values() if isinstance(chain["brochures"], dict) else chain["brochures"]
        for brochure in brochure_values:
            items.append(
                {
                    "chain_key": chain["chain_key"],
                    "chain_name": chain["chain_name"],
                    "content_id": brochure["content_id"],
                    "title": brochure.get("title") or brochure["content_id"],
                    "query": brochure["query"],
                    "page_count": brochure.get("page_count") or 0,
                }
            )
    items.sort(key=lambda item: (item["chain_key"], item["title"], item["content_id"]))
    chain_totals: dict[str, int] = {}
    chain_indices: dict[str, int] = {}
    for item in items:
        chain_totals[item["chain_key"]] = chain_totals.get(item["chain_key"], 0) + 1
    for global_index, item in enumerate(items, start=1):
        chain_key = item["chain_key"]
        chain_indices[chain_key] = chain_indices.get(chain_key, 0) + 1
        item["global_index"] = global_index
        item["global_total"] = len(items)
        item["chain_index"] = chain_indices[chain_key]
        item["chain_total"] = chain_totals[chain_key]
    return items


def brochure_dir(base_dir: Path, brochure: dict[str, Any]) -> Path:
    return base_dir / safe_name(brochure["chain_key"]) / safe_name(brochure["content_id"])


def load_manifest(manifest_path: Path, brochure: dict[str, Any]) -> dict[str, Any]:
    manifest = load_json(manifest_path)
    if manifest:
        return manifest
    return {
        "version": 1,
        "chain_key": brochure["chain_key"],
        "chain_name": brochure["chain_name"],
        "content_id": brochure["content_id"],
        "title": brochure["title"],
        "status": "pending",
        "brochure_type": None,
        "pages_total": 0,
        "pages_completed": 0,
        "pages_failed": 0,
        "attempts": 0,
        "query": brochure["query"],
        "metadata_saved": False,
        "pages_saved": False,
        "last_error": None,
        "updated_at": utc_now(),
        "page_status": {},
    }


def summarize_manifest(manifest: dict[str, Any], brochure_path: Path) -> dict[str, Any]:
    page_status = manifest.get("page_status", {})
    completed = 0
    failed = 0
    for key, info in page_status.items():
        page_number = int(key)
        destination = brochure_path / f"page_{page_number + 1:03d}.jpg"
        if info.get("status") == "done" and destination.exists() and destination.stat().st_size > 0:
            completed += 1
        elif info.get("status") == "failed":
            failed += 1
    manifest["pages_completed"] = completed
    manifest["pages_failed"] = failed
    pages_total = manifest.get("pages_total") or 0
    brochure_type = manifest.get("brochure_type")
    if brochure_type and brochure_type != "static_brochure":
        manifest["status"] = "skipped"
    elif not manifest.get("metadata_saved"):
        manifest["status"] = "pending"
    elif pages_total and completed >= pages_total:
        manifest["status"] = "done"
        manifest["last_error"] = None
    elif completed > 0 and completed < pages_total:
        manifest["status"] = "partial"
    elif failed > 0:
        manifest["status"] = "failed"
    else:
        manifest["status"] = "pending"
    manifest["updated_at"] = utc_now()
    return manifest


def save_manifest(manifest_path: Path, manifest: dict[str, Any], brochure_path: Path) -> None:
    summarize_manifest(manifest, brochure_path)
    atomic_write_json(manifest_path, manifest)


class ProgressTracker:
    def __init__(self, state_path: Path, catalog_path: Path, downloads_dir: Path, brochures: list[dict[str, Any]]) -> None:
        self.state_path = state_path
        self.catalog_path = catalog_path
        self.downloads_dir = downloads_dir
        self.brochures = brochures
        self.entries: dict[str, dict[str, Any]] = {}
        for brochure in brochures:
            path = brochure_dir(downloads_dir, brochure)
            manifest = load_manifest(path / "manifest.json", brochure)
            summarize_manifest(manifest, path)
            self.entries[brochure["content_id"]] = {
                "chain_key": brochure["chain_key"],
                "content_id": brochure["content_id"],
                "title": brochure["title"],
                "status": manifest.get("status"),
                "pages_total": manifest.get("pages_total") or brochure.get("page_count") or 0,
                "pages_completed": manifest.get("pages_completed") or 0,
                "pages_failed": manifest.get("pages_failed") or 0,
                "last_error": manifest.get("last_error"),
            }
        self.current: dict[str, Any] | None = None

    def update(self, brochure: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
        self.entries[brochure["content_id"]] = {
            "chain_key": brochure["chain_key"],
            "content_id": brochure["content_id"],
            "title": brochure["title"],
            "status": manifest.get("status"),
            "pages_total": manifest.get("pages_total") or brochure.get("page_count") or 0,
            "pages_completed": manifest.get("pages_completed") or 0,
            "pages_failed": manifest.get("pages_failed") or 0,
            "last_error": manifest.get("last_error"),
        }
        return self.save()

    def set_current(self, brochure: dict[str, Any], page_number: int | None = None) -> dict[str, Any]:
        self.current = {
            "chain_key": brochure["chain_key"],
            "chain_name": brochure["chain_name"],
            "content_id": brochure["content_id"],
            "title": brochure["title"],
            "page_number": page_number + 1 if page_number is not None else None,
        }
        return self.save()

    def clear_current(self) -> dict[str, Any]:
        self.current = None
        return self.save()

    def snapshot(self) -> dict[str, Any]:
        entries = list(self.entries.values())
        chain_summaries: dict[str, dict[str, Any]] = {}
        for brochure in self.brochures:
            chain_key = brochure["chain_key"]
            if chain_key not in chain_summaries:
                chain_summaries[chain_key] = {
                    "chain_name": brochure["chain_name"],
                    "total_brochures": 0,
                    "finished_brochures": 0,
                    "done_brochures": 0,
                    "skipped_brochures": 0,
                    "failed_brochures": 0,
                }
            chain_summaries[chain_key]["total_brochures"] += 1

        for entry in entries:
            chain_summary = chain_summaries[entry["chain_key"]]
            status = entry["status"]
            if status in {"done", "skipped"}:
                chain_summary["finished_brochures"] += 1
            if status == "done":
                chain_summary["done_brochures"] += 1
            elif status == "skipped":
                chain_summary["skipped_brochures"] += 1
            elif status == "failed":
                chain_summary["failed_brochures"] += 1

        return {
            "updated_at": utc_now(),
            "catalog_path": str(self.catalog_path),
            "downloads_dir": str(self.downloads_dir),
            "total_brochures": len(entries),
            "completed_brochures": sum(1 for entry in entries if entry["status"] in {"done", "skipped"}),
            "skipped_brochures": sum(1 for entry in entries if entry["status"] == "skipped"),
            "failed_brochures": sum(1 for entry in entries if entry["status"] == "failed"),
            "total_pages": sum(entry["pages_total"] for entry in entries),
            "completed_pages": sum(entry["pages_completed"] for entry in entries),
            "chains": chain_summaries,
            "current": self.current,
        }

    def save(self) -> dict[str, Any]:
        payload = self.snapshot()
        atomic_write_json(self.state_path, payload)
        return payload


def format_progress(global_state: dict[str, Any], brochure: dict[str, Any], manifest: dict[str, Any]) -> str:
    total_pages = max(global_state["total_pages"], 1)
    completed_pages = global_state["completed_pages"]
    pages_pct = completed_pages * 100 / total_pages
    total_brochures = max(global_state["total_brochures"], 1)
    brochures_pct = global_state["completed_brochures"] * 100 / total_brochures
    chain_state = global_state["chains"][brochure["chain_key"]]
    chain_total = max(chain_state["total_brochures"], 1)
    chain_pct = chain_state["finished_brochures"] * 100 / chain_total
    current_pages_total = max(manifest.get("pages_total") or brochure.get("page_count") or 0, 1)
    current_pages_done = manifest.get("pages_completed") or 0
    current_pages_pct = current_pages_done * 100 / current_pages_total
    brochure_label = f"{brochure['chain_name']} {brochure['content_id']}"
    return (
        f"[global pages {completed_pages}/{global_state['total_pages']} {pages_pct:6.2f}%] "
        f"[global brochures {global_state['completed_brochures']}/{global_state['total_brochures']} {brochures_pct:6.2f}%] "
        f"[{brochure['chain_key']} {brochure['chain_index']}/{brochure['chain_total']}] "
        f"[{brochure['chain_name']} done {chain_state['finished_brochures']}/{chain_state['total_brochures']} {chain_pct:6.2f}%] "
        f"[current pages {current_pages_done}/{manifest.get('pages_total') or brochure.get('page_count') or 0} {current_pages_pct:6.2f}%] "
        f"{brochure_label}"
    )


def download_to_file(session: Session, url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".part")
    with session.get(url, timeout=90, stream=True) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 128):
                if chunk:
                    handle.write(chunk)
    tmp_path.replace(destination)


def fetch_with_retries(fetch_fn, *args, max_attempts: int, label: str):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fetch_fn(*args)
        except Exception as exc:  # pragma: no cover - network-dependent
            last_error = exc
            wait_seconds = min(30, attempt * 2)
            print(f"[retry] {label} attempt {attempt}/{max_attempts} failed: {exc}")
            if attempt < max_attempts:
                time.sleep(wait_seconds)
    raise RuntimeError(f"{label} failed after {max_attempts} attempts: {last_error}") from last_error


def process_brochure(
    session: Session,
    downloads_dir: Path,
    brochure: dict[str, Any],
    progress: ProgressTracker,
    max_attempts: int,
) -> None:
    path = brochure_dir(downloads_dir, brochure)
    manifest_path = path / "manifest.json"
    metadata_path = path / "metadata.json"
    pages_path = path / "pages.json"
    path.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest(manifest_path, brochure)
    save_manifest(manifest_path, manifest, path)
    global_state = progress.update(brochure, manifest)

    brochure_label = f"{brochure['chain_name']} {brochure['content_id']}"
    print(format_progress(global_state, brochure, manifest))

    if manifest.get("status") == "done":
        print(f"[skip] already complete: {brochure_label}")
        return

    metadata = load_json(metadata_path)
    if metadata is None:
        metadata = fetch_with_retries(
            fetch_brochure,
            session,
            brochure["content_id"],
            brochure["query"],
            max_attempts=max_attempts,
            label=f"metadata {brochure_label}",
        )
        atomic_write_json(metadata_path, metadata)
    manifest["metadata_saved"] = True
    manifest["brochure_type"] = metadata["content"]["type"]

    if manifest["brochure_type"] != "static_brochure":
        manifest["status"] = "skipped"
        manifest["last_error"] = f"unsupported brochure type: {manifest['brochure_type']}"
        save_manifest(manifest_path, manifest, path)
        global_state = progress.update(brochure, manifest)
        print(f"[skip] unsupported brochure type for {brochure_label}: {manifest['brochure_type']}")
        print(format_progress(global_state, brochure, manifest))
        return

    pages_payload = load_json(pages_path)
    if pages_payload is None:
        pages_payload = fetch_with_retries(
            fetch_brochure_pages,
            session,
            brochure["content_id"],
            brochure["query"],
            max_attempts=max_attempts,
            label=f"pages {brochure_label}",
        )
        atomic_write_json(pages_path, pages_payload)
    manifest["pages_saved"] = True
    manifest["pages_total"] = len(pages_payload.get("contents", []))
    save_manifest(manifest_path, manifest, path)
    global_state = progress.update(brochure, manifest)
    print(format_progress(global_state, brochure, manifest))

    for page in pages_payload.get("contents", []):
        page_number = int(page["number"])
        page_key = str(page_number)
        image_url = choose_largest_image(page.get("images", []))
        if not image_url:
            manifest["page_status"][page_key] = {
                "status": "failed",
                "attempts": manifest["page_status"].get(page_key, {}).get("attempts", 0) + 1,
                "url": None,
                "file": None,
                "error": "no image url",
            }
            manifest["last_error"] = f"missing image url for page {page_number + 1}"
            save_manifest(manifest_path, manifest, path)
            global_state = progress.update(brochure, manifest)
            print(f"[warn] missing image url: {brochure_label} page {page_number + 1}")
            print(format_progress(global_state, brochure, manifest))
            continue

        destination = path / f"page_{page_number + 1:03d}.jpg"
        page_state = manifest["page_status"].get(page_key, {})
        if destination.exists() and destination.stat().st_size > 0:
            manifest["page_status"][page_key] = {
                "status": "done",
                "attempts": page_state.get("attempts", 0),
                "url": image_url,
                "file": destination.name,
                "error": None,
            }
            save_manifest(manifest_path, manifest, path)
            global_state = progress.update(brochure, manifest)
            print(format_progress(global_state, brochure, manifest))
            continue

        progress.set_current(brochure, page_number)
        print(
            f"[page] {brochure['chain_name']} {brochure['chain_index']}/{brochure['chain_total']} "
            f"page {page_number + 1}/{manifest['pages_total']} -> {destination.name}"
        )
        try:
            fetch_with_retries(
                download_to_file,
                session,
                image_url,
                destination,
                max_attempts=max_attempts,
                label=f"download {brochure_label} page {page_number + 1}",
            )
            manifest["page_status"][page_key] = {
                "status": "done",
                "attempts": page_state.get("attempts", 0) + 1,
                "url": image_url,
                "file": destination.name,
                "error": None,
            }
            manifest["last_error"] = None
        except Exception as exc:  # pragma: no cover - network-dependent
            manifest["page_status"][page_key] = {
                "status": "failed",
                "attempts": page_state.get("attempts", 0) + 1,
                "url": image_url,
                "file": destination.name,
                "error": str(exc),
            }
            manifest["last_error"] = str(exc)
            print(f"[error] {brochure_label} page {page_number + 1}: {exc}")
        save_manifest(manifest_path, manifest, path)
        global_state = progress.update(brochure, manifest)
        print(format_progress(global_state, brochure, manifest))

    summarize_manifest(manifest, path)
    save_manifest(manifest_path, manifest, path)
    global_state = progress.update(brochure, manifest)
    progress.clear_current()
    print(
        f"[brochure] {brochure_label} status={manifest['status']} "
        f"pages={manifest['pages_completed']}/{manifest['pages_total']}"
    )
    print(format_progress(global_state, brochure, manifest))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all KaufDA brochures with resume support, retries, and persisted progress."
    )
    parser.add_argument(
        "--chains",
        nargs="*",
        default=list(CHAIN_TARGETS.keys()),
        help=f"Chain keys: {', '.join(CHAIN_TARGETS.keys())}",
    )
    parser.add_argument("--output-dir", default="downloads/kaufda_full", help="Output root for catalog, state, and pages.")
    parser.add_argument("--catalog-path", default=None, help="Optional path to an existing brochures.json file.")
    parser.add_argument("--max-workers", type=int, default=6, help="Concurrent requests for the catalog crawl.")
    parser.add_argument("--page-limit", type=int, default=None, help="Optional crawl cap per chain for testing.")
    parser.add_argument("--refresh-catalog", action="store_true", help="Ignore existing catalog data and crawl again.")
    parser.add_argument("--max-attempts", type=int, default=5, help="Max retries for metadata, page list, and image downloads.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    invalid = [chain for chain in args.chains if chain not in CHAIN_TARGETS]
    if invalid:
        raise SystemExit(f"Unknown chain keys: {', '.join(invalid)}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with run_lock(output_dir / ".download.lock"):
        downloads_dir = output_dir / "downloads"
        catalog_path = Path(args.catalog_path) if args.catalog_path else None

        catalog, resolved_catalog_path = ensure_catalog(
            output_dir=output_dir,
            chain_keys=args.chains,
            max_workers=args.max_workers,
            page_limit=args.page_limit,
            refresh_catalog=args.refresh_catalog,
            catalog_path=catalog_path,
        )

        brochures = iter_catalog_brochures(catalog)
        global_state_path = output_dir / "download_state.json"
        progress = ProgressTracker(global_state_path, resolved_catalog_path, downloads_dir, brochures)
        global_state = progress.save()
        print(
            f"[start] brochures={global_state['total_brochures']} pages={global_state['total_pages']} "
            f"completed_pages={global_state['completed_pages']}"
        )

        session = make_session()
        brochure_total = len(brochures)
        for brochure in brochures:
            process_brochure(
                session=session,
                downloads_dir=downloads_dir,
                brochure=brochure,
                progress=progress,
                max_attempts=args.max_attempts,
            )

        final_state = progress.clear_current()
        print(
            f"[done] brochures={final_state['completed_brochures']}/{final_state['total_brochures']} "
            f"pages={final_state['completed_pages']}/{final_state['total_pages']}"
        )


if __name__ == "__main__":
    main()
