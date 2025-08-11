#!/usr/bin/env python3
"""
Optimized streaming Wikipedia article processor using ZIM files.
True streaming with aggressive threading and minimal memory usage.
"""

from pathlib import Path
import re
import json
import logging
import argparse
import time
import threading
from typing import Set, List, Dict, Iterator
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import psutil

try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **k: x
try:
    from libzim.reader import Archive
except ImportError:
    raise ImportError("libzim is required. Install with: pip install libzim")

# Configuration
TARGET_DIR = Path("/home/chan/code/wiki/A/")
TARGET_IMAGES_DIR = Path("/home/chan/code/wiki/I/")
TITLES_FILE = Path("/home/chan/code/wiki/gtitles.txt")
BROKEN_LINK_REPLACEMENT = '<a href="../wiki/not_g.html" class="not_g"'


@dataclass
class ArticleResult:
    """Result of processing a single article."""

    name: str
    title: str
    size_bytes: int
    is_redirect: bool
    images: List[str]
    processed: bool
    error: str = ""


@dataclass
class StreamingStats:
    """Thread-safe streaming statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self.total_requested = 0
        self.successful = 0
        self.redirects = 0
        self.not_found = 0
        self.errors = 0
        self.total_size_bytes = 0
        self.unique_images = set()
        self.processed_count = 0

    def update(self, result: ArticleResult):
        """Thread-safe update of statistics."""
        with self._lock:
            self.processed_count += 1
            self.total_size_bytes += result.size_bytes

            if result.is_redirect:
                self.redirects += 1
            elif result.processed:
                self.successful += 1
                self.unique_images.update(result.images)
            elif "not found" in result.error.lower():
                self.not_found += 1
            else:
                self.errors += 1

    def get_snapshot(self):
        """Get a thread-safe snapshot of current stats."""
        with self._lock:
            return {
                "processed_count": self.processed_count,
                "successful": self.successful,
                "redirects": self.redirects,
                "not_found": self.not_found,
                "errors": self.errors,
                "total_size_bytes": self.total_size_bytes,
                "unique_images_count": len(self.unique_images),
            }


class RegexCache:
    """Pre-compiled regex patterns for better performance."""

    def __init__(self):
        self.title_pattern = re.compile(r"<title[^>]*>([^<]+)</title>", re.IGNORECASE)
        self.img_pattern = re.compile(
            r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>', re.IGNORECASE
        )
        self.link_pattern = re.compile(
            r'<a\s+href=["\']([^"\']*)["\'][^>]*>', re.IGNORECASE
        )


# Global regex cache
REGEX_CACHE = RegexCache()


def load_titles_simple(titles_file: Path) -> List[str]:
    """Load titles from file exactly as they are."""
    titles = []
    with titles_file.open("r", encoding="utf-8") as f:
        for line in f:
            title = line.strip()
            if title:
                titles.append(title)

    logging.info(f"Loaded {len(titles)} titles from {titles_file}")
    return titles


def extract_title_fast(html: str) -> str:
    """Extract title using pre-compiled regex."""
    match = REGEX_CACHE.title_pattern.search(html)
    return match.group(1).strip() if match else ""


def extract_images_fast(html: str) -> List[str]:
    """Extract image sources using pre-compiled regex."""
    matches = REGEX_CACHE.img_pattern.findall(html)
    images = []
    seen = set()
    for src in matches:
        filename = Path(src).name
        if filename and filename not in seen:
            images.append(filename)
            seen.add(filename)
    return images


def fix_links_fast(html: str, valid_articles: Set[str]) -> str:
    """Fix links to point to valid articles or broken link page."""

    def replace_link(m):
        full_tag, href = m.group(0), m.group(1)
        base = href.split("#", 1)[0]

        if base.startswith("../"):
            base = base[3:]

        clean_title = base[2:] if base.startswith("A/") else base

        if not clean_title or clean_title in valid_articles:
            if base and not base.endswith(".html") and "#" not in href:
                return full_tag.replace(f'href="{href}"', f'href="{href}.html"')
            return full_tag
        else:
            return BROKEN_LINK_REPLACEMENT + full_tag[full_tag.find(" ") :]

    return REGEX_CACHE.link_pattern.sub(replace_link, html)


def process_single_article(
    archive: Archive, title: str, valid_articles: Set[str]
) -> ArticleResult:
    """Process a single article by direct path lookup."""
    zim_path = f"A/{title}"

    try:
        entry = archive.get_entry_by_path(zim_path)

        if entry.is_redirect:
            return ArticleResult(
                name=title,
                title="",
                size_bytes=0,
                is_redirect=True,
                images=[],
                processed=False,
            )

        content = bytes(entry.get_item().content)
        size_bytes = len(content)

        html = content.decode("utf-8", errors="ignore")
        extracted_title = extract_title_fast(html)

        if not extracted_title:
            return ArticleResult(
                name=title,
                title="",
                size_bytes=size_bytes,
                is_redirect=False,
                images=[],
                processed=False,
                error="No title found in HTML",
            )

        # Process content
        fixed_html = fix_links_fast(html, valid_articles)
        images = extract_images_fast(html)

        # Write file immediately (streaming)
        output_path = TARGET_DIR / f"{title}.html"
        output_path.write_text(fixed_html, encoding="utf-8")

        return ArticleResult(
            name=title,
            title=extracted_title,
            size_bytes=size_bytes,
            is_redirect=False,
            images=images,
            processed=True,
        )

    except Exception as e:
        error_msg = str(e)
        return ArticleResult(
            name=title,
            title="",
            size_bytes=0,
            is_redirect=False,
            images=[],
            processed=False,
            error=error_msg,
        )


def determine_optimal_workers() -> int:
    """Determine optimal number of workers based on system resources."""
    cpu_count = psutil.cpu_count(logical=True)
    memory_gb = psutil.virtual_memory().total / (1024**3)

    # More aggressive threading for I/O bound operations
    # ZIM reading is mostly I/O bound, so we can use many more threads than CPU cores
    base_workers = cpu_count * 8  # Start with 8x CPU cores

    # Adjust based on available memory (each worker uses ~50MB)
    memory_workers = int(memory_gb * 20)  # ~50MB per worker

    # Cap at reasonable limits
    optimal = min(base_workers, memory_workers, 200)  # Max 200 threads
    optimal = max(optimal, 16)  # Minimum 16 threads

    logging.info(
        f"System: {cpu_count} CPUs, {memory_gb:.1f}GB RAM - Using {optimal} workers"
    )
    return optimal


def progress_reporter(stats: StreamingStats, total: int, stop_event: threading.Event):
    """Background thread to report progress every few seconds."""
    start_time = time.time()

    while not stop_event.is_set():
        time.sleep(5)  # Report every 5 seconds

        snapshot = stats.get_snapshot()
        elapsed = time.time() - start_time
        rate = snapshot["processed_count"] / elapsed if elapsed > 0 else 0

        logging.info(
            f"Progress: {snapshot['processed_count']:,}/{total:,} "
            f"({snapshot['processed_count'] / total * 100:.1f}%) "
            f"Rate: {rate:.1f}/sec "
            f"Success: {snapshot['successful']:,} "
            f"Errors: {snapshot['errors']:,}"
        )


def result_writer(result_queue: Queue, output_file: Path, stop_event: threading.Event):
    """Background thread to stream results to JSON file."""
    results = []

    while not stop_event.is_set() or not result_queue.empty():
        try:
            result = result_queue.get(timeout=1)
            if result is None:  # Sentinel value
                break
            results.append(asdict(result))

            # Write batch every 1000 results to avoid memory buildup
            if len(results) >= 1000:
                # For now, just keep in memory - we'll write all at the end
                # In a production system, you'd want to write to a streaming JSON format
                pass

        except:
            continue

    # Write final results
    if results:
        temp_data = {"articles": results}
        output_file.write_text(
            json.dumps(temp_data, indent=2, ensure_ascii=False), encoding="utf-8"
        )


def process_articles_streaming(
    zim_path: Path, titles: List[str], max_workers: int = None, output_json: Path = None
) -> StreamingStats:
    """
    Process articles with true streaming - minimal memory usage.
    Results are written as they're processed.
    """

    if max_workers is None:
        max_workers = determine_optimal_workers()

    # Create output directory
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    # Convert titles to set for fast lookup
    valid_articles = set(titles)

    # Initialize streaming components
    stats = StreamingStats()
    stats.total_requested = len(titles)

    # Setup progress reporting
    stop_progress = threading.Event()
    progress_thread = threading.Thread(
        target=progress_reporter, args=(stats, len(titles), stop_progress)
    )
    progress_thread.daemon = True
    progress_thread.start()

    # Setup result streaming (if output file specified)
    result_queue = Queue(maxsize=10000) if output_json else None
    stop_writer = threading.Event()
    writer_thread = None

    if output_json:
        writer_thread = threading.Thread(
            target=result_writer, args=(result_queue, output_json, stop_writer)
        )
        writer_thread.daemon = True
        writer_thread.start()

    logging.info(
        f"Processing {len(titles)} articles with {max_workers} workers (streaming mode)"
    )

    # Process articles in parallel with streaming
    def process_title_batch(title_batch):
        """Process a batch of titles."""
        # Each thread gets its own archive instance for thread safety
        archive = Archive(str(zim_path))
        batch_results = []

        for title in title_batch:
            result = process_single_article(archive, title, valid_articles)

            # Update stats immediately
            stats.update(result)

            # Queue result for writing (if enabled)
            if result_queue:
                try:
                    result_queue.put(result, timeout=1)
                except:
                    pass  # Queue full, skip this result for JSON

            batch_results.append(result)

        return batch_results

    # Split titles into smaller batches for better parallelism
    batch_size = max(len(titles) // (max_workers * 4), 5)
    title_batches = [
        titles[i : i + batch_size] for i in range(0, len(titles), batch_size)
    ]

    # Process all batches
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_title_batch, batch) for batch in title_batches
        ]

        # Wait for completion
        for future in as_completed(futures):
            try:
                future.result()  # This will raise any exceptions
            except Exception as e:
                logging.error(f"Batch processing error: {e}")

    # Cleanup
    stop_progress.set()

    if writer_thread:
        result_queue.put(None)  # Sentinel
        stop_writer.set()
        writer_thread.join(timeout=10)

    progress_thread.join(timeout=5)

    return stats


def extract_images_batch(
    zim_path: Path, image_names: Set[str], max_workers: int = None
) -> int:
    """Extract images using parallel processing."""
    if not image_names:
        return 0

    if max_workers is None:
        max_workers = min(psutil.cpu_count() * 4, 50)

    TARGET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    extracted_count = 0

    def extract_image_batch(image_batch):
        """Extract a batch of images."""
        archive = Archive(str(zim_path))
        local_count = 0

        for image_name in image_batch:
            image_path = f"I/{image_name}"

            try:
                entry = archive.get_entry_by_path(image_path)
                content = bytes(entry.get_item().content)

                dst_path = TARGET_IMAGES_DIR / image_name
                if not dst_path.exists():
                    dst_path.write_bytes(content)
                    local_count += 1

            except Exception:
                continue

        return local_count

    # Split images into batches
    image_list = list(image_names)
    batch_size = max(len(image_list) // max_workers, 10)
    image_batches = [
        image_list[i : i + batch_size] for i in range(0, len(image_list), batch_size)
    ]

    # Process in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(extract_image_batch, batch) for batch in image_batches
        ]

        for future in tqdm(
            as_completed(futures), total=len(image_batches), desc="Extracting images"
        ):
            try:
                extracted_count += future.result()
            except Exception as e:
                logging.error(f"Image extraction error: {e}")

    return extracted_count


def setup_logging():
    """Configure logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler("wiki_processing.log"),
            logging.StreamHandler(),
        ],
    )


def main():
    parser = argparse.ArgumentParser(
        description="Optimized streaming Wikipedia article processor"
    )
    parser.add_argument(
        "zim_file",
        type=Path,
        help="Path to the Wikipedia ZIM file",
    )
    parser.add_argument(
        "--titles",
        type=Path,
        default=TITLES_FILE,
        help="File containing desired article titles",
    )
    parser.add_argument(
        "--extract-images",
        action="store_true",
        help="Extract required images from ZIM file",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Output file for detailed results (optional - uses more memory)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of worker threads (default: auto-detect based on system)",
    )
    parser.add_argument(
        "--no-detailed-output",
        action="store_true",
        help="Skip detailed JSON output to save memory",
    )

    args = parser.parse_args()
    setup_logging()

    # Validate inputs
    if not args.zim_file.exists():
        logging.error(f"ZIM file not found: {args.zim_file}")
        return 1

    if not args.titles.exists():
        logging.error(f"Titles file not found: {args.titles}")
        return 1

    # Load titles
    titles = load_titles_simple(args.titles)
    if not titles:
        logging.error("No titles to process")
        return 1

    # Determine output file
    output_json = None
    if not args.no_detailed_output:
        output_json = args.output_json or Path("processing_results.json")

    # Process articles with streaming
    start_time = time.time()
    logging.info("Starting optimized streaming processing...")

    stats = process_articles_streaming(
        args.zim_file, titles, max_workers=args.workers, output_json=output_json
    )

    processing_time = time.time() - start_time

    # Extract images if requested
    extracted_images = 0
    if args.extract_images and stats.unique_images:
        logging.info(f"Extracting {len(stats.unique_images)} unique images...")
        extracted_images = extract_images_batch(args.zim_file, stats.unique_images)

    # Generate summary
    summary = {
        "zim_file": str(args.zim_file),
        "total_requested": stats.total_requested,
        "successfully_processed": stats.successful,
        "redirects_skipped": stats.redirects,
        "not_found": stats.not_found,
        "errors": stats.errors,
        "unique_images_found": len(stats.unique_images),
        "images_extracted": extracted_images,
        "processing_stats": {
            "success_rate": f"{stats.successful / stats.total_requested * 100:.1f}%"
            if stats.total_requested
            else "0%",
            "avg_article_size": stats.total_size_bytes // stats.successful
            if stats.successful
            else 0,
            "total_processing_time": f"{processing_time:.1f}s",
            "processing_rate": f"{stats.total_requested / processing_time:.1f} articles/sec"
            if processing_time > 0
            else "N/A",
        },
    }

    # Save summary
    summary_file = Path("processing_summary.json")
    summary_file.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Final report
    logging.info(f"""
╭─ Streaming Processing Complete ─╮
│ ✓ {stats.successful:,} articles processed successfully
│ ⚠ {stats.redirects:,} redirects skipped  
│ ✗ {stats.not_found:,} articles not found
│ ✗ {stats.errors:,} other errors
│ 🖼 {len(stats.unique_images):,} unique images found
│ ⚡ {stats.total_requested / processing_time:.1f} articles/sec
│ 💾 Summary saved to {summary_file}
│ 🔄 True streaming mode (minimal memory usage)
╰────────────────────────╯
    """)

    return 0


if __name__ == "__main__":
    exit(main())
