#!/usr/bin/env python3
"""
Optimized Wikipedia article extractor from ZIM files.
Fast, efficient, with redirect and image handling.
Combines memory mapping with working content extraction approach.
"""

import argparse
import json
import logging
import time
import re
from pathlib import Path
from typing import List, Set, Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from threading import Lock
from functools import lru_cache

try:
    from libzim.reader import Archive
except ImportError:
    raise ImportError("libzim is required. Install with: pip install libzim")

# Configuration
TARGET_DIR = Path("/home/chan/code/wiki/giki/test/A/")
TARGET_IMAGES_DIR = Path("/home/chan/code/wiki/giki/test/I/")
TITLES_FILE = Path("/home/chan/code/wiki/giki/gtitles.txt")
BROKEN_LINK_REPLACEMENT = '<a href="../wiki/not_g.html" class="not_g"'


@dataclass
class ProcessingStats:
    """Statistics for processing run."""

    total_requested: int = 0
    successful: int = 0
    redirects: int = 0
    errors: int = 0
    already_existed: int = 0
    images: Set[str] = field(default_factory=set)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    @property
    def total_processed(self):
        return self.successful + self.redirects + self.errors

    def update(
        self,
        successful: int = 0,
        redirects: int = 0,
        errors: int = 0,
        images: Optional[Set[str]] = None,
    ):
        """Thread-safe update of stats."""
        with self._lock:
            self.successful += successful
            self.redirects += redirects
            self.errors += errors
            if images:
                self.images.update(images)


class RegexCache:
    """Pre-compiled regex patterns for better performance."""

    def __init__(self):
        self.img_pattern = re.compile(
            r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>', re.IGNORECASE
        )
        self.link_pattern = re.compile(
            r'<a\s+([^>]*?)href=["\']([^"\']*)["\']([^>]*?)>', re.IGNORECASE | re.DOTALL
        )


# Global regex cache
REGEX_CACHE = RegexCache()


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler("wiki_processing.log"),
            logging.StreamHandler(),
        ],
    )


def load_titles(titles_file: Path) -> List[str]:
    """Load titles from file."""
    with titles_file.open("r", encoding="utf-8") as f:
        titles = [line.strip() for line in f if line.strip()]
    logging.info(f"Loaded {len(titles):,} titles from {titles_file}")
    return titles


def get_existing_files(target_dir: Path) -> Set[str]:
    """Get set of existing .html files (without extension)."""
    if not target_dir.exists():
        return set()
    existing = {f.stem for f in target_dir.glob("*.html")}
    if existing:
        logging.info(f"Found {len(existing):,} existing files in {target_dir}")
    return existing


def extract_images_from_html(html: str) -> Set[str]:
    """Extract image filenames from HTML using regex."""
    images = set()
    matches = REGEX_CACHE.img_pattern.findall(html)

    for src in matches:
        # Extract just the filename
        if "/" in src:
            filename = src.split("/")[-1]
        else:
            filename = src

        if filename and not filename.startswith("data:"):
            images.add(filename)

    return images


def fix_links_fast(html: str, valid_articles: Set[str]) -> str:
    """Fix Wikipedia links using regex."""

    def replace_link(match):
        """Replacement function for each link."""
        pre_href = match.group(1) if match.group(1) else ""
        href = match.group(2)
        post_href = match.group(3) if match.group(3) else ""

        # Skip empty hrefs, anchors, and external links
        if not href or href.startswith("#") or href.startswith("http"):
            return match.group(0)

        # Store original href and anchor
        original_href = href
        anchor = ""

        # Extract anchor if present
        if "#" in href:
            href_parts = href.split("#", 1)
            href = href_parts[0]
            anchor = "#" + href_parts[1]

        # Determine if this is a wiki link and extract the article title
        article_title = None
        is_wiki_link = False

        # Remove various prefixes to get the clean article title
        if href.startswith("./"):
            is_wiki_link = True
            article_title = href[2:]
        elif href.startswith("../"):
            remaining = href[3:]
            if remaining.startswith("A/"):
                is_wiki_link = True
                article_title = remaining[2:]
            elif remaining.startswith("wiki/"):
                is_wiki_link = True
                article_title = remaining[5:]
            else:
                # Could be ../something_else
                is_wiki_link = False
        elif href.startswith("A/"):
            is_wiki_link = True
            article_title = href[2:]
        elif href.startswith("/wiki/"):
            is_wiki_link = True
            article_title = href[6:]
        elif not "/" in href or href.count("/") == 0:
            # Plain article name without path
            is_wiki_link = True
            article_title = href

        if is_wiki_link and article_title:
            # Clean up article title
            if article_title.endswith(".html"):
                article_title = article_title[:-5]

            # Check if this article is in our valid set
            if article_title in valid_articles:
                # Valid article - ensure proper path format
                new_href = f"../A/{article_title}.html{anchor}"
                return f'<a {pre_href}href="{new_href}"{post_href}>'
            else:
                # Invalid article - replace with not_g.html link
                # Check for existing class attribute
                full_attrs = pre_href + " " + post_href

                if 'class="' in full_attrs:
                    # Find and update existing class
                    class_match = re.search(r'class="([^"]*)"', full_attrs)
                    if class_match:
                        existing_classes = class_match.group(1)
                        if "not_g" not in existing_classes:
                            new_classes = f"{existing_classes} not_g"
                            # Replace class in the appropriate part
                            if 'class="' in post_href:
                                new_post_href = post_href.replace(
                                    f'class="{existing_classes}"',
                                    f'class="{new_classes}"',
                                )
                                return f'<a {pre_href}href="../wiki/not_g.html"{new_post_href}>'
                            else:
                                new_pre_href = pre_href.replace(
                                    f'class="{existing_classes}"',
                                    f'class="{new_classes}"',
                                )
                                return f'<a {new_pre_href}href="../wiki/not_g.html"{post_href}>'
                    return f'<a {pre_href}href="../wiki/not_g.html"{post_href}>'
                else:
                    # No existing class, add our own
                    return f'<a {pre_href}href="../wiki/not_g.html" class="not_g"{post_href}>'

        # Not a wiki link, return as is
        return match.group(0)

    return REGEX_CACHE.link_pattern.sub(replace_link, html)


def process_article(
    archive: Archive, title: str, valid_articles: Set[str], verbose_debug: bool = False
) -> Tuple[bool, bool, Set[str]]:
    """Process a single article. Returns (success, is_redirect, images_set)."""
    zim_path = f"A/{title}"

    try:
        # Try to get the entry directly by path
        entry = archive.get_entry_by_path(zim_path)

        # Check if it's a redirect
        if entry.is_redirect:
            redirect_entry = entry.get_redirect_entry()
            redirect_target = redirect_entry.path if redirect_entry else "Main_Page"

            # Clean up redirect target
            if redirect_target.startswith("A/"):
                redirect_target = redirect_target[2:]

            redirect_html = f"""<!DOCTYPE html>
<html><head>
<meta http-equiv="refresh" content="0; url=../A/{redirect_target}.html">
<title>Redirect</title>
</head><body>
<p>Redirecting to <a href="../A/{redirect_target}.html">{redirect_target}</a></p>
</body></html>"""

            output_path = TARGET_DIR / f"{title}.html"
            output_path.write_text(redirect_html, encoding="utf-8")

            if verbose_debug:
                logging.debug(f"Created redirect: {title} -> {redirect_target}")

            return (True, True, set())

        # Get content for non-redirect entry
        content = bytes(entry.get_item().content)
        html = content.decode("utf-8", errors="ignore")

        if verbose_debug:
            logging.debug(f"Processing article: {title} (size: {len(html)} bytes)")
            if len(html) > 500:
                logging.debug(f"First 500 chars of HTML: {html[:500]}")

        # Extract images
        images = extract_images_from_html(html)
        if images and verbose_debug:
            logging.debug(f"Found {len(images)} images in {title}")

        # Fix links
        fixed_html = fix_links_fast(html, valid_articles)

        # Write the processed article
        output_path = TARGET_DIR / f"{title}.html"
        output_path.write_text(fixed_html, encoding="utf-8")

        if verbose_debug:
            logging.debug(f"Successfully processed article: {title}")

        return (True, False, images)

    except Exception as e:
        error_msg = str(e)
        if verbose_debug or ("not found" not in error_msg.lower()):
            logging.debug(f"Error processing {title}: {error_msg}")
        return (False, False, set())


def process_batch_worker(
    zim_path: Path,
    titles_batch: List[str],
    valid_articles: Set[str],
    stats: ProcessingStats,
    worker_id: int = 0,
):
    """Worker function that processes a batch with its own Archive instance."""
    # Each worker creates its own Archive instance
    try:
        archive = Archive(str(zim_path))
    except Exception as e:
        logging.error(f"Worker {worker_id} failed to open archive: {e}")
        stats.update(errors=len(titles_batch))
        return

    batch_successful = 0
    batch_redirects = 0
    batch_errors = 0
    batch_images = set()

    # Enable verbose debug for first article of first worker
    first_article_debug = worker_id == 0

    for i, title in enumerate(titles_batch):
        verbose_debug = first_article_debug and i == 0
        success, is_redirect, images = process_article(
            archive, title, valid_articles, verbose_debug
        )

        if success:
            if is_redirect:
                batch_redirects += 1
            else:
                batch_successful += 1
                batch_images.update(images)
        else:
            batch_errors += 1

    # Update stats once per batch
    stats.update(
        successful=batch_successful,
        redirects=batch_redirects,
        errors=batch_errors,
        images=batch_images,
    )


def process_articles(
    zim_path: Path, titles: List[str], max_workers: int = 32
) -> ProcessingStats:
    """Process articles using thread pool."""
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    # Filter out existing files
    existing = get_existing_files(TARGET_DIR)
    titles_to_process = [t for t in titles if t not in existing]

    stats = ProcessingStats(
        total_requested=len(titles),
        already_existed=len(titles) - len(titles_to_process),
    )

    if not titles_to_process:
        logging.info("All titles already processed!")
        return stats

    # Convert to set for O(1) lookup
    valid_articles = set(titles)

    # Test with first article to ensure everything works
    if titles_to_process:
        logging.info(f"Testing with first article: {titles_to_process[0]}")
        test_archive = Archive(str(zim_path))
        success, is_redirect, images = process_article(
            test_archive, titles_to_process[0], valid_articles, verbose_debug=True
        )
        if success:
            if is_redirect:
                logging.info("First article is a redirect")
                stats.update(redirects=1)
            else:
                logging.info(
                    f"First article processed successfully with {len(images)} images"
                )
                stats.update(successful=1, images=images)
            titles_to_process = titles_to_process[1:]
        else:
            logging.warning("First article failed to process")
            stats.update(errors=1)
            titles_to_process = titles_to_process[1:]

    if not titles_to_process:
        return stats

    # Calculate optimal batch size
    batch_size = max(100, len(titles_to_process) // (max_workers * 10))
    batch_size = min(batch_size, 500)

    batches = [
        titles_to_process[i : i + batch_size]
        for i in range(0, len(titles_to_process), batch_size)
    ]

    logging.info(
        f"Processing {len(titles_to_process):,} articles in {len(batches)} batches "
        f"of ~{batch_size} items using {max_workers} workers"
    )

    start_time = time.time()
    last_report_time = start_time

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_batch_worker, zim_path, batch, valid_articles, stats, i
            )
            for i, batch in enumerate(batches)
        ]

        completed = 0
        for future in as_completed(futures):
            completed += 1

            # Report progress every 2 seconds or every 10 batches
            current_time = time.time()
            if current_time - last_report_time >= 2.0 or completed % 10 == 0:
                elapsed = current_time - start_time
                processed = stats.total_processed
                rate = processed / elapsed if elapsed > 0 else 0

                logging.info(
                    f"Progress: {processed:,}/{len(titles_to_process):,} "
                    f"({processed / len(titles_to_process) * 100:.1f}%) "
                    f"Rate: {rate:.1f}/sec - "
                    f"Success: {stats.successful:,}, "
                    f"Redirects: {stats.redirects:,}, "
                    f"Errors: {stats.errors:,} "
                    f"[Batches: {completed}/{len(batches)}]"
                )
                last_report_time = current_time

            # Check for exceptions
            try:
                future.result()
            except Exception as e:
                logging.error(f"Batch processing exception: {e}")

    elapsed = time.time() - start_time
    logging.info(
        f"Processing complete in {elapsed:.1f}s "
        f"({stats.total_processed / elapsed:.1f} articles/sec)"
    )

    return stats


def extract_images(zim_path: Path, image_names: Set[str], max_workers: int = 32) -> int:
    """Extract images from ZIM file."""
    if not image_names:
        return 0

    TARGET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    def extract_batch(image_batch):
        """Extract a batch of images with its own Archive instance."""
        try:
            archive = Archive(str(zim_path))
        except Exception as e:
            logging.error(f"Failed to open archive for image extraction: {e}")
            return 0

        extracted = 0
        for image_name in image_batch:
            try:
                entry = archive.get_entry_by_path(f"I/{image_name}")
                content = bytes(entry.get_item().content)

                output_path = TARGET_IMAGES_DIR / image_name
                if not output_path.exists():
                    output_path.write_bytes(content)
                    extracted += 1
            except Exception:
                pass

        return extracted

    # Split into batches
    image_list = list(image_names)
    batch_size = max(100, len(image_list) // (max_workers * 10))
    batch_size = min(batch_size, 500)

    batches = [
        image_list[i : i + batch_size] for i in range(0, len(image_list), batch_size)
    ]

    logging.info(f"Extracting images in {len(batches)} batches of ~{batch_size} items")

    total_extracted = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(extract_batch, batch) for batch in batches]
        for future in as_completed(futures):
            try:
                total_extracted += future.result()
            except Exception as e:
                logging.error(f"Image extraction error: {e}")

    logging.info(f"Extracted {total_extracted:,} images")
    return total_extracted


def save_image_list(images: Set[str], output_file: Path):
    """Save list of required images to file."""
    with output_file.open("w", encoding="utf-8") as f:
        for image in sorted(images):
            f.write(f"{image}\n")
    logging.info(f"Saved {len(images):,} image names to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Fast Wikipedia ZIM processor with redirect and image support"
    )
    parser.add_argument("zim_file", type=Path, help="Path to Wikipedia ZIM file")
    parser.add_argument(
        "--titles",
        type=Path,
        default=TITLES_FILE,
        help="File with article titles (one per line)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Number of worker threads (default: 32)",
    )
    parser.add_argument(
        "--extract-images",
        action="store_true",
        help="Extract images after processing articles",
    )
    parser.add_argument(
        "--image-list",
        type=Path,
        default=Path("required_images.txt"),
        help="Output file for image list",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("processing_summary.json"),
        help="Output file for processing summary",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Validate inputs
    if not args.zim_file.exists():
        logging.error(f"ZIM file not found: {args.zim_file}")
        return 1

    if not args.titles.exists():
        logging.error(f"Titles file not found: {args.titles}")
        return 1

    # Load titles
    titles = load_titles(args.titles)
    if not titles:
        logging.error("No titles to process")
        return 1

    # Process articles
    logging.info(f"Processing articles from {args.zim_file}")
    start_time = time.time()

    stats = process_articles(args.zim_file, titles, max_workers=args.workers)

    total_time = time.time() - start_time

    # Save image list
    if stats.images:
        save_image_list(stats.images, args.image_list)

    # Extract images if requested
    images_extracted = 0
    if args.extract_images and stats.images:
        logging.info(f"Extracting {len(stats.images):,} unique images...")
        images_extracted = extract_images(
            args.zim_file, stats.images, max_workers=args.workers
        )

    # Generate and save summary
    summary = {
        "zim_file": str(args.zim_file),
        "titles_file": str(args.titles),
        "statistics": {
            "total_requested": stats.total_requested,
            "already_existed": stats.already_existed,
            "newly_processed": stats.total_processed,
            "successful": stats.successful,
            "redirects": stats.redirects,
            "errors": stats.errors,
            "unique_images": len(stats.images),
            "images_extracted": images_extracted,
        },
        "performance": {
            "total_time": f"{total_time:.1f}s",
            "articles_per_second": f"{stats.total_processed / total_time:.1f}"
            if total_time > 0
            else "N/A",
            "success_rate": f"{(stats.successful + stats.redirects) / stats.total_processed * 100:.1f}%"
            if stats.total_processed > 0
            else "N/A",
        },
    }

    args.summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Final report
    print(f"""
╔══════════════════════════════════════╗
║     Wikipedia Processing Complete     ║
╠══════════════════════════════════════╣
║ Files already existed: {stats.already_existed:14,} ║
║ Articles processed:    {stats.successful:14,} ║
║ Redirects created:     {stats.redirects:14,} ║
║ Errors encountered:    {stats.errors:14,} ║
║ Unique images found:   {len(stats.images):14,} ║
║ Images extracted:      {images_extracted:14,} ║
╠══════════════════════════════════════╣
║ Total time: {total_time:25.1f}s ║
║ Rate: {stats.total_processed / total_time if total_time > 0 else 0:28.1f}/sec ║
║ Summary saved to: {str(args.summary):19} ║
╚══════════════════════════════════════╝
    """)

    return 0


if __name__ == "__main__":
    exit(main())
