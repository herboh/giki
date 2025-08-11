#!/usr/bin/env python3
"""
Simplified Wikipedia article processor using ZIM files.
Direct path-based lookup instead of scanning all entries.
"""

from pathlib import Path
import re
import json
import logging
import argparse
import time
from typing import Set, List, Dict
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

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
class ProcessingStats:
    """Processing statistics."""

    total_requested: int = 0
    successful: int = 0
    redirects: int = 0
    not_found: int = 0
    errors: int = 0
    total_size_bytes: int = 0
    unique_images: Set[str] = None

    def __post_init__(self):
        if self.unique_images is None:
            self.unique_images = set()


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
        self.html_tag_pattern = re.compile(r"<[^>]+>")


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
    # Clean and deduplicate efficiently
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

        # Remove ../ prefix if present
        if base.startswith("../"):
            base = base[3:]

        # Remove A/ prefix to get clean title
        clean_title = base[2:] if base.startswith("A/") else base

        # Check if this title is in our valid set
        if not clean_title or clean_title in valid_articles:
            # Valid link - ensure it has .html extension
            if base and not base.endswith(".html") and "#" not in href:
                return full_tag.replace(f'href="{href}"', f'href="{href}.html"')
            return full_tag
        else:
            # Broken link - replace with not_g link
            return BROKEN_LINK_REPLACEMENT + full_tag[full_tag.find(" ") :]

    return REGEX_CACHE.link_pattern.sub(replace_link, html)


def process_single_article(
    archive: Archive, title: str, valid_articles: Set[str]
) -> ArticleResult:
    """Process a single article by direct path lookup."""
    zim_path = f"A/{title}"

    try:
        # Try to get the entry directly
        entry = archive.get_entry_by_path(zim_path)

        # Check if it's a redirect
        if entry.is_redirect:
            return ArticleResult(
                name=title,
                title="",
                size_bytes=0,
                is_redirect=True,
                images=[],
                processed=False,
            )

        # Get content
        content = bytes(entry.get_item().content)
        size_bytes = len(content)

        # Decode and process HTML
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

        # Fix links and extract images
        fixed_html = fix_links_fast(html, valid_articles)
        images = extract_images_fast(html)

        # Write the processed file
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
        # Handle not found or other errors
        error_msg = str(e)
        is_not_found = (
            "not found" in error_msg.lower() or "no entry" in error_msg.lower()
        )

        return ArticleResult(
            name=title,
            title="",
            size_bytes=0,
            is_redirect=False,
            images=[],
            processed=False,
            error=error_msg,
        )


def process_articles_batch(args):
    """Process a batch of articles in a separate thread."""
    zim_path, titles_batch, valid_articles = args

    # Each thread gets its own archive instance
    archive = Archive(str(zim_path))
    results = []

    for title in titles_batch:
        result = process_single_article(archive, title, valid_articles)
        results.append(result)

    return results


def process_all_articles(
    zim_path: Path, titles: List[str], use_threading: bool = True, max_workers: int = 4
) -> List[ArticleResult]:
    """Process all articles using direct path lookup."""

    # Create output directory
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    # Convert titles to set for fast lookup
    valid_articles = set(titles)

    results = []

    if use_threading and len(titles) > 100:
        # Use threading for I/O bound operations
        logging.info(f"Processing {len(titles)} articles using {max_workers} threads")

        # Split titles into batches
        batch_size = max(len(titles) // (max_workers * 4), 10)
        title_batches = [
            titles[i : i + batch_size] for i in range(0, len(titles), batch_size)
        ]

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(
                    process_articles_batch, (str(zim_path), batch, valid_articles)
                ): batch_idx
                for batch_idx, batch in enumerate(title_batches)
            }

            # Collect results as they complete
            for future in tqdm(
                as_completed(future_to_batch),
                total=len(title_batches),
                desc="Processing batches",
            ):
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                except Exception as e:
                    logging.error(f"Batch processing error: {e}")

    else:
        # Single-threaded processing
        logging.info(f"Processing {len(titles)} articles (single-threaded)")
        archive = Archive(str(zim_path))

        for title in tqdm(titles, desc="Processing articles"):
            result = process_single_article(archive, title, valid_articles)
            results.append(result)

    return results


def extract_images_batch(zim_path: Path, image_names: Set[str]) -> int:
    """Extract images by direct path lookup."""
    if not image_names:
        return 0

    TARGET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    archive = Archive(str(zim_path))
    extracted = 0

    for image_name in tqdm(image_names, desc="Extracting images"):
        image_path = f"I/{image_name}"

        try:
            entry = archive.get_entry_by_path(image_path)
            content = bytes(entry.get_item().content)

            dst_path = TARGET_IMAGES_DIR / image_name
            if not dst_path.exists():
                dst_path.write_bytes(content)
                extracted += 1

        except Exception as e:
            logging.debug(f"Could not extract image {image_name}: {e}")
            continue

    return extracted


def generate_summary(
    results: List[ArticleResult], zim_path: Path, extract_images: bool = False
) -> Dict:
    """Generate processing summary."""
    stats = ProcessingStats()
    stats.total_requested = len(results)

    for result in results:
        stats.total_size_bytes += result.size_bytes

        if result.is_redirect:
            stats.redirects += 1
        elif result.processed:
            stats.successful += 1
            stats.unique_images.update(result.images)
        elif "not found" in result.error.lower():
            stats.not_found += 1
        else:
            stats.errors += 1

    # Extract images if requested
    extracted_images = 0
    if extract_images and stats.unique_images:
        logging.info(f"Extracting {len(stats.unique_images)} unique images...")
        extracted_images = extract_images_batch(zim_path, stats.unique_images)

    # Build summary
    summary = {
        "zim_file": str(zim_path),
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
        },
        "articles": [asdict(result) for result in results],
    }

    return summary


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
        description="Process Wikipedia articles from ZIM file using direct path lookup"
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
        default="processing_results.json",
        help="Output file for processing results",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads for parallel processing",
    )
    parser.add_argument(
        "--no-threading",
        action="store_true",
        help="Disable threading (single-threaded processing)",
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

    # Process articles
    start_time = time.time()
    logging.info("Starting article processing...")

    results = process_all_articles(
        args.zim_file,
        titles,
        use_threading=not args.no_threading,
        max_workers=args.threads,
    )

    processing_time = time.time() - start_time
    logging.info(f"Article processing completed in {processing_time:.1f}s")

    # Generate and save summary
    summary = generate_summary(
        results, args.zim_file, extract_images=args.extract_images
    )
    summary["processing_stats"]["total_processing_time"] = f"{processing_time:.1f}s"

    args.output_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Final report
    stats = summary
    logging.info(f"""
╭─ Processing Complete ─╮
│ ✓ {stats["successfully_processed"]:,} articles processed successfully
│ ⚠ {stats["redirects_skipped"]:,} redirects skipped  
│ ✗ {stats["not_found"]:,} articles not found
│ ✗ {stats["errors"]:,} other errors
│ 🖼 {stats["unique_images_found"]:,} unique images found
│ 💾 Results saved to {args.output_json}
│ ⚡ Direct path lookup used (much faster!)
╰────────────────────────╯
    """)

    return 0


if __name__ == "__main__":
    exit(main())
