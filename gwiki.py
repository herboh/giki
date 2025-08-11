#!/usr/bin/env python3
"""
Optimized Wikipedia article extractor from ZIM files.
Fast, efficient, with redirect and image handling.
"""

import argparse
import json
import logging
import time
from pathlib import Path
from typing import List, Set, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

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
class ProcessingStats:
    """Statistics for processing run."""

    total_requested: int = 0
    successful: int = 0
    redirects: int = 0
    errors: int = 0
    already_existed: int = 0
    images: Set[str] = field(default_factory=set)

    @property
    def total_processed(self):
        return self.successful + self.redirects + self.errors


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
    """Extract image filenames from HTML using simple string operations."""
    images = set()
    pos = 0

    while True:
        # Find next img tag
        img_start = html.find("<img", pos)
        if img_start == -1:
            break

        # Find src attribute
        src_start = html.find('src="', img_start)
        if src_start == -1:
            pos = img_start + 4
            continue

        src_start += 5  # Skip 'src="'
        src_end = html.find('"', src_start)
        if src_end == -1:
            pos = img_start + 4
            continue

        src = html[src_start:src_end]

        # Extract filename
        if "/" in src:
            filename = src.split("/")[-1]
            if filename:
                images.add(filename)

        pos = src_end

    return images


def fix_links_fast(html: str, valid_articles: Set[str]) -> str:
    """Fix Wikipedia links - valid ones get .html, invalid ones get broken link replacement."""
    result = []
    pos = 0

    while True:
        # Find next <a tag
        link_start = html.find("<a ", pos)
        if link_start == -1:
            result.append(html[pos:])
            break

        # Append HTML before this link
        result.append(html[pos:link_start])

        # Find end of opening tag
        tag_end = html.find(">", link_start)
        if tag_end == -1:
            result.append(html[link_start:])
            break

        # Extract the full tag
        full_tag = html[link_start : tag_end + 1]

        # Find href attribute
        href_start = full_tag.find('href="')
        if href_start == -1:
            result.append(full_tag)
            pos = tag_end + 1
            continue

        href_start += 6  # Skip 'href="'
        href_end = full_tag.find('"', href_start)
        if href_end == -1:
            result.append(full_tag)
            pos = tag_end + 1
            continue

        href = full_tag[href_start:href_end]

        # Check if this is a Wikipedia article link
        article_title = None
        if href.startswith("A/"):
            article_title = href[2:].split("#")[0]
        elif href.startswith("../A/"):
            article_title = href[5:].split("#")[0]

        if article_title:
            # This is a Wikipedia link
            if article_title in valid_articles:
                # Valid link - add .html if not already there
                if not href.endswith(".html") and "#" not in href:
                    fixed_href = href + ".html"
                    fixed_tag = full_tag.replace(
                        f'href="{href}"', f'href="{fixed_href}"'
                    )
                    result.append(fixed_tag)
                else:
                    result.append(full_tag)
            else:
                # Invalid link - replace with broken link
                space_pos = full_tag.find(" ", 3)  # Skip '<a '
                if space_pos != -1:
                    rest_of_tag = full_tag[space_pos:]
                    result.append(BROKEN_LINK_REPLACEMENT + rest_of_tag)
                else:
                    result.append(BROKEN_LINK_REPLACEMENT + ">")
        else:
            # Not a Wikipedia link, keep as-is
            result.append(full_tag)

        pos = tag_end + 1

    return "".join(result)


def process_article(archive: Archive, title: str, valid_articles: Set[str]) -> tuple:
    """Process a single article. Returns (success, is_redirect, images_set)."""
    try:
        entry = archive.get_entry_by_path(f"A/{title}")

        if entry.is_redirect:
            # Handle redirect
            redirect_target = entry.get_redirect_entry().path
            redirect_html = f'''<!DOCTYPE html>
<html><head>
<meta http-equiv="refresh" content="0; url={redirect_target}.html">
<title>Redirect</title>
</head><body>
<p>Redirecting to <a href="{redirect_target}.html">{redirect_target}</a></p>
</body></html>'''

            output_path = TARGET_DIR / f"{title}.html"
            output_path.write_text(redirect_html, encoding="utf-8")
            return (True, True, set())

        else:
            # Regular article
            content = bytes(entry.get_item().content)
            html = content.decode("utf-8", errors="ignore")

            # Extract images
            images = extract_images_from_html(html)

            # Fix links
            fixed_html = fix_links_fast(html, valid_articles)

            # Write file
            output_path = TARGET_DIR / f"{title}.html"
            output_path.write_text(fixed_html, encoding="utf-8")
            return (True, False, images)

    except Exception as e:
        logging.debug(f"Error processing {title}: {e}")
        return (False, False, set())


def process_batch(
    zim_path: Path, titles_batch: List[str], valid_articles: Set[str]
) -> Dict:
    """Process a batch of titles in a single thread."""
    archive = Archive(str(zim_path))
    results = {"successful": 0, "redirects": 0, "errors": 0, "images": set()}

    for title in titles_batch:
        success, is_redirect, images = process_article(archive, title, valid_articles)
        if success:
            if is_redirect:
                results["redirects"] += 1
            else:
                results["successful"] += 1
                results["images"].update(images)
        else:
            results["errors"] += 1

    return results


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

    # Split into batches for parallel processing
    batch_size = max(len(titles_to_process) // (max_workers * 4), 10)
    batches = [
        titles_to_process[i : i + batch_size]
        for i in range(0, len(titles_to_process), batch_size)
    ]

    start_time = time.time()
    processed = 0

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_batch, zim_path, batch, valid_articles): batch
            for batch in batches
        }

        for future in as_completed(futures):
            try:
                batch_results = future.result()
                stats.successful += batch_results["successful"]
                stats.redirects += batch_results["redirects"]
                stats.errors += batch_results["errors"]
                stats.images.update(batch_results["images"])

                # Progress update
                processed += len(futures[future])
                if processed % 1000 < batch_size:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    logging.info(
                        f"Progress: {processed:,}/{len(titles_to_process):,} "
                        f"({rate:.1f}/sec) - "
                        f"Success: {stats.successful:,}, "
                        f"Redirects: {stats.redirects:,}, "
                        f"Errors: {stats.errors:,}"
                    )
            except Exception as e:
                logging.error(f"Batch processing error: {e}")
                stats.errors += len(futures[future])

    elapsed = time.time() - start_time
    logging.info(
        f"Processing complete in {elapsed:.1f}s "
        f"({len(titles_to_process) / elapsed:.1f} articles/sec)"
    )

    return stats


def extract_images(zim_path: Path, image_names: Set[str], max_workers: int = 32) -> int:
    """Extract images from ZIM file."""
    if not image_names:
        return 0

    TARGET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    def extract_batch(image_batch):
        """Extract a batch of images."""
        archive = Archive(str(zim_path))
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
    batch_size = max(len(image_list) // (max_workers * 4), 10)
    batches = [
        image_list[i : i + batch_size] for i in range(0, len(image_list), batch_size)
    ]

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
        "--workers", type=int, default=32, help="Number of worker threads (default: 32)"
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
