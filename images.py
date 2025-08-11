#!/usr/bin/env python3
"""
Extract images from Wikipedia ZIM file based on required_images.txt
"""

import argparse
import logging
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

try:
    from libzim.reader import Archive
except ImportError:
    raise ImportError("libzim is required. Install with: pip install libzim")


class Stats:
    """Thread-safe statistics."""

    def __init__(self):
        self.lock = Lock()
        self.extracted = 0
        self.skipped = 0
        self.errors = 0
        self.total = 0

    def update(self, extracted=0, skipped=0, errors=0):
        with self.lock:
            self.extracted += extracted
            self.skipped += skipped
            self.errors += errors

    def get_processed(self):
        with self.lock:
            return self.extracted + self.skipped + self.errors


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler("image_extraction.log"),
            logging.StreamHandler(),
        ],
    )


def load_image_list(file_path: Path) -> list:
    """Load image filenames from text file."""
    if not file_path.exists():
        raise FileNotFoundError(f"Image list file not found: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        images = [line.strip() for line in f if line.strip()]

    logging.info(f"Loaded {len(images):,} image filenames from {file_path}")
    return images


def extract_image_batch(
    zim_path: Path, image_batch: list, target_dir: Path, stats: Stats
):
    """Extract a batch of images using its own Archive instance."""
    try:
        archive = Archive(str(zim_path))
    except Exception as e:
        logging.error(f"Failed to open archive: {e}")
        stats.update(errors=len(image_batch))
        return

    batch_extracted = 0
    batch_skipped = 0
    batch_errors = 0

    for image_name in image_batch:
        # Build the ZIM path (images are stored under I/)
        zim_image_path = f"I/{image_name}"

        # Build the output path
        output_path = target_dir / "I" / image_name

        # Skip if already exists
        if output_path.exists():
            batch_skipped += 1
            continue

        try:
            # Get the image from ZIM
            entry = archive.get_entry_by_path(zim_image_path)

            # Ensure parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Extract and write the image
            content = bytes(entry.get_item().content)
            output_path.write_bytes(content)

            batch_extracted += 1

        except Exception as e:
            logging.debug(f"Error extracting {image_name}: {e}")
            batch_errors += 1

    # Update stats once per batch
    stats.update(extracted=batch_extracted, skipped=batch_skipped, errors=batch_errors)


def extract_images(
    zim_path: Path,
    image_list: list,
    target_dir: Path = Path("."),
    max_workers: int = 32,
) -> Stats:
    """Extract images from ZIM file using parallel processing."""

    stats = Stats()
    stats.total = len(image_list)

    if not image_list:
        logging.info("No images to extract")
        return stats

    # Create target directory if needed
    image_dir = target_dir / "I"
    image_dir.mkdir(parents=True, exist_ok=True)

    # Calculate batch size
    batch_size = max(100, len(image_list) // (max_workers * 10))
    batch_size = min(batch_size, 500)

    # Create batches
    batches = [
        image_list[i : i + batch_size] for i in range(0, len(image_list), batch_size)
    ]

    logging.info(
        f"Extracting {len(image_list):,} images in {len(batches)} batches "
        f"of ~{batch_size} items using {max_workers} workers"
    )

    start_time = time.time()
    last_report_time = start_time

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(extract_image_batch, zim_path, batch, target_dir, stats)
            for batch in batches
        ]

        completed = 0
        for future in as_completed(futures):
            completed += 1

            # Report progress every 2 seconds or every 10 batches
            current_time = time.time()
            if current_time - last_report_time >= 2.0 or completed % 10 == 0:
                elapsed = current_time - start_time
                processed = stats.get_processed()
                rate = processed / elapsed if elapsed > 0 else 0

                logging.info(
                    f"Progress: {processed:,}/{stats.total:,} "
                    f"({processed / stats.total * 100:.1f}%) "
                    f"Rate: {rate:.1f}/sec - "
                    f"Extracted: {stats.extracted:,}, "
                    f"Skipped: {stats.skipped:,}, "
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
    processed = stats.get_processed()

    logging.info(
        f"Extraction complete in {elapsed:.1f}s ({processed / elapsed:.1f} images/sec)"
    )

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Extract images from Wikipedia ZIM file"
    )
    parser.add_argument("zim_file", type=Path, help="Path to Wikipedia ZIM file")
    parser.add_argument(
        "--image-list",
        type=Path,
        default=Path("required_images.txt"),
        help="File containing list of image filenames (default: required_images.txt)",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=Path("."),
        help="Target directory for extracted images (default: current directory)",
    )
    parser.add_argument(
        "--workers", type=int, default=32, help="Number of worker threads (default: 32)"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    setup_logging(args.verbose)

    # Validate ZIM file
    if not args.zim_file.exists():
        logging.error(f"ZIM file not found: {args.zim_file}")
        return 1

    # Load image list
    try:
        image_list = load_image_list(args.image_list)
    except FileNotFoundError as e:
        logging.error(str(e))
        return 1

    if not image_list:
        logging.error("No images found in the list file")
        return 1

    # Extract images
    logging.info(f"Starting image extraction from {args.zim_file}")
    logging.info(f"Target directory: {args.target_dir.absolute()}")

    start_time = time.time()
    stats = extract_images(args.zim_file, image_list, args.target_dir, args.workers)
    total_time = time.time() - start_time

    # Print summary
    print(f"""
╔══════════════════════════════════════╗
║    Image Extraction Complete         ║
╠══════════════════════════════════════╣
║ Total images:          {stats.total:14,} ║
║ Successfully extracted: {stats.extracted:14,} ║
║ Already existed:        {stats.skipped:14,} ║
║ Errors:                 {stats.errors:14,} ║
╠══════════════════════════════════════╣
║ Total time: {total_time:25.1f}s ║
║ Rate: {stats.get_processed() / total_time if total_time > 0 else 0:28.1f}/sec ║
║ Output directory: {str(args.target_dir.absolute() / "I"):19} ║
╚══════════════════════════════════════╝
    """)

    if stats.errors > 0:
        logging.warning(
            f"{stats.errors:,} images could not be extracted. Check the log for details."
        )

    return 0 if stats.errors == 0 else 1


if __name__ == "__main__":
    exit(main())
