#!/usr/bin/env python3
"""
Optimized Wikipedia article processor using ZIM files.
Extracts articles from wikidump.zim, fixes links, and reports image requirements.
"""

from pathlib import Path
import re
import json
import logging
import argparse
import shutil
from typing import Set, List, Dict, Tuple, Optional
from dataclasses import dataclass

try:
    from tqdm import tqdm
except ImportError:
    tqdm = lambda x, **k: x

try:
    from libzim.reader import Archive
except ImportError:
    raise ImportError("libzim is required. Install with: pip install libzim")

# Configuration
TARGET_DIR = Path("/home/chan/code/git/blog/wiki/A/")
TARGET_IMAGES_DIR = Path("/home/chan/code/git/blog/wiki/I/")
TITLES_FILE = Path("gtitles.txt")
REDIRECT_THRESHOLD = 1000  # bytes
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


def load_desired_articles(titles_file: Path) -> Set[str]:
    """Load the list of articles we want to process."""
    try:
        content = titles_file.read_text(encoding="utf-8")
        titles = set()
        for line in content.splitlines():
            line = line.strip()
            if line:
                # Remove A/ prefix if present (common in ZIM title lists)
                if line.startswith("A/"):
                    line = line[2:]
                titles.add(line)
        logging.info(f"Loaded {len(titles)} desired article titles")
        return titles
    except FileNotFoundError:
        logging.error(f"Titles file not found: {titles_file}")
        raise


def normalize_title_for_zim(title: str) -> str:
    """
    Normalize title for ZIM lookup.
    ZIM files often use URL-encoded or specific formatting for article titles.
    """
    # Replace spaces with underscores (common in Wikipedia ZIM files)
    normalized = title.replace(" ", "_")
    # Remove any file extensions if present
    if normalized.endswith(".html"):
        normalized = normalized[:-5]
    return normalized


def find_zim_article(archive: Archive, title: str) -> Optional[bytes]:
    """
    Find an article in the ZIM file by trying different title variations.
    """
    # Try different variations of the title
    variations = [
        title,
        normalize_title_for_zim(title),
        title.replace("_", " "),
        title.replace(" ", "_"),
        f"A/{title}",
        f"A/{normalize_title_for_zim(title)}",
    ]

    for variation in variations:
        try:
            entry = archive.get_entry_by_path(variation)
            if entry:
                return bytes(entry.get_item().content)
        except:
            continue

    # If not found with path, try by title
    try:
        entry = archive.get_entry_by_title(title)
        if entry:
            return bytes(entry.get_item().content)
    except:
        pass

    return None


def extract_articles_from_zim(
    zim_path: Path, desired_titles: Set[str]
) -> Dict[str, bytes]:
    """
    Extract desired articles from ZIM file.
    Returns a dict mapping article names to their HTML content.
    """
    logging.info(f"Opening ZIM file: {zim_path}")

    try:
        archive = Archive(str(zim_path))
    except Exception as e:
        logging.error(f"Failed to open ZIM file: {e}")
        raise

    extracted_articles = {}
    found_count = 0

    logging.info(f"Extracting {len(desired_titles)} articles from ZIM file...")

    for title in tqdm(desired_titles, desc="Extracting articles", unit="article"):
        try:
            content = find_zim_article(archive, title)
            if content:
                extracted_articles[title] = content
                found_count += 1
            else:
                logging.debug(f"Article not found in ZIM: {title}")
        except Exception as e:
            logging.warning(f"Error extracting {title}: {e}")

    logging.info(
        f"Successfully extracted {found_count}/{len(desired_titles)} articles from ZIM"
    )

    missing_count = len(desired_titles) - found_count
    if missing_count > 0:
        missing_titles = desired_titles - set(extracted_articles.keys())
        logging.warning(f"Missing {missing_count} articles from ZIM file")
        logging.debug(f"Missing articles: {sorted(list(missing_titles))[:10]}...")

    return extracted_articles


def is_redirect_content(content: bytes, threshold: int = REDIRECT_THRESHOLD) -> bool:
    """Check if content is likely a redirect based on size and content."""
    if len(content) < threshold:
        return True

    # Convert to string for content analysis
    try:
        html = content.decode("utf-8", errors="ignore")
        # Simple heuristic: redirects typically have very little content
        text_content = re.sub(r"<[^>]+>", "", html).strip()
        if len(text_content) < 200:  # Very little actual text content
            return True
    except:
        pass

    return False


def extract_title_from_html(html: str) -> str:
    """Extract title from HTML using regex (faster than BeautifulSoup for this)."""
    match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
    return match.group(1).strip() if match else ""


def extract_images_from_html(html: str) -> List[str]:
    """Extract image sources using regex."""
    pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'
    matches = re.findall(pattern, html, re.IGNORECASE)

    # Clean and deduplicate
    images = []
    seen = set()
    for src in matches:
        # Get just the filename
        filename = Path(src).name
        if filename and filename not in seen:
            images.append(filename)
            seen.add(filename)

    return sorted(images)


def fix_links_fast(html: str, valid_articles: Set[str]) -> str:
    """
    Fix article links using string replacement (faster than BeautifulSoup).
    Keep links to valid articles, mark others as broken.
    """

    def replace_link(match):
        full_tag = match.group(0)
        href_content = match.group(1)

        # Extract the base article name (remove .html and fragments)
        base_name = href_content.split("#")[0]
        if base_name.endswith(".html"):
            base_name = base_name[:-5]

        # Handle relative URLs that might start with '../'
        if base_name.startswith("../"):
            base_name = base_name[3:]

        if not base_name or base_name in valid_articles:
            # Valid link - ensure it has .html extension
            if not href_content.endswith(".html") and "#" not in href_content:
                return full_tag.replace(
                    f'href="{href_content}"', f'href="{href_content}.html"'
                )
            return full_tag
        else:
            # Broken link - replace with broken link markup
            return BROKEN_LINK_REPLACEMENT + full_tag[full_tag.find(" ") :]

    # Pattern to match <a href="..."> tags
    pattern = r'<a\s+href=["\']([^"\']*)["\'][^>]*>'
    return re.sub(pattern, replace_link, html, flags=re.IGNORECASE)


def process_article_content(
    name: str, content: bytes, valid_articles: Set[str]
) -> ArticleResult:
    """Process a single article's content from ZIM file."""

    # Quick redirect check
    if is_redirect_content(content):
        return ArticleResult(
            name=name,
            title="",
            size_bytes=len(content),
            is_redirect=True,
            images=[],
            processed=False,
        )

    try:
        html = content.decode("utf-8", errors="ignore")
        size_bytes = len(content)

        title = extract_title_from_html(html)
        if not title:
            return ArticleResult(
                name=name,
                title="",
                size_bytes=size_bytes,
                is_redirect=False,
                images=[],
                processed=False,
                error="No title found",
            )

        # Fix links and extract images
        fixed_html = fix_links_fast(html, valid_articles)
        images = extract_images_from_html(html)

        # Write processed file
        TARGET_DIR.mkdir(parents=True, exist_ok=True)
        output_path = TARGET_DIR / f"{name}.html"
        output_path.write_text(fixed_html, encoding="utf-8")

        return ArticleResult(
            name=name,
            title=title,
            size_bytes=size_bytes,
            is_redirect=False,
            images=images,
            processed=True,
        )

    except Exception as e:
        return ArticleResult(
            name=name,
            title="",
            size_bytes=len(content),
            is_redirect=False,
            images=[],
            processed=False,
            error=str(e),
        )


def extract_images_from_zim(zim_path: Path, image_names: Set[str]) -> int:
    """
    Extract required images from ZIM file.
    Returns the number of images successfully extracted.
    """
    if not image_names:
        return 0

    TARGET_IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    try:
        archive = Archive(str(zim_path))
    except Exception as e:
        logging.error(f"Failed to open ZIM file for image extraction: {e}")
        return 0

    extracted = 0

    for img_name in tqdm(image_names, desc="Extracting images", unit="img"):
        try:
            # Try different image URL patterns common in ZIM files
            image_urls = [
                f"I/{img_name}",
                f"-/{img_name}",
                img_name,
                f"images/{img_name}",
            ]

            image_content = None
            for url in image_urls:
                try:
                    entry = archive.get_entry_by_path(url)
                    if entry:
                        image_content = bytes(entry.get_item().content)
                        break
                except:
                    continue

            if image_content:
                dst_path = TARGET_IMAGES_DIR / img_name
                if not dst_path.exists():
                    dst_path.write_bytes(image_content)
                    extracted += 1
            else:
                logging.debug(f"Image not found in ZIM: {img_name}")

        except Exception as e:
            logging.warning(f"Failed to extract image {img_name}: {e}")

    return extracted


def main():
    parser = argparse.ArgumentParser(
        description="Process Wikipedia articles from ZIM file efficiently"
    )
    parser.add_argument(
        "zim_file",
        type=Path,
        help="Path to the Wikipedia ZIM file (e.g., wikidump.zim)",
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
    args = parser.parse_args()

    setup_logging()

    # Validate ZIM file exists
    if not args.zim_file.exists():
        logging.error(f"ZIM file not found: {args.zim_file}")
        return 1

    # Load desired articles
    desired_articles = load_desired_articles(args.titles)

    # Extract articles from ZIM file
    extracted_articles = extract_articles_from_zim(args.zim_file, desired_articles)

    if not extracted_articles:
        logging.error("No articles were extracted from the ZIM file")
        return 1

    # Process articles
    results = []
    all_images = set()
    processed_count = 0
    redirect_count = 0
    error_count = 0

    logging.info(f"Processing {len(extracted_articles)} extracted articles...")

    for article_name, content in tqdm(
        extracted_articles.items(), desc="Processing", unit="article"
    ):
        result = process_article_content(article_name, content, desired_articles)
        results.append(result)

        if result.is_redirect:
            redirect_count += 1
        elif result.processed:
            processed_count += 1
            all_images.update(result.images)
        else:
            error_count += 1

    # Extract images if requested
    if args.extract_images and all_images:
        extracted_images = extract_images_from_zim(args.zim_file, all_images)
        logging.info(f"Extracted {extracted_images}/{len(all_images)} images from ZIM")

    # Generate output summary
    summary = {
        "zim_file": str(args.zim_file),
        "total_desired": len(desired_articles),
        "found_in_zim": len(extracted_articles),
        "processed": processed_count,
        "redirects": redirect_count,
        "errors": error_count,
        "total_images": len(all_images),
        "articles": [
            {
                "name": r.name,
                "title": r.title,
                "size_bytes": r.size_bytes,
                "is_redirect": r.is_redirect,
                "processed": r.processed,
                "images": r.images,
                "image_count": len(r.images),
                "error": r.error,
            }
            for r in results
        ],
    }

    # Save results
    args.output_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    logging.info(f"""
Processing complete:
  • {processed_count} articles processed successfully
  • {redirect_count} redirects skipped  
  • {error_count} errors encountered
  • {len(all_images)} unique images referenced
  • Results saved to {args.output_json}
""")

    return 0


if __name__ == "__main__":
    exit(main())
