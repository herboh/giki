#!/usr/bin/env python3
"""
Simple Wikipedia article extractor from ZIM files.
Fast, minimal, no bullshit.
"""

import argparse
import time
from pathlib import Path
from typing import List, Set

try:
    from libzim.reader import Archive
except ImportError:
    raise ImportError("libzim is required. Install with: pip install libzim")

# Configuration
TARGET_DIR = Path("/home/chan/code/wiki/A/")
TITLES_FILE = Path("/home/chan/code/wiki/gtitles.txt")


def load_titles(titles_file: Path) -> List[str]:
    """Load titles from file."""
    with titles_file.open("r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def extract_images_from_html(html: str) -> Set[str]:
    """Extract image filenames from HTML using simple string operations."""
    images = set()

    # Find all img src attributes
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

    BROKEN_REPLACEMENT = '<a href="../wiki/not_g.html" class="not_g"'
    result = []
    pos = 0

    while True:
        # Find next <a tag
        link_start = html.find("<a ", pos)
        if link_start == -1:
            # No more links, append rest of HTML
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
            # No href, keep tag as-is
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
            article_title = href[2:].split("#")[0]  # Remove A/ and any #anchor
        elif href.startswith("../A/"):
            article_title = href[5:].split("#")[0]  # Remove ../A/ and any #anchor

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
                # Keep everything after the first space in the tag
                space_pos = full_tag.find(" ", 3)  # Skip '<a '
                if space_pos != -1:
                    rest_of_tag = full_tag[space_pos:]
                    result.append(BROKEN_REPLACEMENT + rest_of_tag)
                else:
                    result.append(BROKEN_REPLACEMENT + ">")
        else:
            # Not a Wikipedia link, keep as-is
            result.append(full_tag)

        pos = tag_end + 1

    return "".join(result)


def process_articles(zim_file: Path, titles: List[str]) -> Set[str]:
    """Process articles and return set of all image filenames found."""

    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    archive = Archive(str(zim_file))

    # Convert titles to set for O(1) lookup
    valid_articles = set(titles)

    all_images = set()
    successful = 0
    redirects = 0
    errors = 0

    start_time = time.time()

    for i, title in enumerate(titles):
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
                redirects += 1

            else:
                # Regular article
                content = bytes(entry.get_item().content)
                html = content.decode("utf-8", errors="ignore")

                # Extract images
                images = extract_images_from_html(html)
                all_images.update(images)

                # Fix links
                fixed_html = fix_links_fast(html, valid_articles)

                # Write file
                output_path = TARGET_DIR / f"{title}.html"
                output_path.write_text(fixed_html, encoding="utf-8")
                successful += 1

        except Exception as e:
            errors += 1
            print(f"Error processing {title}: {e}")

        # Progress update every 1000 articles
        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print(f"Processed {i + 1:,}/{len(titles):,} articles ({rate:.1f}/sec)")

    elapsed = time.time() - start_time
    print(
        f"\nComplete: {successful:,} articles, {redirects:,} redirects, {errors:,} errors"
    )
    print(f"Time: {elapsed:.1f}s ({len(titles) / elapsed:.1f} articles/sec)")
    print(f"Found {len(all_images):,} unique images")

    return all_images


def save_image_list(images: Set[str], output_file: Path):
    """Save list of required images to file."""
    with output_file.open("w", encoding="utf-8") as f:
        for image in sorted(images):
            f.write(f"{image}\n")
    print(f"Saved image list to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Simple Wikipedia ZIM processor")
    parser.add_argument("zim_file", type=Path, help="Path to Wikipedia ZIM file")
    parser.add_argument(
        "--titles", type=Path, default=TITLES_FILE, help="File with article titles"
    )
    parser.add_argument(
        "--image-list",
        type=Path,
        default=Path("required_images.txt"),
        help="Output file for image list",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.zim_file.exists():
        print(f"Error: ZIM file not found: {args.zim_file}")
        return 1

    if not args.titles.exists():
        print(f"Error: Titles file not found: {args.titles}")
        return 1

    # Load titles
    print(f"Loading titles from {args.titles}")
    titles = load_titles(args.titles)
    print(f"Loaded {len(titles):,} titles")

    # Process articles
    print(f"Processing articles from {args.zim_file}")
    images = process_articles(args.zim_file, titles)

    # Save image list
    save_image_list(images, args.image_list)

    return 0


if __name__ == "__main__":
    exit(main())
