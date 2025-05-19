import os
import re
from pathlib import Path
from bs4 import BeautifulSoup, Comment
import logging

# --- Configuration ---
SOURCE_ARTICLES_DIR = Path("/home/chan/code/wiki/gwiki/A/")

TARGET_CONTENT_A_DIR = Path("/home/chan/code/git/blog/wiki/content/A/")

IMAGE_SRC_PATTERN = re.compile(
    r"""           # start of string ^
        (?:\./|\.\./)*  # any ./ or ../ segments
        /?I/            # the I directory with optional leading slash
        (?P<path>[^?#]+) # capture the rest of the path up to ? or #
        (?P<tail>[?#].*)?$   # query-string or fragment if present
    """,
    re.VERBOSE,
)

# if you copy images to static/wiki/I/ the URL on the live site will be /wiki/I/<file>
IMAGE_NEW_BASE_URL = "/wiki/I/"

BROKEN_LINK_HREF = "../../not_g.html"
BROKEN_LINK_STYLE = "color: #BF3C2C;"  # Inline style for broken links

PLACEHOLDER_DATE = "2025-01-01T00:00:00Z"

# --- End Configuration ---

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("wiki_processing.log"),  # Log file
        logging.StreamHandler(),  # Log to console
    ],
)


def get_g_article_basenames(source_dir: Path) -> set[str]:
    """
    Scans the source directory and returns a set of basenames (filenames without extension)
    for articles that start with the letter 'G'.
    """
    g_articles = set()
    if not source_dir.is_dir():
        logging.error(f"Source directory {source_dir} does not exist.")
        return g_articles

    for item in source_dir.iterdir():
        if item.is_file() and item.name.startswith("G"):
            g_articles.add(item.name)  # item.name is the basename like "Galaxy"
    logging.info(f"Found {len(g_articles)} articles starting with 'G' in {source_dir}")
    return g_articles


def derive_title_from_filename(basename: str) -> str:
    """
    Converts a filename-like string to a more readable title.
    Example: "Gaussian_Distribution" -> "Gaussian Distribution"
    """
    return basename.replace("_", " ").strip()


def process_article_content(
    html_content: str, article_basename: str, g_article_basenames: set[str]
) -> tuple[str | None, str | None]:
    """
    Processes the HTML content of a single article:
    - Extracts or derives the title.
    - Rewrites image paths.
    - Rewrites internal links.
    - Generates Hugo front matter.

    Returns a tuple: (front_matter_string, processed_html_body_string).
    Returns (None, None) if processing fails.
    """
    try:
        soup = BeautifulSoup(html_content, "html.parser")
    except Exception as e:
        logging.error(f"BeautifulSoup parsing failed for {article_basename}: {e}")
        return None, None

    # 1. Extract title
    title_tag = soup.find("title")
    page_title = ""
    if title_tag and title_tag.string:
        page_title = title_tag.string.strip()
    else:
        page_title = derive_title_from_filename(article_basename)
        logging.warning(
            f"No <title> tag found for {article_basename}. Derived title: '{page_title}'"
        )

    # Remove existing title tag as Hugo will generate it from front matter
    if title_tag:
        title_tag.decompose()

    body_content_html = ""
    if soup.body:
        body_content_html = soup.body.decode_contents()  # Get content of body
        soup = BeautifulSoup(
            body_content_html, "html.parser"
        )  # Re-parse to work on body content
    else:
        # If no body tag, maybe it's a fragment. Use the whole parsed content.
        # This might need adjustment based on ZIM export specifics.
        logging.warning(
            f"No <body> tag found in {article_basename}. Processing entire HTML content."
        )

    # 2. Rewrite image paths
    for img_tag in soup.find_all("img"):
        original_src = img_tag.get("src")
        if original_src:
            match = re.match(r"^(?:\.\./I/|/I/)(.*)", original_src)
            if match:
                image_filename = match.group(1)
                img_tag["src"] = f"{IMAGE_NEW_BASE_URL.rstrip('/')}/{image_filename}"
            else:
                logging.warning(
                    f"Image src '{original_src}' in {article_basename} does not match expected pattern (../I/file or /I/file). Skipping."
                )

    # 3. Rewrite internal links (<a> tags)
    for a_tag in soup.find_all("a", href=True):
        original_href = a_tag["href"]

        href_parts = original_href.split("#", 1)
        href_base = href_parts[0]
        href_fragment = f"#{href_parts[1]}" if len(href_parts) > 1 else ""

        target_basename_for_check = href_base
        if target_basename_for_check.lower().endswith(".html"):
            target_basename_for_check = target_basename_for_check[:-5]

        if target_basename_for_check in g_article_basenames:
            # Link to a "G" article, ensure it ends with .html for Hugo file resolution
            a_tag["href"] = f"{target_basename_for_check}.html{href_fragment}"
        else:
            # Link to a non-"G" article (broken link in our context)
            a_tag["href"] = BROKEN_LINK_HREF
            # Add or update style
            current_style = a_tag.get("style", "")
            if BROKEN_LINK_STYLE not in current_style:  # Avoid duplicate styles
                a_tag["style"] = (
                    f"{current_style.rstrip(';')}; {BROKEN_LINK_STYLE}".lstrip("; ")
                )

    # 4. Create Hugo front matter
    front_matter = f"""---
title: "{page_title.replace('"', '\\"')}"
date: {PLACEHOLDER_DATE}
outputs: ["html"]
---

"""
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    return front_matter, str(soup)


def main():
    """Main processing function."""
    logging.info("Starting Wikipedia ZIM dump preprocessing for Hugo.")

    # 1. Ensure target directory exists
    try:
        TARGET_CONTENT_A_DIR.mkdir(parents=True, exist_ok=True)
        logging.info(f"Ensured target directory exists: {TARGET_CONTENT_A_DIR}")
    except OSError as e:
        logging.error(f"Could not create target directory {TARGET_CONTENT_A_DIR}: {e}")
        return

    # 2. Identify all "G" articles from the source directory
    g_article_basenames = get_g_article_basenames(SOURCE_ARTICLES_DIR)
    if not g_article_basenames:
        logging.warning("No articles starting with 'G' found to process.")
        return

    processed_count = 0
    error_count = 0

    # 3. Process each "G" article
    for article_basename in g_article_basenames:
        source_filepath = SOURCE_ARTICLES_DIR / article_basename
        # Hugo expects content files with extensions, e.g., .html or .md
        target_filename = f"{article_basename}.html"
        target_filepath = TARGET_CONTENT_A_DIR / target_filename

        logging.info(f"Processing: {article_basename} -> {target_filepath}")

        try:
            # Read source article content
            with open(source_filepath, "r", encoding="utf-8") as f:
                html_content = f.read()

            # Process the content
            front_matter, processed_html_body = process_article_content(
                html_content, article_basename, g_article_basenames
            )

            if front_matter is None or processed_html_body is None:
                logging.error(
                    f"Skipping article {article_basename} due to processing error."
                )
                error_count += 1
                continue

            # Write the new file with front matter and processed HTML
            with open(target_filepath, "w", encoding="utf-8") as f:
                f.write(front_matter)
                f.write(processed_html_body)

            processed_count += 1

        except FileNotFoundError:
            logging.error(f"Source file not found: {source_filepath}")
            error_count += 1
        except IOError as e:
            logging.error(f"IOError processing file {source_filepath}: {e}")
            error_count += 1
        except Exception as e:
            logging.error(
                f"An unexpected error occurred while processing {article_basename}: {e}"
            )
            error_count += 1

    logging.info("--- Processing Complete ---")
    logging.info(f"Successfully processed: {processed_count} articles.")
    logging.info(f"Failed/skipped articles: {error_count} articles.")
    logging.info(f"Processed articles are in: {TARGET_CONTENT_A_DIR}")
    logging.info(
        f"A log file 'wiki_processing.log' has been created in the script's directory."
    )


if __name__ == "__main__":
    main()
