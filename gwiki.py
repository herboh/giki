import os
import re
from pathlib import Path
from bs4 import BeautifulSoup, Comment
import logging
from tqdm import tqdm

# --- Configuration ---
SOURCE_ARTICLES_DIR = Path("/home/chan/code/wiki/gwiki/A/")
TARGET_CONTENT_A_DIR = Path("/home/chan/code/git/blog/wiki/content/A/")

IMAGE_SRC_REGEX = re.compile(
    r"""^(?:\./|\.\./)*/?I/(?P<image_path>[^?#]+)""", re.VERBOSE
)
IMAGE_NEW_BASE_URL = "/wiki/I/" # Assumes images will be at /static/wiki/I/ in Hugo project

BROKEN_LINK_HREF = "../../not_g.html" # Relative from a page in content/A/
BROKEN_LINK_STYLE = "color: #BF3C2C;"
PLACEHOLDER_DATE = "2025-01-01T00:00:00Z"

# Regex to find <meta http-equiv="refresh" content="0;url=TARGET_URL">
# It captures the TARGET_URL part.
REDIRECT_META_REGEX = re.compile(
    r"""<meta\s+http-equiv=["']refresh["']\s+content=["']\d+;\s*url=([^"']+)["']""",
    re.IGNORECASE
)
# Max bytes to read for redirect detection to keep it fast
REDIRECT_SNIFF_BYTES = 1024
# Max file size for a file to be considered a redirect candidate (can be generous)
REDIRECT_MAX_FILE_SIZE_BYTES = 2048
# --- End Configuration ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("wiki_processing_final_opt.log", mode='w'),
        logging.StreamHandler(),
    ],
)

def get_g_article_basenames(source_dir: Path) -> set[str]:
    g_articles = set()
    if not source_dir.is_dir():
        logging.error(f"Source directory {source_dir} does not exist.")
        return g_articles
    all_items = []
    try:
        all_items = list(source_dir.iterdir())
    except OSError as e:
        logging.error(f"Could not read source directory {source_dir}: {e}")
        return g_articles
    logging.info(f"Scanning {len(all_items)} items in {source_dir} to find 'G' articles...")
    for item in tqdm(all_items, desc="Scanning source", unit="item"):
        if item.is_file() and item.name.startswith("G"):
            g_articles.add(item.name)
    logging.info(f"Found {len(g_articles)} articles starting with 'G'.")
    return g_articles

def derive_title_from_filename(basename: str) -> str:
    return basename.replace("_", " ").strip()

def generate_hugo_redirect_content(
    redirect_filename_basename: str, # e.g., "Ga_Tech" (no .html)
    original_target_url: str,      # e.g., "Georgia_Tech" or "SomePage.html"
    g_article_basenames: set[str]
) -> tuple[str, str]:
    """
    Generates front matter and a simple HTML body for a redirect page.
    The meta refresh URL is updated to point to a valid G-article or the broken link page.
    """
    page_title = derive_title_from_filename(redirect_filename_basename)

    # Determine the final target for the meta refresh and link
    target_url_basename = Path(original_target_url).name
    if target_url_basename.lower().endswith(".html"):
        target_url_basename = target_url_basename[:-5]
    elif target_url_basename.lower().endswith(".htm"):
        target_url_basename = target_url_basename[:-4]

    final_redirect_target_for_hugo = ""
    target_is_g_article = False
    if target_url_basename in g_article_basenames:
        final_redirect_target_for_hugo = f"{target_url_basename}.html" # Assumes target is in same /A/ directory
        target_is_g_article = True
    else:
        final_redirect_target_for_hugo = BROKEN_LINK_HREF # e.g., ../../not_g.html

    # Hugo Aliases: the path *this redirect file* should be known by if accessed directly.
    # This is less about where the meta-refresh goes, and more about search engine hints
    # or if someone links to this redirect page by its old name.
    # For simplicity, we might not need aliases if the meta-refresh is preserved and corrected.
    # If Hugo itself should handle the redirect via its alias system (ignoring meta refresh):
    # aliases_str_part = f"aliases:\n  - \"/wiki/A/{redirect_filename_basename}.html\" # Or its original kiwix path if different
    # redirect_to_str_part = f"redirectTo: \"{final_redirect_target_for_hugo}\"" # Hugo internal redirect
    # For now, let's focus on fixing the meta refresh and providing a simple body.

    front_matter = f"""---
title: "{page_title.replace('"', '\\"')}"
date: {PLACEHOLDER_DATE}
sitemap:
  priority: 0.1 # Lower priority for redirects
outputs: ["html"]
layout: "redirect" # Optional: Suggests using a redirect layout in Hugo
meta_refresh_target: "{final_redirect_target_for_hugo}"
target_is_g_article: {str(target_is_g_article).lower()}
---

"""
    # Preserve the original meta refresh logic but with the corrected URL for Hugo context
    # Ensure proper quoting for the URL in the content attribute
    safe_final_redirect_target = final_redirect_target_for_hugo.replace('"', '&quot;')
    
    html_body = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta http-equiv="refresh" content="0;url={safe_final_redirect_target}" />
  <title>Redirecting...</title>
  <link rel="canonical" href="{safe_final_redirect_target}" />
</head>
<body>
  <p>
    Redirecting to <a href="{safe_final_redirect_target}">{derive_title_from_filename(target_url_basename if target_is_g_article else "page")}</a>...
  </p>
</body>
</html>
"""
    return front_matter, html_body


def process_main_article_content(
    html_content: str, article_basename: str, g_article_basenames: set[str]
) -> tuple[str | None, str | None]:
    try:
        soup = BeautifulSoup(html_content, "lxml")
    except Exception as e_lxml:
        logging.warning(f"lxml parsing failed for {article_basename}: {e_lxml}. Trying html.parser.")
        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except Exception as e_htmlp:
            logging.error(f"html.parser also failed for {article_basename}: {e_htmlp}")
            return None, None

    title_tag = soup.find("title")
    page_title = derive_title_from_filename(article_basename) # Default to filename
    if title_tag and title_tag.string:
        page_title_candidate = title_tag.string.strip()
        if page_title_candidate: # Ensure title string is not empty
             page_title = page_title_candidate
    if title_tag:
        title_tag.decompose()

    node_to_process = soup.body
    if not node_to_process:
        logging.debug(f"No <body> tag in {article_basename}. Processing entire structure.")
        node_to_process = soup

    for img_tag in node_to_process.find_all("img", src=True):
        original_src = img_tag.get("src", "")
        match = IMAGE_SRC_REGEX.match(original_src)
        if match:
            image_file_path = match.group("image_path")
            img_tag["src"] = f"{IMAGE_NEW_BASE_URL.rstrip('/')}/{image_file_path}"

    for a_tag in node_to_process.find_all("a", href=True):
        original_href = a_tag.get("href", "")
        if not original_href or original_href.startswith(("#", "mailto:", "tel:", "ftp:")):
            continue
        href_lower = original_href.lower()
        if href_lower.startswith(("http:", "https:")) or "//" in original_href.split(":", 1)[0]:
            continue
        if any(asset_path in href_lower for asset_path in ["/i/", "/-/common/", "/-/skins/"]) or \
           href_lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
                                ".css", ".js", ".woff", ".woff2", ".ttf", ".otf",
                                ".zip", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")):
            continue

        href_parts = original_href.split("#", 1)
        href_base = href_parts[0]
        href_fragment = f"#{href_parts[1]}" if len(href_parts) > 1 else ""
        
        target_basename_for_check = Path(href_base).name
        if target_basename_for_check.lower().endswith(".html"):
            target_basename_for_check = target_basename_for_check[:-5]
        elif target_basename_for_check.lower().endswith(".htm"):
            target_basename_for_check = target_basename_for_check[:-4]

        if target_basename_for_check in g_article_basenames:
            a_tag["href"] = f"{target_basename_for_check}.html{href_fragment}"
        else:
            a_tag["href"] = BROKEN_LINK_HREF
            current_style = a_tag.get("style", "")
            if BROKEN_LINK_STYLE not in current_style:
                a_tag["style"] = f"{current_style.rstrip(';')}; {BROKEN_LINK_STYLE}".lstrip("; ")

    for comment in node_to_process.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    front_matter = f"""---
title: "{page_title.replace('"', '\\"')}"
date: {PLACEHOLDER_DATE}
outputs: ["html"]
---

"""
    processed_html_string = node_to_process.decode_contents() if soup.body == node_to_process else str(node_to_process)
    return front_matter, processed_html_string

def main():
    logging.info("Starting Wikipedia ZIM dump preprocessing for Hugo (Redirects Optimized).")
    TARGET_CONTENT_A_DIR.mkdir(parents=True, exist_ok=True)
    g_article_basenames = get_g_article_basenames(SOURCE_ARTICLES_DIR)
    if not g_article_basenames:
        logging.warning("No articles starting with 'G' found.")
        return

    processed_main_count = 0
    processed_redirect_count = 0
    error_count = 0
    skipped_non_html_like_count = 0

    for article_basename_with_ext_or_not in tqdm(g_article_basenames, desc="Processing 'G' articles", unit="article"):
        # Note: g_article_basenames stores names *without* .html, as per original script
        # This means article_basename_with_ext_or_not is like "Galaxy", not "Galaxy.html"
        source_filepath = SOURCE_ARTICLES_DIR / article_basename_with_ext_or_not
        
        # Hugo output filename will always be .html
        target_filename_hugo = f"{article_basename_with_ext_or_not}.html"
        target_filepath = TARGET_CONTENT_A_DIR / target_filename_hugo

        try:
            file_size = source_filepath.stat().st_size
            is_redirect_candidate = file_size < REDIRECT_MAX_FILE_SIZE_BYTES

            html_snippet_for_redirect_check = ""
            if is_redirect_candidate:
                with open(source_filepath, "r", encoding="utf-8", errors='ignore') as f:
                    html_snippet_for_redirect_check = f.read(REDIRECT_SNIFF_BYTES)
                
                redirect_match = REDIRECT_META_REGEX.search(html_snippet_for_redirect_check)
                if redirect_match:
                    original_target_url = redirect_match.group(1)
                    front_matter, html_body = generate_hugo_redirect_content(
                        article_basename_with_ext_or_not, original_target_url, g_article_basenames
                    )
                    with open(target_filepath, "w", encoding="utf-8") as f_out:
                        f_out.write(front_matter)
                        f_out.write(html_body)
                    processed_redirect_count += 1
                    continue # Move to next article

            # If not a redirect or redirect check failed, process as main article
            with open(source_filepath, "r", encoding="utf-8", errors='ignore') as f:
                full_html_content = f.read()

            stripped_content_start = full_html_content.lstrip()[:20].lower()
            if not (stripped_content_start.startswith("<!doctype html") or \
                    stripped_content_start.startswith("<html")):
                logging.warning(
                    f"Skipping main article processing for {article_basename_with_ext_or_not}: Does not appear to be standard HTML (starts with: '{full_html_content[:60].replace R('\n',' ').strip()}...')."
                )
                skipped_non_html_like_count +=1
                continue

            front_matter, processed_html_body = process_main_article_content(
                full_html_content, article_basename_with_ext_or_not, g_article_basenames
            )

            if front_matter is None or processed_html_body is None:
                logging.error(f"Skipping {article_basename_with_ext_or_not} due to error in process_main_article_content.")
                error_count += 1
                continue

            with open(target_filepath, "w", encoding="utf-8") as f_out:
                f_out.write(front_matter)
                f_out.write(processed_html_body)
            processed_main_count += 1

        except FileNotFoundError:
            logging.error(f"Source file not found: {source_filepath}")
            error_count += 1
        except IOError as e:
            logging.error(f"IOError for {source_filepath}: {e}")
            error_count += 1
        except Exception as e:
            logging.error(f"Unexpected error processing {article_basename_with_ext_or_not} (from {source_filepath}): {e}", exc_info=True)
            error_count += 1

    logging.info("--- Processing Complete ---")
    logging.info(f"Successfully processed MAIN articles: {processed_main_count}")
    logging.info(f"Successfully processed REDIRECT articles: {processed_redirect_count}")
    logging.info(f"Skipped (non-standard HTML start for main articles): {skipped_non_html_like_count}")
    logging.info(f"Failed or errored: {error_count}")
    logging.info(f"Output in: {TARGET_CONTENT_A_DIR}")
    logging.info(f"Log file: 'wiki_processing_final_opt.log'")

if __name__ == "__main__":
    main()
