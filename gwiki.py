import os
import shutil
from bs4 import BeautifulSoup

# --- Configuration ---
ZIM_DUMP_OUTPUT_DIR = (
    "gwiki/"  # Your input directory (e.g., contains 'A', 'I', '-', 'gtitles')
)

OUTPUT_DIR = "gwiki_output_big"  # Script will create this
TARGET_WIKI_SUBDIR = "wiki"  # Content will go into OUTPUT_DIR/TARGET_WIKI_SUBDIR

# e.g., gwiki_output/wiki/not_g.html
NOT_AVAILABLE_PAGE_FILENAME = "not_g.html"
BROKEN_LINK_INLINE_STYLE_COLOR = "#BF3C2C"  # Wikipedia's typical red link color


def main():
    if not os.path.isdir(ZIM_DUMP_OUTPUT_DIR) or not os.path.exists(
        os.path.join(ZIM_DUMP_OUTPUT_DIR, "gtitles")
    ):
        print(
            f"ERROR: ZIM_DUMP_OUTPUT_DIR '{ZIM_DUMP_OUTPUT_DIR}' is not a valid directory or 'gtitles' is missing."
        )
        print(
            "Please ensure it points to the extracted ZIM dump folder containing 'A', 'I', '-', 'gtitles'."
        )
        return

    base_output_path = os.path.join(OUTPUT_DIR, TARGET_WIKI_SUBDIR)
    articles_output_path = os.path.join(base_output_path, "A")
    images_output_path = os.path.join(base_output_path, "I")
    styles_output_path = os.path.join(
        base_output_path, "-"
    )  # Wikipedia styles often in '-'

    if os.path.exists(OUTPUT_DIR):
        print(f"Output directory '{OUTPUT_DIR}' already exists. Removing it.")
        shutil.rmtree(OUTPUT_DIR)

    os.makedirs(articles_output_path, exist_ok=True)
    os.makedirs(images_output_path, exist_ok=True)
    os.makedirs(styles_output_path, exist_ok=True)

    print(f"Output structure will be created in: {base_output_path}")
    print(
        f"IMPORTANT: You will need to manually create the '{NOT_AVAILABLE_PAGE_FILENAME}' page"
    )
    print(
        f"and place it at: {os.path.join(base_output_path, NOT_AVAILABLE_PAGE_FILENAME)}"
    )

    # Relative path to the "not available" page from an article located in "A/" directory
    # e.g., if article is A/MyGArticle.html, path to not_g.html (at wiki/not_g.html) is ../not_g.html
    relative_path_to_not_available_page = f"../{NOT_AVAILABLE_PAGE_FILENAME}"

    # 3. Identify "G" articles from gtitles
    gtitles_filepath = os.path.join(ZIM_DUMP_OUTPUT_DIR, "gtitles")
    all_article_gtitles_entries = []
    try:
        with open(gtitles_filepath, "r", encoding="utf-8") as f:
            all_article_gtitles_entries = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"ERROR: gtitles file not found at '{gtitles_filepath}'.")
        return
    except Exception as e:
        print(f"Error reading gtitles file: {e}")
        return

    if not all_article_gtitles_entries:
        print("No articles found in gtitles. Exiting.")
        return

    g_article_basenames = set()  # Store basenames like "Galaxy.html"
    g_article_source_file_paths = []  # Store full paths to source files

    print(f"\nFiltering articles from '{gtitles_filepath}'...")
    for article_basename_from_gtitles in all_article_gtitles_entries:
        # article_basename_from_gtitles is now directly the filename, e.g., "Galaxy.html"

        # Ensure it's not an empty line or some other unexpected entry
        if not article_basename_from_gtitles or "/" in article_basename_from_gtitles:
            # print(f"  Skipping potentially invalid gtitles entry: '{article_basename_from_gtitles}'")
            continue

        current_article_basename = article_basename_from_gtitles

        if current_article_basename.lower().startswith("g"):
            # Construct the full path to the source HTML file, which is inside the 'A' subdirectory
            source_html_file = os.path.join(
                ZIM_DUMP_OUTPUT_DIR, "A", current_article_basename
            )

            if os.path.exists(source_html_file):
                g_article_basenames.add(
                    current_article_basename
                )  # Store the actual filename
                g_article_source_file_paths.append(source_html_file)
            else:
                # If gtitles has "ArticleName" but file is "ArticleName.html"
                if not current_article_basename.endswith(".html"):
                    potential_filename_with_ext = current_article_basename + ".html"
                    source_html_file_with_ext = os.path.join(
                        ZIM_DUMP_OUTPUT_DIR, "A", potential_filename_with_ext
                    )
                    if os.path.exists(source_html_file_with_ext):
                        g_article_basenames.add(
                            potential_filename_with_ext
                        )  # Store the .html version
                        g_article_source_file_paths.append(source_html_file_with_ext)
                    else:
                        print(
                            f"  Warning: Article file for '{current_article_basename}' (also tried '{potential_filename_with_ext}') not found in '{os.path.join(ZIM_DUMP_OUTPUT_DIR, 'A')}'."
                        )
                else:
                    print(
                        f"  Warning: Article file '{current_article_basename}' listed in gtitles not found at '{source_html_file}'."
                    )

    if not g_article_basenames:
        print("No articles starting with 'G' found. Exiting.")
        return
    print(f"Identified {len(g_article_basenames)} articles starting with 'G'.")

    # 4. Copy and Process "G" Articles
    print(
        f"\nCopying and processing {len(g_article_source_file_paths)} 'G' articles to '{articles_output_path}'..."
    )
    processed_articles_count = 0
    for source_article_filepath in g_article_source_file_paths:
        article_filename = os.path.basename(source_article_filepath)

        if not article_filename.endswith(
            (".html", ".htm")
        ):  # Check if it already has an html-like extension
            article_filename_html = article_filename + ".html"
        else:
            article_filename_html = article_filename
        dest_article_filepath = os.path.join(
            articles_output_path, article_filename_html
        )

        try:
            shutil.copy2(source_article_filepath, dest_article_filepath)

            with open(dest_article_filepath, "r+", encoding="utf-8") as f:
                content = f.read()
                # Only try to parse and modify if it's likely HTML (basic check)
                if not content.strip().lower().startswith(
                    "<!doctype html"
                ) and not content.strip().lower().startswith("<html"):
                    # print(f"  Skipping link processing for non-standard HTML start: {article_filename}")
                    processed_articles_count += 1
                    continue  # Skip further processing for this file if not clearly HTML

                soup = BeautifulSoup(content, "lxml")

                links_modified_count = 0
                # Process <a> links
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    original_href = href

                    if (
                        href.startswith(
                            ("http:", "https:", "ftp:", "mailto:", "//", "#")
                        )
                        or href.startswith(("../I/", "/I/", "I/", "../-/", "/-/", "-/"))
                        or href.lower().endswith(
                            (
                                ".png",
                                ".jpg",
                                ".jpeg",
                                ".gif",
                                ".svg",
                                ".css",
                                ".js",
                                ".ico",
                            )
                        )
                    ):
                        continue

                    href_check_part = href.split("#")[0].split("?")[0]

                    if "/" in href_check_part:
                        continue

                    target_article_basename = href_check_part
                    if target_article_basename not in g_article_basenames:
                        a_tag["href"] = relative_path_to_not_available_page
                        links_modified_count += 1

                        # Apply inline style for the color
                        our_color_style_declaration = (
                            f"color: {BROKEN_LINK_INLINE_STYLE_COLOR}"
                        )

                        existing_style_str = a_tag.get("style", "")

                        # Parse existing styles: split by ';', filter out existing 'color:'
                        style_properties = [
                            prop.strip()
                            for prop in existing_style_str.split(";")
                            if prop.strip()
                        ]
                        other_styles = [
                            prop
                            for prop in style_properties
                            if not prop.lower().startswith("color:")
                        ]

                        # Add our new color style
                        final_styles_list = other_styles + [our_color_style_declaration]

                        # Join them back, ensuring semicolons correctly
                        new_style_str = "; ".join(s for s in final_styles_list if s)
                        if (
                            new_style_str
                        ):  # Add trailing semicolon only if there's content
                            new_style_str += ";"

                        a_tag["style"] = new_style_str
                # Process <meta http-equiv="refresh"> redirects (Optional Enhancement)
                # for meta_tag in soup.find_all("meta", attrs={"http-equiv": "refresh"}):
                #    content_attr = meta_tag.get("content")
                #    if content_attr:
                #        parts = content_attr.split("url=")
                #        if len(parts) > 1:
                #            redirect_url = parts[1].strip()
                #            redirect_target_basename = redirect_url.split("#")[0].split("?")[0]
                #            if "/" not in redirect_target_basename and redirect_target_basename not in g_article_basenames:
                #                meta_tag["content"] = f"{parts[0]}url={relative_path_to_not_available_page}"
                #                links_modified_count += 1 # Or a different counter

                if links_modified_count > 0:
                    f.seek(0)
                    f.write(str(soup))
                    f.truncate()

            processed_articles_count += 1
            if processed_articles_count % 100 == 0 or processed_articles_count == len(
                g_article_source_file_paths
            ):
                print(
                    f"  Processed {processed_articles_count}/{len(g_article_source_file_paths)} articles..."
                )

        except FileNotFoundError:
            print(
                f"  ERROR: Source file not found during copy: {source_article_filepath}"
            )
        except Exception as e:
            print(
                f"  ERROR processing file {article_filename} (from {source_article_filepath}): {e}"
            )

    print(f"Finished processing {processed_articles_count} articles.")

    # 5. Copy Styles directory (-) and Images directory (I)
    print("\nCopying styles and images...")

    source_styles_dir = os.path.join(ZIM_DUMP_OUTPUT_DIR, "-")
    if os.path.exists(source_styles_dir):
        print(f"Copying styles from '{source_styles_dir}' to '{styles_output_path}'")
        shutil.copytree(source_styles_dir, styles_output_path, dirs_exist_ok=True)
    else:
        print(
            f"Warning: Styles directory '-' not found at '{source_styles_dir}'. Styling might be incomplete."
        )

    source_images_dir = os.path.join(ZIM_DUMP_OUTPUT_DIR, "I")
    if os.path.exists(source_images_dir):
        print(f"Copying images from '{source_images_dir}' to '{images_output_path}'")
        shutil.copytree(source_images_dir, images_output_path, dirs_exist_ok=True)
    else:
        print(
            f"Warning: Images directory 'I' not found at '{source_images_dir}'. Images will be missing."
        )

    print(f"\n--- Processing Complete ---")
    print(f"Subset Wikipedia for 'G' articles is located in: {base_output_path}")
    print(
        f"This entire '{TARGET_WIKI_SUBDIR}' directory (inside '{OUTPUT_DIR}') can now be copied into your HUGO 'static' directory."
    )
    print(f'  Example: cp -r "{base_output_path}" "/path/to/your/hugo_project/static/"')
    print(
        f"Make sure you manually create '{NOT_AVAILABLE_PAGE_FILENAME}' at '{os.path.join(base_output_path, NOT_AVAILABLE_PAGE_FILENAME)}'"
    )
    print(
        f"Access point would be your_site.com/{TARGET_WIKI_SUBDIR}/A/Article_Name.html"
    )
    print(
        f"Non-'G' article links should point to: your_site.com/{TARGET_WIKI_SUBDIR}/{NOT_AVAILABLE_PAGE_FILENAME}"
    )


if __name__ == "__main__":
    main()
