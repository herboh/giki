import bz2
import json
from lxml import etree
import mwparserfromhell
from tqdm import tqdm
import concurrent.futures

# --- CONFIGURATION ---
TITLES_FILE = "g.index"
DATA_FILE = "wikidump/enwiki-20250601-pages-articles-multistream.xml.bz2"
OUTPUT_FILE = "articles.jsonl"  # Output to a single JSON Lines file

# This dictionary will be loaded once and shared with child processes (on Linux/macOS)
offsets_to_titles = {}


def parse_and_clean_wikitext(wikitext):
    """
    Parses wikitext using mwparserfromhell and extracts clean, readable text.
    It no longer prepends the title, as that is now a separate JSON field.
    """
    wikicode = mwparserfromhell.parse(wikitext)
    return wikicode.strip_code().strip()


def process_offset_block(offset):
    """
    WORKER FUNCTION: This function is executed by each process in the pool.
    It seeks to a given offset, decompresses the block, parses the articles,
    and returns a list of dictionaries for the articles found.
    """
    found_articles = []
    # Each process must open its own file handle to avoid conflicts.
    with open(DATA_FILE, "rb") as bz2_file:
        bz2_file.seek(offset)
        decompressor = bz2.BZ2Decompressor()

        xml_data = b""
        CHUNK_SIZE = 16 * 1024 * 1024  # Using a larger 16MB chunk for better I/O
        while not decompressor.eof:
            chunk = bz2_file.read(CHUNK_SIZE)
            if not chunk:
                break
            xml_data += decompressor.decompress(chunk)

        try:
            xml_root = etree.fromstring(b"<root>" + xml_data + b"</root>")
        except etree.XMLSyntaxError:
            return []  # Return an empty list if the XML block is malformed

        target_titles = offsets_to_titles.get(offset, [])
        for page in xml_root.findall("page"):
            title = page.findtext("title")
            if title in target_titles:
                if page.findtext("ns") != "0" or page.find("redirect") is not None:
                    continue

                wikitext = page.findtext("revision/text")
                page_id = page.findtext("id")  # Capture the page ID for metadata
                if not wikitext:
                    continue

                cleaned_text = parse_and_clean_wikitext(wikitext)
                if cleaned_text:
                    found_articles.append(
                        {"id": page_id, "title": title, "text": cleaned_text}
                    )
    return found_articles


def extract_articles_parallel():
    """
    Main function to orchestrate the parallel extraction.
    """
    global offsets_to_titles
    print(f"Loading article index from {TITLES_FILE}...")
    with open(TITLES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(":", 2)
            if len(parts) == 3:
                offset, _, title = parts
                offset = int(offset)
                if offset not in offsets_to_titles:
                    offsets_to_titles[offset] = []
                offsets_to_titles[offset].append(title)

    offsets = sorted(offsets_to_titles.keys())
    print(
        f"Found {len(offsets_to_titles)} articles across {len(offsets)} unique data blocks."
    )
    print("Starting parallel extraction using all available CPU cores...")

    total_extracted = 0
    # The main process will open the output file once.
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        # ProcessPoolExecutor manages a pool of worker processes.
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # executor.map applies the worker function to each offset in parallel.
            # tqdm tracks the progress as results are completed.
            future_to_offset = {
                executor.submit(process_offset_block, offset): offset
                for offset in offsets
            }
            for future in tqdm(
                concurrent.futures.as_completed(future_to_offset), total=len(offsets)
            ):
                # The result from the worker is a list of article dictionaries.
                list_of_articles = future.result()
                if list_of_articles:
                    for article_data in list_of_articles:
                        # Write each article's data as a new JSON line.
                        out_f.write(json.dumps(article_data) + "\n")
                    total_extracted += len(list_of_articles)

    print(f"\nExtraction complete. Wrote {total_extracted} articles to {OUTPUT_FILE}")


if __name__ == "__main__":
    extract_articles_parallel()
