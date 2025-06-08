import bz2
import os
from lxml import etree
import mwparserfromhell
from tqdm import tqdm  # progress bar
import re

# --- CONFIGURATION ---
INDEX = "g.index"
DATA_FILE = "wikidump/enwiki-20250601-pages-articles-multistream.xml.bz2"
OUTPUT_DIR = "./data/"

##Test Config
# INDEX_FILE = "testinput/enwiki-20250601-pages-articles-multistream-index1.txt-p1p41242.bz2"
# DATA_FILE = "testinput/enwiki-20250601-pages-articles-multistream.xml-p1p41242.bz2"
# OUTPUT_DIR = "./data_test/"


def clean_title_for_filename(title):
    """Sanitizes a string to be a valid filename."""
    title = title.replace(" ", "_")
    return re.sub(r"[^\w\-_.]", "", title)


def parse_and_clean_wikitext(wikitext, title):
    """
    Parses wikitext using mwparserfromhell and extracts clean, readable text
    that is well-suited for RAG systems.
    """
    wikicode = mwparserfromhell.parse(wikitext)
    cleaned_text = f"Title: {title}\n\n"
    cleaned_text += (
        wikicode.strip_code().strip()
    )  # strip over gettxt to remove templates links and format
    return cleaned_text


def extract_articles():
    """
    Extracts specific articles from a Wikimedia XML dump using byte offsets.
    This method is highly efficient as it seeks directly to compressed data
    """
    print(f"Loading article index from {INDEX}...")
    offsets_to_titles = {}
    with open(INDEX, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(":", 2)
            if len(parts) == 3:
                offset, _, title = parts
                offset = int(offset)
                if offset not in offsets_to_titles:
                    offsets_to_titles[offset] = []
                offsets_to_titles[offset].append(title)

    # 2. Extract, Parse, and Save the Articles from the main dump
    print(f"Starting extraction from {DATA_FILE}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    extracted_count = 0
    total_titles = sum(len(titles) for titles in offsets_to_titles.values())

    with open(DATA_FILE, "rb") as bz2_file:
        # Use tqdm for a progress bar over the sorted list of unique offsets
        pbar = tqdm(sorted(offsets_to_titles.keys()), unit="block")
        for offset in pbar:
            target_titles = set(offsets_to_titles[offset])
            pbar.set_description(f"Seeking to offset {offset}")

            bz2_file.seek(offset)

            ## Decompresss one block at a time, with many decompressers at once
            decompressor = bz2.BZ2Decompressor()
            xml_data = decompressor.decompress(bz2_file.read())

            # The decompressed data is an XML fragment. We wrap it in a dummy
            # <root> tag so lxml can parse it as a valid XML document.
            xml_root = etree.fromstring(b"<root>" + xml_data + b"</root>")

            for page in xml_root.findall("page"):
                title = page.findtext("title")
                if title in target_titles:
                    # Ensure we only get real articles - g.index should already take care of this, but just in case
                    if page.findtext("ns") != "0":
                        continue
                    # A page can be a redirect. Skip it as it has no content. :pray: emoji. such a nuisance
                    if page.find("redirect") is not None:
                        continue

                    wikitext = page.findtext("revision/text")
                    if not wikitext:
                        continue

                    # Clean the wikitext to make it RAG-friendly
                    cleaned_text = parse_and_clean_wikitext(wikitext, title)

                    if cleaned_text:
                        filename = clean_title_for_filename(title) + ".txt"
                        filepath = os.path.join(OUTPUT_DIR, filename)
                        with open(filepath, "w", encoding="utf-8") as out_file:
                            out_file.write(cleaned_text)

                        extracted_count += 1
                        # Update progress bar description
                        pbar.set_postfix_str(
                            f"Found '{title[:30]}...', Total: {extracted_count}/{total_titles}"
                        )

    print(f"\nExtraction complete. {extracted_count} articles saved to {OUTPUT_DIR}")


# --- Main execution block ---
if __name__ == "__main__":
    extract_articles()
