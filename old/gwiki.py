#!/usr/bin/env python3

import argparse
import os
import re
import sys
from libzim.reader import Archive
from bs4 import BeautifulSoup


def normalize_slug(title):
    # Replace non-alphanumeric with underscore, trim
    slug = re.sub(r"[^0-9A-Za-z]+", "_", title)
    return slug.strip("_")


def fetch_asset(zim, url, outdir, queue, seen):
    """
    Fetch a single asset from ZIM by its URL (leading slash included),
    write it under outdir, and if it's CSS, queue new url(...) refs.
    """
    path = url.lstrip("/")
    outpath = os.path.join(outdir, path)
    os.makedirs(os.path.dirname(outpath), exist_ok=True)

    try:
        entry = zim.get_entry_by_path(path)
    except KeyError:
        print(f"⚠️  missing asset {url}", file=sys.stderr)
        return

    data = bytes(entry.get_item().content)
    with open(outpath, "wb") as f:
        f.write(data)

    # parse CSS for nested url(...) references
    if path.lower().endswith(".css"):
        try:
            text = data.decode("utf-8", errors="ignore")
            for m in re.findall(r"url\(['\"]?(.*?)['\"]?\)", text):
                if m.startswith("/") and m not in seen:
                    queue.add(m)
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--zim", required=True, help="Path to the ZIM file")
    parser.add_argument(
        "--prefix", default="G", help="Article title prefix (default: G)"
    )
    parser.add_argument("--outdir", default="gwiki-html", help="Output directory")
    args = parser.parse_args()

    zim = Archive(args.zim)
    prefix_pattern = re.compile(rf"^{re.escape(args.prefix)}", re.IGNORECASE)

    os.makedirs(args.outdir, exist_ok=True)

    # Iterate once over the entire ZIM index
    for entry in zim:
        item = entry.get_item()
        path = item.path  # e.g. 'wiki/Gravity'
        if not path.startswith("wiki/"):
            continue
        title = path.split("wiki/", 1)[1]
        if not prefix_pattern.match(title):
            continue

        slug = normalize_slug(title)
        article_dir = os.path.join(args.outdir, slug)
        os.makedirs(article_dir, exist_ok=True)
        print(f"👉  Exporting {title} → {article_dir}/")

        # Decode HTML
        html = bytes(item.content).decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")

        # 1) Gather initial asset URLs
        asset_urls = set()
        for tag in soup.find_all(src=True):
            asset_urls.add(tag["src"])
        for tag in soup.find_all(href=True):
            # only stylesheets or scripts
            if tag.name == "link" and tag.get("rel") and "stylesheet" in tag["rel"]:
                asset_urls.add(tag["href"])
        # inline CSS in <style> or style= attributes
        for style in soup.find_all("style"):
            if style.string:
                asset_urls.update(re.findall(r"url\(['\"]?(.*?)['\"]?\)", style.string))
        for tag in soup.find_all(style=True):
            asset_urls.update(re.findall(r"url\(['\"]?(.*?)['\"]?\)", tag["style"]))

        # Filter to internal paths (/A/, /I/, etc.)
        initial = {u for u in asset_urls if u.startswith("/")}

        # 2) Fetch recursively
        queue = set(initial)
        seen = set()
        while queue:
            url = queue.pop()
            if url in seen:
                continue
            seen.add(url)
            fetch_asset(zim, url, article_dir, queue, seen)

        # 3) Rewrite URLs in soup
        for tag in soup.find_all(src=True):
            u = tag["src"]
            if u in seen:
                tag["src"] = u.lstrip("/")
        for tag in soup.find_all(href=True):
            u = tag["href"]
            if u in seen:
                tag["href"] = u.lstrip("/")

        # 4) Write final HTML
        out_html = os.path.join(article_dir, "index.html")
        with open(out_html, "w", encoding="utf-8") as f:
            f.write(str(soup))

    print("🎉 Done! Drop the resulting folder(s) under Hugo's `static/` directory.")
