#!/usr/bin/env python3
import argparse
import os
import re
from libzim.reader import Archive
from bs4 import BeautifulSoup


def sanitize_slug(title: str) -> str:
    """Turn ‘Foo Bar/Baz’ → ‘Foo_Bar_Baz’ for safe folder names."""
    slug = re.sub(r"[^0-9A-Za-z]+", "_", title)
    return slug.strip("_")


def collect_assets(soup: BeautifulSoup) -> set[str]:
    """Find all internal URLs in src, href, and inline CSS url()."""
    assets = set()
    # tags with src/href
    for tag, attr in (("img", "src"), ("script", "src"), ("link", "href")):
        for el in soup.find_all(tag):
            url = el.get(attr, "")
            if url.startswith("/"):
                assets.add(url)
    # inline CSS in <style> blocks and style=""
    css_text = ""
    for st in soup.find_all("style"):
        if st.string:
            css_text += st.string
    for el in soup.find_all(style=True):
        css_text += el["style"]
    for m in re.findall(r'url\((?:["\']?)(/[^)"\']+)', css_text):
        assets.add(m)
    return assets


def rewrite_urls(soup: BeautifulSoup):
    """Convert all leading-slash asset URLs → no-slash (local files)."""

    def fix_attr(el, attr):
        val = el.get(attr, "")
        if val.startswith("/"):
            el[attr] = val.lstrip("/")

    for tag, attr in (("img", "src"), ("script", "src"), ("link", "href")):
        for el in soup.find_all(tag):
            fix_attr(el, attr)

    # inline CSS
    def repl(m):
        url = m.group(1)
        if url.startswith("/"):
            return f"url({url.lstrip('/')})"
        return m.group(0)

    for st in soup.find_all("style"):
        if st.string:
            st.string = re.sub(r'url\((?:["\']?)(/[^)"\']+)', repl, st.string)
    for el in soup.find_all(style=True):
        el["style"] = re.sub(r'url\((?:["\']?)(/[^)"\']+)', repl, el["style"])


def main():
    p = argparse.ArgumentParser(
        description="Extract ZIM ‘G…’ articles → static HTML+assets"
    )
    p.add_argument("--zim", required=True, help="Path to .zim file")
    p.add_argument("--prefix", required=True, help="Article‐title prefix, e.g. ‘G’")
    p.add_argument("--outdir", required=True, help="Output base directory")
    args = p.parse_args()

    zim = Archive.open(args.zim)
    # URL prefix we want to scan:
    prefix_url = f"/wiki/{args.prefix}"
    os.makedirs(args.outdir, exist_ok=True)

    for entry in zim.list_entries_by_url(prefix_url):
        # only main‐namespace (articles)
        if entry.namespace != 0:
            continue
        url = entry.url
        if not url.startswith(prefix_url):
            continue

        title = url.split("/wiki/", 1)[1]
        slug = sanitize_slug(title)
        artdir = os.path.join(args.outdir, slug)
        os.makedirs(artdir, exist_ok=True)

        # --- fetch the HTML page ---
        raw = zim.get_entry_data(entry)  # bytes
        html = raw.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")

        # --- collect all assets referenced by the page ---
        assets = collect_assets(soup)

        # --- fetch each asset, and if it's CSS, harvest its url()s too ---
        for asset_url in list(assets):
            try:
                a_entry = zim.get_entry_by_url(asset_url)
            except AttributeError:
                # some versions call it list/get differently:
                a_entry = zim.get_entry(asset_url)
            if not a_entry:
                print(f"⚠️  missing asset {asset_url}")
                continue

            data = zim.get_entry_data(a_entry)
            relpath = asset_url.lstrip("/")
            dst = os.path.join(artdir, relpath)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            with open(dst, "wb") as f:
                f.write(data)

            # if CSS, scan for more url() references inside it
            if relpath.lower().endswith(".css"):
                text = data.decode("utf-8", errors="replace")
                for m in re.findall(r'url\((?:["\']?)(/[^)"\']+)', text):
                    assets.add(m)

        # --- rewrite all page URLs to local files ---
        rewrite_urls(soup)

        # --- write out final index.html ---
        idx = os.path.join(artdir, "index.html")
        with open(idx, "w", encoding="utf-8") as f:
            f.write(str(soup))

        print(f"✅  {title} → {artdir}/")


if __name__ == "__main__":
    main()
