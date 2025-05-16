#!/usr/bin/env bash
set -euo pipefail

ZIM="wikipedia_en_all_maxi_2024-01.zim"
LIST="g100"
BASEOUT="gwiki-html"

# helper: fetch a ZIM entry by URL, following redirects
fetch_entry() {
  local url="$1" out="$2"
  if zimdump show --url="$url" "$ZIM" >"$out" 2>/dev/null; then
    return 0
  fi

  # probably a redirect—lookup its redirect index
  local ridx
  ridx=$(zimdump list --url="$url" --details "$ZIM" |
    awk '/redirect index:/ { print $NF }')
  if [[ -n "$ridx" ]]; then
    echo "📦  $url → redirect → entry #$ridx"
    zimdump show --idx="$ridx" "$ZIM" >"$out"
    return $?
  fi

  return 1
}

mkdir -p "$BASEOUT"

while IFS= read -r title; do
  [[ -z "$title" || "$title" == \#* ]] && continue
  slug=${title// /_}
  slug=${slug//:/-}
  outdir="${BASEOUT}/${slug}"
  echo "👉  Processing $title → $outdir/"

  mkdir -p "$outdir"
  # 1) dump HTML (follows redirects)
  if ! fetch_entry "$title" "$outdir/index.html"; then
    echo "❌  could not fetch $title, skipping"
    continue
  fi

  # 2) extract only internal asset URLs
  {
    # images
    grep -Eo '<img[^>]*src="[^"]+"' "$outdir/index.html" |
      sed -E 's/.*src="([^"]+)".*/\1/'
    # CSS
    grep -Eo '<link[^>]*rel="stylesheet"[^>]*href="[^"]+"' "$outdir/index.html" |
      sed -E 's/.*href="([^"]+)".*/\1/'
    # scripts
    grep -Eo '<script[^>]*src="[^"]+"' "$outdir/index.html" |
      sed -E 's/.*src="([^"]+)".*/\1/'
  } |
    sed 's|^/||' |
    grep -E '^(A/|I/)' |
    sort -u >"$outdir/asset_urls.txt"

  # 3) fetch each asset
  while IFS= read -r asset; do
    asset_path="$outdir/$asset"
    mkdir -p "$(dirname "$asset_path")"
    if ! fetch_entry "$asset" "$asset_path"; then
      echo "⚠️  missing asset $asset"
    fi
  done <"$outdir/asset_urls.txt"

  rm "$outdir/asset_urls.txt"
done <"$LIST"

echo "🎉  Done! Mini-wiki is in ./${BASEOUT}/"
