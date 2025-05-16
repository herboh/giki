#!/bin/bash
while IFS= read -r title; do
  zimdump dump \
    --dir=gwiki-html \
    --ns=A \
    --url="$title" \
    --redirect \
    ./wikipedia_en_all_maxi_2024-01.zim
done <./gtitles
