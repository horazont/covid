#!/bin/bash
set -euo pipefail
data="$1"
outdir="$2"
prefix="$3"
outfile="$outdir/$prefix-$(date +%Y-%m-%d).csv.gz"
currentfile="$outdir/$prefix.csv.gz"
mkdir -p "$outdir"
if [ ! -f "$outfile" ]; then
    tmpout="$(mktemp -p "$outdir")"
    function cleanup() {
        rm -f "$tmpout"
    }
    trap cleanup EXIT
    wget -O- "$data" | gzip -c9 > "$tmpout"
    mv "$tmpout" "$outfile"
else
    printf 'note: skipping download of data since it is already present for today\n'
fi
ln -sf "$(basename "$outfile")" "$currentfile"
