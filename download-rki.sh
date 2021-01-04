#!/bin/bash
set -euo pipefail
data="https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"
districts="https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.csv"
outdir="$(pwd)/rki"
outfile="$outdir/cases-$(date +%Y-%m-%d).csv"
outfile_gz="$outfile.gz"
currentfile="$outdir/cases.csv.gz"
district_path="$outdir/districts.csv"
mkdir -p "$outdir"
if [ ! -f "$district_path" ]; then
    wget -O "$district_path" "$districts"
else
    printf 'note: skipping download of district information as it is already present\n'
fi
if [ ! -f "$outfile" ] && [ ! -f "$outfile_gz" ]; then
    tmpout="$(mktemp -p "$outdir")"
    function cleanup() {
        rm -f "$tmpout"
    }
    trap cleanup EXIT
    wget -O "$tmpout" "$data"
    mv "$tmpout" "$outfile"
else
    printf 'note: skipping download of cases data since it is already present for today\n'
fi
if [ ! -f "$outfile_gz" ]; then
    gzip -9 "$outfile"
else
    printf 'note: skipping compression of cases data since it is already present for today\n'
fi
ln -sf "$(basename "$outfile")" "$currentfile"
