#!/bin/bash
set -euo pipefail
districts="https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.csv"
outdir="$(pwd)/rki"
district_path="$outdir/districts.csv"
mkdir -p "$outdir"
if [ ! -f "$district_path" ]; then
    wget -O "$district_path" "$districts"
else
    printf 'note: skipping download of district information as it is already present\n'
fi
scriptdir="$(dirname "$0")"
function download() {
    "$scriptdir/download-versioned.sh" "$@"
}
download 'https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data' "$(pwd)/rki" 'cases' "$(date --date='-1 day' +"%Y/%m/%d")"
