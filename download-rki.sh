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
download 'https://www.arcgis.com/sharing/rest/content/items/66876b81065340a4a48710b062319336/data' "$(pwd)/rki" 'cases' "$(date +"%d.%m.%Y, 00:00 Uhr")"
