#!/bin/bash
set -euo pipefail
scratchspace="$(mktemp -d)"
indexfile="$(pwd)/dwd_kl.index"
outdir="$(pwd)/dwd"

function download_index() {
    outfile="$1"
    if [ -n "${DWD_INDEX_FILE:-}" ]; then
        cp "$DWD_INDEX_FILE" "$outfile" && return 0
    fi
    index="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/KL_Tageswerte_Beschreibung_Stationen.txt"
    curl -o "$outfile" -sSL "$index"
}

function download_data() {
    station_id="$1"
    outfile="$2"
    url="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/tageswerte_KL_${station_id}_akt.zip"
    out="$scratchspace/${station_id}.zip"
    station_scratch="$scratchspace/${station_id}.tmp"
    if ! curl -o "$out" -fsSL "$url"; then
        return 1
    fi
    mkdir -p "$station_scratch"
    pushd "$station_scratch" > /dev/null
    unzip -q "$out"
    rm -- "$out"
    mv "$station_scratch/produkt_klima_tag_"* "$outfile"
    popd > /dev/null
}

function cleanup() {
    rm -rf -- "$scratchspace"
}

trap cleanup EXIT

datefilter="$(date +"%Y%m%d" -d '-1 day')"
download_index "$indexfile"
mapfile -t -s2 station_ids < <(cut -d' ' -f1,3 < "$indexfile" | grep -F " $datefilter" | cut -d' ' -f1)
mkdir -p "$outdir"
printf '%d stations have up-to-date data\n' "${#station_ids[@]}"
for id in "${station_ids[@]}"; do
    outfile="$outdir/${id}_kl.txt"
    if ! download_data "$id" "$outfile"; then
        echo "warning: failed to download data for station $id" >&2
    else
        echo "$outfile"
    fi
done
