#!/bin/bash
set -euo pipefail
markerfile=rki/deaths.marker
new_marker="$(readlink rki/cases.csv.gz)"
if [ ! -f "$markerfile" ]; then
    current_marker=''
else
    current_marker="$(cat "$markerfile")"
fi
if [ "x$new_marker" == "x$current_marker" ]; then
    printf 'warning: skipping duplicate calculations\n' >&2
    printf 'hint: delete %s to force\n' "$markerfile" >&2
else
    ./rki-deaths-cases-to-csv.py rki/cases.csv.gz -o rki/deaths.csv
    echo "$new_marker" > "$markerfile"
fi
./rki-deaths-to-influx.py rki/deaths.csv
