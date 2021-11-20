#!/bin/bash
set -euo pipefail
rust_target="${RUST_TARGET:-release}"
rki_diff="${RKI_DIFF:-./target/$rust_target/rki_diff}"

markerfile=rki/diff.marker
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
    "$rki_diff" rki/diff.csv rki/cases.csv.gz "$(date +%Y-%m-%d)"
    echo "$new_marker" > "$markerfile"
fi
