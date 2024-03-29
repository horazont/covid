#!/bin/bash
set -euo pipefail
start_date="$1"
destatis_file="$2"
rust_target="${RUST_TARGET:-release}"
to_influx="${TO_INFLUX:-./target/$rust_target/to_influx}"
./download-jhu.sh
./download-rki.sh
./download-git.sh
./download-divi.sh
./jhu-to-influx.py jhu
./update-daily-non-idempotent.sh
"$to_influx" \
    rki/cases.csv.gz \
    rki/districts.csv \
    rki/diff.csv \
    "$start_date" \
    divi/icu-load.csv.gz \
    'rki/vaccination-git/Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv' \
    'rki/hospitalization-git/Aktuell_Deutschland_COVID-19-Hospitalisierungen.csv' \
    "$destatis_file"
