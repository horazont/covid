#!/bin/bash
set -euo pipefail
./download-jhu.sh
./download-rki.sh
./jhu-to-influx.py jhu
./rki-to-influx.py rki/cases.csv.gz rki/districts.csv
./update-daily-non-idempotent.sh
