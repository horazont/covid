#!/bin/bash
set -euo pipefail
./download-divi.sh
./divi-to-influx.py divi/state-level.csv.gz
./download-dwd-kl.sh
./dwd-to-influx.py
