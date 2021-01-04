#!/bin/bash
set -euo pipefail
./download-dwd-sl.sh
./dwd-to-influx.py
