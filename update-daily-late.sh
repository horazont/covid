#!/bin/bash
set -euo pipefail
./download-dwd-kl.sh
./dwd-to-influx.py
