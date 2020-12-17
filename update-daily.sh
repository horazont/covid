#!/bin/bash
./download-dwd-kl.sh
./download-jhu.sh
./download-rki.sh
./dwd-to-influx.py
./jhu-to-influx.py jhu
./rki-to-influx.py rki
