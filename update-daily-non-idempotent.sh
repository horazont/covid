#!/bin/bash
echo 'DANGER: Running this twice on the same data set corrupts the metrics!' >&2
./rki-deaths-incr-to-influx.py --no-send rki/cases.csv -o rki/deaths.csv
./rki-deaths-incr-to-influx.py rki/deaths.csv -q
