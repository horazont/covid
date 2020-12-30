#!/bin/bash
echo 'DANGER: Running this twice on the same data set corrupts the metrics!' >&2
./rki-deaths-cases-to-csv.py rki/cases.csv -o rki/deaths.csv
./rki-deaths-to-influx.py rki/deaths.csv
