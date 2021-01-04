#!/bin/bash
set -euo pipefail
cases_ts="https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
deaths_ts="https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
recovered_ts="https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"
lut="https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv"

outdir="$(pwd)/jhu"
mkdir -p "$outdir"
wget -O "$outdir/cases.csv" "$cases_ts"
wget -O "$outdir/deaths.csv" "$deaths_ts"
wget -O "$outdir/recovered.csv" "$recovered_ts"
wget -O "$outdir/lut.csv" "$lut"
