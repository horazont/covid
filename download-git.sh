#!/bin/bash
set -euo pipefail
mkdir -p rki/vaccination-git/
wget -O rki/vaccination-git/Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv https://github.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/raw/master/Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv
mkdir -p rki/hospitalization-git/
wget -O rki/hospitalization-git/Aktuell_Deutschland_COVID-19-Hospitalisierungen.csv https://github.com/robert-koch-institut/COVID-19-Hospitalisierungen_in_Deutschland/raw/master/Aktuell_Deutschland_COVID-19-Hospitalisierungen.csv
