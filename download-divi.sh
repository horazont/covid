#!/bin/bash
set -euo pipefail
scriptdir="$(dirname "$0")"
function download() {
    "$scriptdir/download-versioned.sh" "$@"
}
download 'https://diviexchange.blob.core.windows.net/%24web/zeitreihe-tagesdaten.csv' "$(pwd)/divi" 'icu-load'
