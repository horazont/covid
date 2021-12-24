#!/bin/bash
set -euo pipefail
data="$1"
outdir="$2"
prefix="$3"
grepcheck="${4:-}"
outfile="$outdir/$prefix-$(date +%Y-%m-%d).csv"
outfile_gz="$outfile.gz"
currentfile="$outdir/$prefix.csv.gz"
mkdir -p "$outdir"
if [ ! -f "$outfile" ] && [ ! -f "$outfile_gz" ]; then
    tmpout="$(mktemp -p "$outdir")"
    function cleanup() {
        rm -f "$tmpout"
    }
    trap cleanup EXIT
    wget -O "$tmpout" "$data"
    mv "$tmpout" "$outfile"
else
    printf 'note: skipping download of data since it is already present for today\n'
fi
if [ ! -f "$outfile_gz" ]; then
    if [ -n "$grepcheck" ]; then
        printf 'running data sanity check: %s ... ' "$grepcheck"
        if ! grep -qlF "$grepcheck" "$outfile" >/dev/null; then
            printf 'DATA SANITY CHECK FAILED! %s not found in this dataset! quarantining and aborting!\n' "$grepcheck"
            mv "$outfile" "$outfile-q$(date -Iseconds).scheckfail"
            exit 2
        fi
        printf 'passed\n'
    fi
    gzip -9 "$outfile"
else
    printf 'note: skipping compression of data since it is already present for today\n'
fi
ln -sf "$(basename "$outfile_gz")" "$currentfile"
