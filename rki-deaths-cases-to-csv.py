#!/usr/bin/env python3
import contextlib
import csv
import functools
import gzip
import io
import pathlib
import typing
import sys
import zipfile

from datetime import datetime, date, timedelta

import numpy as np

import common


@functools.lru_cache(maxsize=128)
def parse_german_time(ts: str) -> datetime:
    if ts.endswith("Uhr"):
        return datetime.strptime(ts, "%d.%m.%Y, 00:00 Uhr")
    return datetime.strptime(ts, "%Y/%m/%d")


def convert_death_samples(f):
    for i, row in enumerate(csv.DictReader(f)):
        if i % 10000 == 0:
            print(f"\x1b[J{i:>12d}", end="\r", file=sys.stderr)
        # We are only interested in newly reported or retracted deaths
        if row["NeuerTodesfall"] not in ["1", "-1"]:
            continue
        is_retraction = row["NeuerTodesfall"] == "-1"
        # The Datenstand is the day of the (morning) publication, however,
        # the Meldedatum must obviously be on the day before.
        ts_full = parse_german_time(
            row["Datenstand"]
        ) - timedelta(days=1)
        if is_retraction:
            # If we have a retraction, the affected date is the day before
            ts_full -= timedelta(days=1)
        timestamp = date(ts_full.year, ts_full.month, ts_full.day)
        yield (
            timestamp.strftime("%Y-%m-%d"),
            row["Bundesland"],
            row["Landkreis"],
            row["Altersgruppe"],
            row["Geschlecht"],
            int(row["AnzahlTodesfall"]),
        )


def dump_samples_as_csv(data, fout, *, write_header: bool):
    fields = [
        "Datum",
        "Bundesland",
        "Landkreis",
        "Altersgruppe",
        "Geschlecht",
        "AnzahlTodesfall",
    ]

    writer = csv.writer(fout)
    if write_header:
        writer.writerow(fields)

    for row in data:
        writer.writerow(row)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--outfile",
        default=None,
        help="Append the backup to the given file instead of stdout",
    )
    parser.add_argument(
        "-H", "--force-write-header",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "infile",
        type=argparse.FileType("rb"),
    )

    args = parser.parse_args()

    with contextlib.ExitStack() as stack:
        fin = stack.enter_context(common.magic_open(args.infile))

        write_header = args.force_write_header
        if args.outfile is not None:
            try:
                fout = stack.enter_context(open(args.outfile, "x"))
                write_header = True
            except FileExistsError:
                fout = stack.enter_context(open(args.outfile, "a"))
        else:
            fout = sys.stdout

        print("streaming ...", file=sys.stderr)
        dump_samples_as_csv(convert_death_samples(fin),
                            fout,
                            write_header=write_header)

    print(file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main() or 0)
