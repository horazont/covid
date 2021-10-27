#!/usr/bin/env python3
import copy
import csv
import sys

import openpyxl


def find_data_worksheet(workbook):
    return workbook.worksheets[1]


def load_rows(sheet):
    row_iter = iter(sheet.iter_rows(
        min_row=1, min_col=1, max_col=5,
        values_only=True,
    ))
    header = next(row_iter)

    assert header[0].lower().strip().strip("*") == "kalenderwoche"
    assert header[1].lower().strip().strip("*") == "anzahl testungen"
    assert header[2].lower().strip().strip("*") == "positiv getestet"
    assert header[3].lower().strip().strip("*") == "positivenanteil (%)"
    assert header[4].lower().strip().strip("*") == \
        "anzahl übermittelnder labore"

    skipped = next(row_iter)
    assert skipped[0].lower().strip().startswith("bis einschließlich")

    for cw_with_year, ntests, npositives, _, nsites in row_iter:
        if cw_with_year.lower().strip() == "summe":
            break
        cw_s, year_s = cw_with_year.strip("*").split("/", 1)
        cw = int(cw_s)
        year = int(year_s)
        rate = f"{(npositives / ntests) * 100:.2f}"
        yield year, cw, ntests, npositives, rate, nsites


def write_rows(row_iter, outfile):
    dialect = copy.copy(csv.excel)
    dialect.lineterminator = "\n"
    writer = csv.writer(outfile, dialect=dialect)
    writer.writerow([
        "Jahr",
        "Kalenderwoche",
        "AnzahlTestungen",
        "TestsPositiv",
        "PositivenQuote",
        "AnzahlLabore",
    ])

    for year, cw, ntests, npositives, rate, nsites in row_iter:
        writer.writerow([
            year, cw, ntests, npositives, rate, nsites,
        ])


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=argparse.FileType("rb"))
    parser.add_argument(
        "-o", "--output-file",
        default=None,
    )

    args = parser.parse_args()

    with args.infile as f:
        workbook = openpyxl.load_workbook(f)

    sheet = find_data_worksheet(workbook)

    if args.output_file is None:
        output_file = sys.stdout
    else:
        output_file = open(args.output_file, "w")

    with output_file as f:
        write_rows(load_rows(sheet), f)


if __name__ == "__main__":
    main()
