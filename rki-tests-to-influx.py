#!/usr/bin/env python3
import asyncio
import csv
import dataclasses
import functools
import itertools
import numbers
import operator
import pathlib
import typing

from datetime import datetime, timedelta, date

import aiohttp

import numpy as np

import common
import influxdb


T = typing.TypeVar("T")


MEASUREMENT = "rki_data_v1_tests"


def monday_of_calenderweek(year, week):
    # from https://stackoverflow.com/a/59200842/1248008
    first = date(year, 1, 1)
    base = 1 if first.isocalendar()[1] == 1 else 8

    return first + timedelta(
        days=base - first.isocalendar()[2] + 7 * (week - 1)
    )


def import_samples(f):
    for row in csv.DictReader(f):
        year = int(row.get("Jahr", 2020))
        week = int(row["Kalenderwoche"])
        ntests = int(row["AnzahlTestungen"])
        npositive = int(row["TestsPositiv"])
        nsites = int(row["AnzahlLabore"])
        d = monday_of_calenderweek(year, week)
        yield influxdb.InfluxDBSample(
            measurement=MEASUREMENT,
            timestamp=datetime(d.year, d.month, d.day),
            ns_part=0,
            tags=(),
            fields=(
                ("tests", ntests),
                ("positives", npositive),
                ("sites", nsites),
            ),
        )
        print(f"\x1b[J{d}", end="\r")


def main():
    import sys

    print("streaming ...")
    with (pathlib.Path(sys.argv[1]) / "tests.csv").open("r") as f:
        asyncio.run(common.push(
            import_samples(f),
        ))
    print("\x1b[J", end="")

    import os
    os._exit(0)


if __name__ == "__main__":
    main()
