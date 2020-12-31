#!/usr/bin/env python3
import asyncio
import calendar
import csv
import dataclasses
import functools
import operator
import pathlib
import sys
import typing

from datetime import datetime, date, timedelta

import numpy as np

import common


MEASUREMENT_DEMOGRAPHICS = "rki_data_v1_demo"
MEASUREMENT_GEOGRAPHICS = "rki_data_v1_geo"


class DeathSample(typing.NamedTuple):
    timestamp: datetime
    location: common.UnresolvedLocation
    sex: str
    age_group: common.AgeGroup
    count: int


def read_death_samples(f) -> typing.Tuple[typing.Collection[DeathSample],
                                          datetime, datetime]:
    result = []
    min_day = None
    max_day = None
    for i, row in enumerate(csv.DictReader(f)):
        if i % 10000 == 0:
            print(f"\x1b[J{i:>12d}", end="\r", file=sys.stderr)
        ts_full = datetime.strptime(row["Datum"], "%Y-%m-%d")
        if min_day is None or min_day > ts_full:
            min_day = ts_full
        if max_day is None or max_day < ts_full:
            max_day = ts_full
        result.append(DeathSample(
            timestamp=ts_full,
            location=common.UnresolvedLocation(
                state_name=row["Bundesland"],
                district_name=row["Landkreis"],
                district_id=None,
            ),
            age_group=row["Altersgruppe"],
            sex=row["Geschlecht"],
            count=int(row["AnzahlTodesfall"]),
        ))
    return result, min_day, max_day


def load_counters(
        samples: typing.Collection[DeathSample],
        min_day: datetime,
        max_day: datetime,
        ) -> common.Counters:
    def keyfunc(s):
        return (s.location.state_name, s.location.district_name,
                s.age_group, s.sex)

    keys = common.build_axis_keys(
        (s for s in samples),
        4, keyfunc,
    )
    key_indices = [
        {
            k: i
            for i, k in enumerate(ks)
        }
        for ks in keys
    ]

    ndays = (max_day - min_day).days + 1
    counters = np.zeros(
        (ndays,) + tuple(len(ks) for ks in keys) + (1,),
        dtype=np.float32,
    )
    for j, sample in enumerate(samples):
        i = round((sample.timestamp - min_day).days)
        sample_keys = keyfunc(sample)
        indices = tuple(key_indices[i][k]
                        for i, k in enumerate(sample_keys))
        index = (i,) + indices + (0,)
        counters[index] += sample.count
        if j % 10000 == 0:
            print(f"\x1b[J{i:>12d}", end="\r", file=sys.stderr)

    return common.Counters(
        first_date=min_day,
        keys=keys,
        key_indices=key_indices,
        count_axis=len(keys) + 1,
        data=counters,
    )


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "infile",
        type=argparse.FileType("r"),
    )

    args = parser.parse_args()

    print("loading ...", file=sys.stderr)
    with args.infile as f:
        death_samples, min_day, max_day = read_death_samples(f)

    print("\x1b[Jpreparing ...", file=sys.stderr)
    counters = load_counters(death_samples, min_day, max_day)

    print("\x1b[Jcrunching the numbers ...", file=sys.stderr)
    print("  deriving data", file=sys.stderr)
    out = common.derive_data(counters.data)

    # We have to strip out the cumulative column because we do not really
    # have that data thanks to the strange RKI publication format
    sl = tuple([slice(None)] * (len(out.shape) - 1)) + (slice(1, None),)
    out = out[sl]

    print("  clipping", file=sys.stderr)
    out = out.clip(0)

    print("  sum", file=sys.stderr)
    out_geo = np.sum(np.sum(out, axis=3), axis=3)
    out_demo = np.sum(out, axis=2)

    expected_samples = \
        functools.reduce(operator.mul, out_geo.shape) + \
        functools.reduce(operator.mul, out_demo.shape)

    print("sending ...", file=sys.stderr)
    asyncio.run(common.push(
        common.generate_counter_samples(
            dataclasses.replace(
                counters,
                data=out_demo,
                keys=(counters.keys[0], counters.keys[2],
                      counters.keys[3]),
            ),
            MEASUREMENT_DEMOGRAPHICS,
            ["state", "age_group", "sex"],
            ["d1pubdeaths", "d7pubdeaths", "d7pubdeaths_s7"],
        ),
        common.generate_counter_samples(
            dataclasses.replace(
                counters,
                data=out_geo,
                keys=counters.keys[:-2],
            ),
            MEASUREMENT_GEOGRAPHICS,
            ["state", "district"],
            ["d1pubdeaths", "d7pubdeaths", "d7pubdeaths_s7"],
        ),
        expected_samples=expected_samples,
    ))


if __name__ == "__main__":
    main()
