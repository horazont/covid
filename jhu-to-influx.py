#!/usr/bin/env python3
import asyncio
import csv
import collections
import dataclasses
import functools
import operator
import pathlib
import typing

from datetime import datetime, timedelta, date

import numpy as np

import common
import influxdb


MEASUREMENT_GEOGRAPHICS = "jhu_data_v1_geo"


KIND_CASE = 0
KIND_DEATH = 1
KIND_RECOVERED = 2


class JHUSample(typing.NamedTuple):
    country: str
    kind: int
    cases: int


def parse_date(s: str) -> date:
    month, day, year = s.split("/")
    return date(2000 + int(year), int(month), int(day))


def load_jhu_data(
        f,
        kind,
        dest,
        ):
    rows = iter(csv.reader(f))
    header = next(rows)
    date_offset = 4
    dates = [
        parse_date(s) for s in header[date_offset:]
    ]
    for row in csv.reader(f):
        _, country, _, _, *data = row
        for date, value_s in zip(dates, data):
            value = int(value_s)
            dest.setdefault(date, []).append(
                JHUSample(
                    country,
                    kind,
                    value,
                )
            )
        print(f"\x1b[Jproc: {country}", end="\r")


def load_counters(
        samples: typing.Mapping[datetime, typing.Collection[JHUSample]],
        ) -> common.Counters:
    min_day = min(samples.keys())
    max_day = max(samples.keys())

    def keyfunc(s):
        return ((s.country,),)

    keys = common.build_axis_keys(
        (s for date_samples in samples.values() for s in date_samples),
        1, keyfunc,
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
        (ndays,) + tuple(len(ks) for ks in keys) + (3,),
        dtype=np.float32,
    )
    for i, date in enumerate(common.daterange(min_day, max_day)):
        date_samples = samples.get(date, [])
        for sample in date_samples:
            sample_keys = keyfunc(sample)
            indices = tuple(key_indices[i][k]
                            for i, k in enumerate(sample_keys))
            counters[(i,) + indices + (sample.kind,)] += sample.cases
        print(f"proc: {date}", end="\r")

    return common.Counters(
        first_date=min_day,
        keys=keys,
        key_indices=key_indices,
        count_axis=len(keys) + 1,
        data=counters,
    )


def generate_population_samples(
        population_info,
        measurement: str,
        first_date: datetime,
        ndays: int):
    templates = []
    for country, population in population_info.items():
        templates.append(influxdb.InfluxDBSample(
            measurement=measurement,
            tags=(
                ("country", country),
            ),
            fields=(
                ("population", population),
            ),
            timestamp=None,
            ns_part=0,
        ))

    for i in range(ndays+1):
        date = first_date + timedelta(days=i)
        timestamp = datetime(date.year, date.month, date.day)
        yield from (
            template._replace(timestamp=timestamp)
            for template in templates
        )


def load_jhu_population_data(f) -> typing.Mapping[str, int]:
    result = collections.Counter()
    for row in csv.DictReader(f):
        if not row["Lat"]:
            continue
        population_s = row["Population"]
        # using addition here because we strip out provinces etc.
        result[row["Country_Region"]] += int(population_s)
    return result


def main():
    import sys

    mapping = [
        (KIND_CASE, "cases.csv"),
        (KIND_DEATH, "deaths.csv"),
        (KIND_RECOVERED, "recovered.csv"),
    ]

    print("loading ...")
    data = {}
    for kind, filename in mapping:
        with (pathlib.Path(sys.argv[1]) / filename).open("r") as f:
            load_jhu_data(f, kind, data)
    print("\x1b[J", end="")

    print("loading population data ...")
    with (pathlib.Path(sys.argv[1]) / "lut.csv").open("r") as f:
        population = load_jhu_population_data(f)

    print("preparing ...")
    counters = load_counters(data)

    print("crunching the numbers ...")
    print("  deriving data")
    out = common.derive_data(counters.data, is_cumsum=True)

    expected_samples = \
        len(population) * out.shape[0] + \
        functools.reduce(operator.mul, out.shape[:-1])

    print("sending ...")
    asyncio.run(common.push(
        common.generate_counter_samples(
            dataclasses.replace(
                counters,
                data=out,
                keys=counters.keys,
            ),
            MEASUREMENT_GEOGRAPHICS,
            [("country",)],
            ["ccases", "cdeaths", "crecovered",
             "d1cases", "d1deaths", "d1recovered",
             "d7cases", "d7deaths", "d7recovered",
             "d7cases_s7", "d7deaths_s7", "d7recovered_s7"],
        ),
        generate_population_samples(
            population,
            MEASUREMENT_GEOGRAPHICS,
            counters.first_date,
            out.data.shape[0],
        ),
        expected_samples=expected_samples,
    ))

    import os
    os._exit(0)


if __name__ == "__main__":
    main()
