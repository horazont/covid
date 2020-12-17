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

from datetime import datetime, timedelta

import aiohttp

import numpy as np

import common
import influxdb


T = typing.TypeVar("T")


MEASUREMENT_GEOGRAPHICS = "rki_data_v1_geo"
MEASUREMENT_DEMOGRAPHICS = "rki_data_v1_demo"


@dataclasses.dataclass
class DistrictInfo_rki:
    state_name: str
    district_name: str
    population: int


def load_counters(
        samples: typing.Mapping[
            datetime, typing.Collection[common.Sample]
        ],
        ) -> common.Counters:
    min_day = min(samples.keys())
    max_day = max(samples.keys())

    def keyfunc(s):
        return (s.location.state_name, s.location.district_name,
                s.age_group, s.sex)

    keys = common.build_axis_keys(
        (s for date_samples in samples.values() for s in date_samples),
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
        (ndays,) + tuple(len(ks) for ks in keys) + (4,),
        dtype=np.float32,
    )
    for i, date in enumerate(common.daterange(min_day, max_day)):
        date_samples = samples.get(date, [])
        for sample in date_samples:
            sample_keys = keyfunc(sample)
            indices = tuple(key_indices[i][k]
                            for i, k in enumerate(sample_keys))
            counters[(i,) + indices + (0,)] += sample.refcount
            counters[(i,) + indices + (1,)] += sample.pubcount
            counters[(i,) + indices + (2,)] += sample.deathcount
            counters[(i,) + indices + (3,)] += sample.recoveredcount
        print(f"proc: {date}", end="\r")

    return common.Counters(
        first_date=min_day,
        keys=keys,
        key_indices=key_indices,
        count_axis=len(keys) + 1,
        data=counters,
    )


def load_rki_districts(f):
    reader = csv.DictReader(f)
    result = {}
    for row in reader:
        district_id = int(row["RS"])
        population = int(row["EWZ"])
        district_name = row["county"]
        state_name = row["BL"]

        result[district_id] = DistrictInfo_rki(
            district_name=district_name,
            state_name=state_name,
            population=population,
        )

    return result


def generate_population_samples(
        district_info,
        measurement: str,
        first_date: datetime,
        ndays: int):
    templates = []
    for district in district_info.values():
        templates.append(influxdb.InfluxDBSample(
            measurement=measurement,
            tags=(
                ("state", district.state_name),
                ("district", district.district_name),
            ),
            fields=(
                ("population", district.population),
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


def main():
    import sys

    print("loading ...")
    with (pathlib.Path(sys.argv[1]) / "cases.csv").open("r") as f:
        samples_raw = common.datebinned_items(f)
    print("\x1b[J", end="")

    print("preparing ...")
    counters = load_counters(samples_raw)
    del samples_raw

    print("preparing districts ...")
    with (pathlib.Path(sys.argv[1]) / "districts.csv").open("r") as f:
        district_data = load_rki_districts(f)

    print("crunching the numbers ...")
    print("  deriving data")
    out = common.derive_data(counters.data)

    print("  sum")
    out_geo = np.sum(np.sum(out, axis=3), axis=3)
    out_demo = np.sum(out, axis=2)

    expected_samples = \
        len(district_data) * out.shape[0] + \
        functools.reduce(operator.mul, out_geo.shape) + \
        functools.reduce(operator.mul, out_demo.shape)

    print("sending ...")
    asyncio.run(common.push(
        common.generate_counter_samples(
            dataclasses.replace(
                counters,
                data=out_demo,
                keys=(counters.keys[0], counters.keys[2], counters.keys[3]),
            ),
            MEASUREMENT_DEMOGRAPHICS,
            ["state", "age_group", "sex"],
            ["ccases", "cpubcases", "cdeaths", "crecovered",
             "d1cases", "d1pubcases", "d1deaths", "d1recovered",
             "d7cases", "d7pubcases", "d7deaths", "d7recovered",
             "d7cases_s7", "d7pubcases_s7", "d7deaths_s7", "d7recovered_s7"],
        ),
        common.generate_counter_samples(
            dataclasses.replace(
                counters,
                data=out_geo,
                keys=counters.keys[:-2],
            ),
            MEASUREMENT_GEOGRAPHICS,
            ["state", "district"],
            ["ccases", "cpubcases", "cdeaths", "crecovered",
             "d1cases", "d1pubcases", "d1deaths", "d1recovered",
             "d7cases", "d7pubcases", "d7deaths", "d7recovered",
             "d7cases_s7", "d7pubcases_s7", "d7deaths_s7", "d7recovered_s7"],
        ),
        generate_population_samples(
            district_data,
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
