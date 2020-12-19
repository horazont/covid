#!/usr/bin/env python3
# So the RKI data does not have a separate field for the date a death has been
# announced to them.
# That means the cdeaths column we build in influx using rki-to-influx.py
# does *not* signify when people died (most likely), but instead when they
# got sick.
#
# While that bit of information is also interesting, it is not really useful
# to get a sense of the direness of the situation.
#
# To combat this, we can look at the NeuerTodesfall column. The downside is
# that we can only do this for new datasets and we must not miss a dataset
# in order to get proper results.
#
# NOTE: Using this script without keeping a separate backup of the output
# of this script (e.g. by appending it to a separate file) means that you
# cannot simply drop and fully recreate your influx database!
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
    location: common.UnresolvedLocation
    sex: str
    age_group: common.AgeGroup
    count: int


@functools.lru_cache(maxsize=128)
def parse_german_time(ts: str) -> datetime:
    return datetime.strptime(ts, "%d.%m.%Y, %H:%M Uhr")


def datebin_death_samples(f):
    result = {}
    for i, row in enumerate(csv.DictReader(f)):
        if i % 10000 == 0:
            print(f"\x1b[J{i:>12d}", end="\r", file=sys.stderr)
        # We are only interested in newly reported deaths
        if row["NeuerTodesfall"] != "1":
            continue
        # Detect our own format
        try:
            ts_full = datetime.utcfromtimestamp(int(row["date_unix"]))
        except (KeyError, ValueError):
            # The Datenstand is the day of the (morning) publication, however,
            # the Meldedatum must obviously be on the day before.
            ts_full = parse_german_time(
                row["Datenstand"]
            ) - timedelta(days=1)
        timestamp = date(ts_full.year, ts_full.month, ts_full.day)
        result.setdefault(timestamp, []).append(
            DeathSample(
                location=common.UnresolvedLocation(
                    state_name=row["Bundesland"],
                    district_name=row["Landkreis"],
                    district_id=None,
                ),
                age_group=row["Altersgruppe"],
                sex=row["Geschlecht"],
                count=int(row["AnzahlTodesfall"]),
            )
        )
    return result


def load_counters(
        samples: typing.Mapping[
            datetime, typing.Collection[DeathSample]
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
        (ndays,) + tuple(len(ks) for ks in keys) + (1,),
        dtype=np.float32,
    )
    for i, date in enumerate(common.daterange(min_day, max_day)):
        date_samples = samples.get(date, [])
        for sample in date_samples:
            sample_keys = keyfunc(sample)
            indices = tuple(key_indices[i][k]
                            for i, k in enumerate(sample_keys))
            index = (i,) + indices + (0,)
            counters[index] += sample.count
        print(f"\x1b[Jproc: {date}", end="\r")

    return common.Counters(
        first_date=min_day,
        keys=keys,
        key_indices=key_indices,
        count_axis=len(keys) + 1,
        data=counters,
    )


def datetime_to_unix(dt: datetime):
    return calendar.timegm(dt.utctimetuple())


def dump_samples_as_csv(data, fout, *, write_header: bool):
    fields = [
        "date_unix",
        "NeuerTodesfall",
        "Bundesland",
        "Landkreis",
        "Altersgruppe",
        "Geschlecht",
        "AnzahlTodesfall",
    ]

    writer = csv.writer(fout)
    if write_header:
        writer.writerow(fields)

    for d, samples in sorted(data.items(), key=lambda x: x[0]):
        for sample in samples:
            writer.writerow([
                str(int(datetime_to_unix(datetime(d.year, d.month, d.day)))),
                "1",
                sample.location.state_name,
                sample.location.district_name,
                sample.age_group,
                sample.sex,
                sample.count,
            ])


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-q", "--no-dump-backup",
        dest="write_backup",
        default=True,
        action="store_false",
        help="Do not dump a backup of the incremental data to stdout",
    )
    parser.add_argument(
        "-o", "--outfile",
        default=None,
        help="Append the backup to the given file instead of stdout",
    )
    parser.add_argument(
        "--no-send",
        dest="send_to_influx",
        default=True,
        action="store_false",
        help="Do not send data to influxdb."
    )
    parser.add_argument(
        "infile",
        type=argparse.FileType("r"),
    )

    args = parser.parse_args()

    print("loading ...", file=sys.stderr)
    with args.infile as f:
        death_samples = datebin_death_samples(f)

    if args.send_to_influx:
        print("\x1b[Jpreparing ...", file=sys.stderr)
        counters = load_counters(death_samples)

        print("\x1b[Jcrunching the numbers ...", file=sys.stderr)
        print("  deriving data", file=sys.stderr)
        out = common.derive_data(counters.data)

        # We have to strip out the cumulative column because we do not really
        # have that data thanks to the strange RKI publication format
        sl = tuple([slice(None)] * (len(out.shape) - 1)) + (slice(1, None),)
        out = out[sl]

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

    if args.write_backup:
        if args.outfile is not None:
            try:
                outfile = open(args.outfile, "x")
                write_header = True
            except FileExistsError:
                outfile = open(args.outfile, "a")
                write_header = False
        else:
            write_header = True
            outfile = sys.stdout
        print("\x1b[Jsaving ...", file=sys.stderr)
        with outfile as f:
            dump_samples_as_csv(death_samples, f,
                                write_header=write_header)


if __name__ == "__main__":
    main()
