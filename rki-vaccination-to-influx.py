#!/usr/bin/env python3
import asyncio
import csv

from datetime import datetime

import common
import influxdb


MEASUREMENT_GEOGRAPHICS = "rki_data_v1_geo"


def load_samples(f):
    reader = csv.DictReader(f)
    for row in reader:
        timestamp = datetime.strptime(row["Datenstand"], "%Y-%m-%d")
        fields = (
            (k, int(v)) for k, v in filter(
                lambda x: x[1],
                (
                    ("cvacc", row["ImpfungenKumulativ"]),
                    ("cvacc_age", row["IndikationAlter"]),
                    ("cvacc_medical", row["IndikationMedizinisch"]),
                    ("cvacc_occupation", row["IndikationBeruflich"]),
                    ("cvacc_care", row["IndikationPflegeheim"]),
                )
            )
        )
        yield influxdb.InfluxDBSample(
            measurement=MEASUREMENT_GEOGRAPHICS,
            timestamp=timestamp,
            ns_part=0,
            tags=(
                ("state", row["Bundesland"]),
            ),
            fields=fields,
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "infile",
        type=argparse.FileType("rb"),
    )

    args = parser.parse_args()

    print("streaming ...")
    with common.magic_open(args.infile) as fin:
        asyncio.run(common.push(
            load_samples(fin),
            expected_samples=1,
        ))


if __name__ == "__main__":
    main()
