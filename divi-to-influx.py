#!/usr/bin/env python3
import asyncio
import csv
import functools
import sys

from datetime import datetime, timedelta

import common
import influxdb


MEASUREMENT = "divi_data_v1"


STATE_MAP = {
    "BADEN_WUERTTEMBERG": "Baden-Württemberg",
    "THUERINGEN": "Thüringen",
}


@functools.lru_cache(maxsize=32)
def translate_state(s: str) -> str:
    try:
        return STATE_MAP[s]
    except KeyError:
        pass
    return s.replace("_", "-").title()


@functools.lru_cache(maxsize=32)
def parse_date(s: str) -> datetime:
    datepart, tzpart = s.split("+", 1)
    hoffset_s, moffset_s = tzpart.split(":")
    offset = timedelta(hours=int(hoffset_s), minutes=int(moffset_s))
    return datetime.strptime(datepart, "%Y-%m-%dT%H:%M:%S") - offset


def generate_samples(f):
    reader = csv.DictReader(f)
    for row in reader:
        state = translate_state(row["Bundesland"])
        nreports = int(row["Anzahl_Meldebereiche_Erwachsene"])
        ninuse = int(row["Belegte_Intensivbetten_Erwachsene"])
        nfree = int(row["Freie_Intensivbetten_Erwachsene"])
        ninuse_covid = int(row["Aktuelle_COVID_Faelle_Erwachsene_ITS"])
        nemergency_reserve = int(row["7_Tage_Notfallreserve_Erwachsene"])
        yield influxdb.InfluxDBSample(
            timestamp=parse_date(row["Datum"]),
            ns_part=0,
            tags=(
                ("state", state),
            ),
            fields=(
                ("reporting", nreports),
                ("inuse", ninuse),
                ("inuse_covid", ninuse_covid),
                ("emergency_reserve", nemergency_reserve),
                ("free", nfree),
            ),
            measurement=MEASUREMENT,
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "infile",
        type=argparse.FileType("rb"),
    )

    args = parser.parse_args()

    print("streaming to influxdb...")
    with common.magic_open(args.infile) as f:
        asyncio.run(common.push(
            generate_samples(f),
        ))


if __name__ == "__main__":
    sys.exit(main() or 0)
