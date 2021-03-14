#!/usr/bin/env python3
import asyncio
import csv
import os
import pathlib

from datetime import datetime, date

import aiohttp

import influxdb


DATE_CUTOFF = datetime(2020, 1, 1)


FIELDMAP = {
    "TMK": "temp_avg",
    "TNK": "temp_min",
    "TXK": "temp_max",
    "UPM": "hum_avg",
    "VPM": "pres_avg",
    "RSK": "precip",
    "SD_STRAHL": "sun_hrs",
    "ATMO_STRAHL": "atmo_radexp",
    "FD_STRAHL": "sky_diffuse_radexp",
    "FG_STRAHL": "global_radexp",
}


def read_index(f):
    rows = iter(f)
    next(rows)
    next(rows)
    stationmap = {}
    for row in f:
        row = row.strip()
        if not row:
            continue
        station_id, remainder = row.split(" ", 1)
        remainder, state = remainder.rsplit(" ", 1)
        stationmap[int(station_id)] = state
    return stationmap


def read(f, stationmap):
    reader = csv.reader(f, delimiter=";")
    header = next(reader)
    header_index = {
        k.strip(): i
        for i, k in enumerate(header)
    }

    date_index = header_index["MESS_DATUM"]
    station_id_index = header_index["STATIONS_ID"]

    fieldmap = {
        header_index[key]: field
        for key, field in FIELDMAP.items()
        if key in header_index
    }

    for row in reader:
        # parser check
        assert row[-1] == "eor"
        timestamp = datetime.strptime(row[date_index].strip(), "%Y%m%d")
        if timestamp < DATE_CUTOFF:
            continue

        station_id = int(row[station_id_index].strip())
        tags = (
            ("station_id", str(station_id)),
            ("state", stationmap[station_id]),
        )

        fields = []
        for index, field in fieldmap.items():
            value_s = row[index]
            value = float(value_s.strip())
            if value == -999:
                continue
            fields.append(
                (field, value),
            )

        if not fields:
            continue

        yield influxdb.InfluxDBSample(
            measurement="dwd_weather",
            timestamp=timestamp,
            ns_part=0,
            fields=tuple(fields),
            tags=tags,
        )


def to_influx(sourcedir, stationmap):
    for path in sourcedir.iterdir():
        with path.open("r") as f:
            yield from read(f, stationmap)
        print("\x1b[K", path, sep="", end="\r")


async def push(samples):
    batch_size = 10000
    api_url = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(influxdb.batcher(samples, batch_size)):
            if 0:
                list(batch)
            else:
                await influxdb.write(
                    api_url=api_url,
                    session=session,
                    database="covid",
                    retention_policy=None,
                    precision=influxdb.Precision.SECONDS,
                    samples=influxdb._async_batcher(batch, 1000),
                )


def main():
    stationmap = {}
    print("reading indices ...")
    with open("dwd_sl.index", encoding="latin1") as f:
        stationmap.update(read_index(f))
    with open("dwd_kl.index", encoding="latin1") as f:
        stationmap.update(read_index(f))

    print("streaming to influx ...")
    asyncio.run(push(to_influx(pathlib.Path.cwd() / "dwd", stationmap)))


if __name__ == "__main__":
    main()
