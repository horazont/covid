#!/usr/bin/env python3
import asyncio
import pytz
import toml

from datetime import datetime

import common
import influxdb


MEASUREMENT = "events_v1"

INFLUX_BOOL_NAMES = ["false", "true"]


TARGET_TZ = pytz.timezone("Europe/Berlin")


def generate_events(events):
    for ev in events:
        tags = []
        fields = []
        timestamp = datetime.strptime(ev["date"], "%Y-%m-%d")
        fields.append(("title", ev["title"]))
        try:
            text_parts = [ev["text"]]
        except KeyError:
            text_parts = []

        state = ev.get("state")
        district = ev.get("district")
        loc_parts = []
        if district is not None:
            loc_parts.append(district)
        if state is not None:
            loc_parts.append(state)
        if loc_parts:
            text_parts.append(f"<sup>({', '.join(loc_parts)})</sup>")

        is_spreader = bool(ev.get("is_spreader", False))
        is_policy = bool(ev.get("is_policy", False))
        tags.append(("is_spreader", INFLUX_BOOL_NAMES[is_spreader]))
        tags.append(("is_policy", INFLUX_BOOL_NAMES[is_policy]))
        if state is not None:
            tags.append(("state", state))
        if district is not None:
            tags.append(("district", district))

        if is_spreader:
            fields.append(("spreader_class", ev["spreader_class"]))
        fields.append(("text", "\n".join(text_parts)))

        yield influxdb.InfluxDBSample(
            timestamp=timestamp,
            ns_part=0,
            tags=tuple(tags),
            fields=tuple(fields),
            measurement=MEASUREMENT,
        )


def main():
    with open("events.toml") as f:
        events = toml.load(f)["event"]

    print("streaming to influx ...")
    asyncio.run(common.push(
        generate_events(events),
        expected_samples=len(events),
    ))


if __name__ == "__main__":
    main()
