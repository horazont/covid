import csv
import contextlib
import dataclasses
import functools
import gzip
import itertools
import io
import pathlib
import re
import typing
import sys
import zipfile

from datetime import datetime, timedelta

import aiohttp

import numpy as np

import influxdb


NUM_RX = re.compile(r"\d+")


class AgeGroup(typing.NamedTuple):
    low: int
    high: typing.Optional[int]

    @classmethod
    @functools.lru_cache(maxsize=128)
    def from_rki(cls, s: str):
        if s.endswith("+"):
            return cls(int(s[1:-1]), None)
        a1, a2 = s.split("-")
        a1i = int(a1[1:])
        a2i = int(a2[1:]) + 1
        return cls(a1i, a2i)

    @classmethod
    def from_stat(cls, s: str):
        s = s.strip()
        matches = NUM_RX.findall(s)
        if s.startswith("unter"):
            a2, = matches
            a2i = int(a2)
            return AgeGroup(0, a2i)
        if s.endswith("und mehr"):
            a1, = matches
            return AgeGroup(int(a1), None)
        a1, a2 = matches
        a1i = int(a1)
        a2i = int(a2)
        return AgeGroup(a1i, a2i)

    def __str__(self):
        if self.high is None:
            return "A{self.low:02}d+"
        return f"A{self.low:02d}-A{self.high:02d}"


class PopulationInfo(typing.NamedTuple):
    sex: str
    age_group: AgeGroup
    count: int


class DistrictInfo_stat:
    district_name: str
    population: typing.Mapping[typing.Tuple[str, AgeGroup], int]

    def __init__(self, district_name, population):
        self.district_name = district_name
        self.population = population

    def get_population(self, sex):
        return sum(
            count for (sex, age_group), count in self.population.items()
            if sex == sex
        )


class DistrictInfo_pavel:
    population: int
    state_name: str

    def __init__(self, state_name, population):
        self.state_name = state_name
        self.population = population

    def get_population(self):
        return self.population


class UnresolvedLocation(typing.NamedTuple):
    state_name: str
    district_name: str
    district_id: int


class Sample(typing.NamedTuple):
    timestamp: datetime
    location: UnresolvedLocation
    sex: str
    age_group: AgeGroup
    pubcount: int
    refcount: int
    deathcount: int
    recoveredcount: int


def load_popdata_stat(f):
    reader = iter(csv.reader(f, delimiter=";"))
    next(reader)
    next(reader)
    next(reader)
    next(reader)
    sex_headers = [
        None if not col.strip() else col[0].upper()
        for col in next(reader)
    ][3:]
    age_headers = [
        None if not col.strip() else AgeGroup.from_stat(col)
        for col in next(reader)
    ][3:]
    result = {}

    for row in reader:
        if len(row) == 1:
            # end of table marker
            break
        _, district_id, district_name, *data = row
        if "-" in data:
            continue
        district_data = {
            (sex, age): int(count_s)
            for (sex, age), count_s in zip(zip(sex_headers, age_headers), data)
        }
        result[int(district_id)] = DistrictInfo_stat(district_name,
                                                     district_data)

    return result


def load_popdata_pavel(f):
    reader = iter(csv.reader(f, delimiter=","))
    next(reader)

    result = {}
    for state_name, district_id, population in reader:
        result[int(district_id)] = DistrictInfo_pavel(state_name,
                                                      int(population))

    return result


@functools.lru_cache(maxsize=128)
def dualtimestamp(s: str):
    try:
        v = int(s)
    except ValueError:
        return datetime.strptime(s, "%Y/%m/%d %H:%M:%S")
    return datetime.utcfromtimestamp(v / 1000)


def rich_items(f):
    yield from csv.DictReader(f)


def processed_items(items):
    for item in items:
        if int(item["NeuerFall"]) < 0:
            count = 0
        else:
            count = int(item["AnzahlFall"])
        if int(item["NeuerTodesfall"]) < 0:
            deathcount = 0
        else:
            deathcount = int(item["AnzahlTodesfall"])
        if int(item["NeuGenesen"]) < 0:
            recoveredcount = 0
        else:
            recoveredcount = int(item["AnzahlGenesen"])

        if deathcount + count + recoveredcount == 0:
            continue

        loc = UnresolvedLocation(
            state_name=item["Bundesland"],
            district_name=item["Landkreis"],
            district_id=int(item["IdLandkreis"]),
        )

        pubdate = dualtimestamp(item["Meldedatum"])

        if count > 0:
            yield Sample(
                timestamp=dualtimestamp(item["Refdatum"]),
                refcount=count,
                pubcount=0,
                deathcount=0,
                recoveredcount=0,
                sex=item["Geschlecht"],
                age_group=item["Altersgruppe"],
                location=loc,
            )

        yield Sample(
            timestamp=pubdate,
            pubcount=count,
            refcount=0,
            deathcount=deathcount,
            recoveredcount=recoveredcount,
            sex=item["Geschlecht"],
            age_group=item["Altersgruppe"],
            location=loc,
        )


def datebinned_items(f):
    samples = {}
    for i, sample in enumerate(processed_items(rich_items(f))):
        day_bin = sample.timestamp.date()
        samples.setdefault(day_bin, []).append(sample)
        if i % 10000 == 0:
            print(f"proc: {i:>9d}", end="\r", file=sys.stderr)
    return samples


def daterange(d1, d2):
    dt1 = datetime(day=d1.day, month=d1.month, year=d1.year)
    dt2 = datetime(day=d2.day, month=d2.month, year=d2.year)
    days = (dt2 - dt1).days
    for i in range(days+1):
        yield (dt1 + timedelta(days=i)).date()


@contextlib.contextmanager
def _gzip_open(fileref, *, encoding):
    with gzip.open(fileref, "rt", encoding=encoding) as f:
        yield f


@contextlib.contextmanager
def _zip_open(fileref, *, encoding):
    with contextlib.ExitStack() as stack:
        if isinstance(fileref, pathlib.Path):
            fin_zip = stack.enter_context(fileref.open("rb"))
        else:
            fin_zip = stack.enter_context(fileref)
        archive = stack.enter_context(zipfile.ZipFile(fin_zip, "r"))
        names = archive.namelist()
        if len(names) > 1:
            raise ValueError("more than one archive member found in zip file")
        if not names:
            raise ValueError("empty zip archive")

        finb = stack.enter_context(archive.open(names[0], "r"))
        fin = io.TextIOWrapper(finb, encoding=encoding)
        yield fin


@contextlib.contextmanager
def magic_open(fileref, *, encoding="utf-8"):
    if isinstance(fileref, (str, bytes)):
        fileref = pathlib.Path(fileref)

    if isinstance(fileref, pathlib.Path):
        name = fileref.name
    elif hasattr(fileref, "name"):
        name = fileref.name
    else:
        name = None

    if name is None and isinstance(fileref, pathlib.Path):
        with fileref.open("r", encoding=encoding) as f:
            yield f
        return

    if name.endswith(".gz"):
        with _gzip_open(fileref, encoding=encoding) as f:
            yield f
    elif name.endswith(".zip"):
        with _zip_open(fileref, encoding=encoding) as f:
            yield f
    elif isinstance(fileref, pathlib.Path):
        with fileref.open("r", encoding=encoding) as f:
            yield f
    else:
        with fileref:
            yield io.TextIOWrapper(fileref, encoding=encoding)


AXIS_TIME = 0


@dataclasses.dataclass
class Counters:
    first_date: datetime
    keys: typing.Tuple[
        typing.Sequence[str],
        typing.Sequence[str],
        typing.Sequence[str],
        typing.Sequence[str],
    ]
    key_indices: typing.Tuple[
        typing.Mapping[str, int],
        typing.Mapping[str, int],
        typing.Mapping[str, int],
        typing.Mapping[str, int],
    ]
    count_axis: int
    data: np.ndarray


def build_axis_keys(samples, nkeys, keyfunc):
    keysets = [set() for _ in range(nkeys)]
    for s in samples:
        ks = keyfunc(s)
        assert len(ks) == nkeys
        for k, keyset in zip(ks, keysets):
            keyset.add(k)

    return tuple(
        list(sorted(keyset))
        for keyset in keysets
    )


def derive_data(data_in: np.ndarray, is_cumsum: bool = False) -> np.ndarray:
    if is_cumsum:
        cumdata = data_in
        d1data = np.zeros_like(data_in)
        # this is a loss of information, but it is really strange otherwise
        d1data[1:] = np.maximum(np.diff(cumdata, axis=AXIS_TIME), 0)
    else:
        cumdata = np.cumsum(data_in, AXIS_TIME)
        d1data = data_in

    d7data = np.zeros_like(cumdata)
    d7data[7:] = np.maximum(cumdata[7:] - cumdata[:-7], 0)
    d7s7_data = np.zeros_like(d7data)
    d7s7_data[7:] = d7data[:-7]

    w = data_in.shape[-1]
    out = np.zeros(
        data_in.shape[:-1] + (w*4,)
    )
    prefix = tuple([slice(None)] * (len(data_in.shape) - 1))

    for i, part in enumerate((cumdata, d1data, d7data, d7s7_data)):
        out[prefix + (slice(i*w, (i+1)*w),)] = part

    return out


def generate_counter_samples(
        counters: Counters,
        measurement: str,
        key_labels: typing.Sequence[str],
        field_labels: typing.Sequence[str],
        ):
    assert len(key_labels) == len(counters.keys)
    assert len(field_labels) == counters.data.shape[-1]
    data = counters.data
    keysets = list(itertools.product(*counters.keys))
    reshaped = counters.data.reshape(
        (data.shape[0], len(keysets), data.shape[-1]),
    )
    for i in range(counters.data.shape[0]):
        date = counters.first_date + timedelta(days=i)
        timestamp = datetime(date.year, date.month, date.day)
        for j, keyset in enumerate(keysets):
            tags = tuple(
                (k, v)
                for k, v in zip(key_labels, keyset)
            )
            row = tuple(reshaped[i, j])
            if not any(row):
                # skip this sample: we have a lot of those because we have a
                # non-3.NF database: state and district are in the same column
                # and we get the product of all states and districts -> many
                # rows which never have non-zero values.
                continue
            yield influxdb.InfluxDBSample(
                measurement=measurement,
                tags=tags,
                fields=tuple(
                    (k, v)
                    for k, v in zip(field_labels, reshaped[i, j])
                ),
                timestamp=timestamp,
                ns_part=0,
            )


async def push(
        *influx_samples: typing.Iterable[influxdb.InfluxDBSample],
        expected_samples: int,
        ):
    batch_size = 10000
    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(influxdb.batcher(
                itertools.chain(*influx_samples),
                batch_size)):
            await influxdb.write(
                api_url="http://localhost:8086",
                session=session,
                database="covid",
                retention_policy=None,
                precision=influxdb.Precision.SECONDS,
                samples=influxdb._async_batcher(batch, 1000),
            )
            seen = (i+1)*batch_size
            progress = seen / expected_samples
            print(f"\x1b[J~{progress*100:>5.1f}% ({seen}/{expected_samples})",
                  end="\r",
                  file=sys.stderr)
