import base64
import calendar
import dataclasses
import functools
import enum
import itertools
import operator
import re
import typing

from datetime import datetime

import aiohttp


class Precision(enum.Enum):
    AUTO = ""
    NANOSECONDS = "ns"
    MICROSECONDS = "u"
    MILLISECONDS = "ms"
    SECONDS = "s"


InfluxDBValue = typing.Union[str, int, float, bool]


_ESCAPE_MEASUREMENT = re.compile(r"([\\,\s])")
_ESCAPE_NAME = re.compile(r"([\\,\s=])")
_ESCAPE_STR = re.compile(r'([\\"])')


def _escape_re(rx: re.Pattern, s: str) -> str:
    return rx.subn(r"\\\1", s)[0]


@functools.lru_cache(maxsize=128)
def escape_measurement(s: str) -> str:
    return _escape_re(_ESCAPE_MEASUREMENT, s)


def escape_name(s: str) -> str:
    return _escape_re(_ESCAPE_NAME, s)


def escape_str(s: str):
    return '"{}"'.format(_escape_re(_ESCAPE_STR, s))


def encode_field_value(v: InfluxDBValue) -> bytes:
    if isinstance(v, bool):
        return str(v).encode("ascii")
    if isinstance(v, str):
        return escape_str(v).encode("utf-8")
    if isinstance(v, int):
        return f"{v!r}i".format(v).encode("ascii")
    if isinstance(v, float):
        return repr(v).encode("ascii")
    raise TypeError(f"not a valid InfluxDBValue (type {type(v)}): {v!r}")


def encode_measurement_name(v: str) -> bytes:
    return escape_measurement(v).encode("utf-8")


def encode_tag_part(v: str) -> bytes:
    return escape_name(v).encode("utf-8")


def encode_field_key(v: str) -> bytes:
    return escape_name(v).encode("utf-8")


def _divround(v: int, divisor: int) -> int:
    assert divisor % 2 == 0
    v, remainder = divmod(v, divisor)
    if remainder >= divisor // 2:
        v += 1
    return v


@functools.lru_cache(maxsize=128)
def encode_timestamp(dt: datetime, ns_part: int,
                     precision: Precision) -> bytes:
    if precision == Precision.AUTO:
        raise ValueError("auto precision not supported for encoding")
    if not (0 <= ns_part < 1000):
        raise ValueError(
            f"nanosecond part must be in 0..999, got {ns_part}"
        )
    utc_seconds = calendar.timegm(dt.utctimetuple())
    full_timestamp = (
        (utc_seconds * 1000000 + dt.microsecond) * 1000 + ns_part
    )

    if precision == Precision.MICROSECONDS:
        full_timestamp = _divround(full_timestamp, 1000)
    elif precision == Precision.MILLISECONDS:
        full_timestamp = _divround(full_timestamp, 1000000)
    elif precision == Precision.SECONDS:
        full_timestamp = _divround(full_timestamp, 1000000000)

    return str(full_timestamp).encode("utf-8")


def encode_tag_pair(t: typing.Tuple[str, str]) -> bytes:
    return b"=".join(map(encode_tag_part, t))


def encode_field_pair(f: typing.Tuple[str, InfluxDBValue]) -> bytes:
    return b"=".join((
        encode_field_key(f[0]),
        encode_field_value(f[1]),
    ))


@functools.lru_cache(maxsize=None)
def encode_tagset(ts: typing.Tuple[typing.Tuple[str, str]]) -> bytes:
    items = list(map(encode_tag_pair, ts))
    if items:
        items.insert(0, b"")
    return b",".join(items)


@functools.lru_cache(maxsize=None)
def encode_measurement_tagset(name, ts) -> bytes:
    return encode_measurement_name(name) + encode_tagset(ts)


class InfluxDBSample(typing.NamedTuple):
    measurement: str
    tags: typing.Tuple[typing.Tuple[str, str]]
    fields: typing.Tuple[typing.Tuple[str, InfluxDBValue]]
    timestamp: datetime
    ns_part: int

    def encode(self, precision) -> bytes:
        parts = [
            encode_measurement_tagset(self.measurement, self.tags),
        ]

        comma_parts = list(map(encode_field_pair, self.fields))
        parts.append(b",".join(comma_parts))

        parts.append(encode_timestamp(self.timestamp, self.ns_part,
                                      precision))
        return b" ".join(parts) + b"\n"


T = typing.TypeVar("T")


def batcher(
        iterable: typing.Iterable[T],
        batch_size: int,
        ) -> typing.Generator[typing.Iterable[T], None, None]:
    def grouper():
        for i in itertools.count():
            for _ in range(batch_size):
                yield i

    grouped = zip(grouper(), iterable)
    for _, batch_items in itertools.groupby(grouped, key=lambda x: x[0]):
        yield map(operator.itemgetter(1), batch_items)


async def _sample_encoder(
        sample_batches: typing.AsyncIterable[
            typing.Iterable[InfluxDBSample]],
        precision: Precision):
    async for samples in sample_batches:
        yield b"".join(sample.encode(precision) for sample in samples)


async def _async_batcher(
        iterable: typing.Iterable[T],
        batch_size: int,
        ) -> typing.AsyncGenerator[typing.List[T], None]:
    for batch in batcher(iterable, batch_size):
        yield list(batch)


class InfluxDBError(Exception):
    def __init__(self, status, msg):
        super().__init__(f"{msg} ({status})")
        self.status = status
        self.msg = msg


class InfluxDBPermissionError(InfluxDBError):
    pass


class InfluxDBDataError(InfluxDBError):
    pass


class InfluxDBNotFoundError(InfluxDBError):
    pass


async def write(
        api_url: str,
        session: aiohttp.ClientSession,
        database: str,
        retention_policy: typing.Optional[str],
        precision: Precision,
        samples: typing.AsyncIterable[typing.Iterable[InfluxDBSample]],
        ):
    write_url = f"{api_url}/write"
    headers = {}
    params = {}
    params["db"] = database
    params["precision"] = precision.value
    if retention_policy is not None:
        params["rp"] = retention_policy

    async with session.post(
            write_url,
            headers=headers,
            params=params,
            data=_sample_encoder(samples, precision)) as resp:
        if resp.status == 401 or resp.status == 403:
            raise InfluxDBPermissionError(resp.status, resp.reason)
        elif resp.status == 400 or resp.status == 413:
            raise InfluxDBDataError(resp.status, resp.reason)
        elif resp.status == 404:
            raise InfluxDBNotFoundError(resp.status, resp.reason)
        elif resp.status != 204:
            raise InfluxDBError(resp.status, resp.reason)
