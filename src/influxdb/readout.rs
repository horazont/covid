use std::io;

use smartstring::alias::String as SmartString;

use serde::{Deserialize, Serialize};

use enum_map::Enum;

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum Precision {
	Nanoseconds,
	Microseconds,
	Milliseconds,
	Seconds,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
	Numeric(f64),
	Integer(i64),
	Bool(bool),
	String(SmartString),
}

impl From<f64> for FieldValue {
	fn from(other: f64) -> Self {
		Self::Numeric(other)
	}
}

impl From<i64> for FieldValue {
	fn from(other: i64) -> Self {
		Self::Integer(other)
	}
}

impl From<bool> for FieldValue {
	fn from(other: bool) -> Self {
		Self::Bool(other)
	}
}

impl From<SmartString> for FieldValue {
	fn from(other: SmartString) -> Self {
		Self::String(other)
	}
}

impl From<String> for FieldValue {
	fn from(other: String) -> Self {
		Self::String(other.into())
	}
}

impl From<&str> for FieldValue {
	fn from(other: &str) -> Self {
		Self::String(other.into())
	}
}

impl FieldValue {
	fn write_into<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
		match self {
			Self::Numeric(v) => write!(w, "{:?}", v),
			Self::Integer(v) => write!(w, "{:?}i", v),
			Self::Bool(v) => match v {
				true => write!(w, "true"),
				false => write!(w, "false"),
			},
			Self::String(v) => write_str(w, v),
		}
	}
}

fn write_escaped<W: io::Write>(w: &mut W, s: &str, pat: &[char]) -> io::Result<()> {
	let mut prev = 0;
	for (idx, substr) in s.match_indices(pat) {
		w.write(&s.as_bytes()[prev..idx])?;
		w.write(&b"\\"[..])?;
		w.write(&substr.as_bytes()[..])?;
		prev = idx + substr.len();
	}
	if prev != s.len() {
		w.write(&s.as_bytes()[prev..])?;
	}
	Ok(())
}

pub fn write_name<W: io::Write>(w: &mut W, s: &str) -> io::Result<()> {
	write_escaped(w, s, &['\\', ',', ' ', '\t', '\n', '\r', '='])
}

pub fn write_measurement<W: io::Write>(w: &mut W, s: &str) -> io::Result<()> {
	write_escaped(w, s, &['\\', ',', ' ', '\t', '\n', '\r'])
}

// may be useful at some point
#[allow(dead_code)]
fn write_str<W: io::Write>(w: &mut W, s: &str) -> io::Result<()> {
	w.write(&b"\""[..])?;
	write_escaped(w, s, &['\\', '"'])?;
	w.write(&b"\""[..])?;
	Ok(())
}

impl Precision {
	pub fn value(&self) -> &'static str {
		match self {
			Self::Nanoseconds => "ns",
			Self::Microseconds => "u",
			Self::Milliseconds => "ms",
			Self::Seconds => "s",
		}
	}

	pub fn encode_timestamp<W: io::Write>(&self, w: &mut W, ts: &DateTime<Utc>) -> io::Result<()> {
		// XXX: do something about leap seconds
		match self {
			Self::Seconds => write!(w, "{}", ts.timestamp()),
			Self::Milliseconds => {
				let ms = ts.timestamp_subsec_millis();
				let ms = if ms >= 999 { 999 } else { ms };
				write!(w, "{}{:03}", ts.timestamp(), ms)
			}
			Self::Microseconds => {
				let us = ts.timestamp_subsec_micros();
				let us = if us >= 999_999 { 999_999 } else { us };
				write!(w, "{}{:06}", ts.timestamp(), us)
			}
			Self::Nanoseconds => {
				let ns = ts.timestamp_subsec_nanos();
				let ns = if ns >= 999_999_999 { 999_999_999 } else { ns };
				write!(w, "{}{:09}", ts.timestamp(), ns)
			}
		}
	}
}

#[derive(Debug, Clone)]
pub struct Sample {
	pub tagv: Vec<SmartString>,
	pub fieldv: Vec<FieldValue>,
}

#[derive(Debug, Clone)]
pub struct Readout {
	pub ts: DateTime<Utc>,
	pub measurement: SmartString,
	pub precision: Precision,
	pub tags: Vec<SmartString>,
	pub fields: Vec<SmartString>,
	pub samples: Vec<Sample>,
}

impl Readout {
	pub fn write<W: io::Write>(&self, dest: &mut W) -> io::Result<()> {
		for sample in self.samples.iter() {
			write_measurement(dest, &self.measurement)?;
			for (k, v) in self.tags.iter().zip(sample.tagv.iter()) {
				dest.write(b",")?;
				write_name(dest, k)?;
				dest.write(b"=")?;
				write_name(dest, v)?;
			}
			let mut first = true;
			for (k, v) in self.fields.iter().zip(sample.fieldv.iter()) {
				dest.write(if first { b" " } else { b"," })?;
				write_name(dest, k)?;
				dest.write(b"=")?;
				v.write_into(dest)?;
				first = false;
			}
			dest.write_all(&b" "[..])?;
			self.precision.encode_timestamp(dest, &self.ts)?;
			dest.write_all(&b"\n"[..])?;
		}
		Ok(())
	}
}
