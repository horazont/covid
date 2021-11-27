use std::env;
use std::io::Write;
use std::sync::Arc;

use chrono::{NaiveDate, Utc, TimeZone, Datelike};

use bytes::{Bytes, BytesMut, BufMut};

use smartstring::alias::{String as SmartString};

pub mod influxdb;
mod ioutil;
mod context;
mod destatis;
mod rki;
mod progress;
mod divi;
mod timeseries;

pub use ioutil::magic_open;
pub use context::*;
pub use rki::*;
pub use progress::*;
pub use divi::*;
pub use destatis::*;
pub use timeseries::*;


pub fn naive_today() -> NaiveDate {
	Utc::today().naive_local()
}

pub fn global_start_date() -> NaiveDate {
	NaiveDate::from_ymd(2020, 1, 1)
}


#[derive(Debug, Clone)]
pub struct FieldDescriptor<T>{
	name: &'static str,
	inner: T,
}

impl<T> FieldDescriptor<T> {
	pub fn new(inner: T, name: &'static str) -> Self {
		Self{inner, name}
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn inner(&self) -> &T {
		&self.inner
	}
}


pub fn prepare_keyset<'x, K: TimeSeriesKey, I: Iterator<Item = &'x K>, F: Fn(&K, &mut Vec<SmartString>) -> ()>(
	tags: &[&str],
	keys: I,
	f: F,
) -> Vec<(&'x K, Bytes)> {
	let mut result = Vec::new();
	let mut tmp = Vec::with_capacity(tags.len());
	for k in keys {
		tmp.clear();
		f(k, &mut tmp);
		assert_eq!(tmp.len(), tags.len());
		let mut buffer = BytesMut::new().writer();
		for (tagname, tagv) in tags.iter().zip(tmp.drain(..)) {
			buffer.get_mut().put_u8(b',');
			influxdb::readout::write_name(&mut buffer, tagname).expect("write to BytesMut failed");
			buffer.get_mut().put_u8(b'=');
			influxdb::readout::write_name(&mut buffer, &tagv).expect("write to BytesMut failed");
		}
		result.push((k, buffer.into_inner().freeze()));
	}
	result
}


pub fn stream_dynamic<K: TimeSeriesKey, S: ProgressSink + ?Sized>(
	sink: &influxdb::Client,
	progress: &mut S,
	measurement: &str,
	start: NaiveDate,
	ndays: usize,
	keyset: &[(&K, Bytes)],
	fields: &[FieldDescriptor<Arc<dyn ViewTimeSeries<K>>>],
) -> Result<(), influxdb::Error> {
	#[cfg(debug_assertions)]
	{
		for (_, ts) in keyset.iter() {
			assert!(ts.len() == tags.len());
		}
	}

	static TARGET_METRICS_PER_CHUNK: usize = 5000;

	let chunk_size = (TARGET_METRICS_PER_CHUNK / keyset.len()).max(1);

	let measurement_bytes = {
		let mut buf = BytesMut::new().writer();
		influxdb::readout::write_measurement(&mut buf, measurement).expect("write to BytesMut failed");
		buf.into_inner().freeze()
	};

	let precision = influxdb::Precision::Seconds;

	let mut buffer = BytesMut::new();
	let mut pm = StepMeter::new(progress, ndays);
	let mut fields_serialized = BytesMut::new().writer();
	let mut timestamp_serialized = BytesMut::new().writer();
	for (i, date) in start.iter_days().take(ndays).enumerate() {
		timestamp_serialized.get_mut().clear();
		precision.encode_timestamp(&mut timestamp_serialized, &Utc.ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0)).expect("write to BytesMut failed");

		for (k, tagset) in keyset.iter() {
			fields_serialized.get_mut().clear();
			for desc in fields.iter() {
				let v = desc.inner().getf(k, date);
				if let Some(v) = v {
					if fields_serialized.get_mut().len() > 0 {
						// write separator
						fields_serialized.get_mut().put_u8(b',');
					}
					influxdb::readout::write_name(&mut fields_serialized, desc.name()).expect("write to BytesMut failed");
					fields_serialized.get_mut().put_u8(b'=');
					write!(&mut fields_serialized, "{:?}", v).expect("write to BytesMut failed");
				}
			}

			if fields_serialized.get_mut().len() == 0 {
				continue;
			}

			buffer.put(&measurement_bytes[..]);
			buffer.put(&tagset[..]);
			buffer.put_u8(b' ');
			buffer.put(&fields_serialized.get_mut()[..]);
			buffer.put_u8(b' ');
			buffer.put(&timestamp_serialized.get_mut()[..]);
			buffer.put_u8(b'\n');
		}

		if i % chunk_size == 0 {
			let mut to_submit = BytesMut::with_capacity(buffer.capacity());
			std::mem::swap(&mut to_submit, &mut buffer);
			sink.post_raw(
				"covid",
				None,
				None,
				precision,
				to_submit.freeze(),
			)?;
			pm.update(i+1);
		}
	}
	if buffer.len() > 0 {
		sink.post_raw(
			"covid",
			None,
			None,
			precision,
			buffer.freeze(),
		)?;
	}
	pm.finish();
	Ok(())
}

pub fn env_client() -> influxdb::Client {
	let user = env::var("INFLUXDB_USER");
	let pass = env::var("INFLUXDB_PASSWORD");
	let auth = match (user, pass) {
		(Ok(username), Ok(password)) => influxdb::Auth::HTTP{
			username,
			password
		},
		(Ok(_), Err(e)) | (Err(e), Ok(_)) => panic!("failed to read env for INFLUXDB_USER/INFLUXDB_PASSWORD: {}", e),
		(Err(_), Err(_)) => influxdb::Auth::None,
	};
	influxdb::Client::new(
		env::var("INFLUXDB_URL").unwrap_or("http://127.0.0.1:8086".into()),
		auth,
	)
}
