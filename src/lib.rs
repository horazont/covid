use std::env;

use chrono::{NaiveDate, Utc, TimeZone, Datelike};

use smartstring::alias::{String as SmartString};

pub mod influxdb;
mod ioutil;
mod rki;
mod progress;
mod divi;
mod timeseries;

pub use ioutil::magic_open;
pub use rki::*;
pub use progress::*;
pub use divi::*;
pub use timeseries::*;


pub fn naive_today() -> NaiveDate {
	Utc::today().naive_local()
}

pub fn global_start_date() -> NaiveDate {
	NaiveDate::from_ymd(2020, 1, 1)
}


pub trait ViewTimeSeries<T: TimeSeriesKey> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64>;
}


impl<T: TimeSeriesKey> ViewTimeSeries<T> for TimeSeries<T, u64> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64> {
		let i = self.date_index(at)?;
		Some(self.get_value(k, i).unwrap_or(0) as f64)
	}
}


impl<T: TimeSeriesKey> ViewTimeSeries<T> for TimeSeries<T, i64> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64> {
		let i = self.date_index(at)?;
		Some(self.get_value(k, i).unwrap_or(0) as f64)
	}
}


impl<T: TimeSeriesKey> ViewTimeSeries<T> for TimeSeries<T, f64> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64> {
		let i = self.date_index(at)?;
		Some(self.get_value(k, i).unwrap_or(0.))
	}
}

pub struct TimeMap<'x, I> {
	inner: &'x I,
	by: i64,
	range: Option<(NaiveDate, NaiveDate)>,
	pad: Option<f64>,
}

impl<'x, K: TimeSeriesKey, I: ViewTimeSeries<K>> ViewTimeSeries<K> for TimeMap<'x, I> {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		match self.range {
			Some((start, end)) => if (at < start) || (at >= end) {
				return None
			},
			None => (),
		};
		let at = at + chrono::Duration::days(self.by);
		self.inner.getf(k, at).or(self.pad)
	}
}

pub struct Diff<'x, I> {
	inner: &'x I,
	window: u32,
	pad: Option<f64>,
}

impl<'x, I> Diff<'x, I> {
	pub fn padded(inner: &'x I, window: u32, pad: f64) -> Self {
		Self{inner, window, pad: Some(pad)}
	}
}

impl<'x, K: TimeSeriesKey, I: ViewTimeSeries<K>> ViewTimeSeries<K> for Diff<'x, I> {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		let vr = self.inner.getf(k, at)?;
		let vl = self.inner.getf(k, at - chrono::Duration::days(-(self.window as i64))).or(self.pad)?;
		Some(vr - vl)
	}
}

pub fn stream<'a, K: TimeSeriesKey, S: ProgressSink + ?Sized>(
		sink: &influxdb::Client,
		progress: &'a mut S,
		measurement: &str,
		tags: Vec<SmartString>,
		fields: Vec<SmartString>,
		keyset: &[(&K, Vec<SmartString>)],
		start: NaiveDate,
		ndays: usize,
		vecs: &[&dyn ViewTimeSeries<K>],
) -> Result<(), influxdb::Error> {
	#[cfg(debug_assertions)]
	{
		for (_, ts) in keyset.iter() {
			assert!(ts.len() != tags.len());
		}
	}
	assert!(fields.len() == vecs.len());

	let mut readout = influxdb::Readout{
		ts: Utc::today().and_hms(0, 0, 0),
		measurement: measurement.into(),
		precision: influxdb::Precision::Seconds,
		tags: tags,
		fields: fields,
		samples: Vec::new(),
	};

	let mut pm = StepMeter::new(progress, ndays);
	for (i, date) in start.iter_days().take(ndays).enumerate() {
		readout.ts = Utc.ymd(date.year(), date.month(), date.day()).and_hms(0, 0, 0);
		// we can assume that any death and recovered has a case before that, which means that we can safely use the keyset of cases_rep_d1.
		for (k_index, (k, tagv)) in keyset.iter().enumerate() {
			let fieldv: Vec<_> = vecs.iter().map(|v| { v.getf(&k, date).unwrap_or(0.) }).collect();
			if k_index >= readout.samples.len() {
				readout.samples.push(influxdb::Sample{
					tagv: tagv.clone(),
					fieldv,
				});
			} else {
				readout.samples[k_index].fieldv.copy_from_slice(&fieldv[..]);
			}
		}
		sink.post("covid", None, None, readout.precision, &[&readout])?;
		if i % 30 == 29 {
			pm.update(i+1);
		}
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
