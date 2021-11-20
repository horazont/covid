use std::hash::Hash;

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


pub struct CounterGroup<T: Hash + Eq + Clone> {
	cum: Counters<T>,
	d1: Counters<T>,
	d7: Counters<T>,
	d7s7: Counters<T>,
}

impl<T: Hash + Eq + Clone> CounterGroup<T> {
	pub fn from_d1(d1: Counters<T>) -> Self {
		let mut cum = d1.clone();
		cum.cumsum();
		let mut d7 = cum.clone();
		d7.diff(7);
		let mut d7s7 = d7.clone();
		d7s7.shift_fwd(7);
		Self{
			cum,
			d1,
			d7,
			d7s7,
		}
	}

	pub fn rekeyed<U: Hash + Clone + Eq, F: Fn(&T) -> U>(&self, f: F) -> CounterGroup<U> {
		CounterGroup::<U>{
			cum: self.cum.rekeyed(&f),
			d1: self.d1.rekeyed(&f),
			d7: self.d7.rekeyed(&f),
			d7s7: self.d7s7.rekeyed(&f),
		}
	}

	pub fn cum(&self) -> &Counters<T> {
		&self.cum
	}

	pub fn d1(&self) -> &Counters<T> {
		&self.d1
	}

	pub fn d7(&self) -> &Counters<T> {
		&self.d7
	}

	pub fn d7s7(&self) -> &Counters<T> {
		&self.d7s7
	}
}


pub struct SubmittableCounterGroup<T: Hash + Eq + Clone> {
	pub cum: Submittable<T>,
	pub d1: Submittable<T>,
	pub d7: Submittable<T>,
	pub d7s7: Submittable<T>,
}

impl<T: Hash + Eq + Clone> From<CounterGroup<T>> for SubmittableCounterGroup<T> {
	fn from(other: CounterGroup<T>) -> SubmittableCounterGroup<T> {
		Self{
			cum: other.cum.into(),
			d1: other.d1.into(),
			d7: other.d7.into(),
			d7s7: other.d7s7.into(),
		}
	}
}


pub fn stream<'a, K: Hash + Eq + Clone, S: ProgressSink>(
		sink: &influxdb::Client,
		progress: &'a mut S,
		measurement: &str,
		tags: Vec<SmartString>,
		fields: Vec<SmartString>,
		keyset: &[(&K, Vec<SmartString>)],
		vecs: &[&Submittable<K>],
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

	let ref_vec = &vecs[0];
	let n = ref_vec.len();
	let mut pm = StepMeter::new(progress, n);
	for i in 0..n {
		let nds = ref_vec.index_date(i as i64).unwrap();
		readout.ts = Utc.ymd(nds.year(), nds.month(), nds.day()).and_hms(0, 0, 0);
		// we can assume that any death and recovered has a case before that, which means that we can safely use the keyset of cases_rep_d1.
		for (k_index, (k, tagv)) in keyset.iter().enumerate() {
			let fieldv: Vec<_> = vecs.iter().map(|v| { v.get_value(&k, i).unwrap_or(0.0)}).collect();
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
