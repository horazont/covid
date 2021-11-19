use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use smartstring::alias::{String as SmartString};

use chrono::{NaiveDate, Utc, TimeZone, Datelike};

use csv;

use covid::{StateId, DistrictId, DistrictInfo, InfectionRecord, Counters, FullCaseKey, ProgressMeter, ProgressSink};


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


struct RawCaseData {
	pub cases_by_ref: Counters<FullCaseKey>,
	pub cases_by_report: Counters<FullCaseKey>,
	pub deaths: Counters<FullCaseKey>,
	pub recovered: Counters<FullCaseKey>,
}

impl RawCaseData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			cases_by_ref: Counters::new(start, end),
			cases_by_report: Counters::new(start, end),
			deaths: Counters::new(start, end),
			recovered: Counters::new(start, end),
		}
	}

	fn submit(
			&mut self,
			district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
			rec: &InfectionRecord)
	{
		let case_count = if rec.case.valid() {
			rec.case_count
		} else {
			0
		};
		assert!(case_count >= 0);
		let death_count = if rec.death.valid() {
			rec.death_count
		} else {
			0
		};
		assert!(death_count >= 0);
		let recovered_count = if rec.recovered.valid() {
			rec.recovered_count
		} else {
			0
		};
		assert!(recovered_count >= 0);

		let district_info = district_map.get(&rec.district_id).expect("unknown district");
		let k = (district_info.state.id, rec.district_id, rec.age_group, rec.sex);
		let ref_index = self.cases_by_ref.date_index(rec.reference_date).expect("date out of range");
		if case_count > 0 {
			self.cases_by_ref.get_or_create(k)[ref_index] += case_count as u64;
			let report_index = self.cases_by_report.date_index(rec.report_date).expect("date out of range");
			self.cases_by_report.get_or_create(k)[report_index] += case_count as u64;
		}
		if death_count > 0 {
			self.deaths.get_or_create(k)[ref_index] += death_count as u64;
		}
		if recovered_count > 0 {
			self.recovered.get_or_create(k)[ref_index] += recovered_count as u64;
		}
	}
}

struct CookedCaseData<T: Hash + Clone + Eq> {
	pub cases_by_ref: CounterGroup<T>,
	pub cases_by_report: CounterGroup<T>,
	pub deaths: CounterGroup<T>,
	pub recovered: CounterGroup<T>,
}

impl CookedCaseData<FullCaseKey> {
	fn cook(raw: RawCaseData) -> Self {
		Self{
			cases_by_ref: CounterGroup::from_d1(raw.cases_by_ref),
			cases_by_report: CounterGroup::from_d1(raw.cases_by_report),
			deaths: CounterGroup::from_d1(raw.deaths),
			recovered: CounterGroup::from_d1(raw.recovered),
		}
	}
}

impl<T: Hash + Clone + Eq> CookedCaseData<T> {
	pub fn rekeyed<U: Hash + Clone + Eq, F: Fn(&T) -> U>(&self, f: F) -> CookedCaseData<U> {
		CookedCaseData::<U>{
			cases_by_ref: self.cases_by_ref.rekeyed(&f),
			cases_by_report: self.cases_by_report.rekeyed(&f),
			deaths: self.deaths.rekeyed(&f),
			recovered: self.recovered.rekeyed(&f),
		}
	}
}

fn stream_data<K: Hash + Clone + Eq>(
		sink: &covid::influxdb::Client,
		measurement: &str,
		tags: Vec<SmartString>,
		keyset: &[(&K, Vec<SmartString>)],
		data: &CookedCaseData<K>,
		population: Option<&covid::Counters<K>>,
		) -> Result<(), covid::influxdb::Error>
{
	#[cfg(debug_assertions)]
	{
		for (_, ts) in keyset.iter() {
			assert!(ts.len() != tags.len());
		}
	}

	let mut readout = covid::influxdb::Readout{
		ts: Utc::today().and_hms(0, 0, 0),
		measurement: measurement.into(),
		precision: covid::influxdb::Precision::Seconds,
		tags: tags,
		fields: vec![
			"cases_rep_cum".into(),
			"cases_rep_d1".into(),
			"cases_rep_d7".into(),
			"cases_rep_d7s7".into(),
			"cases_ref_cum".into(),
			"cases_ref_d1".into(),
			"cases_ref_d7".into(),
			"cases_ref_d7s7".into(),
			"deaths_ref_cum".into(),
			"deaths_ref_d1".into(),
			"deaths_ref_d7".into(),
			"deaths_ref_d7s7".into(),
			"recovered_ref_cum".into(),
			"recovered_ref_d1".into(),
			"recovered_ref_d7".into(),
			"recovered_ref_d7s7".into(),
			"population".into(),
		],
		samples: Vec::new(),
	};

	let src_vecs = [
		&data.cases_by_report.cum(),
		&data.cases_by_report.d1(),
		&data.cases_by_report.d7(),
		&data.cases_by_report.d7s7(),
		&data.cases_by_ref.cum(),
		&data.cases_by_ref.d1(),
		&data.cases_by_ref.d7(),
		&data.cases_by_ref.d7s7(),
		&data.deaths.cum(),
		&data.deaths.d1(),
		&data.deaths.d7(),
		&data.deaths.d7s7(),
		&data.recovered.cum(),
		&data.recovered.d1(),
		&data.recovered.d7(),
		&data.recovered.d7s7(),
	];

	let ref_vec = &data.cases_by_report.cum();
	let n = ref_vec.len();
	let mut pm = ProgressMeter::start(Some(n));
	for i in 0..n {
		let nds = ref_vec.index_date(i as i64).unwrap();
		readout.ts = Utc.ymd(nds.year(), nds.month(), nds.day()).and_hms(0, 0, 0);
		// we can assume that any death and recovered has a case before that, which means that we can safely use the keyset of cases_rep_d1.
		for (k_index, (k, tagv)) in keyset.iter().enumerate() {
			let mut fieldv: Vec<_> = src_vecs.iter().map(|v| { v.get_value(&k, i).unwrap_or(0) as f64}).collect();
			if let Some(population) = population {
				fieldv.push(population.get_value(&k, i).unwrap_or(0) as f64);
			}
			if k_index >= readout.samples.len() {
				readout.samples.push(covid::influxdb::Sample{
					tagv: tagv.clone(),
					fieldv: (&fieldv[..]).to_vec(),
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
	pm.finish(Some(n));
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let cases = &argv[1];
	let districts = &argv[2];
	let (states, districts) = {
		let mut r = std::fs::File::open(districts)?;
		covid::load_rki_districts(&mut r)?
	};
	let start = NaiveDate::from_ymd(2020, 1, 1);
	let end = NaiveDate::from_ymd(2021, 11, 18);

	println!("loading population data ...");
	let mut population = covid::Counters::<(StateId, DistrictId)>::new(start, end);
	for district in districts.values() {
		let k = (district.state.id, district.id);
		population.get_or_create(k).fill(district.population);
	}

	println!("processing input data ...");
	let mut raw_counters = RawCaseData::new(start, end);
	let mut fr = covid::magic_open(cases)?;
	let mut r = csv::Reader::from_reader(&mut fr);
	let mut pm = ProgressMeter::start(None);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row.unwrap();
		raw_counters.submit(&districts, &rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i;
	}
	pm.finish(Some(n));
	println!("crunching ...");
	let counters = CookedCaseData::cook(raw_counters);

	let client = covid::influxdb::Client::new("http://127.0.0.1:8086".into(), covid::influxdb::Auth::None);

	{
		println!("preparing rki_data_v1_geo ...");

		let counters = counters.rekeyed(|(state_id, district_id, _, _)| {
			(*state_id, *district_id)
		});
		let keys: Vec<_> = counters.cases_by_report.cum().keys().map(|k| {
			let state_id = k.0;
			let district_id = k.1;
			let state_name = &states.get(&state_id).unwrap().name;
			let district_name = &districts.get(&district_id).unwrap().name;
			let tagv: Vec<SmartString> = vec![
				state_name.into(),
				district_name.into(),
			];
			(k, tagv)
		}).collect();

		println!("streaming rki_data_v1_geo ...");

		stream_data(
			&client,
			"rki_data_v1_geo",
			vec![
				"state".into(),
				"district".into(),
			],
			&keys,
			&counters,
			Some(&population),
		)?;
	}

	{
		println!("preparing rki_data_v1_demo ...");

		let counters = counters.rekeyed(|(state_id, _, ag, s)| {
			(*state_id, *ag, *s)
		});
		let keys: Vec<_> = counters.cases_by_report.cum().keys().map(|k| {
			let state_id = k.0;
			let state_name = &states.get(&state_id).unwrap().name;
			let tagv: Vec<SmartString> = vec![
				state_name.into(),
				k.1.to_string().into(),
				k.2.to_string().into(),
			];
			(k, tagv)
		}).collect();

		println!("streaming rki_data_v1_demo ...");

		stream_data(
			&client,
			"rki_data_v1_demo",
			vec![
				"state".into(),
				"age".into(),
				"sex".into(),
			],
			&keys,
			&counters,
			None,
		)?;
	}

	Ok(())
}
