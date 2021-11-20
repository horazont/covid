use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::path::Path;
use std::sync::Arc;

use smartstring::alias::{String as SmartString};

use chrono::NaiveDate;

use csv;

use covid;
use covid::{StateId, DistrictId, DistrictInfo, InfectionRecord, Counters, FullCaseKey, StepMeter, CountMeter, TtySink, global_start_date, naive_today, DiffRecord, CounterGroup, SubmittableCounterGroup, Submittable, GeoCaseKey, ProgressSink, ICULoadRecord};


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


struct ParboiledCaseData {
	pub cases_by_pub: Counters<FullCaseKey>,
	pub case_delay_total: Counters<FullCaseKey>,
	pub deaths_by_pub: Counters<FullCaseKey>,
	pub recovered_by_pub: Counters<FullCaseKey>,
}

impl ParboiledCaseData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			cases_by_pub: Counters::new(start, end),
			case_delay_total: Counters::new(start, end),
			deaths_by_pub: Counters::new(start, end),
			recovered_by_pub: Counters::new(start, end),
		}
	}

	fn submit(
			&mut self,
			district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
			rec: &DiffRecord)
	{
		let district_info = district_map.get(&rec.district_id).expect("unknown district");
		let k = (district_info.state.id, rec.district_id, rec.age_group, rec.sex);
		let ref_index = self.cases_by_pub.date_index(rec.date).expect("date out of range");
		if rec.cases > 0 {
			self.cases_by_pub.get_or_create(k)[ref_index] += rec.cases;
			self.case_delay_total.get_or_create(k)[ref_index] += rec.delay_total;
		}
		if rec.deaths > 0 {
			self.deaths_by_pub.get_or_create(k)[ref_index] += rec.deaths;
		}
		if rec.recovered > 0 {
			self.recovered_by_pub.get_or_create(k)[ref_index] += rec.recovered;
		}
	}
}


struct CookedCaseData<T: Hash + Clone + Eq> {
	pub cases_by_pub: CounterGroup<T>,
	pub case_delay_total: Counters<T>,
	pub cases_by_ref: CounterGroup<T>,
	pub cases_by_report: CounterGroup<T>,
	pub deaths: CounterGroup<T>,
	pub deaths_by_pub: CounterGroup<T>,
	pub recovered: CounterGroup<T>,
}

impl CookedCaseData<FullCaseKey> {
	fn cook(raw: RawCaseData, parboiled: ParboiledCaseData) -> Self {
		Self{
			cases_by_pub: CounterGroup::from_d1(parboiled.cases_by_pub),
			case_delay_total: parboiled.case_delay_total,
			cases_by_ref: CounterGroup::from_d1(raw.cases_by_ref),
			cases_by_report: CounterGroup::from_d1(raw.cases_by_report),
			deaths: CounterGroup::from_d1(raw.deaths),
			deaths_by_pub: CounterGroup::from_d1(parboiled.deaths_by_pub),
			recovered: CounterGroup::from_d1(raw.recovered),
		}
	}
}

impl<T: Hash + Clone + Eq> CookedCaseData<T> {
	pub fn rekeyed<U: Hash + Clone + Eq, F: Fn(&T) -> U>(&self, f: F) -> CookedCaseData<U> {
		CookedCaseData::<U>{
			cases_by_pub: self.cases_by_pub.rekeyed(&f),
			case_delay_total: self.case_delay_total.rekeyed(&f),
			cases_by_ref: self.cases_by_ref.rekeyed(&f),
			cases_by_report: self.cases_by_report.rekeyed(&f),
			deaths: self.deaths.rekeyed(&f),
			deaths_by_pub: self.deaths_by_pub.rekeyed(&f),
			recovered: self.recovered.rekeyed(&f),
		}
	}
}


struct SubmittableCaseData<T: Hash + Clone + Eq> {
	pub cases_by_pub: SubmittableCounterGroup<T>,
	pub case_delay_total: Submittable<T>,
	pub cases_by_ref: SubmittableCounterGroup<T>,
	pub cases_by_report: SubmittableCounterGroup<T>,
	pub deaths: SubmittableCounterGroup<T>,
	pub deaths_by_pub: SubmittableCounterGroup<T>,
	pub recovered: SubmittableCounterGroup<T>,
}

impl<T: Hash + Clone + Eq> From<CookedCaseData<T>> for SubmittableCaseData<T> {
	fn from(other: CookedCaseData<T>) -> Self {
		Self{
			cases_by_pub: other.cases_by_pub.into(),
			case_delay_total: other.case_delay_total.into(),
			cases_by_ref: other.cases_by_ref.into(),
			cases_by_report: other.cases_by_report.into(),
			deaths: other.deaths.into(),
			deaths_by_pub: other.deaths_by_pub.into(),
			recovered: other.recovered.into(),
		}
	}
}


struct ICULoadData {
	pub curr_covid_cases: Counters<GeoCaseKey>,
	pub curr_covid_cases_invasive: Counters<GeoCaseKey>,
	pub curr_beds_free: Counters<GeoCaseKey>,
	pub curr_beds_in_use: Counters<GeoCaseKey>,
}

impl ICULoadData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			curr_covid_cases: Counters::new(start, end),
			curr_covid_cases_invasive: Counters::new(start, end),
			curr_beds_free: Counters::new(start, end),
			curr_beds_in_use: Counters::new(start, end),
		}
	}
}


fn stream_data<K: Hash + Clone + Eq>(
		sink: &covid::influxdb::Client,
		measurement: &str,
		tags: Vec<SmartString>,
		keyset: &[(&K, Vec<SmartString>)],
		data: &SubmittableCaseData<K>,
		population: Option<&covid::Submittable<K>>,
		) -> Result<(), covid::influxdb::Error>
{
	let mut fields = vec![
		"cases_rep_cum".into(),
		"cases_rep_d1".into(),
		"cases_rep_d7".into(),
		"cases_rep_d7s7".into(),
		"cases_ref_cum".into(),
		"cases_ref_d1".into(),
		"cases_ref_d7".into(),
		"cases_ref_d7s7".into(),
		"cases_pub_cum".into(),
		"cases_pub_d1".into(),
		"cases_pub_d7".into(),
		"cases_pub_d7s7".into(),
		"cases_pub_delay".into(),
		"deaths_ref_cum".into(),
		"deaths_ref_d1".into(),
		"deaths_ref_d7".into(),
		"deaths_ref_d7s7".into(),
		"deaths_pub_cum".into(),
		"deaths_pub_d1".into(),
		"deaths_pub_d7".into(),
		"deaths_pub_d7s7".into(),
		"recovered_ref_cum".into(),
		"recovered_ref_d1".into(),
		"recovered_ref_d7".into(),
		"recovered_ref_d7s7".into(),
	];

	let mut vecs = vec![
		&data.cases_by_report.cum,
		&data.cases_by_report.d1,
		&data.cases_by_report.d7,
		&data.cases_by_report.d7s7,
		&data.cases_by_ref.cum,
		&data.cases_by_ref.d1,
		&data.cases_by_ref.d7,
		&data.cases_by_ref.d7s7,
		&data.cases_by_pub.cum,
		&data.cases_by_pub.d1,
		&data.cases_by_pub.d7,
		&data.cases_by_pub.d7s7,
		&data.case_delay_total,
		&data.deaths.cum,
		&data.deaths.d1,
		&data.deaths.d7,
		&data.deaths.d7s7,
		&data.deaths_by_pub.cum,
		&data.deaths_by_pub.d1,
		&data.deaths_by_pub.d7,
		&data.deaths_by_pub.d7s7,
		&data.recovered.cum,
		&data.recovered.d1,
		&data.recovered.d7,
		&data.recovered.d7s7,
	];
	if let Some(population) = population {
		vecs.push(&population);
		fields.push("population".into());
	}

	covid::stream(
		sink,
		&mut TtySink::stdout(),
		measurement,
		tags,
		fields,
		keyset,
		&vecs[..],
	)
}


fn load_diff_data<'s, P: AsRef<Path>, S: ProgressSink>(
		s: &'s mut S,
		p: P,
		district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
		cases: &mut ParboiledCaseData
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: DiffRecord = row?;
		cases.submit(district_map, &rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}


fn load_case_data<'s, P: AsRef<Path>, S: ProgressSink>(
		s: &'s mut S,
		p: P,
		district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
		cases: &mut RawCaseData
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		cases.submit(district_map, &rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}


fn load_divi_load_data<P: AsRef<Path>>(p: P, data: &mut ICULoadData) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	for row in r.deserialize() {
		let rec: ICULoadRecord = row?;
		let index = data.curr_covid_cases.date_index(rec.date).expect("date out of range");
		let k = (rec.state_id, rec.district_id);
		data.curr_covid_cases.get_or_create(k)[index] = rec.current_covid_cases as u64;
		data.curr_covid_cases_invasive.get_or_create(k)[index] = rec.current_covid_cases_invasive_ventilation as u64;
		data.curr_beds_free.get_or_create(k)[index] = rec.beds_free as u64;
		data.curr_beds_in_use.get_or_create(k)[index] = rec.beds_in_use as u64;
	}
	Ok(())
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let cases = &argv[1];
	let districts = &argv[2];
	let difffile = &argv[3];
	let (states, districts) = {
		let mut r = std::fs::File::open(districts)?;
		covid::load_rki_districts(&mut r)?
	};
	let start = global_start_date();
	let end = naive_today();

	println!("loading population data ...");
	let mut population = covid::Counters::<(StateId, DistrictId)>::new(start, end);
	for district in districts.values() {
		let k = (district.state.id, district.id);
		population.get_or_create(k).fill(district.population);
	}
	let population: Submittable<_> = population.into();

	let mut raw_counters = RawCaseData::new(start, end);
	println!("loading case data ...");
	load_case_data(&mut TtySink::stdout(), cases, &districts, &mut raw_counters)?;

	let mut diff_counters = ParboiledCaseData::new(start, end);
	println!("loading diff data ...");
	load_diff_data(&mut TtySink::stdout(), difffile, &districts, &mut diff_counters)?;

	println!("crunching ...");
	let counters = CookedCaseData::cook(raw_counters, diff_counters);

	let client = covid::influxdb::Client::new("http://127.0.0.1:8086".into(), covid::influxdb::Auth::None);

	{
		println!("preparing rki_data_v1_geo ...");

		let data: SubmittableCaseData<_> = counters.rekeyed(|(state_id, district_id, _, _)| {
			(*state_id, *district_id)
		}).into();
		let mut keys = covid::joined_keyset_ref!(
			_,
			&data.cases_by_report.cum,
			&data.cases_by_ref.cum,
			&data.cases_by_pub.cum,
			&data.deaths.cum,
			&data.deaths_by_pub.cum,
			&data.recovered.cum
		);
		let keys: Vec<_> = keys.drain().map(|k| {
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
			&data,
			Some(&population),
		)?;
	}

	{
		println!("preparing rki_data_v1_demo ...");

		let data: SubmittableCaseData<_> = counters.rekeyed(|(state_id, _, ag, s)| {
			(*state_id, *ag, *s)
		}).into();
		drop(counters);
		let mut keys = covid::joined_keyset_ref!(
			_,
			&data.cases_by_report.cum,
			&data.cases_by_ref.cum,
			&data.cases_by_pub.cum,
			&data.deaths.cum,
			&data.deaths_by_pub.cum,
			&data.recovered.cum
		);
		let keys: Vec<_> = keys.drain().map(|k| {
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
			&data,
			None,
		)?;
	}

	Ok(())
}
