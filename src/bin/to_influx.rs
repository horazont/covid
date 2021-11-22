use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use smartstring::alias::{String as SmartString};

use chrono::NaiveDate;

use csv;

use covid;
use covid::{StateId, DistrictId, DistrictInfo, InfectionRecord, Counters, FullCaseKey, CountMeter, global_start_date, naive_today, DiffRecord, CounterGroup, SubmittableCounterGroup, Submittable, GeoCaseKey, ProgressSink, ICULoadRecord, VaccinationKey, VaccinationRecord, VaccinationLevel, HospitalizationRecord, AgeGroup, TimeSeriesKey};


static GEO_MEASUREMENT_NAME: &'static str = "data_v2_geo";
static GEO_LIGHT_MEASUREMENT_NAME: &'static str = "data_v2_geo_light";
static DEMO_MEASUREMENT_NAME: &'static str = "data_v2_demo";
// static DEMO_LIGHT_MEASUREMENT_NAME: &'static str = "data_v2_demo_light";


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

	fn remapped<F: Fn(&FullCaseKey) -> Option<FullCaseKey>>(&self, f: F) -> RawCaseData {
		RawCaseData{
			cases_by_ref: self.cases_by_ref.rekeyed(&f),
			cases_by_report: self.cases_by_report.rekeyed(&f),
			deaths: self.deaths.rekeyed(&f),
			recovered: self.recovered.rekeyed(&f),
		}
	}
}


struct ParboiledCaseData {
	pub cases_by_pub: Counters<FullCaseKey>,
	pub case_delay_total: Counters<FullCaseKey>,
	pub cases_delayed: Counters<FullCaseKey>,
	pub late_cases: Counters<FullCaseKey>,
	pub deaths_by_pub: Counters<FullCaseKey>,
	pub recovered_by_pub: Counters<FullCaseKey>,
}

impl ParboiledCaseData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			cases_by_pub: Counters::new(start, end),
			cases_delayed: Counters::new(start, end),
			case_delay_total: Counters::new(start, end),
			late_cases: Counters::new(start, end),
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
		self.cases_by_pub.get_or_create(k)[ref_index] += rec.cases;
		self.case_delay_total.get_or_create(k)[ref_index] += rec.delay_total;
		self.cases_delayed.get_or_create(k)[ref_index] += rec.cases_delayed;
		self.late_cases.get_or_create(k)[ref_index] += rec.late_cases;
		self.deaths_by_pub.get_or_create(k)[ref_index] += rec.deaths;
		self.recovered_by_pub.get_or_create(k)[ref_index] += rec.recovered;
	}

	fn remapped<F: Fn(&FullCaseKey) -> Option<FullCaseKey>>(&self, f: F) -> ParboiledCaseData {
		ParboiledCaseData{
			cases_by_pub: self.cases_by_pub.rekeyed(&f),
			case_delay_total: self.case_delay_total.rekeyed(&f),
			cases_delayed: self.cases_delayed.rekeyed(&f),
			late_cases: self.late_cases.rekeyed(&f),
			deaths_by_pub: self.deaths_by_pub.rekeyed(&f),
			recovered_by_pub: self.recovered_by_pub.rekeyed(&f),
		}
	}
}


struct CookedCaseData<T: TimeSeriesKey> {
	pub cases_by_pub: CounterGroup<T>,
	pub case_delay_total: Counters<T>,
	pub cases_by_ref: CounterGroup<T>,
	pub cases_by_report: CounterGroup<T>,
	pub deaths: CounterGroup<T>,
	pub deaths_by_pub: CounterGroup<T>,
	pub recovered: CounterGroup<T>,
	pub recovered_by_pub: CounterGroup<T>,
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
			recovered_by_pub: CounterGroup::from_d1(parboiled.recovered_by_pub),
		}
	}
}

impl<T: TimeSeriesKey> CookedCaseData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CookedCaseData<U> {
		CookedCaseData::<U>{
			cases_by_pub: self.cases_by_pub.rekeyed(&f),
			case_delay_total: self.case_delay_total.rekeyed(&f),
			cases_by_ref: self.cases_by_ref.rekeyed(&f),
			cases_by_report: self.cases_by_report.rekeyed(&f),
			deaths: self.deaths.rekeyed(&f),
			deaths_by_pub: self.deaths_by_pub.rekeyed(&f),
			recovered: self.recovered.rekeyed(&f),
			recovered_by_pub: self.recovered_by_pub.rekeyed(&f),
		}
	}

	// We may at some point do something about berlin, see the XXX below.
	#[allow(dead_code)]
	pub fn synthesize<U: TimeSeriesKey>(&mut self, kin: &[&T], kout: T) {
		self.cases_by_pub.synthesize(kin, kout.clone());
		self.case_delay_total.synthesize(kin, kout.clone());
		self.cases_by_ref.synthesize(kin, kout.clone());
		self.cases_by_report.synthesize(kin, kout.clone());
		self.deaths.synthesize(kin, kout.clone());
		self.deaths_by_pub.synthesize(kin, kout.clone());
		self.recovered.synthesize(kin, kout.clone());
		self.recovered_by_pub.synthesize(kin, kout.clone());
	}
}


struct SubmittableCaseData<T: TimeSeriesKey> {
	pub cases_by_pub: SubmittableCounterGroup<T>,
	pub case_delay_total: Submittable<T>,
	pub cases_by_ref: SubmittableCounterGroup<T>,
	pub cases_by_report: SubmittableCounterGroup<T>,
	pub deaths: SubmittableCounterGroup<T>,
	pub deaths_by_pub: SubmittableCounterGroup<T>,
	pub recovered: SubmittableCounterGroup<T>,
	pub recovered_by_pub: SubmittableCounterGroup<T>,
}

impl<T: TimeSeriesKey> From<CookedCaseData<T>> for SubmittableCaseData<T> {
	fn from(other: CookedCaseData<T>) -> Self {
		Self{
			cases_by_pub: other.cases_by_pub.into(),
			case_delay_total: other.case_delay_total.into(),
			cases_by_ref: other.cases_by_ref.into(),
			cases_by_report: other.cases_by_report.into(),
			deaths: other.deaths.into(),
			deaths_by_pub: other.deaths_by_pub.into(),
			recovered: other.recovered.into(),
			recovered_by_pub: other.recovered_by_pub.into(),
		}
	}
}


#[derive(Clone)]
struct ICULoadData<T: TimeSeriesKey> {
	pub curr_covid_cases: Counters<T>,
	pub curr_covid_cases_invasive: Counters<T>,
	pub curr_beds_free: Counters<T>,
	pub curr_beds_in_use: Counters<T>,
}

impl ICULoadData<GeoCaseKey> {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			curr_covid_cases: Counters::new(start, end),
			curr_covid_cases_invasive: Counters::new(start, end),
			curr_beds_free: Counters::new(start, end),
			curr_beds_in_use: Counters::new(start, end),
		}
	}
}

impl<T: TimeSeriesKey> ICULoadData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> ICULoadData<U> {
		ICULoadData::<U>{
			curr_covid_cases: self.curr_covid_cases.rekeyed(&f),
			curr_covid_cases_invasive: self.curr_covid_cases_invasive.rekeyed(&f),
			curr_beds_free: self.curr_beds_free.rekeyed(&f),
			curr_beds_in_use: self.curr_beds_in_use.rekeyed(&f),
		}
	}
}


struct SubmittableICULoadData<T: TimeSeriesKey> {
	pub curr_covid_cases: Submittable<T>,
	pub curr_covid_cases_invasive: Submittable<T>,
	pub curr_beds_free: Submittable<T>,
	pub curr_beds_in_use: Submittable<T>,
}

impl<T: TimeSeriesKey> From<ICULoadData<T>> for SubmittableICULoadData<T> {
	fn from(other: ICULoadData<T>) -> Self {
		Self{
			curr_covid_cases: other.curr_covid_cases.into(),
			curr_covid_cases_invasive: other.curr_covid_cases_invasive.into(),
			curr_beds_free: other.curr_beds_free.into(),
			curr_beds_in_use: other.curr_beds_in_use.into(),
		}
	}
}


struct RawVaccinationData {
	pub first_vacc: Counters<VaccinationKey>,
	pub basic_vacc: Counters<VaccinationKey>,
	pub full_vacc: Counters<VaccinationKey>,
}

impl RawVaccinationData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			first_vacc: Counters::new(start, end),
			basic_vacc: Counters::new(start, end),
			full_vacc: Counters::new(start, end),
		}
	}

	fn submit(
			&mut self,
			district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
			rec: &VaccinationRecord)
	{
		let mapped_district_id = match rec.district_id.0 {
			// Bundesfoo, unmap
			Some(district_id) if district_id == 17000 => None,
			v => v,
		};
		let state_id = match mapped_district_id {
			Some(district_id) => {
				let district_info = district_map.get(&district_id).expect("district not found");
				Some(district_info.state.id)
			},
			None => None,
		};
		let k = (state_id, mapped_district_id, rec.age_group);
		let ts = match rec.level {
			VaccinationLevel::First => &mut self.first_vacc,
			VaccinationLevel::Basic => &mut self.basic_vacc,
			VaccinationLevel::Full => &mut self.full_vacc,
		};
		let index = ts.date_index(rec.date).expect("date out of range");
		ts.get_or_create(k)[index] += rec.count;
	}

	pub fn remapped<F: Fn(&VaccinationKey) -> Option<VaccinationKey>>(&self, f: F) -> RawVaccinationData {
		RawVaccinationData{
			first_vacc: self.first_vacc.rekeyed(&f),
			basic_vacc: self.basic_vacc.rekeyed(&f),
			full_vacc: self.full_vacc.rekeyed(&f),
		}
	}
}


struct CookedVaccinationData<T: TimeSeriesKey> {
	pub first_vacc: CounterGroup<T>,
	pub basic_vacc: CounterGroup<T>,
	pub full_vacc: CounterGroup<T>,
}

impl CookedVaccinationData<VaccinationKey> {
	fn cook(raw: RawVaccinationData) -> Self {
		Self{
			first_vacc: CounterGroup::from_d1(raw.first_vacc),
			basic_vacc: CounterGroup::from_d1(raw.basic_vacc),
			full_vacc: CounterGroup::from_d1(raw.full_vacc),
		}
	}
}

impl<T: TimeSeriesKey> CookedVaccinationData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CookedVaccinationData<U> {
		CookedVaccinationData::<U>{
			first_vacc: self.first_vacc.rekeyed(&f),
			basic_vacc: self.basic_vacc.rekeyed(&f),
			full_vacc: self.full_vacc.rekeyed(&f),
		}
	}

	// We may at some point do something about berlin, see the XXX below.
	#[allow(dead_code)]
	pub fn synthesize<U: TimeSeriesKey>(&mut self, kin: &[&T], kout: T) {
		self.first_vacc.synthesize(kin, kout.clone());
		self.basic_vacc.synthesize(kin, kout.clone());
		self.full_vacc.synthesize(kin, kout.clone());
	}
}


struct SubmittableVaccinationData<T: TimeSeriesKey> {
	pub first_vacc: SubmittableCounterGroup<T>,
	pub basic_vacc: SubmittableCounterGroup<T>,
	pub full_vacc: SubmittableCounterGroup<T>,
}

impl<T: TimeSeriesKey> From<CookedVaccinationData<T>> for SubmittableVaccinationData<T> {
	fn from(other: CookedVaccinationData<T>) -> Self {
		Self{
			first_vacc: other.first_vacc.into(),
			basic_vacc: other.basic_vacc.into(),
			full_vacc: other.full_vacc.into(),
		}
	}
}


struct RawHospitalizationData {
	pub cases_d7: Counters<(StateId, AgeGroup)>,
}

impl RawHospitalizationData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			cases_d7: Counters::new(start, end),
		}
	}

	fn submit(
			&mut self,
			rec: &HospitalizationRecord)
	{
		// sum of everything, we don't want that
		if rec.state_id == 0 {
			return
		}
		let index = match self.cases_d7.date_index(rec.date) {
			Some(i) => i,
			// hospitalization data may have today's data, which does not
			// match the publication rhythm of the data -> skip
			None => return,
		};
		let k = (rec.state_id, rec.age_group);
		self.cases_d7.get_or_create(k)[index] += rec.cases_d7;
	}
}

struct CookedHospitalizationData<T: TimeSeriesKey> {
	pub cases: CounterGroup<T>,
}

impl CookedHospitalizationData<(StateId, AgeGroup)> {
	fn cook(raw: RawHospitalizationData) -> Self {
		Self{
			cases: CounterGroup::from_d7(raw.cases_d7),
		}
	}
}

impl<T: TimeSeriesKey> CookedHospitalizationData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CookedHospitalizationData<U> {
		CookedHospitalizationData::<U>{
			cases: self.cases.rekeyed(&f),
		}
	}

	// We may at some point do something about berlin, see the XXX below.
	#[allow(dead_code)]
	pub fn synthesize<U: TimeSeriesKey>(&mut self, kin: &[&T], kout: T) {
		self.cases.synthesize(kin, kout.clone());
	}
}

struct SubmittableHospitalizationData<T: TimeSeriesKey> {
	pub cases: SubmittableCounterGroup<T>,
}

impl<T: TimeSeriesKey> From<CookedHospitalizationData<T>> for SubmittableHospitalizationData<T> {
	fn from(other: CookedHospitalizationData<T>) -> Self {
		Self{
			cases: other.cases.into(),
		}
	}
}



fn stream_data<K: TimeSeriesKey>(
		sink: &covid::influxdb::Client,
		measurement: &str,
		tags: Vec<SmartString>,
		keyset: &[(&K, Vec<SmartString>)],
		data: &SubmittableCaseData<K>,
		extra_fields: &[SmartString],
		extra_vecs: &[&Submittable<K>],
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
		"recovered_pub_cum".into(),
		"recovered_pub_d1".into(),
		"recovered_pub_d7".into(),
		"recovered_pub_d7s7".into(),
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
		&data.recovered_by_pub.cum,
		&data.recovered_by_pub.d1,
		&data.recovered_by_pub.d7,
		&data.recovered_by_pub.d7s7,
	];
	fields.extend_from_slice(extra_fields);
	vecs.extend_from_slice(extra_vecs);

	covid::stream(
		sink,
		&mut *covid::default_output(),
		measurement,
		tags,
		fields,
		keyset,
		&vecs[..],
	)
}


fn load_diff_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
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


fn load_case_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
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


fn load_divi_load_data<P: AsRef<Path>, S: ProgressSink + ?Sized>(s: &mut S, p: P, data: &mut ICULoadData<GeoCaseKey>) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: ICULoadRecord = row?;
		let index = match data.curr_covid_cases.date_index(rec.date) {
			Some(i) => i,
			// DIVI data may have today's data, which does not match the
			// publication rhythm of the data -> skip
			None => continue,
		};
		let k = (rec.state_id, rec.district_id);
		data.curr_covid_cases.get_or_create(k)[index] = rec.current_covid_cases as u64;
		data.curr_covid_cases_invasive.get_or_create(k)[index] = rec.current_covid_cases_invasive_ventilation as u64;
		data.curr_beds_free.get_or_create(k)[index] = rec.beds_free as u64;
		data.curr_beds_in_use.get_or_create(k)[index] = rec.beds_in_use as u64;
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}


fn load_vacc_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
		s: &'s mut S,
		p: P,
		district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
		data: &mut RawVaccinationData,
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: VaccinationRecord = row?;
		data.submit(district_map, &rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}


fn load_hosp_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
		s: &'s mut S,
		p: P,
		data: &mut RawHospitalizationData
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: HospitalizationRecord = match row {
			Ok(v) => v,
			// for some reason, they have NA in some cells?!
			Err(_) => continue,
		};
		data.submit(&rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}


fn remap_berlin(id: DistrictId) -> DistrictId {
	if id >= 11000 && id < 12000 {
		11000
	} else {
		id
	}
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let casefile = &argv[1];
	let districts = &argv[2];
	let difffile = &argv[3];
	let divifile = &argv[4];
	let vaccfile = &argv[5];
	let hospfile = &argv[6];
	let (states, mut districts) = {
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
	let population = population.rekeyed(|(state_id, district_id)| {
		Some((*state_id, remap_berlin(*district_id)))
	});

	// We inject berlin only later. This allows us to rekey the population above to eliminate the separate berlin districts.
	covid::inject_berlin(&states, &mut districts);

	let mut cases = RawCaseData::new(start, end);
	println!("loading case data ...");
	load_case_data(&mut *covid::default_output(), casefile, &districts, &mut cases)?;
	let cases = cases.remapped(|(state_id, district_id, mag, sex)| {
		Some((*state_id, remap_berlin(*district_id), *mag, *sex))
	});

	let mut diff_cases = ParboiledCaseData::new(start, end);
	println!("loading diff data ...");
	load_diff_data(&mut *covid::default_output(), difffile, &districts, &mut diff_cases)?;
	let diff_cases = diff_cases.remapped(|(state_id, district_id, mag, sex)| {
		Some((*state_id, remap_berlin(*district_id), *mag, *sex))
	});

	let mut icu_load = ICULoadData::new(start, end);
	println!("loading ICU data ...");
	load_divi_load_data(&mut *covid::default_output(), divifile, &mut icu_load)?;
	let icu_load = icu_load.rekeyed(|(state_id, district_id)| {
		Some((*state_id, remap_berlin(*district_id)))
	});

	let mut vacc = RawVaccinationData::new(start, end);
	println!("loading vaccination data ...");
	load_vacc_data(&mut *covid::default_output(), vaccfile, &districts, &mut vacc)?;
	let vacc = vacc.remapped(|(state_id, district_id, ag)| {
		Some((*state_id, district_id.map(remap_berlin), *ag))
	});

	let mut hosp = RawHospitalizationData::new(start, end);
	println!("loading hospitalization data ...");
	load_hosp_data(&mut *covid::default_output(), hospfile, &mut hosp)?;

	println!("crunching ...");
	let cases = CookedCaseData::cook(cases, diff_cases);
	let vacc = CookedVaccinationData::cook(vacc);
	let hosp = CookedHospitalizationData::cook(hosp);

	let client = covid::env_client();

	{
		println!("preparing {} ...", GEO_MEASUREMENT_NAME);

		let cases = cases.rekeyed(|(state_id, district_id, _, _)| {
			Some((*state_id, *district_id))
		});
		// XXX: This is dangerous and needs to be accounted for in the dashboar **carefully**, otherwise we accidentally double the numbers of berlin...
		/* let berlin = covid::find_berlin_districts(&districts);
		data.synthesize(&berlin[..], &(11, 11000)); */
		let cases: SubmittableCaseData<_> = cases.into();
		let vacc = vacc.rekeyed(|(state_id, district_id, _)| {
			// drop vaccinations without properly defined state + district
			match (state_id, district_id) {
				(Some(state_id), Some(district_id)) => Some((*state_id, *district_id)),
				_ => None,
			}
		});
		let vacc: SubmittableVaccinationData<_> = vacc.into();
		let icu_load: SubmittableICULoadData<_> = icu_load.clone().into();
		let population: Submittable<_> = population.clone().into();
		let mut keys = covid::joined_keyset_ref!(
			_,
			&cases.cases_by_report.cum,
			&cases.cases_by_ref.cum,
			&cases.cases_by_pub.cum,
			&cases.deaths.cum,
			&cases.deaths_by_pub.cum,
			&cases.recovered.cum,
			&icu_load.curr_beds_free,
			&vacc.first_vacc.cum,
			&vacc.basic_vacc.cum,
			&vacc.full_vacc.cum
		);
		let keys: Vec<_> = keys.drain().map(|k| {
			let state_id = k.0;
			let district_id = k.1;
			let state_name = &states.get(&state_id).unwrap().name;
			let district_name = match &districts.get(&district_id) {
				Some(i) => &i.name,
				None => panic!("failed to find district {} in data", district_id),
			};
			let tagv: Vec<SmartString> = vec![
				state_name.into(),
				district_name.into(),
			];
			(k, tagv)
		}).collect();

		println!("streaming {} ...",GEO_MEASUREMENT_NAME);

		stream_data(
			&client,
			GEO_MEASUREMENT_NAME,
			vec![
				"state".into(),
				"district".into(),
			],
			&keys,
			&cases,
			&[
				"population".into(),
				"icu_covid_cases".into(),
				"icu_covid_cases_invasive".into(),
				"icu_beds_free".into(),
				"icu_beds_in_use".into(),
				"vacc_first_cum".into(),
				"vacc_first_d1".into(),
				"vacc_first_d7".into(),
				"vacc_first_d7s7".into(),
				"vacc_basic_cum".into(),
				"vacc_basic_d1".into(),
				"vacc_basic_d7".into(),
				"vacc_basic_d7s7".into(),
				"vacc_full_cum".into(),
				"vacc_full_d1".into(),
				"vacc_full_d7".into(),
				"vacc_full_d7s7".into(),
			],
			&[
				&population,
				&icu_load.curr_covid_cases,
				&icu_load.curr_covid_cases_invasive,
				&icu_load.curr_beds_free,
				&icu_load.curr_beds_in_use,
				&vacc.first_vacc.cum,
				&vacc.first_vacc.d1,
				&vacc.first_vacc.d7,
				&vacc.first_vacc.d7s7,
				&vacc.basic_vacc.cum,
				&vacc.basic_vacc.d1,
				&vacc.basic_vacc.d7,
				&vacc.basic_vacc.d7s7,
				&vacc.full_vacc.cum,
				&vacc.full_vacc.d1,
				&vacc.full_vacc.d7,
				&vacc.full_vacc.d7s7,
			],
		)?;
	}

	{
		println!("preparing {} ...", GEO_LIGHT_MEASUREMENT_NAME);

		let cases = cases.rekeyed(|(state_id, _, _, _)| {
			Some(*state_id)
		});
		// XXX: This is dangerous and needs to be accounted for in the dashboar **carefully**, otherwise we accidentally double the numbers of berlin...
		/* let berlin = covid::find_berlin_districts(&districts);
		data.synthesize(&berlin[..], &(11, 11000)); */
		let cases: SubmittableCaseData<_> = cases.into();
		let vacc = vacc.rekeyed(|(state_id, district_id, _)| {
			// drop vaccinations without properly defined state + district
			match (state_id, district_id) {
				(Some(state_id), Some(_)) => Some(*state_id),
				_ => None,
			}
		});
		let vacc: SubmittableVaccinationData<_> = vacc.into();
		let icu_load: SubmittableICULoadData<_> = icu_load.rekeyed(|(state_id, _)| {
			Some(*state_id)
		}).into();
		let hosp: SubmittableHospitalizationData<_> = hosp.rekeyed(|(state_id, _)| {
			Some(*state_id)
		}).into();
		let population: Submittable<_> = population.rekeyed(|(state_id, _)| {
			Some(*state_id)
		}).into();
		let mut keys = covid::joined_keyset_ref!(
			_,
			&cases.cases_by_report.cum,
			&cases.cases_by_ref.cum,
			&cases.cases_by_pub.cum,
			&cases.deaths.cum,
			&cases.deaths_by_pub.cum,
			&cases.recovered.cum,
			&icu_load.curr_beds_free,
			&vacc.first_vacc.cum,
			&vacc.basic_vacc.cum,
			&vacc.full_vacc.cum,
			&hosp.cases.cum,
			&population
		);
		let keys: Vec<_> = keys.drain().map(|k| {
			let state_id = k;
			let state_name = &states.get(&state_id).unwrap().name;
			let tagv: Vec<SmartString> = vec![
				state_name.into(),
			];
			(k, tagv)
		}).collect();

		println!("streaming {} ...",GEO_LIGHT_MEASUREMENT_NAME);

		stream_data(
			&client,
			GEO_LIGHT_MEASUREMENT_NAME,
			vec![
				"state".into(),
			],
			&keys,
			&cases,
			&[
				"population".into(),
				"icu_covid_cases".into(),
				"icu_covid_cases_invasive".into(),
				"icu_beds_free".into(),
				"icu_beds_in_use".into(),
				"vacc_first_cum".into(),
				"vacc_first_d1".into(),
				"vacc_first_d7".into(),
				"vacc_first_d7s7".into(),
				"vacc_basic_cum".into(),
				"vacc_basic_d1".into(),
				"vacc_basic_d7".into(),
				"vacc_basic_d7s7".into(),
				"vacc_full_cum".into(),
				"vacc_full_d1".into(),
				"vacc_full_d7".into(),
				"vacc_full_d7s7".into(),
				"hosp_cum".into(),
				"hosp_d1".into(),
				"hosp_d7".into(),
				"hosp_d7s7".into(),
			],
			&[
				&population,
				&icu_load.curr_covid_cases,
				&icu_load.curr_covid_cases_invasive,
				&icu_load.curr_beds_free,
				&icu_load.curr_beds_in_use,
				&vacc.first_vacc.cum,
				&vacc.first_vacc.d1,
				&vacc.first_vacc.d7,
				&vacc.first_vacc.d7s7,
				&vacc.basic_vacc.cum,
				&vacc.basic_vacc.d1,
				&vacc.basic_vacc.d7,
				&vacc.basic_vacc.d7s7,
				&vacc.full_vacc.cum,
				&vacc.full_vacc.d1,
				&vacc.full_vacc.d7,
				&vacc.full_vacc.d7s7,
				&hosp.cases.cum,
				&hosp.cases.d1,
				&hosp.cases.d7,
				&hosp.cases.d7s7,
			],
		)?;
	}

	{
		println!("preparing {} ...", DEMO_MEASUREMENT_NAME);

		let new_cases: SubmittableCaseData<_> = cases.rekeyed(|(state_id, _, ag, s)| {
			Some((*state_id, *ag, *s))
		}).into();
		drop(cases);
		let cases = new_cases;
		let mut keys = covid::joined_keyset_ref!(
			_,
			&cases.cases_by_report.cum,
			&cases.cases_by_ref.cum,
			&cases.cases_by_pub.cum,
			&cases.deaths.cum,
			&cases.deaths_by_pub.cum,
			&cases.recovered.cum
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

		println!("streaming {} ...", DEMO_MEASUREMENT_NAME);

		stream_data(
			&client,
			DEMO_MEASUREMENT_NAME,
			vec![
				"state".into(),
				"age".into(),
				"sex".into(),
			],
			&keys,
			&cases,
			&[],
			&[],
		)?;
	}

	Ok(())
}
