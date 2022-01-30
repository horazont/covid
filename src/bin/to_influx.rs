use std::collections::HashMap;
use std::convert::TryInto;
use std::io;
use std::path::Path;
use std::sync::Arc;

use chrono::NaiveDate;

use csv;

use covid;
use covid::{
	global_start_date, naive_today, AgeGroup, CountMeter, CounterGroup, Counters, Diff, DiffRecord,
	DistrictId, DistrictInfo, Filled, FullCaseKey, GeoCaseKey, HospitalizationRecord,
	ICULoadRecord, InfectionRecord, ProgressSink, RawDestatisRow, Sex, StateId, TimeMap,
	TimeSeriesKey, VaccinationKey, VaccinationLevel, VaccinationRecord, ViewTimeSeries,
};

static GEO_MEASUREMENT_NAME: &'static str = "data_v2_geo";
static GEO_LIGHT_MEASUREMENT_NAME: &'static str = "data_v2_geo_light";
static DEMO_MEASUREMENT_NAME: &'static str = "data_v2_demo";
static VACC_MEASUREMENT_NAME: &'static str = "data_v2_vacc";
// static DEMO_LIGHT_MEASUREMENT_NAME: &'static str = "data_v2_demo_light";

struct RawCaseData {
	pub cases_by_ref: Counters<FullCaseKey>,
	pub cases_by_report: Counters<FullCaseKey>,
	pub deaths: Counters<FullCaseKey>,
	pub recovered: Counters<FullCaseKey>,
}

impl RawCaseData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self {
			cases_by_ref: Counters::new(start, end),
			cases_by_report: Counters::new(start, end),
			deaths: Counters::new(start, end),
			recovered: Counters::new(start, end),
		}
	}

	fn submit(
		&mut self,
		district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
		rec: &InfectionRecord,
	) {
		let case_count = if rec.case.valid() { rec.case_count } else { 0 };
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

		let district_info = district_map
			.get(&rec.district_id)
			.expect("unknown district");
		let k = (
			district_info.state.id,
			rec.district_id,
			rec.age_group,
			rec.sex,
		);
		let ref_index = self
			.cases_by_ref
			.date_index(rec.reference_date)
			.expect("date out of range");
		if case_count > 0 {
			self.cases_by_ref.get_or_create(k)[ref_index] += case_count as u64;
			let report_index = self
				.cases_by_report
				.date_index(rec.report_date)
				.expect("date out of range");
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
		RawCaseData {
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
	pub deaths_by_pub: Counters<FullCaseKey>,
	pub recovered_by_pub: Counters<FullCaseKey>,
	pub cases_by_pubrep_d7: Counters<FullCaseKey>,
	pub cases_retracted: Counters<FullCaseKey>,
}

impl ParboiledCaseData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self {
			cases_by_pub: Counters::new(start, end),
			case_delay_total: Counters::new(start, end),
			cases_delayed: Counters::new(start, end),
			deaths_by_pub: Counters::new(start, end),
			recovered_by_pub: Counters::new(start, end),
			cases_by_pubrep_d7: Counters::new(start, end),
			cases_retracted: Counters::new(start, end),
		}
	}

	fn submit(&mut self, district_map: &HashMap<DistrictId, Arc<DistrictInfo>>, rec: &DiffRecord) {
		let district_info = district_map
			.get(&rec.district_id)
			.expect("unknown district");
		let k = (
			district_info.state.id,
			rec.district_id,
			rec.age_group,
			rec.sex,
		);
		let ref_index = self
			.cases_by_pub
			.date_index(rec.date)
			.expect("date out of range");
		self.cases_by_pub.get_or_create(k)[ref_index] += rec.cases;
		self.case_delay_total.get_or_create(k)[ref_index] += rec.delay_total;
		self.cases_delayed.get_or_create(k)[ref_index] += rec.cases_delayed;
		self.deaths_by_pub.get_or_create(k)[ref_index] += rec.deaths;
		self.cases_by_pubrep_d7.get_or_create(k)[ref_index] += rec.cases_rep_d7;
		self.cases_retracted.get_or_create(k)[ref_index] += rec.cases_retracted;
	}

	fn remapped<F: Fn(&FullCaseKey) -> Option<FullCaseKey>>(&self, f: F) -> ParboiledCaseData {
		ParboiledCaseData {
			cases_by_pub: self.cases_by_pub.rekeyed(&f),
			case_delay_total: self.case_delay_total.rekeyed(&f),
			cases_delayed: self.cases_delayed.rekeyed(&f),
			deaths_by_pub: self.deaths_by_pub.rekeyed(&f),
			recovered_by_pub: self.recovered_by_pub.rekeyed(&f),
			cases_by_pubrep_d7: self.cases_by_pubrep_d7.rekeyed(&f),
			cases_retracted: self.cases_retracted.rekeyed(&f),
		}
	}
}

struct CookedCaseData<T: TimeSeriesKey> {
	pub cases_by_pub: CounterGroup<T>,
	pub case_delay_total: Arc<Counters<T>>,
	pub cases_delayed: Arc<Counters<T>>,
	pub cases_by_ref: CounterGroup<T>,
	pub cases_by_report: CounterGroup<T>,
	pub deaths: CounterGroup<T>,
	pub deaths_by_pub: CounterGroup<T>,
	pub recovered: CounterGroup<T>,
	pub recovered_by_pub: CounterGroup<T>,
	pub cases_by_pubrep_d7: Arc<Counters<T>>,
	pub cases_retracted: Arc<Counters<T>>,
	diffstart: NaiveDate,
}

impl CookedCaseData<FullCaseKey> {
	fn cook(raw: RawCaseData, parboiled: ParboiledCaseData, diffstart: NaiveDate) -> Self {
		Self {
			cases_by_pub: CounterGroup::from_d1(parboiled.cases_by_pub),
			case_delay_total: Arc::new(parboiled.case_delay_total),
			cases_delayed: Arc::new(parboiled.cases_delayed),
			cases_by_ref: CounterGroup::from_d1(raw.cases_by_ref),
			cases_by_report: CounterGroup::from_d1(raw.cases_by_report),
			deaths: CounterGroup::from_d1(raw.deaths),
			deaths_by_pub: CounterGroup::from_d1(parboiled.deaths_by_pub),
			recovered: CounterGroup::from_d1(raw.recovered),
			recovered_by_pub: CounterGroup::from_d1(parboiled.recovered_by_pub),
			cases_by_pubrep_d7: Arc::new(parboiled.cases_by_pubrep_d7),
			cases_retracted: Arc::new(parboiled.cases_retracted),
			diffstart,
		}
	}
}

impl<T: TimeSeriesKey> CookedCaseData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CookedCaseData<U> {
		CookedCaseData::<U> {
			cases_by_pub: self.cases_by_pub.rekeyed(&f),
			case_delay_total: Arc::new(self.case_delay_total.rekeyed(&f)),
			cases_delayed: Arc::new(self.cases_delayed.rekeyed(&f)),
			cases_by_ref: self.cases_by_ref.rekeyed(&f),
			cases_by_report: self.cases_by_report.rekeyed(&f),
			deaths: self.deaths.rekeyed(&f),
			deaths_by_pub: self.deaths_by_pub.rekeyed(&f),
			recovered: self.recovered.rekeyed(&f),
			recovered_by_pub: self.recovered_by_pub.rekeyed(&f),
			cases_by_pubrep_d7: Arc::new(self.cases_by_pubrep_d7.rekeyed(&f)),
			cases_retracted: Arc::new(self.cases_retracted.rekeyed(&f)),
			diffstart: self.diffstart,
		}
	}
}

impl<T: TimeSeriesKey + 'static> CookedCaseData<T> {
	fn clamp_result<I>(&self, t: I) -> Arc<TimeMap<I>> {
		let end = self.cases_by_ref.cum.end() - chrono::Duration::days(28);
		Arc::new(TimeMap::clamp(t, None, Some(end)))
	}

	fn clamp_diff<I>(&self, t: I, offset: i64) -> Arc<TimeMap<I>> {
		Arc::new(TimeMap::clamp(
			t,
			Some(self.diffstart + chrono::Duration::days(offset)),
			None,
		))
	}

	fn write_field_descriptors(
		&self,
		out: &mut Vec<covid::FieldDescriptor<Arc<dyn covid::ViewTimeSeries<T>>>>,
	) {
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.cases_by_pub.d1.clone(), 0),
			"cases_pub_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.cases_by_pub.d7.clone(), 6),
			"cases_pub_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.cases_by_pub.d7s7.clone(), 13),
			"cases_pub_d7s7",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_ref.cum.clone(),
			"cases_ref_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_ref.d1.clone(),
			"cases_ref_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_ref.d7.clone(),
			"cases_ref_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_ref.d7s7.clone(),
			"cases_ref_d7s7",
		));
		out.push(covid::FieldDescriptor::new(
			Arc::new(Diff::padded(self.cases_by_ref.cum.clone(), 28, 0.)),
			"cases_ref_d28",
		));
		out.push(covid::FieldDescriptor::new(
			Arc::new(Diff::padded(self.cases_by_ref.cum.clone(), 112, 0.)),
			"cases_ref_d112",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_report.cum.clone(),
			"cases_rep_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_report.d1.clone(),
			"cases_rep_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_report.d7.clone(),
			"cases_rep_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.cases_by_report.d7s7.clone(),
			"cases_rep_d7s7",
		));

		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.cases_by_pubrep_d7.clone(), 7),
			"cases_pubrep_d7",
		));

		out.push(covid::FieldDescriptor::new(
			self.deaths.cum.clone(),
			"deaths_ref_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.deaths.d1.clone(),
			"deaths_ref_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_result(self.deaths.d7.clone()),
			"deaths_ref_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_result(self.deaths.d7s7.clone()),
			"deaths_ref_d7s7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_result(Arc::new(Diff::padded(self.deaths.cum.clone(), 28, 0.))),
			"deaths_ref_d28",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_result(Arc::new(Diff::padded(self.deaths.cum.clone(), 112, 0.))),
			"deaths_ref_d112",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.deaths_by_pub.d1.clone(), 0),
			"deaths_pub_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.deaths_by_pub.d7.clone(), 6),
			"deaths_pub_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.deaths_by_pub.d7s7.clone(), 13),
			"deaths_pub_d7s7",
		));

		out.push(covid::FieldDescriptor::new(
			self.recovered.cum.clone(),
			"recovered_ref_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.recovered.d1.clone(),
			"recovered_ref_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_result(self.recovered.d7.clone()),
			"recovered_ref_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_result(self.recovered.d7s7.clone()),
			"recovered_ref_d7s7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.recovered_by_pub.d1.clone(), 0),
			"recovered_pub_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.recovered_by_pub.d7.clone(), 6),
			"recovered_pub_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.recovered_by_pub.d7s7.clone(), 13),
			"recovered_pub_d7s7",
		));

		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.cases_delayed.clone(), 0),
			"meta_delay_cases",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.case_delay_total.clone(), 0),
			"meta_delay_total",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamp_diff(self.cases_retracted.clone(), 0),
			"cases_retracted",
		));
	}
}

struct RawICULoadData {
	pub curr_covid_cases: Counters<GeoCaseKey>,
	pub curr_covid_cases_invasive: Counters<GeoCaseKey>,
	pub curr_beds_free: Counters<GeoCaseKey>,
	pub curr_beds_in_use: Counters<GeoCaseKey>,
}

impl RawICULoadData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self {
			curr_covid_cases: Counters::new(start, end),
			curr_covid_cases_invasive: Counters::new(start, end),
			curr_beds_free: Counters::new(start, end),
			curr_beds_in_use: Counters::new(start, end),
		}
	}

	pub fn rekeyed<F: Fn(&GeoCaseKey) -> Option<GeoCaseKey>>(&self, f: F) -> RawICULoadData {
		Self {
			curr_covid_cases: self.curr_covid_cases.rekeyed(&f),
			curr_covid_cases_invasive: self.curr_covid_cases_invasive.rekeyed(&f),
			curr_beds_free: self.curr_beds_free.rekeyed(&f),
			curr_beds_in_use: self.curr_beds_in_use.rekeyed(&f),
		}
	}
}

struct CookedICULoadData<T: TimeSeriesKey> {
	pub curr_covid_cases: Arc<Counters<T>>,
	pub curr_covid_cases_invasive: Arc<Counters<T>>,
	pub curr_beds_free: Arc<Counters<T>>,
	pub curr_beds_in_use: Arc<Counters<T>>,
}

impl CookedICULoadData<GeoCaseKey> {
	fn cook(raw: RawICULoadData) -> Self {
		Self {
			curr_covid_cases: Arc::new(raw.curr_covid_cases),
			curr_covid_cases_invasive: Arc::new(raw.curr_covid_cases_invasive),
			curr_beds_free: Arc::new(raw.curr_beds_free),
			curr_beds_in_use: Arc::new(raw.curr_beds_in_use),
		}
	}
}

impl<T: TimeSeriesKey> CookedICULoadData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CookedICULoadData<U> {
		CookedICULoadData::<U> {
			curr_covid_cases: Arc::new(self.curr_covid_cases.rekeyed(&f)),
			curr_covid_cases_invasive: Arc::new(self.curr_covid_cases_invasive.rekeyed(&f)),
			curr_beds_free: Arc::new(self.curr_beds_free.rekeyed(&f)),
			curr_beds_in_use: Arc::new(self.curr_beds_in_use.rekeyed(&f)),
		}
	}
}

impl<T: TimeSeriesKey + 'static> CookedICULoadData<T> {
	fn clamp<I>(inner: I) -> Arc<TimeMap<I>> {
		// no data available before 2020-04-24
		Arc::new(TimeMap::clamp(
			inner,
			Some(NaiveDate::from_ymd(2020, 4, 24)),
			None,
		))
	}

	fn write_field_descriptors(
		&self,
		out: &mut Vec<covid::FieldDescriptor<Arc<dyn covid::ViewTimeSeries<T>>>>,
	) {
		out.push(covid::FieldDescriptor::new(
			Self::clamp(self.curr_covid_cases.clone()),
			"icu_covid_cases",
		));
		out.push(covid::FieldDescriptor::new(
			Self::clamp(self.curr_covid_cases_invasive.clone()),
			"icu_covid_cases_invasive",
		));
		out.push(covid::FieldDescriptor::new(
			Self::clamp(self.curr_beds_free.clone()),
			"icu_beds_free",
		));
		out.push(covid::FieldDescriptor::new(
			Self::clamp(self.curr_beds_in_use.clone()),
			"icu_beds_in_use",
		));
	}
}

struct RawVaccinationData {
	pub first_vacc: Counters<VaccinationKey>,
	pub basic_vacc: Counters<VaccinationKey>,
	pub full_vacc: Counters<VaccinationKey>,
}

impl RawVaccinationData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self {
			first_vacc: Counters::new(start, end),
			basic_vacc: Counters::new(start, end),
			full_vacc: Counters::new(start, end),
		}
	}

	fn submit(
		&mut self,
		district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
		rec: &VaccinationRecord,
	) {
		let mapped_district_id = match rec.district_id.0 {
			// Bundesfoo, unmap
			Some(district_id) if district_id == 17000 => None,
			v => v,
		};
		let state_id = match mapped_district_id {
			Some(district_id) => {
				let district_info = district_map.get(&district_id).expect("district not found");
				Some(district_info.state.id)
			}
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

	pub fn remapped<F: Fn(&VaccinationKey) -> Option<VaccinationKey>>(
		&self,
		f: F,
	) -> RawVaccinationData {
		RawVaccinationData {
			first_vacc: self.first_vacc.rekeyed(&f),
			basic_vacc: self.basic_vacc.rekeyed(&f),
			full_vacc: self.full_vacc.rekeyed(&f),
		}
	}
}

struct CookedVaccinationData<T: TimeSeriesKey> {
	pub first_vacc: CounterGroup<T>,
	pub basic_vacc: CounterGroup<T>,
	pub basic_vacc_d180: Arc<Diff<Arc<Counters<T>>>>,
	pub full_vacc: CounterGroup<T>,
}

impl CookedVaccinationData<VaccinationKey> {
	fn cook(raw: RawVaccinationData) -> Self {
		let basic_vacc = CounterGroup::from_d1(raw.basic_vacc);
		let basic_vacc_d180 = Arc::new(Diff::padded(basic_vacc.cum.clone(), 180, 0.));
		Self {
			first_vacc: CounterGroup::from_d1(raw.first_vacc),
			basic_vacc,
			basic_vacc_d180,
			full_vacc: CounterGroup::from_d1(raw.full_vacc),
		}
	}
}

impl<T: TimeSeriesKey> CookedVaccinationData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(
		&self,
		f: F,
	) -> CookedVaccinationData<U> {
		let basic_vacc = self.basic_vacc.rekeyed(&f);
		let basic_vacc_d180 = Arc::new(Diff::padded(basic_vacc.cum.clone(), 180, 0.));
		CookedVaccinationData::<U> {
			first_vacc: self.first_vacc.rekeyed(&f),
			basic_vacc,
			basic_vacc_d180,
			full_vacc: self.full_vacc.rekeyed(&f),
		}
	}
}

impl<T: TimeSeriesKey + 'static> CookedVaccinationData<T> {
	fn write_field_descriptors(
		&self,
		out: &mut Vec<covid::FieldDescriptor<Arc<dyn covid::ViewTimeSeries<T>>>>,
	) {
		out.push(covid::FieldDescriptor::new(
			self.first_vacc.cum.clone(),
			"vacc_first_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.first_vacc.d1.clone(),
			"vacc_first_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.first_vacc.d7.clone(),
			"vacc_first_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.first_vacc.d7s7.clone(),
			"vacc_first_d7s7",
		));

		out.push(covid::FieldDescriptor::new(
			self.basic_vacc.cum.clone(),
			"vacc_basic_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.basic_vacc.d1.clone(),
			"vacc_basic_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.basic_vacc.d7.clone(),
			"vacc_basic_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.basic_vacc.d7s7.clone(),
			"vacc_basic_d7s7",
		));
		out.push(covid::FieldDescriptor::new(
			self.basic_vacc_d180.clone() as Arc<dyn ViewTimeSeries<T>>,
			"vacc_basic_d180",
		));

		out.push(covid::FieldDescriptor::new(
			self.full_vacc.cum.clone(),
			"vacc_full_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.full_vacc.d1.clone(),
			"vacc_full_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.full_vacc.d7.clone(),
			"vacc_full_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.full_vacc.d7s7.clone(),
			"vacc_full_d7s7",
		));
	}
}

struct RawHospitalizationData {
	pub cases_d7: Counters<(StateId, AgeGroup)>,
}

impl RawHospitalizationData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self {
			cases_d7: Counters::new(start, end),
		}
	}

	fn submit(&mut self, rec: &HospitalizationRecord) {
		// sum of everything, we don't want that
		if rec.state_id == 0 {
			return;
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
		Self {
			cases: CounterGroup::from_d7(raw.cases_d7),
		}
	}
}

impl<T: TimeSeriesKey> CookedHospitalizationData<T> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(
		&self,
		f: F,
	) -> CookedHospitalizationData<U> {
		CookedHospitalizationData::<U> {
			cases: self.cases.rekeyed(&f),
		}
	}
}

impl<T: TimeSeriesKey + 'static> CookedHospitalizationData<T> {
	fn clamped<I>(&self, t: I) -> Arc<TimeMap<I>> {
		let end = self.cases.cum.end() - chrono::Duration::days(21);
		Arc::new(TimeMap::clamp(t, None, Some(end)))
	}

	fn write_field_descriptors(
		&self,
		out: &mut Vec<covid::FieldDescriptor<Arc<dyn covid::ViewTimeSeries<T>>>>,
	) {
		out.push(covid::FieldDescriptor::new(
			self.clamped(self.cases.cum.clone()),
			"hosp_cum",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamped(self.cases.d1.clone()),
			"hosp_d1",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamped(self.cases.d7.clone()),
			"hosp_d7",
		));
		out.push(covid::FieldDescriptor::new(
			self.clamped(self.cases.d7s7.clone()),
			"hosp_d7s7",
		));
	}
}

struct RawPopulationData<T: TimeSeriesKey> {
	pub count: Counters<T>,
}

impl<T: TimeSeriesKey> RawPopulationData<T> {
	fn ref_date() -> NaiveDate {
		// arbitrary
		NaiveDate::from_ymd(2020, 1, 1)
	}

	pub fn new() -> Self {
		let ref_date = Self::ref_date();
		Self {
			count: Counters::new(ref_date, ref_date + chrono::Duration::days(1)),
		}
	}

	pub fn remapped<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> RawPopulationData<U> {
		RawPopulationData::<U> {
			count: self.count.rekeyed(&f),
		}
	}
}

impl RawPopulationData<(StateId, AgeGroup, Sex)> {
	pub fn submit(&mut self, rec: RawDestatisRow) {
		let k = (rec.state_id, rec.age_group, rec.sex);
		self.count.get_or_create(k)[0] += rec.count;
	}
}

struct CookedPopulationData<T: TimeSeriesKey> {
	count: Arc<Counters<T>>,
}

impl<T: TimeSeriesKey> CookedPopulationData<T> {
	pub fn cook(raw: RawPopulationData<T>) -> Self {
		Self {
			count: Arc::new(raw.count),
		}
	}

	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(
		&self,
		f: F,
	) -> CookedPopulationData<U> {
		CookedPopulationData::<U> {
			count: Arc::new(self.count.rekeyed(&f)),
		}
	}

	pub fn view(&self) -> Arc<Filled<Arc<Counters<T>>>> {
		Arc::new(Filled::new(
			self.count.clone(),
			RawPopulationData::<T>::ref_date(),
		))
	}
}

impl<T: TimeSeriesKey + 'static> CookedPopulationData<T> {
	fn write_field_descriptors(
		&self,
		out: &mut Vec<covid::FieldDescriptor<Arc<dyn covid::ViewTimeSeries<T>>>>,
	) {
		out.push(covid::FieldDescriptor::new(self.view(), "population"));
	}
}

fn load_diff_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &'s mut S,
	p: P,
	district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
	cases: &mut ParboiledCaseData,
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: DiffRecord = row?;
		cases.submit(district_map, &rec);
		if i % 500000 == 499999 {
			pm.update(i + 1);
		}
		n = i + 1;
	}
	pm.finish(n);
	Ok(())
}

fn load_case_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &'s mut S,
	p: P,
	district_map: &HashMap<DistrictId, Arc<DistrictInfo>>,
	cases: &mut RawCaseData,
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		cases.submit(district_map, &rec);
		if i % 500000 == 499999 {
			pm.update(i + 1);
		}
		n = i + 1;
	}
	pm.finish(n);
	Ok(())
}

fn load_divi_load_data<P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &mut S,
	p: P,
	data: &mut RawICULoadData,
) -> io::Result<()> {
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
		data.curr_covid_cases_invasive.get_or_create(k)[index] =
			rec.current_covid_cases_invasive_ventilation as u64;
		data.curr_beds_free.get_or_create(k)[index] = rec.beds_free as u64;
		data.curr_beds_in_use.get_or_create(k)[index] = rec.beds_in_use as u64;
		if i % 500000 == 499999 {
			pm.update(i + 1);
		}
		n = i + 1;
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
			pm.update(i + 1);
		}
		n = i + 1;
	}
	pm.finish(n);
	Ok(())
}

fn load_hosp_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &'s mut S,
	p: P,
	data: &mut RawHospitalizationData,
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
			pm.update(i + 1);
		}
		n = i + 1;
	}
	pm.finish(n);
	Ok(())
}

fn load_destatis_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &'s mut S,
	p: P,
	data: &mut RawPopulationData<(StateId, AgeGroup, Sex)>,
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: RawDestatisRow = match row {
			Ok(v) => v,
			// for some reason, they have NA in some cells?!
			Err(_) => continue,
		};
		data.submit(rec);
		if i % 100 == 99 {
			pm.update(i + 1);
		}
		n = i + 1;
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

fn load_cooked_case_data(
	districts: &HashMap<DistrictId, Arc<covid::DistrictInfo>>,
	start: NaiveDate,
	diffstart: NaiveDate,
	end: NaiveDate,
	casefile: &str,
	difffile: &str,
) -> Result<CookedCaseData<FullCaseKey>, io::Error> {
	let cases = {
		let mut cases = RawCaseData::new(start, end);
		println!("loading case data ...");
		load_case_data(
			&mut *covid::default_output(),
			casefile,
			&districts,
			&mut cases,
		)?;
		cases.remapped(|(state_id, district_id, mag, sex)| {
			Some((*state_id, remap_berlin(*district_id), *mag, *sex))
		})
	};

	let diff_cases = {
		let mut diff_cases = ParboiledCaseData::new(diffstart, end);
		println!("loading diff data ...");
		load_diff_data(
			&mut *covid::default_output(),
			difffile,
			&districts,
			&mut diff_cases,
		)?;
		diff_cases.remapped(|(state_id, district_id, mag, sex)| {
			Some((*state_id, remap_berlin(*district_id), *mag, *sex))
		})
	};

	println!("crunching case data...");
	let cooked_cases = CookedCaseData::cook(cases, diff_cases, diffstart);

	Ok(cooked_cases)
}

fn load_cooked_hosp_data(
	start: NaiveDate,
	end: NaiveDate,
	hospfile: &str,
) -> Result<CookedHospitalizationData<(StateId, AgeGroup)>, io::Error> {
	let mut hosp = RawHospitalizationData::new(start, end);
	println!("loading hospitalization data ...");
	load_hosp_data(&mut *covid::default_output(), hospfile, &mut hosp)?;
	let cooked_hosp = CookedHospitalizationData::cook(hosp);

	Ok(cooked_hosp)
}

fn load_cooked_divi_data(
	start: NaiveDate,
	end: NaiveDate,
	divifile: &str,
) -> Result<CookedICULoadData<GeoCaseKey>, io::Error> {
	let mut icu_load = RawICULoadData::new(start, end);
	println!("loading ICU data ...");
	load_divi_load_data(&mut *covid::default_output(), divifile, &mut icu_load)?;
	let icu_load =
		icu_load.rekeyed(|(state_id, district_id)| Some((*state_id, remap_berlin(*district_id))));
	Ok(CookedICULoadData::cook(icu_load))
}

fn load_cooked_vacc_data(
	districts: &HashMap<DistrictId, Arc<covid::DistrictInfo>>,
	start: NaiveDate,
	end: NaiveDate,
	vaccfile: &str,
) -> Result<CookedVaccinationData<VaccinationKey>, io::Error> {
	let mut vacc = RawVaccinationData::new(start, end);
	println!("loading vaccination data ...");
	load_vacc_data(
		&mut *covid::default_output(),
		vaccfile,
		&districts,
		&mut vacc,
	)?;
	let vacc = vacc.remapped(|(state_id, district_id, ag)| {
		Some((*state_id, district_id.map(remap_berlin), *ag))
	});
	Ok(CookedVaccinationData::cook(vacc))
}

fn load_all_data(
	states: &HashMap<DistrictId, Arc<covid::StateInfo>>,
	districts: &mut HashMap<DistrictId, Arc<covid::DistrictInfo>>,
	start: NaiveDate,
	diffstart: NaiveDate,
	end: NaiveDate,
	casefile: &str,
	difffile: &str,
	divifile: &str,
	vaccfile: &str,
	hospfile: &str,
	destatisfile: &str,
) -> Result<
	(
		CookedPopulationData<GeoCaseKey>,
		CookedPopulationData<(StateId, AgeGroup)>,
		CookedPopulationData<(StateId, AgeGroup, Sex)>,
		CookedCaseData<FullCaseKey>,
		CookedVaccinationData<VaccinationKey>,
		CookedHospitalizationData<(StateId, AgeGroup)>,
		CookedICULoadData<GeoCaseKey>,
	),
	io::Error,
> {
	assert!(diffstart >= start);
	assert!(end >= diffstart);

	println!("loading population data ...");
	let mut population = RawPopulationData::<(StateId, DistrictId)>::new();
	for district in districts.values() {
		let k = (district.state.id, district.id);
		population.count.get_or_create(k).fill(district.population);
	}
	let cooked_population = CookedPopulationData::cook(
		population
			.remapped(|(state_id, district_id)| Some((*state_id, remap_berlin(*district_id)))),
	);

	// We inject berlin only later. This allows us to rekey the population above to eliminate the separate berlin districts.
	covid::inject_berlin(states, districts);

	let mut destatis_population = RawPopulationData::new();
	println!("loading destatis population data ...");
	load_destatis_data(
		&mut *covid::default_output(),
		destatisfile,
		&mut destatis_population,
	)?;

	let cooked_vacc_population =
		CookedPopulationData::cook(destatis_population.remapped(|(state_id, ag, _)| {
			assert!(ag.high.is_none() || ag.low == ag.high.unwrap());
			let age = ag.low;
			let ag = if age < 5 {
				AgeGroup {
					low: 0,
					high: Some(4),
				}
			} else if age < 12 {
				AgeGroup {
					low: 5,
					high: Some(11),
				}
			} else if age < 18 {
				AgeGroup {
					low: 12,
					high: Some(17),
				}
			} else if age < 60 {
				AgeGroup {
					low: 18,
					high: Some(59),
				}
			} else {
				AgeGroup {
					low: 60,
					high: None,
				}
			};
			Some((*state_id, ag))
		}));
	let cooked_demo_population =
		CookedPopulationData::cook(destatis_population.remapped(|(state_id, ag, sex)| {
			assert!(ag.high.is_none() || ag.low == ag.high.unwrap());
			let age = ag.low;
			let ag = if age < 5 {
				AgeGroup {
					low: 0,
					high: Some(4),
				}
			} else if age < 15 {
				AgeGroup {
					low: 5,
					high: Some(14),
				}
			} else if age < 35 {
				AgeGroup {
					low: 15,
					high: Some(34),
				}
			} else if age < 60 {
				AgeGroup {
					low: 35,
					high: Some(59),
				}
			} else if age < 80 {
				AgeGroup {
					low: 60,
					high: Some(79),
				}
			} else {
				AgeGroup {
					low: 80,
					high: None,
				}
			};
			Some((*state_id, ag, *sex))
		}));
	drop(destatis_population);

	let cooked_cases = load_cooked_case_data(districts, start, diffstart, end, casefile, difffile)?;
	let cooked_vacc = load_cooked_vacc_data(districts, start, end, vaccfile)?;
	let cooked_icu_load = load_cooked_divi_data(start, end, divifile)?;
	let cooked_hosp = load_cooked_hosp_data(start, end, hospfile)?;

	Ok((
		cooked_population,
		cooked_vacc_population,
		cooked_demo_population,
		cooked_cases,
		cooked_vacc,
		cooked_hosp,
		cooked_icu_load,
	))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let casefile = &argv[1];
	let districts = &argv[2];
	let difffile = &argv[3];
	let diffstart = &argv[4];
	let divifile = &argv[5];
	let vaccfile = &argv[6];
	let hospfile = &argv[7];
	let destatisfile = &argv[8];

	let (states, mut districts) = {
		let mut r = std::fs::File::open(districts)?;
		covid::load_rki_districts(&mut r)?
	};
	let start = global_start_date();
	let diffstart = diffstart.parse::<NaiveDate>()?;
	let end = naive_today();
	let ndays: usize = (end - start).num_days().try_into().unwrap();

	let (population, population_vacc, population_demo, cases, vacc, hosp, icu_load) =
		load_all_data(
			&states,
			&mut districts,
			start,
			diffstart,
			end,
			casefile,
			difffile,
			divifile,
			vaccfile,
			hospfile,
			destatisfile,
		)?;

	let client = covid::env_client();

	{
		println!("preparing {} ...", GEO_MEASUREMENT_NAME);

		let cases = cases.rekeyed(|(state_id, district_id, _, _)| Some((*state_id, *district_id)));
		let vacc = vacc.rekeyed(|(state_id, district_id, _)| {
			// drop vaccinations without properly defined state + district
			match (state_id, district_id) {
				(Some(state_id), Some(district_id)) => Some((*state_id, *district_id)),
				_ => None,
			}
		});
		let keys: Vec<_> = covid::prepare_keyset(
			&["state", "district"][..],
			population.count.keys(),
			|k, out| {
				let state_id = k.0;
				let district_id = k.1;
				let state_name = &states.get(&state_id).unwrap().name;
				let district_name = match &districts.get(&district_id) {
					Some(i) => &i.name,
					None => panic!("failed to find district {} in data", district_id),
				};
				out.push(state_name.into());
				out.push(district_name.into());
			},
		);

		println!("streaming {} ...", GEO_MEASUREMENT_NAME);

		let mut fields = Vec::new();
		cases.write_field_descriptors(&mut fields);
		vacc.write_field_descriptors(&mut fields);
		icu_load.write_field_descriptors(&mut fields);
		population.write_field_descriptors(&mut fields);

		covid::stream_dynamic(
			&client,
			&mut *covid::default_output(),
			GEO_MEASUREMENT_NAME,
			start,
			ndays,
			&keys,
			&fields[..],
		)?;
	}

	{
		println!("preparing {} ...", GEO_LIGHT_MEASUREMENT_NAME);

		let cases = cases.rekeyed(|(state_id, _, _, _)| Some(*state_id));
		let vacc = vacc.rekeyed(|(state_id, district_id, _)| {
			// drop vaccinations without properly defined state + district
			match (state_id, district_id) {
				(Some(state_id), Some(_)) => Some(*state_id),
				_ => None,
			}
		});
		let icu_load = icu_load.rekeyed(|(state_id, _)| Some(*state_id));
		let hosp = hosp.rekeyed(|(state_id, _)| Some(*state_id));
		let population = Arc::new(population.rekeyed(|(state_id, _)| Some(*state_id)));
		let keys: Vec<_> =
			covid::prepare_keyset(&["state"][..], population.count.keys(), |k, out| {
				let state_id = k;
				let state_name = &states.get(&state_id).unwrap().name;
				out.push(state_name.into());
			});

		println!("streaming {} ...", GEO_LIGHT_MEASUREMENT_NAME);

		let mut fields = Vec::new();
		cases.write_field_descriptors(&mut fields);
		vacc.write_field_descriptors(&mut fields);
		icu_load.write_field_descriptors(&mut fields);
		hosp.write_field_descriptors(&mut fields);
		population.write_field_descriptors(&mut fields);

		covid::stream_dynamic(
			&client,
			&mut *covid::default_output(),
			GEO_LIGHT_MEASUREMENT_NAME,
			start,
			ndays,
			&keys,
			&fields[..],
		)?;
	}

	{
		println!("preparing {} ...", DEMO_MEASUREMENT_NAME);

		let new_cases = cases.rekeyed(|(state_id, _, ag, s)| Some((*state_id, (**ag)?, *s)));
		drop(cases);
		let cases = new_cases;
		let keys: Vec<_> = covid::prepare_keyset(
			&["state", "age", "sex"][..],
			population_demo.count.keys(),
			|k, out| {
				let state_id = k.0;
				let state_name = &states.get(&state_id).unwrap().name;
				out.push(state_name.into());
				out.push(k.1.to_string().into());
				out.push(k.2.to_string().into());
			},
		);

		println!("streaming {} ...", DEMO_MEASUREMENT_NAME);

		let mut fields = Vec::new();
		cases.write_field_descriptors(&mut fields);
		population_demo.write_field_descriptors(&mut fields);

		covid::stream_dynamic(
			&client,
			&mut *covid::default_output(),
			DEMO_MEASUREMENT_NAME,
			start,
			ndays,
			&keys,
			&fields[..],
		)?;
	}

	{
		println!("preparing {} ...", VACC_MEASUREMENT_NAME);

		let vacc = vacc.rekeyed(|(state_id, _, ag)| {
			// drop vaccinations without properly defined state + district
			match (state_id, **ag) {
				(Some(state_id), Some(ag)) => Some((*state_id, ag)),
				_ => None,
			}
		});
		let keys: Vec<_> = covid::prepare_keyset(
			&["state", "age"][..],
			population_vacc.count.keys(),
			|k, out| {
				let state_id = k.0;
				let state_name = &states.get(&state_id).unwrap().name;
				out.push(state_name.into());
				out.push(k.1.to_string().into());
			},
		);

		println!("streaming {} ...", VACC_MEASUREMENT_NAME);

		let mut fields = Vec::new();
		vacc.write_field_descriptors(&mut fields);
		population_vacc.write_field_descriptors(&mut fields);

		covid::stream_dynamic(
			&client,
			&mut *covid::default_output(),
			VACC_MEASUREMENT_NAME,
			start,
			ndays,
			&keys,
			&fields[..],
		)?;
	}

	Ok(())
}
