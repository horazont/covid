use std::fs::File;
use std::io;
use std::path::Path;

use chrono::NaiveDate;

use covid::timeseries;
use covid::{
	global_start_date, naive_today, CountMeter, Counters, DiffRecord, DistrictId, InfectionRecord,
	MaybeAgeGroup, ProgressSink, ReportFlag, Sex, StepMeter, ViewTimeSeries,
};

type PartialCaseKey = (DistrictId, MaybeAgeGroup, Sex);

const DELAY_CUTOFF: i64 = 28;

struct PartialDiffData {
	pub cases_by_pub: Counters<PartialCaseKey>,
	pub cases_delayed: Counters<PartialCaseKey>,
	pub case_delay_total: Counters<PartialCaseKey>,
	pub late_cases: Counters<PartialCaseKey>,
	pub deaths_by_pub: Counters<PartialCaseKey>,
	pub recovered_by_pub: Counters<PartialCaseKey>,
	pub cases_by_rep_buf: Counters<PartialCaseKey>,
	pub cases_by_rep_d7: Counters<PartialCaseKey>,
	pub cases_retracted: Counters<PartialCaseKey>,
}

fn saturating_add_u64_i32(reg: &mut u64, v: i32) {
	if v < 0 {
		let v = (-v) as u64;
		*reg = reg.saturating_sub(v);
	} else {
		*reg = reg.checked_add(v as u64).unwrap();
	}
}

impl PartialDiffData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self {
			cases_by_pub: Counters::new(start, end),
			cases_delayed: Counters::new(start, end),
			case_delay_total: Counters::new(start, end),
			late_cases: Counters::new(start, end),
			deaths_by_pub: Counters::new(start, end),
			recovered_by_pub: Counters::new(start, end),
			cases_by_rep_buf: Counters::new(start, end),
			cases_by_rep_d7: Counters::new(start, end),
			cases_retracted: Counters::new(start, end),
		}
	}

	fn submit(&mut self, date: NaiveDate, rec: &InfectionRecord) {
		let index = self
			.cases_by_pub
			.date_index(date)
			.expect("date out of range");

		let (case_index, case_diff, cases_retracted) = match rec.case {
			ReportFlag::NewlyReported => (index, rec.case_count, 0),
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => (index - 1, rec.case_count, -rec.case_count),
			_ => (0, 0, 0),
		};
		let (rep_case_index, rep_case_diff) = match rec.case {
			ReportFlag::NewlyReported | ReportFlag::Consistent => (
				self.cases_by_rep_buf
					.date_index(rec.report_date)
					.expect("date out of range"),
				rec.case_count,
			),
			_ => (0, 0),
		};
		let (death_index, death_diff) = match rec.death {
			ReportFlag::NewlyReported => (index, rec.death_count),
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => (index - 1, rec.death_count),
			_ => (0, 0),
		};
		let (recovered_index, recovered_diff) = match rec.recovered {
			ReportFlag::NewlyReported => (index, rec.recovered_count),
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => (index - 1, rec.recovered_count),
			_ => (0, 0),
		};

		let k = (rec.district_id, rec.age_group, rec.sex);
		if rep_case_diff != 0 {
			// we don't want to instantiate the key if there's nothing going on
			saturating_add_u64_i32(
				&mut self.cases_by_rep_buf.get_or_create(k)[rep_case_index],
				rep_case_diff,
			);
		}
		if cases_retracted != 0 {
			// we don't want to instantiate the key if there's nothing going on
			saturating_add_u64_i32(
				&mut self.cases_retracted.get_or_create(k)[case_index],
				cases_retracted,
			);
		}

		if case_diff == 0 && death_diff == 0 && recovered_diff == 0 {
			return;
		}

		let (case_delay, case_delay_count, late_case_count) = match rec.case {
			ReportFlag::NewlyReported => {
				let delay = (date - rec.report_date).num_days();
				assert!(delay >= 0);
				// we only want to include cases which take part in the pandemic situation, because that's what's relevant. if someone found a case from three months ago in some file, we don't really care... or do we?!
				if delay > DELAY_CUTOFF {
					(0, 0, rec.case_count)
				} else {
					(delay as i32, rec.case_count, 0)
				}
			}
			_ => (0, 0, 0),
		};

		saturating_add_u64_i32(
			&mut self.cases_by_pub.get_or_create(k)[case_index],
			case_diff,
		);
		saturating_add_u64_i32(
			&mut self.cases_delayed.get_or_create(k)[case_index],
			case_delay_count,
		);
		saturating_add_u64_i32(
			&mut self.case_delay_total.get_or_create(k)[case_index],
			case_delay * case_delay_count,
		);
		saturating_add_u64_i32(
			&mut self.late_cases.get_or_create(k)[case_index],
			late_case_count,
		);
		saturating_add_u64_i32(
			&mut self.deaths_by_pub.get_or_create(k)[death_index],
			death_diff,
		);
		saturating_add_u64_i32(
			&mut self.recovered_by_pub.get_or_create(k)[recovered_index],
			recovered_diff,
		);
	}

	fn write_all<W: io::Write, S: ProgressSink + ?Sized>(
		&self,
		s: &mut S,
		w: &mut W,
	) -> io::Result<()> {
		let start = self.cases_by_pub.start();
		let len = self.cases_by_pub.len();
		let mut pm = StepMeter::new(s, len);
		for i in 0..len {
			let date = start + chrono::Duration::days(i as i64);
			for k in self.cases_by_pub.keys() {
				let cases = self.cases_by_pub.get_value(k, i).unwrap_or(0);
				let cases_delayed = self.cases_delayed.get_value(k, i).unwrap_or(0);
				let delay_total = self.case_delay_total.get_value(k, i).unwrap_or(0);
				let late_cases = self.late_cases.get_value(k, i).unwrap_or(0);
				let deaths = self.deaths_by_pub.get_value(k, i).unwrap_or(0);
				let recovered = self.recovered_by_pub.get_value(k, i).unwrap_or(0);
				let cases_rep_d7 = self.cases_by_rep_d7.get_value(k, i).unwrap_or(0);
				let cases_retracted = self.cases_retracted.get_value(k, i).unwrap_or(0);
				if cases == 0
					&& deaths == 0 && recovered == 0
					&& cases_rep_d7 == 0 && cases_retracted == 0
				{
					continue;
				}
				let (district_id, age_group, sex) = *k;
				DiffRecord {
					date,
					district_id,
					age_group,
					sex,
					cases,
					delay_total,
					cases_delayed,
					late_cases,
					deaths,
					recovered,
					cases_rep_d7,
					cases_retracted,
				}
				.write(w)?;
			}
			if i % 30 == 29 {
				pm.update(i + 1);
			}
		}
		pm.finish();
		Ok(())
	}
}

fn load_existing<R: io::Read, S: ProgressSink + ?Sized>(
	s: &mut S,
	r: &mut R,
	d: &mut PartialDiffData,
) -> io::Result<()> {
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: DiffRecord = row?;
		let index = d
			.cases_by_pub
			.date_index(rec.date)
			.expect("date out of range");
		let k = (rec.district_id, rec.age_group, rec.sex);
		d.cases_by_pub.get_or_create(k)[index] = rec.cases;
		d.deaths_by_pub.get_or_create(k)[index] = rec.deaths;
		d.recovered_by_pub.get_or_create(k)[index] = rec.recovered;
		d.case_delay_total.get_or_create(k)[index] = rec.delay_total;
		d.cases_delayed.get_or_create(k)[index] = rec.cases_delayed;
		d.late_cases.get_or_create(k)[index] = rec.late_cases;
		d.cases_by_rep_d7.get_or_create(k)[index] = rec.cases_rep_d7;
		d.cases_retracted.get_or_create(k)[index] = rec.cases_retracted;
		if i % 500000 == 499999 {
			pm.update(i + 1);
		}
		n = i + 1;
	}
	pm.finish(n);
	Ok(())
}

fn try_load_existing<P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &mut S,
	path: P,
	d: &mut PartialDiffData,
) -> io::Result<()> {
	// not using magic open as a safeguard: the output will always be uncompressed and refusing compressed input protects against accidentally overwriting a source file
	let mut r = match File::open(path) {
		Ok(f) => f,
		// ignore missing files here
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(other) => return Err(other),
	};
	load_existing(s, &mut r, d)
}

fn merge_new<P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &mut S,
	path: P,
	date: NaiveDate,
	d: &mut PartialDiffData,
) -> io::Result<()> {
	let r = covid::magic_open(path)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	// the trick here is that we re-calculate the entire thing on each merge of new data and then carry over the d7 into the cases_by_rep_d7 timeseries
	d.cases_by_rep_buf.clear();
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		d.submit(date, &rec);
		if i % 500000 == 499999 {
			pm.update(i + 1);
		}
		n = i + 1;
	}
	// and now, we use the cases_by_rep_buf data to form a _d7 which we then write out for *this* date.
	{
		d.cases_by_rep_buf.cumsum();
		let index = d
			.cases_by_rep_d7
			.date_index(date)
			.expect("date out of range");
		let d7 = timeseries::Diff::padded(&d.cases_by_rep_buf, 7, 0.);
		for k in d.cases_by_rep_buf.keys() {
			d.cases_by_rep_d7.get_or_create(*k)[index] = d7.getf(k, date).expect("no data") as u64;
		}
	}
	pm.finish(n);
	Ok(())
}

fn writeback<P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &mut S,
	path: P,
	d: &PartialDiffData,
) -> io::Result<()> {
	let mut f = File::create(path)?;
	DiffRecord::write_header(&mut f)?;
	d.write_all(s, &mut f)?;
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let datafile = &argv[1];

	let start = global_start_date();
	let end = naive_today();
	let mut counters = PartialDiffData::new(start, end);

	println!("loading existing records ...");
	try_load_existing(&mut *covid::default_output(), datafile, &mut counters)?;

	for pair in argv[2..].chunks(2) {
		let newfile = &pair[0];
		// subtract one because the publication refers to the day before
		let date = pair[1].parse::<NaiveDate>()? - chrono::Duration::days(1);
		println!("merging new records ({} -> {}) ...", newfile, date);
		merge_new(&mut *covid::default_output(), newfile, date, &mut counters)?;
	}

	println!("rewriting records ...");
	writeback(&mut *covid::default_output(), datafile, &counters)?;

	Ok(())
}
