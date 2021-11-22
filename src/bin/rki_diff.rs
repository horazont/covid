use std::io;
use std::path::Path;
use std::fs::File;

use chrono::NaiveDate;

use covid::{DistrictId, MaybeAgeGroup, Sex, Counters, ReportFlag, InfectionRecord, global_start_date, naive_today, StepMeter, CountMeter, ProgressSink, DiffRecord};


type PartialCaseKey = (DistrictId, MaybeAgeGroup, Sex);

const DELAY_CUTOFF: i64 = 28;

struct PartialDiffData {
	pub cases_by_pub: Counters<PartialCaseKey>,
	pub cases_delayed: Counters<PartialCaseKey>,
	pub case_delay_total: Counters<PartialCaseKey>,
	pub late_cases: Counters<PartialCaseKey>,
	pub deaths_by_pub: Counters<PartialCaseKey>,
	pub recovered_by_pub: Counters<PartialCaseKey>,
}

fn saturating_add_u64_i32(reg: &mut u64, v: i32) {
	if v < 0 {
		let v = (-v) as u64;
		*reg = reg.saturating_sub(v);
	} else {
		*reg = reg.checked_add(v as u64).unwrap();
	}
}

fn checked_add_u64_i64(a: u64, b: i64) -> Option<u64> {
	if b < 0 {
		let b = (-b) as u64;
		a.checked_sub(b)
	} else {
		let b = b as u64;
		a.checked_add(b)
	}
}

impl PartialDiffData {
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

	fn submit_initial(&mut self, rec: &InfectionRecord) {
		// For the priming of the database, we fill the d1 column with the data based on the report date (which is generally closer to the publication date than the reference date).
		// We need the full d1 columns, even for the priming data, to be able to process later retractions correctly.
		let index = self.cases_by_pub.date_index(rec.report_date).expect("date out of range");
		let k = (rec.district_id, rec.age_group, rec.sex);

		// If we see retractions in this data sample, we count them positively, because we will **also** call .submit() for all entries in the first sample to process them to gain extra data.
		// That means that the retracted cases will be subtracted from their report dates (if possible), hence, we insert them here positively.
		let case_diff = match rec.case {
			ReportFlag::Consistent => rec.case_count,
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => -rec.case_count,
			_ => 0,
		};
		assert!(case_diff >= 0);

		// For recovered/death, the report date is grossly incorrect, but we cannot do anything about that in the historic dataset. At some point, we might want to change the heuristic for those.
		let death_diff = match rec.death {
			ReportFlag::Consistent => rec.death_count,
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => -rec.death_count,
			_ => 0,
		};
		assert!(death_diff >= 0);

		let recovered_diff = match rec.recovered {
			ReportFlag::Consistent => rec.recovered_count,
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => -rec.recovered_count,
			_ => 0,
		};
		assert!(recovered_diff >= 0);

		self.cases_by_pub.get_or_create(k)[index] += case_diff as u64;
		self.deaths_by_pub.get_or_create(k)[index] += death_diff as u64;
		self.recovered_by_pub.get_or_create(k)[index] += recovered_diff as u64;
	}

	fn prepare_diff(
			k: &PartialCaseKey,
			target_index: usize,
			target_ts: &Counters<PartialCaseKey>,
			report_date: NaiveDate,
			flag: ReportFlag,
			count: i32,
	) -> (usize, i64) {
		// Find a location to place a diff based on a given case count + report flag.
		// This will try to find the best possible location for a retraction, but drop retractions (with a warning message) if no such location can be found.

		// TODO: logging
		let count = count as i64;
		match flag {
			ReportFlag::NewlyReported => (target_index, count),
			// Note: the data is negative in the source already.
			ReportFlag::Retracted => {
				let start_at = target_ts.date_index(report_date).expect("date out of range");
				assert!(count <= 0);
				let retract_index = match target_ts.find_ge(k, start_at, (-count) as u64) {
					Some(i) => i,
					None => {
						// TODO: use logging
						eprintln!("warn: retraction found, but no matching case in dataset: k={:?}, count={}, report_date={}", k, count, report_date);
						return (0, 0)
					},
				};
				(retract_index, count)
			},
			_ => (0, 0),
		}
	}

	fn apply_diff(
			k: &PartialCaseKey,
			target_index: usize,
			target_ts: &mut Counters<PartialCaseKey>,
			report_date: NaiveDate,
			flag: ReportFlag,
			count: i32,
	) {
		let (index, diff) = Self::prepare_diff(
			k,
			target_index,
			target_ts,
			report_date,
			flag,
			count,
		);
		if diff == 0 {
			return
		}
		let ts = target_ts.get_or_create(*k);
		ts[index] = match checked_add_u64_i64(ts[index], diff) {
			Some(v) => v,
			None => panic!("attempt to decrease diff below zero!"),
		}
	}

	fn submit(&mut self, date: NaiveDate, rec: &InfectionRecord)
	{
		let index = self.cases_by_pub.date_index(date).expect("date out of range");
		let k = (rec.district_id, rec.age_group, rec.sex);

		Self::apply_diff(
			&k,
			index,
			&mut self.cases_by_pub,
			rec.report_date,
			rec.case,
			rec.case_count,
		);
		Self::apply_diff(
			&k,
			index,
			&mut self.deaths_by_pub,
			rec.report_date,
			rec.death,
			rec.death_count,
		);
		Self::apply_diff(
			&k,
			index,
			&mut self.recovered_by_pub,
			rec.report_date,
			rec.recovered,
			rec.recovered_count,
		);

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
			},
			_ => (0, 0, 0),
		};

		saturating_add_u64_i32(&mut self.cases_delayed.get_or_create(k)[index], case_delay_count);
		saturating_add_u64_i32(&mut self.case_delay_total.get_or_create(k)[index], case_delay * case_delay_count);
		saturating_add_u64_i32(&mut self.late_cases.get_or_create(k)[index], late_case_count);
	}

	fn write_all<W: io::Write, S: ProgressSink + ?Sized>(&self, s: &mut S, w: &mut W) -> io::Result<()> {
		let len = self.cases_by_pub.len();
		let mut pm = StepMeter::new(s, len);
		let mut w = csv::Writer::from_writer(w);
		for i in 0..len {
			let date = self.cases_by_pub.index_date(i as i64).unwrap();
			for k in self.cases_by_pub.keys() {
				let cases = self.cases_by_pub.get_value(k, i).unwrap_or(0);
				let cases_delayed = self.cases_delayed.get_value(k, i).unwrap_or(0);
				let delay_total = self.case_delay_total.get_value(k, i).unwrap_or(0);
				let late_cases = self.late_cases.get_value(k, i).unwrap_or(0);
				let deaths = self.deaths_by_pub.get_value(k, i).unwrap_or(0);
				let recovered = self.recovered_by_pub.get_value(k, i).unwrap_or(0);
				if cases == 0 && deaths == 0 && recovered == 0 {
					continue
				}
				let (district_id, age_group, sex) = *k;
				w.serialize(DiffRecord{
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
				})?;
			}
			if i % 30 == 29 {
				pm.update(i+1);
			}
		}
		pm.finish();
		Ok(())
	}
}

fn load_existing<R: io::Read, S: ProgressSink + ?Sized>(s: &mut S, r: &mut R, d: &mut PartialDiffData) -> io::Result<usize> {
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: DiffRecord = row?;
		let index = d.cases_by_pub.date_index(rec.date).expect("date out of range");
		let k = (rec.district_id, rec.age_group, rec.sex);
		d.cases_by_pub.get_or_create(k)[index] = rec.cases;
		d.deaths_by_pub.get_or_create(k)[index] = rec.deaths;
		d.recovered_by_pub.get_or_create(k)[index] = rec.recovered;
		d.case_delay_total.get_or_create(k)[index] = rec.delay_total;
		d.late_cases.get_or_create(k)[index] = rec.late_cases;
		d.cases_delayed.get_or_create(k)[index] = rec.cases_delayed;
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(n)
}

fn try_load_existing<P: AsRef<Path>, S: ProgressSink + ?Sized>(s: &mut S, path: P, d: &mut PartialDiffData) -> io::Result<bool> {
	let r = match File::open(path) {
		Ok(f) => f,
		// ignore missing files here
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
		Err(other) => return Err(other),
	};
	let mut r = flate2::read::GzDecoder::new(r);
	Ok(load_existing(s, &mut r, d)? > 0)
}

fn prime<P: AsRef<Path>, S: ProgressSink + ?Sized>(s: &mut S, path: P, d: &mut PartialDiffData) -> io::Result<()> {
	let r = covid::magic_open(path)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		d.submit_initial(&rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}

fn merge_new<P: AsRef<Path>, S: ProgressSink + ?Sized>(s: &mut S, path: P, date: NaiveDate, d: &mut PartialDiffData) -> io::Result<()> {
	let r = covid::magic_open(path)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		d.submit(date, &rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}

fn writeback<P: AsRef<Path>, S: ProgressSink + ?Sized>(s: &mut S, path: P, d: &PartialDiffData) -> io::Result<()> {
	let f = File::create(path)?;
	let mut w = flate2::write::GzEncoder::new(f, flate2::Compression::new(5));
	d.write_all(s, &mut w)?;
	w.finish()?;
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let datafile = &argv[1];

	let start = global_start_date();
	let end = naive_today();
	let mut counters = PartialDiffData::new(start, end);

	println!("loading existing records ...");
	let mut found_anything = try_load_existing(&mut *covid::default_output(), datafile, &mut counters)?;

	for pair in argv[2..].chunks(2) {
		let newfile = &pair[0];
		if !found_anything {
			println!("priming dataset using {}...", newfile);
			prime(&mut *covid::default_output(), newfile, &mut counters)?;
			found_anything = true;
		}
		// subtract one because the publication refers to the day before
		let date = pair[1].parse::<NaiveDate>()? - chrono::Duration::days(1);
		println!("merging new records ({} -> {}) ...", newfile, date);
		merge_new(&mut *covid::default_output(), newfile, date, &mut counters)?;
	}

	println!("rewriting records ...");
	writeback(&mut *covid::default_output(), datafile, &counters)?;

	Ok(())
}
