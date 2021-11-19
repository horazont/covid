use std::io;
use std::path::Path;
use std::fs::File;

use chrono::NaiveDate;

use covid::{DistrictId, MaybeAgeGroup, Sex, Counters, ReportFlag, InfectionRecord, global_start_date, naive_today, ProgressMeter, ProgressSink, DiffRecord};


type PartialCaseKey = (DistrictId, MaybeAgeGroup, Sex);

struct PartialDiffData {
	pub cases_by_pub: Counters<PartialCaseKey>,
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

impl PartialDiffData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			cases_by_pub: Counters::new(start, end),
			deaths_by_pub: Counters::new(start, end),
			recovered_by_pub: Counters::new(start, end),
		}
	}

	fn submit(&mut self, index: usize, rec: &InfectionRecord)
	{
		let case_diff = match rec.case {
			// Note: the data is negative in the source already.
			ReportFlag::NewlyReported | ReportFlag::Retracted => rec.case_count,
			_ => 0,
		};
		let death_diff = match rec.death {
			// Note: the data is negative in the source already.
			ReportFlag::NewlyReported | ReportFlag::Retracted => rec.death_count,
			_ => 0,
		};
		let recovered_diff = match rec.recovered {
			// Note: the data is negative in the source already.
			ReportFlag::NewlyReported | ReportFlag::Retracted => rec.recovered_count,
			_ => 0,
		};

		if case_diff == 0 && death_diff == 0 && recovered_diff == 0 {
			return
		}

		let k = (rec.district_id, rec.age_group, rec.sex);
		saturating_add_u64_i32(&mut self.cases_by_pub.get_or_create(k)[index], case_diff);
		saturating_add_u64_i32(&mut self.deaths_by_pub.get_or_create(k)[index], death_diff);
		saturating_add_u64_i32(&mut self.recovered_by_pub.get_or_create(k)[index], recovered_diff);
	}

	fn write_all<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
		let start = self.cases_by_pub.start();
		let len = self.cases_by_pub.len();
		let mut pm = ProgressMeter::start(Some(len));
		for i in 0..len {
			let date = start + chrono::Duration::days(i as i64);
			for k in self.cases_by_pub.keys() {
				let cases = self.cases_by_pub.get_value(k, i).unwrap_or(0);
				let deaths = self.deaths_by_pub.get_value(k, i).unwrap_or(0);
				let recovered = self.recovered_by_pub.get_value(k, i).unwrap_or(0);
				if cases == 0 && deaths == 0 && recovered == 0 {
					continue
				}
				let (district_id, age_group, sex) = *k;
				DiffRecord{
					date,
					district_id,
					age_group,
					sex,
					cases,
					deaths,
					recovered,
				}.write(w)?;
			}
			if i % 30 == 29 {
				pm.update(i+1);
			}
		}
		pm.finish(Some(len));
		Ok(())
	}
}

fn load_existing<R: io::Read>(r: &mut R, d: &mut PartialDiffData) -> io::Result<()> {
	let mut r = csv::Reader::from_reader(r);
	let mut pm = ProgressMeter::start(None);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: DiffRecord = row?;
		let index = d.cases_by_pub.date_index(rec.date).expect("date out of range");
		let k = (rec.district_id, rec.age_group, rec.sex);
		d.cases_by_pub.get_or_create(k)[index] = rec.cases;
		d.deaths_by_pub.get_or_create(k)[index] = rec.deaths;
		d.recovered_by_pub.get_or_create(k)[index] = rec.recovered;
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(Some(n));
	Ok(())
}

fn try_load_existing<P: AsRef<Path>>(path: P, d: &mut PartialDiffData) -> io::Result<()> {
	let mut r = match covid::magic_open(path) {
		Ok(f) => f,
		// ignore missing files here
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(other) => return Err(other),
	};
	load_existing(&mut r, d)
}

fn merge_new<P: AsRef<Path>>(path: P, date: NaiveDate, d: &mut PartialDiffData) -> io::Result<()> {
	let r = covid::magic_open(path)?;
	let mut r = csv::Reader::from_reader(r);
	let index = d.cases_by_pub.date_index(date).expect("date out of range");
	let mut pm = ProgressMeter::start(None);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		d.submit(index, &rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(Some(n));
	Ok(())
}

fn writeback<P: AsRef<Path>>(path: P, d: &PartialDiffData) -> io::Result<()> {
	let mut f = File::create(path)?;
	DiffRecord::write_header(&mut f)?;
	d.write_all(&mut f)?;
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let datafile = &argv[1];
	let newfile = &argv[2];
	let date = argv[3].parse::<NaiveDate>().unwrap();

	let start = global_start_date();
	let end = naive_today();
	let mut counters = PartialDiffData::new(start, end);

	println!("loading existing records ...");
	try_load_existing(datafile, &mut counters)?;

	println!("merging new records ...");
	merge_new(newfile, date, &mut counters)?;

	println!("rewriting records ...");
	writeback(datafile, &counters)?;

	Ok(())
}
