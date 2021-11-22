use std::io;
use std::path::Path;
use std::fs::File;

use chrono::NaiveDate;

use covid::{DistrictId, MaybeAgeGroup, Sex, Counters, ReportFlag, InfectionRecord, global_start_date, StepMeter, CountMeter, ProgressSink, DiffBaseRecord};


type PartialCaseKey = (DistrictId, MaybeAgeGroup, Sex);

struct PartialBaseData {
	pub cases_by_pub_cum: Counters<PartialCaseKey>,
	pub deaths_by_pub_cum: Counters<PartialCaseKey>,
	pub recovered_by_pub_cum: Counters<PartialCaseKey>,
}

impl PartialBaseData {
	fn new(start: NaiveDate, end: NaiveDate) -> Self {
		Self{
			cases_by_pub_cum: Counters::new(start, end),
			deaths_by_pub_cum: Counters::new(start, end),
			recovered_by_pub_cum: Counters::new(start, end),
		}
	}

	fn submit(&mut self, rec: &InfectionRecord) {
		let index = self.cases_by_pub_cum.date_index(rec.report_date).expect("date out of range");
		let k = (rec.district_id, rec.age_group, rec.sex);

		// NOTE: we have to include **retracted** cases here, because the diff tooling will in fact subtract those again -> we'd end up with too few cases.
		let (case_index, case_count) = match rec.case {
			ReportFlag::Consistent => (index, rec.case_count),
			ReportFlag::Retracted => (self.cases_by_pub_cum.len()-2, -rec.case_count),
			_ => (index, 0),
		};
		assert!(case_count >= 0);
		let (death_index, death_count) = match rec.death {
			ReportFlag::Consistent => (index, rec.death_count),
			ReportFlag::Retracted => (self.cases_by_pub_cum.len()-2, -rec.death_count),
			_ => (index, 0),
		};
		assert!(death_count >= 0);
		let (recovered_index, recovered_count) = match rec.recovered {
			ReportFlag::Consistent => (index, rec.recovered_count),
			ReportFlag::Retracted => (self.cases_by_pub_cum.len()-2, -rec.recovered_count),
			_ => (index, 0),
		};
		assert!(recovered_count >= 0);

		self.cases_by_pub_cum.get_or_create(k)[case_index] += case_count as u64;
		self.deaths_by_pub_cum.get_or_create(k)[death_index] += death_count as u64;
		self.recovered_by_pub_cum.get_or_create(k)[recovered_index] += recovered_count as u64;
	}

	fn write_all<W: io::Write, S: ProgressSink + ?Sized>(&self, s: &mut S, w: &mut W) -> io::Result<()> {
		let len = self.cases_by_pub_cum.len();
		let mut pm = StepMeter::new(s, len);
		let mut w = csv::Writer::from_writer(w);
		for i in 0..len {
			let date = self.cases_by_pub_cum.index_date(i as i64).unwrap();
			for k in self.cases_by_pub_cum.keys() {
				let cases_cum = self.cases_by_pub_cum.get_value(k, i).unwrap_or(0);
				let deaths_cum = self.deaths_by_pub_cum.get_value(k, i).unwrap_or(0);
				let recovered_cum = self.recovered_by_pub_cum.get_value(k, i).unwrap_or(0);
				if cases_cum == 0 && deaths_cum == 0 && recovered_cum == 0 {
					continue
				}
				let (district_id, age_group, sex) = *k;
				w.serialize(DiffBaseRecord{
					date,
					district_id,
					age_group,
					sex,
					cases_cum,
					deaths_cum,
					recovered_cum,
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

fn load_case_data<'s, P: AsRef<Path>, S: ProgressSink + ?Sized>(
		s: &'s mut S,
		p: P,
		cases: &mut PartialBaseData,
) -> io::Result<()> {
	let r = covid::magic_open(p)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: InfectionRecord = row?;
		cases.submit(&rec);
		if i % 500000 == 499999 {
			pm.update(i+1);
		}
		n = i+1;
	}
	pm.finish(n);
	Ok(())
}

fn writeback<P: AsRef<Path>, S: ProgressSink + ?Sized>(s: &mut S, path: P, d: &PartialBaseData) -> io::Result<()> {
	let w = File::create(path)?;
	let mut w = flate2::write::GzEncoder::new(w, flate2::Compression::best());
	d.write_all(s, &mut w)?;
	w.finish()?;
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let basefile = &argv[1];
	let basedate = &argv[2];
	let outfile = &argv[3];

	let start = global_start_date();
	let end = basedate.parse::<NaiveDate>()?;
	let mut counters = PartialBaseData::new(start, end);

	println!("loading case data ...");
	load_case_data(&mut *covid::default_output(), basefile, &mut counters)?;

	println!("cumulating ...");
	counters.cases_by_pub_cum.cumsum();
	counters.deaths_by_pub_cum.cumsum();
	counters.recovered_by_pub_cum.cumsum();

	println!("writing base file ...");
	writeback(&mut *covid::default_output(), outfile, &counters)?;

	Ok(())
}
