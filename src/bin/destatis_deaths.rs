use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use csv;

use covid::{
	DestatisDeathCurrent, DestatisDeathHistoric, ProgressSink, RawDestatisDeathByMonthRow,
};

static FIRST_YEAR: i32 = 2020;
static LAST_YEAR: Option<i32> = None;

#[derive(Debug, Clone)]
struct RawMonthlyData {
	pre_pandemic_samples: [Vec<f64>; 12],
	pandemic_samples: BTreeMap<(i32, u32), f64>,
}

impl RawMonthlyData {
	fn new() -> Self {
		Self {
			pre_pandemic_samples: Default::default(),
			pandemic_samples: BTreeMap::new(),
		}
	}

	fn write_pre_pandemics<W: io::Write>(&self, w: W) -> io::Result<()> {
		let mut w = csv::Writer::from_writer(w);
		for index in 0..12 {
			let month = (index + 1) as u32;
			w.serialize(DestatisDeathHistoric::from_sorted_slice(
				month,
				&self.pre_pandemic_samples[index][..],
			))?;
		}
		w.flush()?;
		Ok(())
	}

	fn write_pandemics<W: io::Write>(&self, w: W) -> io::Result<()> {
		let mut w = csv::Writer::from_writer(w);
		for ((year, month), v) in self.pandemic_samples.iter() {
			w.serialize(DestatisDeathCurrent {
				year: *year,
				month: *month,
				death_incidence_per_inhabitant: *v,
			})?;
		}
		w.flush()?;
		Ok(())
	}

	fn submit(&mut self, rec: RawDestatisDeathByMonthRow) {
		let incidence = match Self::get_incidence(&rec) {
			Some(v) => v,
			None => return,
		};
		if rec.year < FIRST_YEAR || rec.year > LAST_YEAR.unwrap_or(rec.year + 1) {
			self.submit_outside(rec.month, incidence);
		} else {
			self.pandemic_samples
				.insert((rec.year, rec.month), incidence);
		}
	}

	fn get_incidence(rec: &RawDestatisDeathByMonthRow) -> Option<f64> {
		Some(rec.death_incidence_per_1k? / 1000.0)
	}

	fn submit_outside(&mut self, month: u32, v: f64) {
		assert!(month >= 1);
		let vec = &mut self.pre_pandemic_samples[(month - 1) as usize];
		let index = match vec.binary_search_by(|a| a.partial_cmp(&v).unwrap()) {
			Ok(i) => i,
			Err(i) => i,
		};
		vec.insert(index, v);
	}
}

fn load_data<P: AsRef<Path>, S: ProgressSink + ?Sized>(
	s: &mut S,
	datafile: P,
	out: &mut RawMonthlyData,
) -> io::Result<()> {
	let r = covid::magic_open(datafile)?;
	let mut r = csv::Reader::from_reader(r);
	let mut pm = covid::CountMeter::new(s);
	let mut n = 0;
	for (i, row) in r.deserialize().enumerate() {
		let rec: RawDestatisDeathByMonthRow = row?;
		out.submit(rec);
		if i % 100 == 99 {
			pm.update(i + 1);
		}
		n = i + 1;
	}
	pm.finish(n);
	Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let datafile = &argv[1];
	let out_pre_pandemic = &argv[2];
	let out_pandemic = &argv[3];
	let mut data = RawMonthlyData::new();
	println!("loading destatis data ...");
	load_data(&mut *covid::default_output(), datafile, &mut data)?;
	println!("writing pre-pandemic summary ...");
	{
		let w = std::fs::File::create(out_pre_pandemic)?;
		data.write_pre_pandemics(w)?;
	}
	println!("writing pandemic monthly data ...");
	{
		let w = std::fs::File::create(out_pandemic)?;
		data.write_pandemics(w)?;
	}
	Ok(())
}
