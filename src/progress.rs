use std::io;
use std::io::Write;
use std::time;


pub struct ProgressMeter {
	t0: time::Instant,
	tprev: time::Instant,
	iprev: usize,
	n: Option<usize>,
}

pub trait ProgressSink {
	fn update(&mut self, inow: usize);
	fn finish(self, inow: Option<usize>);
}

impl ProgressMeter {
	pub fn start(n: Option<usize>) -> Self {
		let now = time::Instant::now();
		match n {
			Some(_) => print!("{:6.0}% [{:6.2}/s]\r", 0.0, 0),
			None => print!("{:12} [{:6.2}/s]\r", 0, 0),
		}
		io::stdout().flush().unwrap();
		Self{
			t0: now,
			tprev: now,
			iprev: 0,
			n,
		}
	}
}

impl ProgressSink for ProgressMeter {
	fn update(&mut self, inow: usize) {
		let now = time::Instant::now();
		let dt = (now - self.tprev).as_secs_f64();
		let rate = (inow - self.iprev) as f64 / dt;
		match self.n {
			Some(n) => {
				let done = (inow as f64) / (n as f64);
				print!("{:6.0}% [{:6.2}/s]\r", done * 100.0, rate);
			},
			None => {
				print!("{:12} [{:6.2}/s]\r", inow, rate);
			},
		}
		io::stdout().flush().unwrap();
		self.iprev = inow;
		self.tprev = now;
	}

	fn finish(self, inow: Option<usize>) {
		let (inow, tnow) = match inow.or(self.n) {
			Some(inow) => (inow, time::Instant::now()),
			None => (self.iprev, self.tprev),
		};
		let dt = (tnow - self.t0).as_secs_f64();
		let rate = inow as f64 / dt;
		match self.n {
			Some(_) => {
				println!("{:6.0}% [{:6.2}/s]\r", 100.0, rate);
			},
			None => {
				println!("{:12} [{:6.2}/s]\r", inow, rate);
			},
		}
	}
}
