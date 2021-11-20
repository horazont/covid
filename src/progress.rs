use std::fmt::Write;
use std::io;
use std::time;


/* pub struct ProgressMeter {
	t0: time::Instant,
	tprev: time::Instant,
	iprev: usize,
	n: Option<usize>,
} */

pub struct NullSink();

impl ProgressSink for NullSink {
	fn update(&mut self, _status: Status, _rate: f64) {}
	fn finish(&mut self) {}
}

pub enum Status {
	Count(usize),
	Step(usize, usize),
}

pub trait ProgressSink {
	fn update(&mut self, status: Status, rate: f64);
	fn finish(&mut self);
}

pub struct StepMeter<'x, S: ProgressSink> {
	s: &'x mut S,
	t0: time::Instant,
	tprev: time::Instant,
	iprev: usize,
	n: usize,
}

impl<'x, S: ProgressSink> StepMeter<'x, S> {
	pub fn new(s: &'x mut S, n: usize) -> Self {
		let t0 = time::Instant::now();
		Self{
			s,
			t0,
			tprev: t0,
			iprev: 0,
			n,
		}
	}

	pub fn update(&mut self, inow: usize) {
		let tnow = time::Instant::now();
		let dt = (tnow - self.tprev).as_secs_f64();
		let di = inow - self.iprev;
		self.iprev = inow;
		self.tprev = tnow;

		self.s.update(Status::Step(inow, self.n), (di as f64) / dt);
	}

	pub fn finish(self) {
		let tnow = time::Instant::now();
		let dt = (tnow - self.t0).as_secs_f64();
		self.s.update(Status::Step(self.n, self.n), self.n as f64 / dt);
		self.s.finish();
	}
}

pub struct CountMeter<'x, S: ProgressSink> {
	s: &'x mut S,
	t0: time::Instant,
	tprev: time::Instant,
	iprev: usize,
}

impl<'x, S: ProgressSink> CountMeter<'x, S> {
	pub fn new(s: &'x mut S) -> Self {
		let t0 = time::Instant::now();
		Self{
			s,
			t0,
			tprev: t0,
			iprev: 0,
		}
	}

	pub fn update(&mut self, inow: usize) {
		let tnow = time::Instant::now();
		let dt = (tnow - self.tprev).as_secs_f64();
		let di = inow - self.iprev;
		self.iprev = inow;
		self.tprev = tnow;

		self.s.update(Status::Count(inow), (di as f64) / dt);
	}

	pub fn finish(self, total: usize) {
		let tnow = time::Instant::now();
		let dt = (tnow - self.t0).as_secs_f64();
		self.s.update(Status::Count(total), total as f64 / dt);
		self.s.finish();
	}
}

impl Status {
	fn ratio(&self) -> Option<f64> {
		match self {
			Self::Count(_) => None,
			Self::Step(i, n) => {
				Some((*i as f64) / (*n as f64))
			},
		}
	}

	fn count(&self) -> Option<usize> {
		match self {
			Self::Count(v) => Some(*v),
			Self::Step(i, _) => Some(*i),
		}
	}
}

const TICKS: &[u8] = b"\\|/-";

pub struct TtySink<W: io::Write> {
	w: W,
	tick: u8,
	longest_rate: usize,
}

impl TtySink<io::Stdout> {
	pub fn stdout() -> Self {
		Self{
			w: io::stdout(),
			tick: 0,
			longest_rate: 0,
		}
	}
}

impl<W: io::Write> ProgressSink for TtySink<W> {
	fn update(&mut self, status: Status, rate: f64) {
		let ratio = status.ratio();
		let count = status.count();
		let mut rate_s = String::new();
		let _ = write!(rate_s, "{:.2}/s", rate);
		if rate_s.len() > self.longest_rate {
			self.longest_rate = rate_s.len();
		} else if rate_s.len() < self.longest_rate {
			let missing = self.longest_rate - rate_s.len();
			rate_s.reserve(missing);
			for _ in 0..missing {
				rate_s.insert_str(0, " ");
			}
		}

		let mut lhs = String::new();
		lhs.reserve(14+4+4+2);
		match ratio {
			Some(v) => {
				let _ = write!(lhs, " {:>9.0}% ", v*100.0);
			},
			None => match count {
				Some(c) => {
					self.tick = (self.tick + 1) % (TICKS.len() as u8);
					let _ = write!(lhs, "{:>12} {}", c, TICKS[self.tick as usize] as char);
				},
				None => {
					self.tick = (self.tick + 1) % (TICKS.len() as u8);
					let _ = write!(lhs, "{}", TICKS[self.tick as usize] as char);
				},
			},
		}

		match ratio {
			Some(v) => {
				let pos = (v * lhs.len() as f64).round() as usize;
				lhs.insert_str(pos, "\x1b[0m");
				lhs.insert_str(0, "\x1b[7m");
				lhs.insert_str(0, "|");
				lhs.push_str("|");
			},
			None => (),
		}

		let _ = write!(self.w, "\x1b[K  {}  [{}]\r", lhs, rate_s);
		let _ = self.w.flush();
	}

	fn finish(&mut self) {
		let _ = write!(self.w, "\n");
	}
}
