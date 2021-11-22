use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hash;

use num_traits::Zero;

use chrono::NaiveDate;


pub trait TimeSeriesKey: Hash + Eq + Clone + std::fmt::Debug {}
impl<T: Hash + Eq + Clone + std::fmt::Debug> TimeSeriesKey for T {}


#[derive(Debug, Clone)]
pub struct TimeSeries<T: Hash + Eq, V: Copy> {
	start: NaiveDate,
	keys: HashMap<T, usize>,
	time_series: Vec<Vec<V>>,
	len: usize,
}

impl<T: Hash + Eq, V: Copy> TimeSeries<T, V> {
	pub fn new(start: NaiveDate, last: NaiveDate) -> Self {
		let len = (last - start).num_days();
		assert!(len >= 0);
		let len = len as usize;
		Self{
			start,
			len,
			keys: HashMap::new(),
			time_series: Vec::new(),
		}
	}

	#[inline(always)]
	pub fn date_index(&self, other: NaiveDate) -> Option<usize> {
		let days = (other - self.start).num_days();
		if days < 0 || days as usize >= self.len {
			return None
		}
		return Some(days as usize)
	}

	#[inline(always)]
	pub fn index_date(&self, i: i64) -> Option<NaiveDate> {
		if i < 0 || i as usize >= self.len {
			return None
		}
		return Some(self.start + chrono::Duration::days(i))
	}

	#[inline(always)]
	pub fn start(&self) -> NaiveDate {
		self.start
	}

	#[inline(always)]
	pub fn len(&self) -> usize {
		self.len
	}
}

impl<T: TimeSeriesKey, V: Copy + Zero> TimeSeries<T, V> {
	pub fn get_or_create(&mut self, k: T) -> &mut [V] {
		let index = self.get_index_or_create(k);
		&mut self.time_series[index][..]
	}

	pub fn get_index_or_create(&mut self, k: T) -> usize {
		match self.keys.get(&k) {
			Some(v) => *v,
			None => {
				let v = self.time_series.len();
				let mut vec = Vec::with_capacity(self.len);
				vec.resize(self.len, V::zero());
				self.time_series.push(vec);
				self.keys.insert(k, v);
				v
			},
		}
	}

	fn get_index_or_insert(&mut self, k: T, vec: Vec<V>) -> usize {
		assert_eq!(vec.len(), self.len);
		match self.keys.get(&k) {
			Some(v) => *v,
			None => {
				let v = self.time_series.len();
				self.time_series.push(vec);
				self.keys.insert(k, v);
				v
			},
		}
	}

	pub fn get_index(&self, k: &T) -> Option<usize> {
		Some(*self.keys.get(k)?)
	}

	pub fn get(&self, k: &T) -> Option<&[V]> {
		let index = self.get_index(k)?;
		Some(&self.time_series[index][..])
	}

	pub fn get_value(&self, k: &T, i: usize) -> Option<V> {
		if i >= self.len {
			return None
		}
		self.get(k).and_then(|v| { Some(v[i]) })
	}

	pub fn keys(&self) -> std::collections::hash_map::Keys<'_, T, usize> {
		self.keys.keys()
	}

	// occassionally useful for debugging
	#[allow(dead_code)]
	fn reverse_index(&self, i: usize) -> Option<&T> {
		for (k, v) in self.keys.iter() {
			if *v == i {
				return Some(k)
			}
		}
		None
	}
}

fn saturating_add_u64_i64(reg: &mut u64, v: i64) {
	if v < 0 {
		let v = (-v) as u64;
		*reg = reg.saturating_sub(v);
	} else {
		*reg = reg.checked_add(v as u64).unwrap();
	}
}

impl<T: TimeSeriesKey> TimeSeries<T, u64> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> TimeSeries<U, u64> {
		let mut result = TimeSeries::<U, u64>{
			start: self.start,
			len: self.len,
			keys: HashMap::new(),
			time_series: Vec::new(),
		};
		for (k_old, index_old) in self.keys.iter() {
			let k_new = match f(&k_old) {
				Some(k) => k,
				None => continue,
			};
			let ts_new = result.get_or_create(k_new);
			let ts_old = &self.time_series[*index_old][..];
			assert_eq!(ts_new.len(), ts_old.len());
			for i in 0..ts_new.len() {
				// This is safe because we asserted that both slices have the
				// same length and the loop is only going up to that length
				// minus one.
				unsafe {
					*ts_new.get_unchecked_mut(i) += *ts_old.get_unchecked(i);
				}
			}
		}
		result
	}

	pub fn synthesize(&mut self, kin: &[&T], kout: T) {
		let mut vtemp = Vec::new();
		vtemp.resize(self.len, 0);
		for k in kin {
			let tsin = match self.get(k) {
				Some(ts) => ts,
				None => continue,
			};
			assert_eq!(tsin.len(), vtemp.len());
			for i in 0..tsin.len() {
				// This is safe because we asserted that both slices have the
				// same length and the loop is only going up to that length
				// minus one.
				unsafe {
					*vtemp.get_unchecked_mut(i) += *tsin.get_unchecked(i);
				}
			}
		}
		self.get_index_or_insert(kout, vtemp);
	}

	pub fn cumsum(&mut self) {
		for vec in self.time_series.iter_mut() {
			let mut accum: u64 = 0;
			for v in vec.iter_mut() {
				accum += *v;
				*v = accum;
			}
		}
	}

	pub fn diff(&mut self, offset: usize) {
		for (vec_index, vec) in self.time_series.iter_mut().enumerate() {
			for i in offset..vec.len() {
				let r = vec[i];
				let i_l = i - offset;
				vec[i_l] = match r.checked_sub(vec[i_l]) {
					Some(v) => v,
					None => {
						let vl = vec[i_l];
						drop(vec);
						panic!("diff operation on non-cumulative input: prev={}, curr={} at index {} ({:?}) with window size {} in key {:?}", vl, r, i, self.index_date(i as i64), offset, self.reverse_index(vec_index))
					}
				};
			}
			vec.rotate_right(offset);
			vec[..offset].fill(0);
		}
	}

	pub fn rfill_zeroes(&mut self) {
		for vec in self.time_series.iter_mut() {
			let mut last_nonzero: Option<usize> = None;
			for (i, v) in vec.iter().enumerate().rev() {
				if *v != 0 {
					last_nonzero = Some(i);
					break;
				}
			}
			match last_nonzero {
				Some(i) if i + 1 < vec.len() => {
					let v = vec[i];
					vec[i+1..].fill(v);
				},
				_ => (),
			}
		}
	}

	pub fn add(&mut self, other: &Counters<T>) {
		for (k, my_index) in self.keys.iter() {
			let remote = match other.get(&k) {
				Some(v) => v,
				None => continue,
			};
			let local = &mut self.time_series[*my_index];
			assert_eq!(local.len(), remote.len());
			for i in 0..local.len() {
				// SAFETY: iterating only up to len() and asserted that remote and local have the same length.
				unsafe {
					*local.get_unchecked_mut(i) += remote.get_unchecked(i);
				}
			}
		}
	}

	pub fn checked_add_signed(&mut self, other: &Diff<T>) {
		for (k, other_index) in other.keys.iter() {
			let remote = &other.time_series[*other_index];
			let local = self.get_or_create(k.clone());
			assert_eq!(local.len(), remote.len());
			for i in 0..local.len() {
				// SAFETY: iterating only up to len() and asserted that remote and local have the same length.
				unsafe {
					saturating_add_u64_i64(local.get_unchecked_mut(i), *remote.get_unchecked(i));
				}
			}
		}
	}

	pub fn sub_at(&mut self, at_local: usize, other: &Counters<T>, at_remote: usize) {
		for (k, my_index) in self.keys.iter() {
			let remote = match other.get(&k) {
				Some(v) => v,
				None => continue,
			};
			let local = &mut self.time_series[*my_index];
			local[at_local] = match local[at_local].checked_sub(remote[at_remote]) {
				Some(v) => v,
				None => panic!("sub_at decreases below zero with diff {} on value {} at index {} in key {:?}", remote[at_remote], local[at_local], at_local, k)
			};
		}
	}

	pub fn find_ge(&self, k: &T, start_at: usize, value: u64) -> Option<usize> {
		let vec = self.get(k)?;
		for i in start_at..vec.len() {
			let v = vec[i];
			if v >= value {
				return Some(i)
			}
		}
		None
	}

	pub fn unrolled(&self, window_size: usize) -> Self {
		// NOTE: this unrolling isn't perfect. In some corner cases (probably with actual bogus data), we end up in situations where a counter would go negative. We then carry the number over to the next slot, which is fine as far as the totals go.
		// The problem is however that this still causes a slight difference when compared to influxdb outputs, I guess can happen in some weird cases (for instane, if a hospitalization is recorded in a 7 day sum in district A and then retracted on the next day (without back-correctign the previous one) and moved to another district and district A never sees another hospitalization again (i.e. the negative carry can never be resolved)).
		// The overall difference is something like a dozen or so, so good enough™.
		// Most of the difference is also currently accured during the beginning of the pandemic, so it's rather likely that these are artifacts caused by retractions or somesuch.
		let mut result = self.clone();
		for (vec_index, dst) in result.time_series.iter_mut().enumerate() {
			let src = &self.time_series[vec_index];
			let mut neg_carry: u64 = 0;
			for i in 0..dst.len() {
				let v_l: i64 = if i < window_size {
					0
				} else {
					dst[i-window_size].try_into().unwrap()
				};
				let v_p: i64 = if i > 0 {
					src[i-1].try_into().unwrap()
				} else {
					0
				};
				let v_c: i64 = src[i].try_into().unwrap();
				let new = (v_c - v_p) + v_l;
				let new: u64 = if new < 0 {
					// this can happen on weird data. we smooth this out by carrying the negative result downward
					let to_carry = (-new) as u64;
					neg_carry += to_carry;
					0
				} else {
					let mut new = new as u64;
					// this is essentially a saturating sub, while keeping the leftover in neg_carry
					if neg_carry >= new {
						neg_carry -= new;
						new = 0;
					} else {
						new -= neg_carry;
						neg_carry = 0;
					}
					new
				};
				dst[i] = new as u64;
			}
		}
		result
	}

	pub fn shift_fwd(&mut self, offset: usize) {
		if offset >= self.len {
			for vec in self.time_series.iter_mut() {
				vec.fill(0);
			}
		}
		for vec in self.time_series.iter_mut() {
			vec.rotate_right(offset);
			vec[..offset].fill(0);
		}
	}
}

impl<T: TimeSeriesKey> TimeSeries<T, i64> {
	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> TimeSeries<U, i64> {
		let mut result = TimeSeries::<U, i64>{
			start: self.start,
			len: self.len,
			keys: HashMap::new(),
			time_series: Vec::new(),
		};
		for (k_old, index_old) in self.keys.iter() {
			let k_new = match f(&k_old) {
				Some(k) => k,
				None => continue,
			};
			let ts_new = result.get_or_create(k_new);
			let ts_old = &self.time_series[*index_old][..];
			assert_eq!(ts_new.len(), ts_old.len());
			for i in 0..ts_new.len() {
				// This is safe because we asserted that both slices have the
				// same length and the loop is only going up to that length
				// minus one.
				unsafe {
					*ts_new.get_unchecked_mut(i) += *ts_old.get_unchecked(i);
				}
			}
		}
		result
	}

	pub fn cumsum(&mut self) {
		for vec in self.time_series.iter_mut() {
			let mut accum: i64 = 0;
			for v in vec.iter_mut() {
				accum += *v;
				*v = accum;
			}
		}
	}

	pub fn find_ge(&self, k: &T, start_at: usize, value: i64) -> Option<usize> {
		let vec = self.get(k)?;
		for i in start_at..vec.len() {
			let v = vec[i];
			if v >= value {
				return Some(i)
			}
		}
		None
	}

	pub fn clamped(mut self) -> TimeSeries<T, u64> {
		// the most evil thing.
		for vec in self.time_series.iter_mut() {
			for v in vec.iter_mut() {
				unsafe {
					*v = std::mem::transmute::<u64, i64>(
						if *v < 0 {
							0u64
						} else {
							*v as u64
						}
					);
				}
			}
		}
		TimeSeries::<T, u64>{
			start: self.start,
			len: self.len,
			keys: self.keys,
			time_series: unsafe { std::mem::transmute::<Vec<Vec<i64>>, Vec<Vec<u64>>>(self.time_series) },
		}
	}
}


#[macro_export]
macro_rules! joined_keyset_ref {
	($t:ty, $($b:expr),*) => {
		{
			let mut keyset: std::collections::HashSet<&$t> = std::collections::HashSet::new();
			$(
				for k in $b.keys() {
					keyset.insert(k);
				}
			)*
			keyset
		}
	}
}


impl<T: TimeSeriesKey> From<TimeSeries<T, u64>> for TimeSeries<T, f64> {
	fn from(mut other: TimeSeries<T, u64>) -> Self {
		// the most evil thing.
		for vec in other.time_series.iter_mut() {
			for v in vec.iter_mut() {
				unsafe {
					*v = std::mem::transmute::<f64, u64>(*v as f64);
				}
			}
		}
		Self{
			start: other.start,
			len: other.len,
			keys: other.keys,
			time_series: unsafe { std::mem::transmute::<Vec<Vec<u64>>, Vec<Vec<f64>>>(other.time_series) },
		}
	}
}


pub struct CounterGroup<T: TimeSeriesKey> {
	cum: Counters<T>,
	d1: Counters<T>,
	d7: Counters<T>,
	d7s7: Counters<T>,
}

impl<T: TimeSeriesKey> CounterGroup<T> {
	pub fn from_d1(d1: Counters<T>) -> Self {
		let mut cum = d1.clone();
		cum.cumsum();
		let mut d7 = cum.clone();
		d7.diff(7);
		let mut d7s7 = d7.clone();
		d7s7.shift_fwd(7);
		Self{
			cum,
			d1,
			d7,
			d7s7,
		}
	}

	pub fn from_d1_and_cum(d1: Counters<T>, cum: Counters<T>) -> Self {
		let mut d7 = cum.clone();
		d7.diff(7);
		let mut d7s7 = d7.clone();
		d7s7.shift_fwd(7);
		Self{
			cum,
			d1,
			d7,
			d7s7,
		}
	}

	pub fn from_d7(d7: Counters<T>) -> Self {
		let d1 = d7.unrolled(7);
		let mut cum = d1.clone();
		cum.cumsum();
		let mut d7s7 = d7.clone();
		d7s7.shift_fwd(7);
		Self{
			cum,
			d1,
			d7,
			d7s7,
		}
	}

	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CounterGroup<U> {
		CounterGroup::<U>{
			cum: self.cum.rekeyed(&f),
			d1: self.d1.rekeyed(&f),
			d7: self.d7.rekeyed(&f),
			d7s7: self.d7s7.rekeyed(&f),
		}
	}

	pub fn synthesize(&mut self, kin: &[&T], kout: T) {
		self.cum.synthesize(kin, kout.clone());
		self.d1.synthesize(kin, kout.clone());
		self.d7.synthesize(kin, kout.clone());
		self.d7s7.synthesize(kin, kout);
	}

	pub fn cum(&self) -> &Counters<T> {
		&self.cum
	}

	pub fn d1(&self) -> &Counters<T> {
		&self.d1
	}

	pub fn d7(&self) -> &Counters<T> {
		&self.d7
	}

	pub fn d7s7(&self) -> &Counters<T> {
		&self.d7s7
	}
}


pub struct SubmittableCounterGroup<T: TimeSeriesKey> {
	pub cum: Submittable<T>,
	pub d1: Submittable<T>,
	pub d7: Submittable<T>,
	pub d7s7: Submittable<T>,
}

impl<T: TimeSeriesKey> From<CounterGroup<T>> for SubmittableCounterGroup<T> {
	fn from(other: CounterGroup<T>) -> SubmittableCounterGroup<T> {
		Self{
			cum: other.cum.into(),
			d1: other.d1.into(),
			d7: other.d7.into(),
			d7s7: other.d7s7.into(),
		}
	}
}


pub type Counters<T> = TimeSeries<T, u64>;
pub type IGauge<T> = TimeSeries<T, u64>;
pub type FGauge<T> = TimeSeries<T, f64>;
pub type Submittable<T> = TimeSeries<T, f64>;
pub type Diff<T> = TimeSeries<T, i64>;
