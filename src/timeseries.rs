use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hash;
use std::sync::Arc;

use num_traits::Zero;

use chrono::NaiveDate;


pub trait TimeSeriesKey: Hash + Eq + Clone + std::fmt::Debug + 'static {}
impl<T: Hash + Eq + Clone + std::fmt::Debug + 'static> TimeSeriesKey for T {}


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
		for vec in self.time_series.iter_mut() {
			for i in offset..vec.len() {
				let r = vec[i];
				let i_l = i - offset;
				vec[i_l] = r.checked_sub(vec[i_l]).expect("diff needs cumsum as input");
			}
			vec.rotate_right(offset);
			vec[..offset].fill(0);
		}
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


pub trait ViewTimeSeries<T: TimeSeriesKey> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64>;
}


impl<T: TimeSeriesKey> ViewTimeSeries<T> for TimeSeries<T, u64> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64> {
		let i = self.date_index(at)?;
		Some(self.get_value(k, i).unwrap_or(0) as f64)
	}
}


impl<T: TimeSeriesKey> ViewTimeSeries<T> for TimeSeries<T, i64> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64> {
		let i = self.date_index(at)?;
		Some(self.get_value(k, i).unwrap_or(0) as f64)
	}
}


impl<T: TimeSeriesKey> ViewTimeSeries<T> for TimeSeries<T, f64> {
	fn getf(&self, k: &T, at: NaiveDate) -> Option<f64> {
		let i = self.date_index(at)?;
		Some(self.get_value(k, i).unwrap_or(0.))
	}
}

pub struct TimeMap<I> {
	inner: I,
	by: i64,
	range: Option<(NaiveDate, NaiveDate)>,
	pad: Option<f64>,
}

impl<I> TimeMap<I> {
	pub fn shift(inner: I, by: i64) -> Self {
		Self{
			inner,
			by,
			range: None,
			pad: None,
		}
	}
}

impl<K: TimeSeriesKey, I: ViewTimeSeries<K>> ViewTimeSeries<K> for TimeMap<I> {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		match self.range {
			Some((start, end)) => if (at < start) || (at >= end) {
				return None
			},
			None => (),
		};
		let at = at + chrono::Duration::days(self.by);
		self.inner.getf(k, at).or(self.pad)
	}
}

pub struct Filled<I> {
	inner: I,
	from: NaiveDate,
}

impl<I> Filled<I> {
	pub fn new(inner: I, from: NaiveDate) -> Self {
		Self{inner, from}
	}
}

impl<K: TimeSeriesKey, I: ViewTimeSeries<K>> ViewTimeSeries<K> for Filled<I> {
	fn getf(&self, k: &K, _at: NaiveDate) -> Option<f64> {
		self.inner.getf(k, self.from)
	}
}

pub struct Diff<I> {
	inner: I,
	window: u32,
	pad: Option<f64>,
}

impl<I> Diff<I> {
	pub fn padded(inner: I, window: u32, pad: f64) -> Self {
		Self{inner, window, pad: Some(pad)}
	}
}

impl<K: TimeSeriesKey, I: ViewTimeSeries<K>> ViewTimeSeries<K> for Diff<I> {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		let vr = self.inner.getf(k, at)?;
		let vl = self.inner.getf(k, at - chrono::Duration::days(self.window as i64)).or(self.pad)?;
		Some(vr - vl)
	}
}

pub struct MovingSum<I> {
	inner: I,
	window: u32,
}

impl<I> MovingSum<I> {
	pub fn new(inner: I, window: u32) -> Self {
		Self{inner, window}
	}
}

impl<K: TimeSeriesKey, I: ViewTimeSeries<K>> ViewTimeSeries<K> for MovingSum<I> {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		let mut accum = self.inner.getf(k, at)?;
		for i in (1..self.window).rev() {
			accum += self.inner.getf(k, at - chrono::Duration::days(i as i64)).unwrap_or(0.)
		}
		Some(accum)
	}
}

impl<K: TimeSeriesKey, T: ViewTimeSeries<K>> ViewTimeSeries<K> for &T {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		(**self).getf(k, at)
	}
}

impl<K: TimeSeriesKey, T: ViewTimeSeries<K>> ViewTimeSeries<K> for Arc<T> {
	fn getf(&self, k: &K, at: NaiveDate) -> Option<f64> {
		(**self).getf(k, at)
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
	pub cum: Arc<Counters<T>>,
	pub d1: Arc<Diff<Arc<Counters<T>>>>,
	pub d7: Arc<Diff<Arc<Counters<T>>>>,
	pub d7s7: Arc<TimeMap<Arc<Diff<Arc<Counters<T>>>>>>,
}

impl<T: TimeSeriesKey> CounterGroup<T> {
	pub fn from_cum(cum: Counters<T>) -> Self {
		let cum = Arc::new(cum);
		let d7 = Arc::new(Diff::padded(cum.clone(), 7, 0.));
		Self{
			cum: cum.clone(),
			d1: Arc::new(Diff::padded(cum.clone(), 1, 0.)),
			d7: d7.clone(),
			d7s7: Arc::new(TimeMap::shift(d7.clone(), 7)),
		}
	}

	pub fn from_d1(d1: Counters<T>) -> Self {
		let mut cum = d1.clone();
		cum.cumsum();
		Self::from_cum(cum)
	}

	pub fn from_d7(d7: Counters<T>) -> Self {
		let d1 = d7.unrolled(7);
		Self::from_d1(d1)
	}

	pub fn rekeyed<U: TimeSeriesKey, F: Fn(&T) -> Option<U>>(&self, f: F) -> CounterGroup<U> {
		CounterGroup::<U>::from_cum(self.cum.rekeyed(&f))
	}

	pub fn cum(&self) -> Arc<dyn ViewTimeSeries<T>> {
		self.cum.clone() as _
	}

	pub fn d1(&self) -> Arc<dyn ViewTimeSeries<T>> {
		self.d1.clone() as _
	}

	pub fn d7(&self) -> Arc<dyn ViewTimeSeries<T>> {
		self.d7.clone() as _
	}

	pub fn d7s7(&self) -> Arc<dyn ViewTimeSeries<T>> {
		self.d7s7.clone() as _
	}
}


pub type Counters<T> = TimeSeries<T, u64>;
pub type IGauge<T> = TimeSeries<T, u64>;
pub type FGauge<T> = TimeSeries<T, f64>;
