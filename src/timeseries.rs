use std::collections::HashMap;
use std::hash::Hash;

use num_traits::Zero;

use chrono::NaiveDate;


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

impl<T: Hash + Clone + Eq, V: Copy + Zero> TimeSeries<T, V> {
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
}

impl<T: Hash + Clone + Eq> TimeSeries<T, u64> {
	pub fn rekeyed<U: Hash + Clone + Eq, F: Fn(&T) -> U>(&self, f: F) -> TimeSeries<U, u64> {
		let mut result = TimeSeries::<U, u64>{
			start: self.start,
			len: self.len,
			keys: HashMap::new(),
			time_series: Vec::new(),
		};
		for (k_old, index_old) in self.keys.iter() {
			let k_new = f(&k_old);
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


pub type Counters<T> = TimeSeries<T, u64>;
