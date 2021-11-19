use std::collections::HashMap;
use std::fmt;
use std::io;
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use std::hash::Hash;

use serde::{de, Deserialize, Deserializer};

use chrono::naive::NaiveDate;

pub mod influxdb;

pub type DistrictId = u32;
pub type StateId = u32;


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum Sex {
	#[serde(rename = "M")]
	Male,
	#[serde(rename = "W")]
	Female,
	#[serde(rename = "unbekannt")]
	Unknown,
}

impl fmt::Display for Sex {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Male => f.write_str("M"),
			Self::Female => f.write_str("F"),
			Self::Unknown => f.write_str("unbekannt"),
		}
	}
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum ReportFlag {
	// a bit of a hack, because the things are actual numeric
	#[serde(rename = "1")]
	NewlyReported,
	#[serde(rename = "-1")]
	Retracted,
	#[serde(rename = "-9")]
	NotApplicable,
	#[serde(rename = "0")]
	Consistent,
}

impl ReportFlag {
	pub fn valid(&self) -> bool {
		match self {
			Self::NewlyReported => true,
			Self::Consistent => true,
			_ => false,
		}
	}
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AgeGroup {
	pub low: u16,
	pub high: Option<u16>,
}


#[derive(Debug, Clone)]
pub enum ParseAgeGroupError {
	NoLeadingA,
	NoSeparator,
	InvalidNumber(ParseIntError),
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MaybeAgeGroup(Option<AgeGroup>);

impl Deref for MaybeAgeGroup {
	type Target = Option<AgeGroup>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for MaybeAgeGroup {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl From<MaybeAgeGroup> for Option<AgeGroup> {
	fn from(other: MaybeAgeGroup) -> Self {
		other.0
	}
}

impl From<Option<AgeGroup>> for MaybeAgeGroup {
	fn from(other: Option<AgeGroup>) -> Self {
		Self(other)
	}
}

impl fmt::Display for ParseAgeGroupError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::NoLeadingA => f.write_str("missing leading A on age group"),
			Self::NoSeparator => f.write_str("missing dash or trailing plus"),
			Self::InvalidNumber(e) => fmt::Display::fmt(e, f),
		}
	}
}

impl From<ParseIntError> for ParseAgeGroupError {
	fn from(other: ParseIntError) -> Self {
		Self::InvalidNumber(other)
	}
}


impl FromStr for AgeGroup {
	type Err = ParseAgeGroupError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if !s.starts_with("A") {
			return Err(ParseAgeGroupError::NoLeadingA)
		}
		if s.ends_with("+") {
			let num = &s[1..(s.len()-1)];
			let lower_bound = FromStr::from_str(num)?;
			return Ok(Self{low: lower_bound, high: None})
		}
		let (low, high) = match s.split_once('-') {
			Some(v) => v,
			None => return Err(ParseAgeGroupError::NoSeparator),
		};
		if !low.starts_with("A") || !high.starts_with("A") {
			return Err(ParseAgeGroupError::NoLeadingA);
		}
		let low = &low[1..];
		let high = &high[1..];
		Ok(Self {
			low: FromStr::from_str(low)?,
			high: Some(FromStr::from_str(high)?),
		})
	}
}

impl fmt::Display for AgeGroup {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "A{:02}", self.low)?;
		match self.high {
			Some(v) => write!(f, "-A{:02}", v),
			None => f.write_str("+"),
		}
	}
}

impl fmt::Display for MaybeAgeGroup {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self.0 {
			Some(v) => fmt::Display::fmt(&v, f),
			None => f.write_str("unbekannt"),
		}
	}
}


impl<'de> Deserialize<'de> for AgeGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
    }
}


impl<'de> Deserialize<'de> for MaybeAgeGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        if s == "unbekannt" {
			return Ok(MaybeAgeGroup(None))
		} else {
			return Ok(MaybeAgeGroup(Some(FromStr::from_str(&s).map_err(de::Error::custom)?)))
		}
    }
}


#[derive(Debug, Clone, Deserialize)]
pub struct InfectionRecord {
	#[serde(rename = "IdLandkreis")]
	pub district_id: DistrictId,
	#[serde(rename = "Altersgruppe")]
	pub age_group: MaybeAgeGroup,
	#[serde(rename = "Geschlecht")]
	pub sex: Sex,
	#[serde(rename = "Meldedatum")]
	pub report_date: NaiveDate,
	#[serde(rename = "Refdatum")]
	pub reference_date: NaiveDate,
	#[serde(rename = "IstErkrankungsbeginn")]
	pub is_start_of_case: u8,
	#[serde(rename = "NeuerFall")]
	pub case: ReportFlag,
	#[serde(rename = "NeuerTodesfall")]
	pub death: ReportFlag,
	#[serde(rename = "NeuGenesen")]
	pub recovered: ReportFlag,
	#[serde(rename = "AnzahlFall")]
	pub case_count: i32,
	#[serde(rename = "AnzahlTodesfall")]
	pub death_count: i32,
	#[serde(rename = "AnzahlGenesen")]
	pub recovered_count: i32,
}


#[derive(Debug, Clone)]
pub struct StateInfo {
	pub id: DistrictId,
	pub name: String,
}


#[derive(Debug, Clone)]
pub struct DistrictInfo {
	pub id: DistrictId,
	pub name: String,
	pub state: Arc<StateInfo>,
	pub population: u64,
}


#[derive(Debug, Clone, Deserialize)]
pub struct RawDistrictRow {
	#[serde(rename = "BL_ID")]
	pub state_id: DistrictId,
	#[serde(rename = "BL")]
	pub state_name: String,
	#[serde(rename = "RS")]
	pub district_id: DistrictId,
	#[serde(rename = "county")]
	pub district_name: String,
	#[serde(rename = "EWZ")]
	pub population: u64,
}


pub fn load_rki_districts<R: io::Read>(r: &mut R) -> Result<(HashMap<DistrictId, Arc<StateInfo>>, HashMap<DistrictId, Arc<DistrictInfo>>), io::Error> {
	let mut states: HashMap<DistrictId, Arc<StateInfo>> = HashMap::new();
	let mut districts = HashMap::new();
	let mut r = csv::Reader::from_reader(r);
	for row in r.deserialize() {
		let rec: RawDistrictRow = row?;
		let state_entry = match states.get(&rec.state_id) {
			Some(e) => e.clone(),
			None => {
				let state = Arc::new(StateInfo{
					id: rec.state_id,
					name: rec.state_name,
				});
				states.insert(rec.state_id, state.clone());
				state
			},
		};
		let district = Arc::new(DistrictInfo{
			id: rec.district_id,
			name: rec.district_name,
			population: rec.population,
			state: state_entry,
		});
		districts.insert(district.id, district);
	}
	Ok((states, districts))
}

#[derive(Debug, Clone)]
pub struct Counters<T: Hash + Eq> {
	start: NaiveDate,
	keys: HashMap<T, usize>,
	time_series: Vec<Vec<u64>>,
	len: usize,
}

impl<T: Hash + Eq> Counters<T> {
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

impl<T: Hash + Clone + Eq> Counters<T> {
	pub fn get_or_create(&mut self, k: T) -> &mut [u64] {
		let index = self.get_index_or_create(k);
		&mut self.time_series[index][..]
	}

	pub fn get_index_or_create(&mut self, k: T) -> usize {
		match self.keys.get(&k) {
			Some(v) => *v,
			None => {
				let v = self.time_series.len();
				let mut vec = Vec::with_capacity(self.len);
				vec.resize(self.len, 0);
				self.time_series.push(vec);
				self.keys.insert(k, v);
				v
			},
		}
	}

	pub fn get_index(&self, k: &T) -> Option<usize> {
		Some(*self.keys.get(k)?)
	}

	pub fn get(&self, k: &T) -> Option<&[u64]> {
		let index = self.get_index(k)?;
		Some(&self.time_series[index][..])
	}

	pub fn get_value(&self, k: &T, i: usize) -> Option<u64> {
		if i >= self.len {
			return None
		}
		self.get(k).and_then(|v| { Some(v[i]) })
	}

	pub fn keys(&self) -> std::collections::hash_map::Keys<'_, T, usize> {
		self.keys.keys()
	}

	pub fn rekeyed<U: Hash + Clone + Eq, F: Fn(&T) -> U>(&self, f: F) -> Counters<U> {
		let mut result = Counters::<U>{
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
