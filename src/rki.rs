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

pub type DistrictId = u32;
pub type StateId = u32;
pub type FullCaseKey = (StateId, DistrictId, MaybeAgeGroup, Sex);


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
			Self::Female => f.write_str("W"),
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


fn legacy_date_compat<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
	where D: Deserializer<'de>
{
	let mut s = String::deserialize(deserializer)?;
	if s.len() == 10 {
		// plain ISO date
		s.parse::<NaiveDate>().map_err(de::Error::custom)
	} else if s.len() == 19 {
		// full pseudo-ISO date
		s.truncate(10);
		let s = s.replace("/", "-");
		s.parse::<NaiveDate>().map_err(de::Error::custom)
	} else {
		Err(de::Error::custom("invalid length for date, must be eiter 10 or 19 bytes"))
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
	#[serde(rename = "Meldedatum", deserialize_with="legacy_date_compat")]
	pub report_date: NaiveDate,
	#[serde(rename = "Refdatum", deserialize_with="legacy_date_compat")]
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

fn zero_u64() -> u64 {
	0
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiffRecord {
	#[serde(rename = "Datum")]
	pub date: NaiveDate,
	#[serde(rename = "LandkreisId")]
	pub district_id: DistrictId,
	#[serde(rename = "Altersgruppe")]
	pub age_group: MaybeAgeGroup,
	#[serde(rename = "Geschlecht")]
	pub sex: Sex,
	#[serde(rename = "AnzahlFall", default = "zero_u64")]
	pub cases: u64,
	#[serde(rename = "AnzahlTodesfall")]
	pub deaths: u64,
	#[serde(rename = "AnzahlGenesen", default = "zero_u64")]
	pub recovered: u64,
}

impl DiffRecord {
	pub fn write_header<W: io::Write>(w: &mut W) -> io::Result<()> {
		w.write("Datum,LandkreisId,Altersgruppe,Geschlecht,AnzahlFall,AnzahlTodesfall,AnzahlGenesen\n".as_bytes())?;
		Ok(())
	}

	pub fn write<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
		write!(w, "{},{},{},{},{},{},{}\n", self.date, self.district_id, self.age_group, self.sex, self.cases, self.deaths, self.recovered)
	}
}
