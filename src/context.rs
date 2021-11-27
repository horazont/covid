use std::fmt;
use std::num::ParseIntError;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer};

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
			Self::Female => f.write_str("W"),
			Self::Unknown => f.write_str("unbekannt"),
		}
	}
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MaybeDistrictId(pub Option<DistrictId>);

impl Deref for MaybeDistrictId {
	type Target = Option<DistrictId>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for MaybeDistrictId {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl From<MaybeDistrictId> for Option<DistrictId> {
	fn from(other: MaybeDistrictId) -> Self {
		other.0
	}
}

impl From<Option<DistrictId>> for MaybeDistrictId {
	fn from(other: Option<DistrictId>) -> Self {
		Self(other)
	}
}

impl FromStr for MaybeDistrictId {
	type Err = ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s == "u" {
			return Ok(MaybeDistrictId(None))
		}
		Ok(MaybeDistrictId(Some(s.parse::<DistrictId>()?)))
	}
}

impl<'de> Deserialize<'de> for MaybeDistrictId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(de::Error::custom)
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
		let s = if s.starts_with("A") {
			&s[1..]
		} else {
			s
		};
		if s.ends_with("+") {
			let num = &s[..(s.len()-1)];
			let lower_bound = FromStr::from_str(num)?;
			return Ok(Self{low: lower_bound, high: None})
		}
		let (low, high) = match s.split_once('-') {
			Some(v) => v,
			None => return Err(ParseAgeGroupError::NoSeparator),
		};
		let high = if high.starts_with("A") {
			&high[1..]
		} else {
			high
		};
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
        if s == "unbekannt" || s == "u" {
			return Ok(MaybeAgeGroup(None))
		} else {
			return Ok(MaybeAgeGroup(Some(FromStr::from_str(&s).map_err(de::Error::custom)?)))
		}
    }
}
