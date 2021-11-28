use serde::{de, Deserialize, Deserializer, Serialize};

use super::context::{StateId, AgeGroup, Sex};


fn destatis_age_group<'de, D>(deserializer: D) -> Result<AgeGroup, D::Error>
	where D: Deserializer<'de>
{
	let s = String::deserialize(deserializer)?;
	if !s.starts_with("ALT") {
		return Err(de::Error::custom("destatis age must start with ALT"))
	}
	let low_s = &s[3..6];
	let low = low_s.parse::<u16>().map_err(de::Error::custom)?;
	if s.ends_with("UM") {
		Ok(AgeGroup{low, high: None})
	} else {
		Ok(AgeGroup{low, high: Some(low)})
	}
}


fn destatis_sex<'de, D>(deserializer: D) -> Result<Sex, D::Error>
	where D: Deserializer<'de>
{
	let s = String::deserialize(deserializer)?;
	if !s.starts_with("GES") {
		return Err(de::Error::custom("destatis sex must start with GES"))
	}
	match s.as_bytes()[3] {
		b'M' => Ok(Sex::Male),
		b'W' | b'F' => Ok(Sex::Female),
		_ => Err(de::Error::custom("unrecognized destatis sex"))
	}
}


fn destatis_month<'de, D>(deserializer: D) -> Result<u32, D::Error>
	where D: Deserializer<'de>
{
	let s = String::deserialize(deserializer)?;
	if !s.starts_with("MONAT") {
		return Err(de::Error::custom("destatis month must start with MONAT"))
	}
	let low_s = &s[5..7];
	Ok(low_s.parse::<u32>().map_err(de::Error::custom)?)
}


fn destatis_maybe_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
	where D: Deserializer<'de>
{
	let s = String::deserialize(deserializer)?;
	if s == "..." {
		return Ok(None)
	}
	Ok(Some(s.parse::<f64>().map_err(de::Error::custom)?))
}



#[derive(Debug, Clone, Deserialize)]
pub struct RawDestatisRow {
	#[serde(rename = "1_Auspraegung_Code")]
	pub state_id: StateId,
	#[serde(rename = "2_Auspraegung_Code", deserialize_with = "destatis_sex")]
	pub sex: Sex,
	#[serde(rename = "3_Auspraegung_Code", deserialize_with = "destatis_age_group")]
	pub age_group: AgeGroup,
	#[serde(rename = "BEVSTD__Bevoelkerungsstand__Anzahl")]
	pub count: u64,
}


#[derive(Debug, Clone, Deserialize)]
pub struct RawDestatisDeathByMonthRow {
	#[serde(rename = "Zeit")]
	pub year: i32,
	#[serde(rename = "2_Auspraegung_Code", deserialize_with = "destatis_month")]
	pub month: u32,
	#[serde(rename = "BEV074__Sterbefaelle_je_1000_Einwohner__Anzahl", deserialize_with = "destatis_maybe_f64")]
	pub death_incidence_per_1k: Option<f64>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestatisDeathHistoric {
	// types chosen for conversion-less compat with chrono::Datelike
	pub month: u32,
	pub min: f64,
	pub mean: f64,
	pub median: f64,
	pub max: f64,
	pub sum: f64,
}

impl DestatisDeathHistoric {
	pub fn from_sorted_slice(month: u32, sl: &[f64]) -> Self {
		assert!(sl.len() >= 1);
		let mut prev = None;
		let mut sum = 0.;
		for v in sl.iter() {
			match prev {
				Some(prev) => assert!(v >= prev),
				None => (),
			};
			prev = Some(v);
			sum += v;
		}
		let mean = sum / (sl.len() as f64);
		let median = if sl.len() % 2 == 0 {
			// neither of these can panic, because we assert that there is at least one element in the slice at the beginning
			let v1 = sl[sl.len() / 2];
			let v2 = sl[sl.len() / 2 + 1];
			(v1 + v2) / 2.
		} else {
			// if odd, this will select the center element, as / will implicitly round down and the index is zero-based
			sl[sl.len() / 2]
		};
		Self{
			month,
			min: sl[0],
			mean,
			median,
			max: sl[sl.len() - 1],
			sum,
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestatisDeathCurrent {
	// types chosen for conversion-less compat with chrono::Datelike
	pub year: i32,
	pub month: u32,
	pub death_incidence_per_inhabitant: f64,
}
