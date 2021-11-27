use serde::{de, Deserialize, Deserializer};

use super::context::{StateId, AgeGroup};


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


#[derive(Debug, Clone, Deserialize)]
pub struct RawDestatisRow {
	#[serde(rename = "1_Auspraegung_Code")]
	pub state_id: StateId,
	#[serde(rename = "2_Auspraegung_Code", deserialize_with = "destatis_age_group")]
	pub age_group: AgeGroup,
	#[serde(rename = "BEVSTD__Bevoelkerungsstand__Anzahl")]
	pub count: u64,
}
