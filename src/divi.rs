use serde::{Deserialize};

use chrono::naive::NaiveDate;

use super::context::{StateId, DistrictId};


#[derive(Debug, Clone, Deserialize)]
pub struct ICULoadRecord {
	pub date: NaiveDate,
	#[serde(rename = "bundesland")]
	pub state_id: StateId,
	#[serde(rename = "gemeindeschluessel")]
	pub district_id: DistrictId,
	#[serde(rename = "anzahl_standorte")]
	pub num_stations: u32,
	#[serde(rename = "anzahl_meldebereiche")]
	pub num_regions: u32,
	#[serde(rename = "faelle_covid_aktuell")]
	pub current_covid_cases: u32,
	#[serde(rename = "faelle_covid_aktuell_invasiv_beatmet")]
	pub current_covid_cases_invasive_ventilation: u32,
	#[serde(rename = "betten_frei")]
	pub beds_free: u32,
	#[serde(rename = "betten_belegt")]
	pub beds_in_use: u32,
	#[serde(rename = "betten_belegt_nur_erwachsen")]
	pub beds_in_use_adult_only: u32,
	#[serde(rename = "betten_frei_nur_erwachsen")]
	pub beds_free_adult_only: u32,
}


/* fn divi_date_compat<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
	where D: Deserializer<'de>
{
	let mut s = String::deserialize(deserializer)?;
	s.split(".").collect();
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
} */


/* #[derive(Debug, Clone, Deserialize)]
pub struct ICUUnavailableReasonRecord {
	#[serde(deserialize_with = "divi_date_compat")]
	pub date: NaiveDate,
	#[serde(rename = "AnzMeldebereiche")]
	pub num_regions: u32,
	#[serde(rename = "einschraenkung_personal")]
	pub missing_staff: u32,
	#[serde(rename = "einschraenkung_raum")]
	pub missing_space: u32,
	#[serde(rename = "einschraenkung_material")]
	pub missing_material: u32,
	#[serde(rename = "einschraenkung_beatmungsgeraet")]
	pub missing_ventilator: u32,
} */
