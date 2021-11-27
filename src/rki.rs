use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::hash::Hash;

use serde::{de, Deserialize, Deserializer};

use chrono::naive::NaiveDate;

use super::context::{StateId, DistrictId, AgeGroup, MaybeAgeGroup, MaybeDistrictId, Sex};

pub type FullCaseKey = (StateId, DistrictId, MaybeAgeGroup, Sex);
pub type GeoCaseKey = (StateId, DistrictId);


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
	#[serde(rename = "VerzugGesamt")]
	pub delay_total: u64,
	#[serde(rename = "AnzahlFallVerzoegert")]
	pub cases_delayed: u64,
	#[serde(rename = "AnzahlFallVerspaetet")]
	pub late_cases: u64,
	#[serde(rename = "AnzahlFall")]
	pub cases: u64,
	#[serde(rename = "AnzahlTodesfall")]
	pub deaths: u64,
	#[serde(rename = "AnzahlGenesen")]
	pub recovered: u64,
}

impl DiffRecord {
	pub fn write_header<W: io::Write>(w: &mut W) -> io::Result<()> {
		w.write("Datum,LandkreisId,Altersgruppe,Geschlecht,VerzugGesamt,AnzahlFallVerzoegert,AnzahlFallVerspaetet,AnzahlFall,AnzahlTodesfall,AnzahlGenesen\n".as_bytes())?;
		Ok(())
	}

	pub fn write<W: io::Write>(&self, w: &mut W) -> io::Result<()> {
		write!(w, "{},{},{},{},{},{},{},{},{},{}\n", self.date, self.district_id, self.age_group, self.sex, self.delay_total, self.cases_delayed, self.late_cases, self.cases, self.deaths, self.recovered)
	}
}


pub type VaccinationKey = (Option<StateId>, Option<DistrictId>, MaybeAgeGroup);

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize)]
pub enum VaccinationLevel {
	#[serde(rename = "1")]
	First,
	#[serde(rename = "2")]
	Basic,
	#[serde(rename = "3")]
	Full,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VaccinationRecord {
	#[serde(rename = "Impfdatum")]
	pub date: NaiveDate,
	#[serde(rename = "LandkreisId_Impfort")]
	pub district_id: MaybeDistrictId,
	#[serde(rename = "Altersgruppe")]
	pub age_group: MaybeAgeGroup,
	#[serde(rename = "Impfschutz")]
	pub level: VaccinationLevel,
	#[serde(rename = "Anzahl")]
	pub count: u64,
}


#[derive(Debug, Clone, Deserialize)]
pub struct HospitalizationRecord {
	#[serde(rename = "Datum")]
	pub date: NaiveDate,
	#[serde(rename = "Bundesland_Id")]
	pub state_id: StateId,
	#[serde(rename = "Altersgruppe")]
	pub age_group: AgeGroup,
	#[serde(rename = "7T_Hospitalisierung_Faelle")]
	pub cases_d7: u64,
}


pub fn find_berlin_districts(districts: &HashMap<DistrictId, Arc<DistrictInfo>>) -> Vec<GeoCaseKey> {
	let mut result = Vec::new();
	for district in districts.values() {
		let state_id = district.state.id;
		if state_id != 11 {
			continue
		}

		result.push((state_id, district.id));
	}
	result
}

pub fn inject_berlin(
		states: &HashMap<DistrictId, Arc<StateInfo>>,
		districts: &mut HashMap<DistrictId, Arc<DistrictInfo>>,
) {
	let mut total_pop = 0;
	for (id, district) in districts.iter() {
		if *id >= 11000 && *id < 12000 {
			total_pop += district.population;
		}
	}

	districts.insert(11000, Arc::new(DistrictInfo{
		id: 11000,
		state: states.get(&11).unwrap().clone(),
		name: "SK Berlin".into(),
		population: total_pop,
	}));
}
