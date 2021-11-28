use std::io;

use smartstring::alias::{String as SmartString};

use chrono::{NaiveDate, Utc, TimeZone, Datelike};

use serde::Deserialize;

use csv;

use covid::{ProgressSink, CountMeter};


static EVENTS_MEASUREMENT: &'static str = "events_v1";


#[derive(Debug, Clone, Deserialize)]
struct HolidayRecord {
	state: SmartString,
	holiday: SmartString,
	start: NaiveDate,
	end: NaiveDate,
}


fn stream_holidays<R: io::Read, S: ProgressSink + ?Sized>(
	s: &mut S,
	mut r: csv::Reader<R>,
	client: &covid::influxdb::Client,
) -> io::Result<()> {
	let tags: Vec<SmartString> = vec![
		"state".into(),
		"is_holiday".into(),
		"holiday_kind".into(),
	];
	let fields: Vec<SmartString> = vec![
		"text".into(),
		"end".into(),
	];

	let mut pm = CountMeter::new(s);
	let mut n = 0;
	let mut readout_buf = Vec::with_capacity(16);
	for (i, row) in r.deserialize().enumerate() {
		let rec: HolidayRecord = row?;
		let start = Utc.ymd(rec.start.year(), rec.start.month(), rec.start.day()).and_hms(0, 0, 0);
		let end = Utc.ymd(rec.end.year(), rec.end.month(), rec.end.day()).and_hms(0, 0, 0);
		if (end - start).num_days() < 3 {
			continue
		}
		readout_buf.push(covid::influxdb::Readout{
			ts: start,
			measurement: EVENTS_MEASUREMENT.into(),
			precision: covid::influxdb::Precision::Seconds,
			fields: fields.clone(),
			tags: tags.clone(),
			samples: vec![
				covid::influxdb::Sample{
					fieldv: vec![
						format!("{}\n\n<sup>{}</sup>", rec.holiday, rec.state).into(),
						format!("{}000", end.timestamp()).into(),
					],
					tagv: vec![
						rec.state,
						"true".into(),
						rec.holiday,
					],
				},
			],
		});
		if readout_buf.len() == readout_buf.capacity() {
			client.post(
				"covid",
				None,
				None,
				readout_buf[0].precision,
				&readout_buf[..],
			)?;
			readout_buf.clear();
			pm.update(i+1);
		}
		n = i + 1;
	}
	if readout_buf.len() > 0 {
		client.post(
			"covid",
			None,
			None,
			readout_buf[0].precision,
			&readout_buf[..],
		)?;
	}
	pm.finish(n);
	Ok(())
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = std::env::args().collect();
	let client = covid::env_client();
	for name in argv[1..].iter() {
		println!("streaming {} to influxdb ...", name);
		let r = covid::magic_open(name)?;
		let r = csv::Reader::from_reader(r);
		stream_holidays(
			&mut *covid::default_output(),
			r,
			&client,
		)?;
	}
	Ok(())
}
