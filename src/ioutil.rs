use std::io;
use std::io::Read;
use std::fs;
use std::path::Path;

use flate2;


pub fn magic_open<P: AsRef<Path>>(path: P) -> io::Result<Box<dyn Read>> {
	let path = path.as_ref();
	match path.extension() {
		Some(x) if x == "gz" => {
			Ok(Box::new(flate2::read::GzDecoder::new(fs::File::open(path)?)))
		},
		_ => Ok(Box::new(fs::File::open(path)?)),
	}
}
