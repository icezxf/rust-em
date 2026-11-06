use quick_xml::de::from_str;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct NfoMovie {
    pub title: Option<String>,
    pub plot: Option<String>,
    pub year: Option<u16>,
}

pub fn read_nfo(path: &std::path::Path) -> Option<NfoMovie> {
    let xml = std::fs::read_to_string(path).ok()?;
    from_str(&xml).ok()
}
