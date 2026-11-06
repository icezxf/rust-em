use quick_xml::de::from_str;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::Deserialize;

const MAX_NFO_SIZE: u64 = 2 * 1024 * 1024;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NfoThumb {
    #[serde(rename = "$text")]
    pub text: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NfoFanart {
    #[serde(rename = "thumb", default)]
    pub thumbs: Vec<NfoThumb>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NfoActor {
    pub name: Option<String>,
    pub role: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NfoUniqueId {
    #[serde(rename = "@type")]
    pub id_type: Option<String>,
    #[serde(rename = "$text")]
    pub text: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NfoMovie {
    pub title: Option<String>,
    pub originaltitle: Option<String>,
    pub sorttitle: Option<String>,
    pub plot: Option<String>,
    pub tagline: Option<String>,
    pub year: Option<u16>,
    pub premiered: Option<String>,
    pub runtime: Option<u32>,
    pub rating: Option<f64>,
    pub mpaa: Option<String>,
    #[serde(rename = "genre", default)]
    pub genres: Vec<String>,
    #[serde(rename = "director", default)]
    pub directors: Vec<String>,
    #[serde(rename = "studio", default)]
    pub studios: Vec<String>,
    #[serde(rename = "actor", default)]
    pub actors: Vec<NfoActor>,
    #[serde(rename = "uniqueid", default)]
    pub unique_ids: Vec<NfoUniqueId>,
    #[serde(rename = "thumb", default)]
    pub thumbs: Vec<NfoThumb>,
    pub fanart: Option<NfoFanart>,
}

pub fn read_nfo(path: &std::path::Path) -> Option<NfoMovie> {
    let metadata = std::fs::metadata(path).ok()?;
    if metadata.len() > MAX_NFO_SIZE { return None; }

    let xml = std::fs::read(path).ok()?;
    
    // 安全检查：拒绝 DTD
    let mut reader = Reader::from_reader(xml.as_slice());
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::DocType(_)) => return None,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }

    let xml_str = String::from_utf8_lossy(&xml);
    from_str(&xml_str).ok()
}
