use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs;

#[derive(Debug, Clone, Default)]
pub struct NfoActor {
    pub name: Option<String>,
    pub role: Option<String>,
    pub thumb: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct NfoMovie {
    pub title: Option<String>,
    pub plot: Option<String>,
    pub year: Option<String>,
    pub premiered: Option<String>,
    pub runtime: Option<i32>,
    pub rating: Option<f64>,
    pub mpaa: Option<String>,
    pub tagline: Option<String>,
    pub series: Option<String>,
    pub label: Option<String>,
    pub set: Option<String>,
    pub poster: Option<String>,
    pub fanart: Option<String>,
    pub genres: Vec<String>,
    pub directors: Vec<String>,
    pub studios: Vec<String>,
    pub actors: Vec<NfoActor>,
    pub unique_ids: Vec<(String, String)>, // (type, value)
}

fn extract_text(reader: &mut Reader<&[u8]>) -> String {
    let mut text = String::new();
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Text(e)) => {
                if let Ok(decoded) = e.unescape() { text.push_str(&decoded); }
            }
            Ok(Event::CData(e)) => {
                if let Ok(decoded) = e.decode() { text.push_str(&decoded); }
            }
            Ok(Event::End(_)) => break,
            _ => {}
        }
        buf.clear();
    }
    text.trim().to_string()
}

pub fn read_nfo(path: &std::path::Path) -> Option<NfoMovie> {
    let xml = fs::read_to_string(path).ok()?;
    let mut reader = Reader::from_str(&xml);
    let mut movie = NfoMovie::default();
    let mut buf = Vec::new();
    let mut current_actor: Option<NfoActor> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                let is_empty = matches!(reader.read_event_into(&mut Vec::new()), Ok(Event::End(_)));
                let text = if is_empty { String::new() } else { extract_text(&mut reader) };

                match tag_name.as_str() {
                    "title" => movie.title = Some(text),
                    "plot" => movie.plot = Some(text),
                    "year" => movie.year = Some(text),
                    "premiered" => movie.premiered = Some(text),
                    "runtime" => movie.runtime = text.parse::<i32>().ok(),
                    "rating" => movie.rating = text.parse::<f64>().ok(),
                    "mpaa" => movie.mpaa = Some(text),
                    "tagline" => movie.tagline = Some(text),
                    "series" => movie.series = Some(text),
                    "label" => movie.label = Some(text),
                    "poster" | "cover" | "thumb" => {
                        if current_actor.is_none() && movie.poster.is_none() { movie.poster = Some(text); }
                    }
                    "fanart" => movie.fanart = Some(text),
                    "genre" => movie.genres.push(text),
                    "director" => movie.directors.push(text),
                    "studio" => movie.studios.push(text),
                    "tmdbid" => movie.unique_ids.push(("tmdb".to_string(), text)),
                    "imdbid" => movie.unique_ids.push(("imdb".to_string(), text)),
                    "uniqueid" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"type" {
                                let val = String::from_utf8_lossy(&attr.value).to_lowercase();
                                movie.unique_ids.push((val, text.clone()));
                            }
                        }
                    }
                    "set" => movie.set = Some(text),
                    "actor" => current_actor = Some(NfoActor::default()),
                    "name" => { if let Some(actor) = current_actor.as_mut() { actor.name = Some(text); } }
                    "role" => { if let Some(actor) = current_actor.as_mut() { actor.role = Some(text); } }
                    "thumb" => { if let Some(actor) = current_actor.as_mut() { actor.thumb = Some(text); } }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                if String::from_utf8_lossy(e.name().as_ref()).to_lowercase() == "actor" {
                    if let Some(actor) = current_actor.take() {
                        if actor.name.is_some() { movie.actors.push(actor); }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
    Some(movie)
}
