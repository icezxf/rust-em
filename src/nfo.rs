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
    pub num: Option<String>,
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
    pub unique_ids: Vec<(String, String)>,
}

pub fn read_nfo(path: &std::path::Path) -> Option<NfoMovie> {
    let bytes = fs::read(path).ok()?;
    let xml = String::from_utf8_lossy(&bytes);
    
    let mut reader = Reader::from_str(&xml);
    let mut movie = NfoMovie::default();
    let mut buf = Vec::new();
    let mut current_actor: Option<NfoActor> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                current_tag = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                if current_tag == "actor" {
                    current_actor = Some(NfoActor::default());
                }
            }
            Ok(Event::Text(e)) => {
                if let Ok(decoded) = e.unescape() {
                    let text = decoded.trim().to_string();
                    if !text.is_empty() {
                        process_text(&text, &current_tag, &mut movie, &mut current_actor);
                    }
                }
            }
            Ok(Event::CData(e)) => {
                if let Ok(decoded) = e.decode() {
                    let text = decoded.trim().to_string();
                    if !text.is_empty() {
                        process_text(&text, &current_tag, &mut movie, &mut current_actor);
                    }
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_lowercase();
                if tag == "actor" {
                    if let Some(actor) = current_actor.take() {
                        if actor.name.is_some() { movie.actors.push(actor); }
                    }
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
    Some(movie)
}

fn process_text(text: &str, tag: &str, movie: &mut NfoMovie, current_actor: &mut Option<NfoActor>) {
    match tag {
        "title" => movie.title = Some(text.to_string()),
        "num" => movie.num = Some(text.to_string()),
        "plot" => movie.plot = Some(text.to_string()),
        "year" => movie.year = Some(text.to_string()),
        "premiered" => movie.premiered = Some(text.to_string()),
        "runtime" => movie.runtime = text.parse::<i32>().ok(),
        "rating" => movie.rating = text.parse::<f64>().ok(),
        "mpaa" => movie.mpaa = Some(text.to_string()),
        "tagline" => movie.tagline = Some(text.to_string()),
        "series" => movie.series = Some(text.to_string()),
        "label" => movie.label = Some(text.to_string()),
        "set" => movie.set = Some(text.to_string()),
        "poster" | "cover" => movie.poster = Some(text.to_string()),
        "fanart" => movie.fanart = Some(text.to_string()),
        "genre" => movie.genres.push(text.to_string()),
        "director" => movie.directors.push(text.to_string()),
        "studio" => movie.studios.push(text.to_string()),
        "tmdbid" => movie.unique_ids.push(("tmdb".to_string(), text.to_string())),
        "imdbid" => movie.unique_ids.push(("imdb".to_string(), text.to_string())),
        "name" => { if let Some(actor) = current_actor.as_mut() { actor.name = Some(text.to_string()); } }
        "role" => { if let Some(actor) = current_actor.as_mut() { actor.role = Some(text.to_string()); } }
        "thumb" => {
            if let Some(actor) = current_actor.as_mut() { actor.thumb = Some(text.to_string()); }
            else if movie.poster.is_none() { movie.poster = Some(text.to_string()); }
        }
        _ => {}
    }
}
