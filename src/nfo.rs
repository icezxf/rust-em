use quick_xml::de::from_str;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::Deserialize;

// 限制 NFO 文件最大 2MB，防止恶意大文件撑爆内存
const MAX_NFO_SIZE: u64 = 2 * 1024 * 1024;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NfoMovie {
    pub title: Option<String>,
    pub plot: Option<String>,
    pub year: Option<u16>,
    // 还可以继续添加你想提取的字段，比如 rating, runtime 等
}

pub fn read_nfo(path: &std::path::Path) -> Option<NfoMovie> {
    // 1. 检查文件大小
    let metadata = std::fs::metadata(path).ok()?;
    if metadata.len() > MAX_NFO_SIZE {
        println!("⚠️ NFO 文件过大，已跳过: {:?}", path);
        return None;
    }

    let xml = std::fs::read(path).ok()?;
    
    // 2. 预检：快速扫描一遍，拒绝 DTD（防止 XXE 实体注入攻击）
    let mut reader = Reader::from_reader(xml.as_slice());
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::DocType(_)) => {
                println!("🚫 检测到非法的 DTD 标签，出于安全原因拒绝解析: {:?}", path);
                return None;
            }
            Err(_) => return None, // XML 格式损坏
            _ => {}
        }
        buf.clear();
    }

    // 3. 安全通过，使用 serde 解析为结构体
    let xml_str = String::from_utf8_lossy(&xml);
    from_str(&xml_str).ok()
}