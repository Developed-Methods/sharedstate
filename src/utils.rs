use std::time::UNIX_EPOCH;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn unknown_id_err(id: u16, name: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("unknown id for {}: {}", name, id))
}

pub fn unknown_version_err(version: u16, name: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("unknown version for {}: {}", name, version))
}
