use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
};

#[derive(Serialize, Deserialize)]
struct Export {
    format: u32,
    schema: u32,
    digest: String,
    state: serde_json::Value,
}

/// Export an application state after admission stops and outstanding operations are reconciled.
/// The caller owns quiescence; the legacy enqueue API cannot prove completion.
pub fn export_quiesced<S: Serialize>(path: &Path, schema: u32, state: &S) -> Result<(), String> {
    let state = super::protocol::canonical_value(state).map_err(|e| e.to_string())?;
    let canonical = serde_json::to_vec(&state).map_err(|e| e.to_string())?;
    let export = Export {
        format: 1,
        schema,
        digest: format!("{:x}", Sha256::digest(&canonical)),
        state,
    };
    let bytes = super::protocol::encode(&export, 256 * 1024 * 1024).map_err(|e| e.to_string())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    file.write_all(&bytes).map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Verify a quiesced export before supplying its state to new, empty voter databases.
pub fn import_quiesced<S: DeserializeOwned>(path: &Path, expected_schema: u32) -> Result<S, String> {
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let mut bytes = Vec::new();
    file.take(256 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > 256 * 1024 * 1024 {
        return Err("migration export limit exceeded".into());
    }
    let mut export: Export = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    if export.format != 1 || export.schema != expected_schema {
        return Err("migration format or schema mismatch".into());
    }
    export.state.sort_all_objects();
    let canonical = serde_json::to_vec(&export.state).map_err(|e| e.to_string())?;
    if format!("{:x}", Sha256::digest(&canonical)) != export.digest {
        return Err("migration digest mismatch".into());
    }
    serde_json::from_value(export.state).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn migration_roundtrip_rejects_corruption_and_preserves_original_export() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy.json");
        export_quiesced(&path, 9, &vec![1u64, 2, 3]).unwrap();
        assert_eq!(import_quiesced::<Vec<u64>>(&path, 9).unwrap(), vec![1, 2, 3]);
        assert!(import_quiesced::<Vec<u64>>(&path, 10).is_err());
        assert!(export_quiesced(&path, 9, &vec![4]).is_err());
        let mut bytes = std::fs::read_to_string(&path).unwrap();
        bytes = bytes.replace("[1,2,3]", "[1,2,4]");
        std::fs::write(&path, bytes).unwrap();
        assert!(import_quiesced::<Vec<u64>>(&path, 9).is_err());
    }
}
