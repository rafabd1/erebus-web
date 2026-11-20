use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};
use uuid::Uuid;

const COMPRESS_MIN_BYTES: usize = 1_024;

#[derive(Clone, Debug)]
pub struct BlobStore {
    root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct StoredBlob {
    pub hash: String,
    pub size: u64,
    pub stored_size: u64,
    pub codec: &'static str,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepStats {
    pub removed_orphans: u64,
    pub removed_parts: u64,
    pub retained: u64,
}

impl BlobStore {
    pub fn new(data_dir: &Path) -> anyhow::Result<Self> {
        let root = data_dir.join("blobs");
        fs::create_dir_all(&root)
            .with_context(|| format!("failed to create blob store {}", root.display()))?;
        Ok(Self { root })
    }

    pub fn put(&self, bytes: &[u8]) -> anyhow::Result<StoredBlob> {
        let hash = blake3::hash(bytes).to_hex().to_string();
        let (payload, codec) = if bytes.len() >= COMPRESS_MIN_BYTES {
            let compressed = zstd::encode_all(bytes, 3).context("failed to compress body")?;
            if compressed.len() < bytes.len() {
                (compressed, "zstd")
            } else {
                (bytes.to_vec(), "identity")
            }
        } else {
            (bytes.to_vec(), "identity")
        };
        let path = self.path_for(&hash, codec)?;
        if !path.exists() {
            let parent = path.parent().expect("blob path always has a parent");
            fs::create_dir_all(parent)?;
            let temporary = parent.join(format!(".{}.{}.part", hash, Uuid::new_v4()));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&payload)?;
            file.sync_all()?;
            match fs::rename(&temporary, &path) {
                Ok(()) => sync_directory(parent)?,
                Err(error) if path.exists() => {
                    let _ = fs::remove_file(&temporary);
                    tracing::debug!(%error, hash, "blob was stored concurrently");
                }
                Err(error) => {
                    let _ = fs::remove_file(&temporary);
                    return Err(error).context("failed to publish blob");
                }
            }
        }
        Ok(StoredBlob {
            hash,
            size: bytes.len() as u64,
            stored_size: payload.len() as u64,
            codec,
        })
    }

    pub fn read(&self, hash: &str, codec: &str, max_bytes: usize) -> anyhow::Result<Vec<u8>> {
        let path = self.path_for(hash, codec)?;
        let file = fs::File::open(&path)
            .with_context(|| format!("failed to open blob {}", path.display()))?;
        let mut output = Vec::new();
        match codec {
            "identity" => file.take(max_bytes as u64).read_to_end(&mut output)?,
            "zstd" => zstd::Decoder::new(file)?
                .take(max_bytes as u64)
                .read_to_end(&mut output)?,
            other => bail!("unsupported blob codec {other}"),
        };
        Ok(output)
    }

    pub fn sweep_orphans(
        &self,
        referenced_hashes: &HashSet<String>,
        minimum_age: Duration,
    ) -> anyhow::Result<SweepStats> {
        let mut stats = SweepStats::default();
        for first in directory_entries(&self.root)? {
            if !first.file_type()?.is_dir() {
                continue;
            }
            for second in directory_entries(&first.path())? {
                if !second.file_type()?.is_dir() {
                    continue;
                }
                for entry in directory_entries(&second.path())? {
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let path = entry.path();
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if !old_enough(&entry.metadata()?, minimum_age) {
                        stats.retained += 1;
                        continue;
                    }
                    if name.starts_with('.') && name.ends_with(".part") {
                        fs::remove_file(&path)?;
                        stats.removed_parts += 1;
                        continue;
                    }
                    let Some((hash, extension)) = name.rsplit_once('.') else {
                        stats.retained += 1;
                        continue;
                    };
                    let valid_blob = matches!(extension, "raw" | "zst")
                        && hash.len() == 64
                        && hash.bytes().all(|byte| byte.is_ascii_hexdigit());
                    if !valid_blob || referenced_hashes.contains(hash) {
                        stats.retained += 1;
                        continue;
                    }
                    fs::remove_file(&path)?;
                    stats.removed_orphans += 1;
                }
            }
        }
        Ok(stats)
    }

    fn path_for(&self, hash: &str, codec: &str) -> anyhow::Result<PathBuf> {
        if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("invalid blob hash");
        }
        let extension = match codec {
            "identity" => "raw",
            "zstd" => "zst",
            other => bail!("unsupported blob codec {other}"),
        };
        Ok(self
            .root
            .join(&hash[0..2])
            .join(&hash[2..4])
            .join(format!("{hash}.{extension}")))
    }
}

fn directory_entries(path: &Path) -> anyhow::Result<Vec<fs::DirEntry>> {
    fs::read_dir(path)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn old_enough(metadata: &fs::Metadata, minimum_age: Duration) -> bool {
    metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age >= minimum_age)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    fs::File::open(path)?.sync_all().map_err(Into::into)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_and_reads_small_and_compressible_bodies() {
        let temp = tempfile::tempdir().unwrap();
        let store = BlobStore::new(temp.path()).unwrap();
        for body in [b"hello".to_vec(), vec![b'a'; 20_000]] {
            let stored = store.put(&body).unwrap();
            let read = store
                .read(&stored.hash, stored.codec, body.len() + 1)
                .unwrap();
            assert_eq!(read, body);
            assert_eq!(store.put(&body).unwrap().hash, stored.hash);
        }
    }

    #[test]
    fn sweep_keeps_referenced_blobs_and_removes_old_orphans() {
        let temp = tempfile::tempdir().unwrap();
        let store = BlobStore::new(temp.path()).unwrap();
        let kept = store.put(b"kept").unwrap();
        let orphan = store.put(b"orphan").unwrap();
        let stats = store
            .sweep_orphans(&HashSet::from([kept.hash.clone()]), Duration::from_secs(0))
            .unwrap();
        assert_eq!(stats.removed_orphans, 1);
        assert_eq!(store.read(&kept.hash, kept.codec, 16).unwrap(), b"kept");
        assert!(store.read(&orphan.hash, orphan.codec, 16).is_err());
    }
}
