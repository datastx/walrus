use pgiceberg_common::models::CdcRecord;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Number of records to read back from a spill file at a time during commit.
pub const SPILL_CHUNK_SIZE: usize = 10_000;

/// Manages a temporary file for spilling large transaction records to disk.
///
/// When an in-flight transaction exceeds the configured memory threshold,
/// records are serialized as length-prefixed JSON to a temporary file.
/// On commit, records are read back in chunks via [`SpillReader`].
/// On abort (drop without calling `into_reader`), the file is cleaned up.
pub struct SpillFile {
    path: Option<PathBuf>,
    writer: BufWriter<File>,
    record_count: usize,
}

impl SpillFile {
    /// Create a new spill file under `{staging_root}/spill/`.
    pub fn create(staging_root: &Path) -> std::io::Result<Self> {
        let spill_dir = staging_root.join("spill");
        std::fs::create_dir_all(&spill_dir)?;
        let file_name = format!("spill_{}.tmp", uuid::Uuid::new_v4());
        let path = spill_dir.join(file_name);
        let file = File::create(&path)?;
        Ok(Self {
            path: Some(path),
            writer: BufWriter::with_capacity(256 * 1024, file),
            record_count: 0,
        })
    }

    /// Append a single record to the spill file.
    pub fn append(&mut self, record: &CdcRecord) -> std::io::Result<()> {
        let json = serde_json::to_vec(record).map_err(std::io::Error::other)?;
        let len = json.len() as u32;
        self.writer.write_all(&len.to_be_bytes())?;
        self.writer.write_all(&json)?;
        self.record_count += 1;
        Ok(())
    }

    /// Write a batch of records to the spill file.
    pub fn write_batch(&mut self, records: &[CdcRecord]) -> std::io::Result<()> {
        for record in records {
            self.append(record)?;
        }
        Ok(())
    }

    /// Finalize writing and return a reader for reading records back in chunks.
    /// The spill file is deleted when the reader is dropped.
    pub fn into_reader(mut self) -> std::io::Result<SpillReader> {
        self.writer.flush()?;
        let path = self.path.take().unwrap();
        match File::open(&path) {
            Ok(file) => Ok(SpillReader {
                path,
                reader: BufReader::with_capacity(256 * 1024, file),
            }),
            Err(e) => {
                // Put path back so Drop cleans up
                self.path = Some(path);
                Err(e)
            }
        }
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        if let Some(ref path) = self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Reader for consuming spilled records in chunks.
/// Deletes the spill file on drop.
pub struct SpillReader {
    path: PathBuf,
    reader: BufReader<File>,
}

impl SpillReader {
    /// Read up to `chunk_size` records from the spill file.
    /// Returns an empty vec when all records have been read.
    pub fn read_chunk(&mut self, chunk_size: usize) -> std::io::Result<Vec<CdcRecord>> {
        let mut records = Vec::with_capacity(chunk_size);
        for _ in 0..chunk_size {
            let mut len_buf = [0u8; 4];
            match self.reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut json_buf = vec![0u8; len];
            self.reader.read_exact(&mut json_buf)?;
            let record: CdcRecord = serde_json::from_slice(&json_buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            records.push(record);
        }
        Ok(records)
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
#[path = "spill_test.rs"]
mod spill_test;
