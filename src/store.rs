//! Per-node persistence (redb): process records and node identity survive restart.
//! Stays independent per node — no cross-node coordination.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh::SecretKey;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::process::Record;
use crate::proto::Handle;

const RECORDS: TableDefinition<Handle, &[u8]> = TableDefinition::new("records");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const KEY_SECRET: &str = "secret_key";
const KEY_NEXT: &str = "next_handle";

/// Reloaded state: persisted records (as stored) plus the next handle to hand out.
pub struct Loaded {
    pub records: Vec<(Handle, Record)>,
    pub next_handle: u64,
}

/// Handle to the on-disk store. Cheap, blocking redb calls (low write volume).
#[derive(Debug)]
pub struct Store {
    db: Database,
    path: PathBuf,
}

impl Store {
    /// Path to the store file on disk (e.g. for hiding it from isolated children).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open (or create) the store file and ensure both tables exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let db = Database::create(path).context("open redb store")?;

        // The store holds the node's secret key — keep it owner-only.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .context("restrict store file permissions")?;
        }

        let txn = db.begin_write()?;
        {
            txn.open_table(RECORDS)?;
            txn.open_table(META)?;
        }
        txn.commit()?;

        Ok(Self {
            db,
            path: path.to_path_buf(),
        })
    }

    /// Load this node's secret key, generating and persisting one on first run.
    pub fn secret_key(&self) -> Result<SecretKey> {
        if let Some(bytes) = self.get_meta(KEY_SECRET)? {
            let arr: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .context("stored secret key has wrong length")?;
            return Ok(SecretKey::from_bytes(&arr));
        }
        let key = SecretKey::generate();
        self.put_meta(KEY_SECRET, &key.to_bytes())?;

        Ok(key)
    }

    /// All persisted records and the next-handle counter.
    pub fn load(&self) -> Result<Loaded> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RECORDS)?;

        let mut records = Vec::new();
        for entry in table.iter()? {
            let (k, v) = entry?;
            let rec: Record = postcard::from_bytes(v.value()).context("decode record")?;
            records.push((k.value(), rec));
        }
        records.sort_by_key(|(h, _)| *h);

        // `next_handle` must exceed every persisted key so a fresh spawn can never
        // reuse a live handle. `put` advances KEY_NEXT to the handle it writes, so a
        // relaunch of a non-maximum handle can regress it below the highest record;
        // guard against that by taking the max with the highest key on disk.
        let max_key = records.iter().map(|(h, _)| *h).max().unwrap_or(0);
        let stored = match self.get_meta(KEY_NEXT)? {
            Some(b) => u64::from_le_bytes(b.as_slice().try_into().context("bad counter")?),
            None => 0,
        };
        let next_handle = stored.max(max_key);

        Ok(Loaded {
            records,
            next_handle,
        })
    }

    /// Persist a new record and advance the handle counter, in one transaction.
    pub fn put(&self, handle: Handle, rec: &Record) -> Result<()> {
        let bytes = postcard::to_allocvec(rec)?;
        let txn = self.db.begin_write()?;
        {
            txn.open_table(RECORDS)?.insert(handle, bytes.as_slice())?;
            txn.open_table(META)?
                .insert(KEY_NEXT, handle.to_le_bytes().as_slice())?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Delete a record (used by `forget`).
    pub fn remove(&self, handle: Handle) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            txn.open_table(RECORDS)?.remove(handle)?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Update an existing record's status (read-modify-write); a no-op if it is gone.
    pub fn set_status(&self, handle: Handle, status: crate::proto::ProcState) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RECORDS)?;
            let Some(guard) = table.get(handle)? else {
                return Ok(());
            };
            let mut rec: Record = postcard::from_bytes(guard.value()).context("decode record")?;
            drop(guard);

            rec.status = status;
            let bytes = postcard::to_allocvec(&rec)?;
            table.insert(handle, bytes.as_slice())?;
        }
        txn.commit()?;

        Ok(())
    }

    fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META)?;

        Ok(table.get(key)?.map(|v| v.value().to_vec()))
    }

    fn put_meta(&self, key: &str, val: &[u8]) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            txn.open_table(META)?.insert(key, val)?;
        }
        txn.commit()?;

        Ok(())
    }
}
