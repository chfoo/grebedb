//! Lightweight embedded key-value store/database backed by files
//! in a virtual file system interface.
//!
//! To open a database, use [`Database`]:
//!
//! ```
//! use grebedb::{Database, DatabaseOptions};
//!
//! # fn main() -> Result<(), grebedb::Error> {
//! let options = DatabaseOptions::default();
//! // let mut db = Database::open_memory("path/to/empty/directory/", options)?;
//! let mut db = Database::open_memory(options)?;
//!
//! db.put("my_key", "hello world!")?;
//! db.flush()?;
//!
//! # Ok(())
//! # }
//! ```
//!
//! For important details, such as limitations and guarantees, see the
//! README.md file in the project's source code repository.

#![warn(missing_docs)]

pub mod error;
mod format;
mod lru;
mod page;
mod tree;
pub mod vfs;

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

pub use crate::error::Error;
use crate::page::{OpenMode, PageTableOptions};
use crate::tree::{Tree, TreeCursor};
use crate::vfs::{MemoryVfs, OsVfs, ReadOnlyVfs, Vfs};

/// Type alias for an owned key-value pair.
pub type KeyValuePair = (Vec<u8>, Vec<u8>);

/// Database configuration options.
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    /// Option when opening a database. Default: LoadOrCreate.
    pub open_mode: DatabaseOpenMode,
    /// Maximum number of keys per node. Default: 1024.
    pub keys_per_node: usize,
    /// Number of pages held in memory cache. Default: 64.
    pub page_cache_size: usize,
    /// Whether to use file locking to prevent corruption by multiple processes.
    /// Default: true.
    pub file_locking: bool,
    /// Whether to flush the data to the file system periodically when a
    /// database operation is performed.
    /// Default: true.
    pub automatic_flush: bool,
    /// Number of modifications required for automatic flush to be considered.
    /// Default: 2048
    pub automatic_flush_threshold: usize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            open_mode: DatabaseOpenMode::default(),
            keys_per_node: 1024,
            page_cache_size: 64,
            file_locking: true,
            automatic_flush: true,
            automatic_flush_threshold: 2048,
        }
    }
}

impl DatabaseOptions {
    fn validate(&self) -> Result<(), Error> {
        if self.keys_per_node < 2 {
            return Err(Error::InvalidConfig {
                message: "required keys_per_node >= 2",
            });
        }
        if self.page_cache_size < 1 {
            return Err(Error::InvalidConfig {
                message: "required page_cache_size >= 1",
            });
        }

        Ok(())
    }
}

impl From<DatabaseOptions> for PageTableOptions {
    fn from(options: DatabaseOptions) -> Self {
        Self {
            open_mode: options.open_mode.into(),
            page_cache_size: options.page_cache_size,
            file_locking: options.file_locking,
        }
    }
}

/// Database open modes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseOpenMode {
    /// Open an existing database only if it exists.
    LoadOnly,
    /// Create a database only if it does not already exist.
    CreateOnly,
    /// Open a database, creating it if it does not exist.
    LoadOrCreate,
    /// Open an existing database and avoid modifying it.
    ReadOnly,
}

impl Default for DatabaseOpenMode {
    fn default() -> Self {
        Self::LoadOrCreate
    }
}

impl From<DatabaseOpenMode> for OpenMode {
    fn from(option: DatabaseOpenMode) -> Self {
        match option {
            DatabaseOpenMode::LoadOnly => OpenMode::LoadOnly,
            DatabaseOpenMode::CreateOnly => OpenMode::CreateOnly,
            DatabaseOpenMode::LoadOrCreate => OpenMode::LoadOrCreate,
            DatabaseOpenMode::ReadOnly => OpenMode::ReadOnly,
        }
    }
}

/// GrebeDB database interface.
pub struct Database {
    options: DatabaseOptions,
    tree: Tree,
    flush_tracker: Option<FlushTracker>,
}

impl Database {
    /// Open a database using the given virtual file system and options.
    pub fn open(vfs: Box<dyn Vfs + Send>, options: DatabaseOptions) -> Result<Self, Error> {
        options.validate()?;

        let vfs: Box<dyn Vfs + Send> = if options.open_mode == DatabaseOpenMode::ReadOnly {
            Box::new(ReadOnlyVfs::new(vfs))
        } else {
            vfs
        };

        let mut tree = Tree::open(vfs, options.clone().into(), options.keys_per_node)?;

        match options.open_mode {
            DatabaseOpenMode::CreateOnly | DatabaseOpenMode::LoadOrCreate => {
                tree.init_if_empty()?
            }
            _ => {}
        }

        let flush_tracker =
            if options.automatic_flush && options.open_mode != DatabaseOpenMode::ReadOnly {
                Some(FlushTracker::new(options.automatic_flush_threshold))
            } else {
                None
            };

        Ok(Self {
            options,
            tree,
            flush_tracker,
        })
    }

    /// Open a database in temporary memory.
    pub fn open_memory(options: DatabaseOptions) -> Result<Self, Error> {
        Self::open(Box::new(MemoryVfs::default()), options)
    }

    /// Open a database to a path on the disk.
    ///
    /// The path must be a directory.
    pub fn open_path<P>(root_path: P, options: DatabaseOptions) -> Result<Self, Error>
    where
        P: Into<PathBuf>,
    {
        Self::open(Box::new(OsVfs::new(root_path)), options)
    }

    /// Return whether the key exists.
    pub fn contains_key<K>(&mut self, key: K) -> Result<bool, Error>
    where
        K: AsRef<[u8]>,
    {
        self.tree.contains_key(key.as_ref())
    }

    /// Retrieve a stored value, by its key, as a vector.
    pub fn get<K>(&mut self, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        self.maybe_flush(false)?;
        let mut value = Vec::new();
        if self.tree.get(key.as_ref(), &mut value)? {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Retrieve a stored value, by its key, into the given buffer.
    ///
    /// Returns true if the key-value pair was found.
    pub fn get_buf<K>(&mut self, key: K, value_destination: &mut Vec<u8>) -> Result<bool, Error>
    where
        K: AsRef<[u8]>,
    {
        self.maybe_flush(false)?;
        self.tree.get(key.as_ref(), value_destination)
    }

    /// Store a key-value pair.
    pub fn put<K, V>(&mut self, key: K, value: V) -> Result<(), Error>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        self.maybe_flush(true)?;
        self.tree.put(key.into(), value.into())
    }

    /// Remove a key-value pair by its key.
    ///
    /// No error occurs if the key does not exist.
    pub fn remove<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.maybe_flush(true)?;
        self.tree.remove(key.as_ref())
    }

    /// Return a cursor for iterating all the key-value pairs.
    pub fn cursor(&mut self) -> DatabaseCursor<'_> {
        DatabaseCursor::new(&mut self.tree)
    }

    /// Persist all internally cached data to the file system.
    ///
    /// Calling this function ensures that all modifications cached in memory
    /// are written to the file system before this function returns.
    ///
    /// If automatic flushing is enabled in the options, data is flushed
    /// when the database is dropped or when enough modifications accumulate.
    ///
    /// There is no background maintenance thread that does automatic flushing;
    /// automatic flushing occurs when a database operation function is called.
    pub fn flush(&mut self) -> Result<(), Error> {
        self.tree.flush()
    }

    fn maybe_flush(&mut self, increment: bool) -> Result<(), Error> {
        if let Some(flush_tracker) = &mut self.flush_tracker {
            if increment {
                flush_tracker.increment_modification();
            }

            if flush_tracker.should_flush() {
                self.flush()?;
            }
        }

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.options.automatic_flush && self.options.open_mode != DatabaseOpenMode::ReadOnly {
            let _ = self.flush();
        }
    }
}

/// Cursor for navigating key-value pairs in sorted order.
pub struct DatabaseCursor<'a> {
    tree: &'a mut Tree,
    tree_cursor: TreeCursor,
    error: Option<Error>,
    has_seeked: bool,
}

impl<'a> DatabaseCursor<'a> {
    fn new(tree: &'a mut Tree) -> Self {
        Self {
            tree,
            tree_cursor: TreeCursor::default(),
            error: None,
            has_seeked: false,
        }
    }

    /// Return the most recent error.
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    /// Reposition the cursor so the position is at or before the given key.
    pub fn seek<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.has_seeked = true;
        self.tree.cursor_start(&mut self.tree_cursor, key.as_ref())
    }

    /// Advance the cursor forward and write the key-value pair to the given buffer.
    pub fn next_buf(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<bool, Error> {
        if !self.has_seeked {
            self.has_seeked = true;
            self.tree.cursor_start(&mut self.tree_cursor, b"")?;
        }

        if self.tree.cursor_next(&mut self.tree_cursor, key, value)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<'a> Iterator for DatabaseCursor<'a> {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key_buffer = Vec::new();
        let mut value_buffer = Vec::new();

        match self.next_buf(&mut key_buffer, &mut value_buffer) {
            Ok(success) => {
                if success {
                    Some((key_buffer, value_buffer))
                } else {
                    None
                }
            }
            Err(error) => {
                self.error = Some(error);
                None
            }
        }
    }
}

struct FlushTracker {
    base_threshold: usize,
    modification_count: usize,
    last_flush_time: Instant,
}

impl FlushTracker {
    pub fn new(base_threshold: usize) -> Self {
        Self {
            base_threshold,
            modification_count: 0,
            last_flush_time: Instant::now(),
        }
    }

    pub fn increment_modification(&mut self) {
        self.modification_count += 1;
    }

    pub fn should_flush(&mut self) -> bool {
        let level_long = self.modification_count >= self.base_threshold
            && self.last_flush_time.elapsed() >= Duration::from_secs(300);
        let level_short = self.modification_count >= self.base_threshold * 2
            && self.last_flush_time.elapsed() >= Duration::from_secs(60);

        if level_long || level_short {
            self.modification_count = 0;
            self.last_flush_time = Instant::now();
            true
        } else {
            false
        }
    }
}
