//! Lightweight embedded key-value store/database backed by files
//! in a virtual file system interface.
//!
//! To open a database, use [`Database`]:
//!
//! ```
//! use grebedb::{Database, Options};
//!
//! # fn main() -> Result<(), grebedb::Error> {
//! let options = Options::default();
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

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod export;
mod format;
mod lru;
mod page;
mod system;
mod tree;
pub mod vfs;

use std::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub use crate::error::Error;
use crate::format::Format;
use crate::page::{Metadata as PageMetadata, Page, PageOpenMode, PageTableOptions};
use crate::tree::{Node, Tree, TreeCursor, TreeMetadata};
use crate::vfs::{MemoryVfs, OsVfs, ReadOnlyVfs, Vfs, VfsSyncOption};

/// Type alias for an owned key-value pair.
pub type KeyValuePair = (Vec<u8>, Vec<u8>);

/// Database configuration options.
#[derive(Debug, Clone)]
pub struct Options {
    /// Option when opening a database. Default: LoadOrCreate.
    pub open_mode: OpenMode,

    /// Maximum number of keys-value pairs per node. Default: 1024.
    ///
    /// This value specifies the threshold when a tree node is considered full.
    /// When a node is full, it is split into two and the tree is rebalanced.
    ///
    /// A page contains a single node and a page is stored on disk as one file.
    ///
    /// This option shouldn't be changed without making performance and resource usage
    /// benchmarks.
    pub keys_per_node: usize,

    /// Maximum number of pages held in memory cache. Default: 64.
    ///
    /// The cache is used to store frequently accessed pages for reducing disk operations.
    ///
    /// If memory usage is too high, consider decreasing this value first.
    pub page_cache_size: usize,

    /// Whether to use file locking to prevent corruption by multiple processes.
    /// Default: true.
    pub file_locking: bool,

    /// Level of file synchronization to increase durability on disk file systems.
    /// Default: Data
    pub file_sync: SyncOption,

    /// Whether to flush the data to the file system periodically when a
    /// database operation is performed.
    /// Default: true.
    ///
    /// When true, data is flushed when the database is dropped or when enough
    /// modifications accumulate. Setting this option to false allows you to
    /// manually persist changes at more optimal points.
    ///
    /// There is no background maintenance thread that does automatic flushing;
    /// automatic flushing occurs when a database modifying function,
    /// such as put() or remove(), is called.
    pub automatic_flush: bool,

    /// Number of modifications required for automatic flush to be considered.
    /// Default: 2048
    ///
    /// When the threshold is reached after 300 seconds,
    /// or the threshold × 2 is reached after 60 seconds,
    /// a flush is scheduled to be performed on the next modification.
    pub automatic_flush_threshold: usize,

    /// Compression level for each page. Default: Low.
    pub compression_level: CompressionLevel,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            open_mode: OpenMode::default(),
            keys_per_node: 1024,
            page_cache_size: 64,
            file_locking: true,
            file_sync: SyncOption::default(),
            automatic_flush: true,
            automatic_flush_threshold: 2048,
            compression_level: CompressionLevel::default(),
        }
    }
}

impl Options {
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

impl From<Options> for PageTableOptions {
    fn from(options: Options) -> Self {
        Self {
            open_mode: options.open_mode.into(),
            page_cache_size: options.page_cache_size,
            file_locking: options.file_locking,
            file_sync: options.file_sync.into(),
            keys_per_node: options.keys_per_node,
            compression_level: options.compression_level.to_zstd(),
        }
    }
}

/// Database open modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Open an existing database only if it exists.
    LoadOnly,
    /// Create a database only if it does not already exist.
    CreateOnly,
    /// Open a database, creating it if it does not exist.
    LoadOrCreate,
    /// Open an existing database and avoid modifying it.
    ReadOnly,
}

impl Default for OpenMode {
    fn default() -> Self {
        Self::LoadOrCreate
    }
}

impl From<OpenMode> for PageOpenMode {
    fn from(option: OpenMode) -> Self {
        match option {
            OpenMode::LoadOnly => PageOpenMode::LoadOnly,
            OpenMode::CreateOnly => PageOpenMode::CreateOnly,
            OpenMode::LoadOrCreate => PageOpenMode::LoadOrCreate,
            OpenMode::ReadOnly => PageOpenMode::ReadOnly,
        }
    }
}

/// Database data compression level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionLevel {
    /// Disable compression.
    None,

    /// Very fast compression speeds at the expense of low compression ratios.
    ///
    /// Currently, this corresponds to Zstandard level 1.
    VeryLow,

    /// Fast compression speeds at the expense of somewhat low compression ratios.
    ///
    /// Currently, this corresponds to Zstandard level 3, the default value.
    Low,

    /// Higher compression ratios at the expense of slower compression speeds.
    ///
    /// Currently, this corresponds to Zstandard level 9.
    Medium,

    /// Best compression ratios at the expense of very slow compression speeds.
    ///
    /// Currently, this corresponds to Zstandard level 19.
    High,
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::Low
    }
}

impl CompressionLevel {
    fn to_zstd(self) -> Option<i32> {
        match self {
            Self::None => None,
            Self::VeryLow => Some(1),
            Self::Low => Some(3),
            Self::Medium => Some(9),
            Self::High => Some(19),
        }
    }
}

/// Level of file synchronization for files created by the database.
///
/// These options are equivalent to [`vfs::VfsSyncOption`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOption {
    /// Don't require any flushing and simply overwrite files.
    None,

    /// Flush file content only and use file rename technique.
    ///
    /// Flush command is equivalent to `File::sync_data()` or Unix `fdatasync()`.
    Data,

    /// Flush file content including metadata and use file rename technique.
    ///
    /// Flush command is equivalent to `File::sync_all()` or Unix `fsync()`.
    All,
}

impl Default for SyncOption {
    fn default() -> Self {
        Self::Data
    }
}

impl From<SyncOption> for VfsSyncOption {
    fn from(option: SyncOption) -> Self {
        match option {
            SyncOption::None => Self::None,
            SyncOption::Data => Self::Data,
            SyncOption::All => Self::All,
        }
    }
}

/// GrebeDB database interface.
pub struct Database {
    options: Options,
    tree: Tree,
    flush_tracker: Option<FlushTracker>,
}

impl Database {
    /// Open a database using the given virtual file system and options.
    pub fn open(vfs: Box<dyn Vfs + Sync + Send>, options: Options) -> Result<Self, Error> {
        options.validate()?;

        let vfs: Box<dyn Vfs + Sync + Send> = if options.open_mode == OpenMode::ReadOnly {
            Box::new(ReadOnlyVfs::new(vfs))
        } else {
            vfs
        };

        let mut tree = Tree::open(vfs, options.clone().into())?;

        match options.open_mode {
            OpenMode::CreateOnly | OpenMode::LoadOrCreate => {
                tree.init_if_empty()?;
                tree.upgrade()?;
            }
            OpenMode::LoadOnly => {
                tree.upgrade()?;
            }
            _ => {}
        }

        let flush_tracker = if options.automatic_flush && options.open_mode != OpenMode::ReadOnly {
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
    pub fn open_memory(options: Options) -> Result<Self, Error> {
        Self::open(Box::new(MemoryVfs::default()), options)
    }

    /// Open a database to a path on the disk.
    ///
    /// The path must be a directory.
    pub fn open_path<P>(root_path: P, options: Options) -> Result<Self, Error>
    where
        P: Into<PathBuf>,
    {
        Self::open(Box::new(OsVfs::new(root_path)), options)
    }

    /// Return database metadata information.
    pub fn metadata(&self) -> Metadata {
        Metadata {
            tree_metadata: self.tree.metadata(),
        }
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
        let mut value = Vec::new();
        if self.tree.get(key.as_ref(), &mut value)? {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Retrieve a stored value, by its key, into the given buffer.
    ///
    /// Returns true if the key-value pair was found. The vector will be
    /// cleared and resized.
    pub fn get_buf<K>(&mut self, key: K, value_destination: &mut Vec<u8>) -> Result<bool, Error>
    where
        K: AsRef<[u8]>,
    {
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
    pub fn cursor(&mut self) -> Result<Cursor<'_>, Error> {
        Ok(Cursor::new(&mut self.tree))
    }

    /// Return a cursor for iterating all the key-value pairs within the given
    /// range.
    ///
    /// This method is equivalent of obtaining a cursor and calling
    /// [`Cursor::seek()`] and [`Cursor::set_range()`]
    pub fn cursor_range<K, R>(&mut self, range: R) -> Result<Cursor<'_>, Error>
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        let mut cursor = Cursor::new(&mut self.tree);

        match range.start_bound() {
            Bound::Included(key) => {
                cursor.seek(key)?;
            }
            Bound::Excluded(key) => {
                let mut key = key.as_ref().to_vec();
                key.push(0);
                cursor.seek(key)?;
            }
            Bound::Unbounded => {}
        }

        cursor.set_range(range);

        Ok(cursor)
    }

    /// Persist all modifications to the file system.
    ///
    /// Calling this function ensures that all changes pending, whether cached
    /// in memory or in files, are atomically saved on the file system
    /// before this function returns. If the database is not flushed when
    /// dropped or the program exits, changes since the last successful flush
    /// will be discarded. This function effectively emulates a transaction.
    ///
    /// For details about automatic flushing, see [`Options`].
    pub fn flush(&mut self) -> Result<(), Error> {
        self.tree.flush()
    }

    /// Check the database for internal consistency and data integrity.
    ///
    /// The provided callback function is called with the number of items
    /// processed and the estimated number of items.
    ///
    /// The function returns an error on the first verification failure or
    /// other error.
    pub fn verify<P>(&mut self, progress_callback: P) -> Result<(), Error>
    where
        P: FnMut(usize, usize),
    {
        self.tree.verify_tree(progress_callback)
    }

    /// Print the tree for debugging purposes.
    pub fn debug_print_tree(&mut self) -> Result<(), Error> {
        self.tree.dump_tree()
    }

    fn maybe_flush(&mut self, increment: bool) -> Result<(), Error> {
        if let Some(flush_tracker) = &mut self.flush_tracker {
            if increment {
                flush_tracker.increment_modification();
            }

            if flush_tracker.check_should_flush() {
                self.flush()?;
            }
        }

        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.options.automatic_flush && self.options.open_mode != OpenMode::ReadOnly {
            let _ = self.flush();
        }
    }
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database {{ open_mode: {:?} }}", self.options.open_mode)
    }
}

/// Cursor for navigating key-value pairs in sorted order.
pub struct Cursor<'a> {
    tree: &'a mut Tree,
    tree_cursor: TreeCursor,
    error: Option<Error>,
    has_seeked: bool,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl<'a> Cursor<'a> {
    fn new(tree: &'a mut Tree) -> Self {
        Self {
            tree,
            tree_cursor: TreeCursor::default(),
            error: None,
            has_seeked: false,
            range: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    /// Return the most recent error.
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    /// Reposition the cursor at or after the given key.
    ///
    /// In other words, the cursor will be positioned to return key-value pairs
    /// that are equal or greater than the given key.
    ///
    /// If a range has been set and the cursor is positioned outside the range,
    /// the iteration is considered terminated and no key-value pairs will returned.
    pub fn seek<K>(&mut self, key: K) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        self.has_seeked = true;
        self.tree.cursor_start(&mut self.tree_cursor, key.as_ref())
    }

    /// Limit the key-value pairs within a range of keys.
    ///
    /// The cursor will return key-value pairs where the keys are contained
    /// within the given range.
    ///
    /// This function will not reposition the cursor to a position within the
    /// range. You must call [`Self::seek()`] manually since the cursor will not
    /// automatically seek forward to a range's starting bound.
    pub fn set_range<K, R>(&mut self, range: R)
    where
        K: AsRef<[u8]>,
        R: RangeBounds<K>,
    {
        self.range = concrete_range(range);
    }

    /// Advance the cursor forward and write the key-value pair to the given buffers.
    ///
    /// Returns true if the key-value pair was written.
    /// Returns false if there are no more key-value pairs
    /// or the cursor is positioned outside the range if set.
    ///
    /// The vectors will be cleared and resized.
    pub fn next_buf(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<bool, Error> {
        if !self.has_seeked {
            self.has_seeked = true;
            self.tree.cursor_start(&mut self.tree_cursor, b"")?;
        }

        if self
            .tree
            .cursor_next(&mut self.tree_cursor, key, value, &slice_range(&self.range))?
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<'a> Iterator for Cursor<'a> {
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

impl<'a> Debug for Cursor<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DatabaseCursor")
    }
}

#[derive(Debug)]
/// Additional non-critical information associated with the database.
pub struct Metadata<'a> {
    tree_metadata: Option<&'a TreeMetadata>,
}

impl<'a> Metadata<'a> {
    /// Return the approximate number of key-value pairs in the database.
    pub fn key_value_count(&self) -> u64 {
        if let Some(meta) = self.tree_metadata {
            meta.key_value_count
        } else {
            0
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

    pub fn check_should_flush(&mut self) -> bool {
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

/// Print the page contents for debugging purposes.
pub fn debug_print_page(path: &Path) -> Result<(), Error> {
    let mut format = Format::default();
    let mut vfs = ReadOnlyVfs::new(Box::new(OsVfs::new(path.parent().unwrap())));

    let filename = path.file_name().unwrap().to_str().unwrap();

    if filename.contains("meta") {
        let payload: PageMetadata<TreeMetadata> = format.read_file(&mut vfs, filename)?;

        eprintln!("{:?}", payload);
    } else {
        let payload: Page<Node> = format.read_file(&mut vfs, filename)?;

        eprintln!("{:?}", payload);
    }

    Ok(())
}

fn concrete_range<K, R>(range: R) -> (Bound<Vec<u8>>, Bound<Vec<u8>>)
where
    K: AsRef<[u8]>,
    R: RangeBounds<K>,
{
    let start_bound: Bound<Vec<u8>> = match range.start_bound() {
        Bound::Included(bound) => Bound::Included(bound.as_ref().to_vec()),
        Bound::Excluded(bound) => Bound::Excluded(bound.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end_bound: Bound<Vec<u8>> = match range.end_bound() {
        Bound::Included(bound) => Bound::Included(bound.as_ref().to_vec()),
        Bound::Excluded(bound) => Bound::Excluded(bound.as_ref().to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start_bound, end_bound)
}

fn slice_range<'a>(
    range: &'a (Bound<Vec<u8>>, Bound<Vec<u8>>),
) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
    let start_bound: Bound<&'a [u8]> = match range.start_bound() {
        Bound::Included(bound) => Bound::Included(bound),
        Bound::Excluded(bound) => Bound::Excluded(bound),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end_bound: Bound<&'a [u8]> = match range.end_bound() {
        Bound::Included(bound) => Bound::Included(bound),
        Bound::Excluded(bound) => Bound::Excluded(bound),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start_bound, end_bound)
}
