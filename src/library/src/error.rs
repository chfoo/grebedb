//! Errors

/// Error type returned by database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Support for compression is not available due to a disabled feature.
    #[error("compression support not available")]
    CompressionUnavailable,

    /// Support for file locking is not available due to a disabled feature.
    #[error("file locking support not available")]
    FileLockingUnavailable,

    /// Provided configuration is invalid.
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        /// Custom message.
        message: &'static str,
    },

    /// A calculated checksum does not match.
    #[error("bad checksum: {path}")]
    BadChecksum {
        /// Path to file with bad checksum.
        path: String,
    },

    /// A file is not format correctly.
    #[error("invalid file format: {message}, {path}")]
    InvalidFileFormat {
        /// Path to file.
        path: String,
        /// Custom message.
        message: &'static str,
    },

    /// The metadata file contains invalid data.
    #[error("invalid page metadata: {message}")]
    InvalidMetadata {
        /// Custom message.
        message: &'static str,
    },

    /// A page file contains invalid data.
    #[error("invalid page data: {message}, {page}")]
    InvalidPageData {
        /// Page ID.
        page: u64,
        /// Custom message
        message: &'static str,
    },

    /// An execution or resource limit was exceeded.
    ///
    /// This error occurs if the tree is corrupted in such a way that it
    /// causes infinite loops.
    #[error("execution or resource limit exceeded")]
    LimitExceeded,

    /// Database is closed.
    ///
    /// This occurs if the database experienced an error and will refuse to
    /// process future operations to prevent further corruption.
    #[error("database closed")]
    DatabaseClosed,

    /// A modification to a database opened in read-only mode was requested.
    #[error("database read only")]
    ReadOnly,

    /// Other std IO error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Other internal errors.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl From<vfs::VfsError> for Error {
    fn from(error: vfs::VfsError) -> Self {
        Self::Other(Box::new(error))
    }
}
